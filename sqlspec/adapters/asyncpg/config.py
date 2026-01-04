"""AsyncPG database configuration with direct field-based configuration."""

import importlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

from asyncpg import Connection, Record
from asyncpg import create_pool as asyncpg_create_pool
from asyncpg.connection import ConnectionMeta
from asyncpg.pool import Pool, PoolConnectionProxy, PoolConnectionProxyMeta
from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.asyncpg._typing import AsyncpgConnection, AsyncpgPool, AsyncpgPreparedStatement
from sqlspec.adapters.asyncpg.driver import (
    AsyncpgCursor,
    AsyncpgDriver,
    AsyncpgExceptionHandler,
    AsyncpgSessionContext,
    asyncpg_statement_config,
    build_asyncpg_statement_config,
)
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.extensions.events._hints import EventRuntimeHints
from sqlspec.typing import CLOUD_SQL_CONNECTOR_INSTALLED, PGVECTOR_INSTALLED
from sqlspec.utils.config_normalization import apply_pool_deprecations, normalize_connection_config
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop
    from collections.abc import Awaitable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


__all__ = ("AsyncpgConfig", "AsyncpgConnectionConfig", "AsyncpgDriverFeatures", "AsyncpgPoolConfig")


logger = get_logger(__name__)


def _is_missing_vector_error(error: Exception) -> bool:
    """Check if error indicates missing vector type.

    Args:
        error: Exception to check.

    Returns:
        True if error indicates missing vector type.

    """
    message = str(error).lower()
    return 'type "vector" does not exist' in message or "unknown type" in message


async def register_json_codecs(connection: Any, encoder: Any, decoder: Any) -> None:
    """Register JSON type codecs on asyncpg connection.

    Configures both JSON and JSONB types with custom serializer/deserializer
    functions. This allows using custom JSON libraries like orjson or msgspec
    for better performance.

    Args:
        connection: AsyncPG connection instance.
        encoder: Function to serialize Python objects to JSON strings.
        decoder: Function to deserialize JSON strings to Python objects.

    """
    try:
        await connection.set_type_codec("json", encoder=encoder, decoder=decoder, schema="pg_catalog")
        await connection.set_type_codec("jsonb", encoder=encoder, decoder=decoder, schema="pg_catalog")
        logger.debug("Registered JSON type codecs on asyncpg connection")
    except Exception:
        logger.exception("Failed to register JSON type codecs")


async def register_pgvector_support(connection: Any) -> None:
    """Register pgvector extension support on asyncpg connection.

    Enables automatic conversion between Python vector types and PostgreSQL
    VECTOR columns when the pgvector library is installed.

    Args:
        connection: AsyncPG connection instance.

    """
    if not PGVECTOR_INSTALLED:
        logger.debug("pgvector not installed - skipping vector type support")
        return

    try:
        pgvector_asyncpg = importlib.import_module("pgvector.asyncpg")
        await pgvector_asyncpg.register_vector(connection)
        logger.debug("Registered pgvector support on asyncpg connection")
    except Exception:
        logger.exception("Failed to register pgvector support")


class AsyncpgConnectionConfig(TypedDict):
    """TypedDict for AsyncPG connection parameters."""

    dsn: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    ssl: NotRequired[Any]
    passfile: NotRequired[str]
    direct_tls: NotRequired[bool]
    connect_timeout: NotRequired[float]
    command_timeout: NotRequired[float]
    statement_cache_size: NotRequired[int]
    max_cached_statement_lifetime: NotRequired[int]
    max_cacheable_statement_size: NotRequired[int]
    server_settings: NotRequired[dict[str, str]]


class AsyncpgPoolConfig(AsyncpgConnectionConfig):
    """TypedDict for AsyncPG pool parameters, inheriting connection parameters."""

    min_size: NotRequired[int]
    max_size: NotRequired[int]
    max_queries: NotRequired[int]
    max_inactive_connection_lifetime: NotRequired[float]
    setup: NotRequired["Callable[[AsyncpgConnection], Awaitable[None]]"]
    init: NotRequired["Callable[[AsyncpgConnection], Awaitable[None]]"]
    loop: NotRequired["AbstractEventLoop"]
    connection_class: NotRequired[type["AsyncpgConnection"]]
    record_class: NotRequired[type[Record]]
    extra: NotRequired[dict[str, Any]]


class AsyncpgDriverFeatures(TypedDict):
    """AsyncPG driver feature flags.

    json_serializer: Custom JSON serializer function for PostgreSQL JSON/JSONB types.
        Defaults to sqlspec.utils.serializers.to_json.
        Use for performance optimization (e.g., orjson) or custom encoding behavior.
        Applied when enable_json_codecs is True.
    json_deserializer: Custom JSON deserializer function for PostgreSQL JSON/JSONB types.
        Defaults to sqlspec.utils.serializers.from_json.
        Use for performance optimization (e.g., orjson) or custom decoding behavior.
        Applied when enable_json_codecs is True.
    enable_json_codecs: Enable automatic JSON/JSONB codec registration on connections.
        Defaults to True for seamless Python dict/list to PostgreSQL JSON/JSONB conversion.
        Set to False to disable automatic codec registration (manual handling required).
    enable_pgvector: Enable pgvector extension support for vector similarity search.
        Requires pgvector-python package (pip install pgvector) and PostgreSQL with pgvector extension.
        Defaults to True when pgvector-python is installed.
        Provides automatic conversion between Python objects and PostgreSQL vector types.
        Enables vector similarity operations and index support.
    enable_cloud_sql: Enable Google Cloud SQL connector integration.
        Requires cloud-sql-python-connector package.
        Defaults to False (explicit opt-in required).
        Auto-configures IAM authentication, SSL, and IP routing.
        Mutually exclusive with enable_alloydb.
    cloud_sql_instance: Cloud SQL instance connection name.
        Format: "project:region:instance"
        Required when enable_cloud_sql is True.
    cloud_sql_enable_iam_auth: Enable IAM database authentication.
        Defaults to False for passwordless authentication.
        When False, requires user/password in connection_config.
    cloud_sql_ip_type: IP address type for connection.
        Options: "PUBLIC", "PRIVATE", "PSC"
        Defaults to "PRIVATE".
    enable_alloydb: Enable Google AlloyDB connector integration.
        Requires cloud-alloydb-python-connector package.
        Defaults to False (explicit opt-in required).
        Auto-configures IAM authentication and private networking.
        Mutually exclusive with enable_cloud_sql.
    alloydb_instance_uri: AlloyDB instance URI.
        Format: "projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE"
        Required when enable_alloydb is True.
    alloydb_enable_iam_auth: Enable IAM database authentication.
        Defaults to False for passwordless authentication.
    alloydb_ip_type: IP address type for connection.
        Options: "PUBLIC", "PRIVATE", "PSC"
        Defaults to "PRIVATE".
    enable_events: Enable database event channel support.
        Defaults to True when extension_config["events"] is configured.
        Provides pub/sub capabilities via LISTEN/NOTIFY or table-backed fallback.
        Requires extension_config["events"] for migration setup when using table_queue backend.
    events_backend: Event channel backend selection.
        Options: "listen_notify", "table_queue", "listen_notify_durable"
        - "listen_notify": Zero-copy PostgreSQL LISTEN/NOTIFY (ephemeral, real-time)
        - "table_queue": Durable table-backed queue with retries and exactly-once delivery
        - "listen_notify_durable": Hybrid - combines real-time LISTEN/NOTIFY with table durability (recommended for production)
        Defaults to "listen_notify" for backward compatibility.
        Note: "listen_notify_durable" provides best of both worlds - <100ms latency with full durability.
    """

    json_serializer: NotRequired[Callable[[Any], str]]
    json_deserializer: NotRequired[Callable[[str], Any]]
    enable_json_codecs: NotRequired[bool]
    enable_pgvector: NotRequired[bool]
    enable_cloud_sql: NotRequired[bool]
    cloud_sql_instance: NotRequired[str]
    cloud_sql_enable_iam_auth: NotRequired[bool]
    cloud_sql_ip_type: NotRequired[str]
    enable_alloydb: NotRequired[bool]
    alloydb_instance_uri: NotRequired[str]
    alloydb_enable_iam_auth: NotRequired[bool]
    alloydb_ip_type: NotRequired[str]


class AsyncpgConnectionContext:
    """Async context manager for AsyncPG connections."""

    __slots__ = ("_config", "_connection")

    def __init__(self, config: "AsyncpgConfig") -> None:
        self._config = config
        self._connection: AsyncpgConnection | None = None

    async def __aenter__(self) -> "AsyncpgConnection":
        if self._config.connection_instance is None:
            self._config.connection_instance = await self._config._create_pool()  # pyright: ignore[reportPrivateUsage]
        self._connection = await self._config.connection_instance.acquire()
        return self._connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._connection is not None:
            if self._config.connection_instance:
                await self._config.connection_instance.release(self._connection)  # type: ignore[arg-type]
            self._connection = None
        return None


@mypyc_attr(native_class=False)
class AsyncpgConfig(AsyncDatabaseConfig[AsyncpgConnection, "Pool[Record]", AsyncpgDriver]):
    """Configuration for AsyncPG database connections using TypedDict."""

    driver_type: "ClassVar[type[AsyncpgDriver]]" = AsyncpgDriver
    connection_type: "ClassVar[type[AsyncpgConnection]]" = type(AsyncpgConnection)  # type: ignore[assignment]
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        connection_config: "AsyncpgPoolConfig | dict[str, Any] | None" = None,
        connection_instance: "Pool[Record] | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "AsyncpgDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize AsyncPG configuration.

        Args:
            connection_config: Connection and pool configuration parameters (TypedDict or dict)
            connection_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Statement configuration override
            driver_features: Driver features configuration (TypedDict or dict)
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments (handles deprecated pool_config/pool_instance)
        """
        connection_config, connection_instance = apply_pool_deprecations(
            kwargs=kwargs, connection_config=connection_config, connection_instance=connection_instance
        )

        features_dict: dict[str, Any] = dict(driver_features) if driver_features else {}

        serializer = features_dict.setdefault("json_serializer", to_json)
        deserializer = features_dict.setdefault("json_deserializer", from_json)
        features_dict.setdefault("enable_json_codecs", True)
        features_dict.setdefault("enable_pgvector", PGVECTOR_INSTALLED)
        features_dict.setdefault("enable_cloud_sql", False)
        features_dict.setdefault("enable_alloydb", False)

        base_statement_config = statement_config or build_asyncpg_statement_config(
            json_serializer=serializer, json_deserializer=deserializer
        )

        super().__init__(
            connection_config=normalize_connection_config(connection_config),
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=base_statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

        self._cloud_sql_connector: Any | None = None
        self._alloydb_connector: Any | None = None
        self._pgvector_available: bool | None = None

        self._validate_connector_config()

    def _validate_connector_config(self) -> None:
        """Validate Google Cloud connector configuration.

        Raises:
            ImproperConfigurationError: If configuration is invalid.
            MissingDependencyError: If required connector packages are not installed.
        """
        enable_cloud_sql = self.driver_features.get("enable_cloud_sql", False)
        enable_alloydb = self.driver_features.get("enable_alloydb", False)

        match (enable_cloud_sql, enable_alloydb):
            case (True, True):
                msg = (
                    "Cannot enable both Cloud SQL and AlloyDB connectors simultaneously. "
                    "Use separate configs for each database."
                )
                raise ImproperConfigurationError(msg)
            case (False, False):
                return
            case (True, False):
                if not CLOUD_SQL_CONNECTOR_INSTALLED:
                    raise MissingDependencyError(package="cloud-sql-python-connector", install_package="cloud-sql")

                instance = self.driver_features.get("cloud_sql_instance")
                if not instance:
                    msg = "cloud_sql_instance required when enable_cloud_sql is True. Format: 'project:region:instance'"
                    raise ImproperConfigurationError(msg)

                cloud_sql_instance_parts_expected = 2
                if instance.count(":") != cloud_sql_instance_parts_expected:
                    msg = f"Invalid Cloud SQL instance format: {instance}. Expected format: 'project:region:instance'"
                    raise ImproperConfigurationError(msg)

    def _get_pool_config_dict(self) -> "dict[str, Any]":
        """Get pool configuration as plain dict for external library.

        Returns:
            Dictionary with pool parameters, filtering out None values.
        """
        return {k: v for k, v in self.connection_config.items() if v is not None}

    def _setup_cloud_sql_connector(self, config: "dict[str, Any]") -> None:
        """Setup Cloud SQL connector and configure pool for connection factory pattern.

        Args:
            config: Pool configuration dictionary to modify in-place.
        """
        from google.cloud.sql.connector import Connector  # type: ignore[import-untyped,unused-ignore]

        self._cloud_sql_connector = Connector()

        user = config.get("user")
        password = config.get("password")
        database = config.get("database")

        async def get_conn() -> "AsyncpgConnection":
            conn_kwargs: dict[str, Any] = {
                "instance_connection_string": self.driver_features["cloud_sql_instance"],
                "driver": "asyncpg",
                "enable_iam_auth": self.driver_features.get("cloud_sql_enable_iam_auth", False),
                "ip_type": self.driver_features.get("cloud_sql_ip_type", "PRIVATE"),
            }

            if user:
                conn_kwargs["user"] = user
            if password:
                conn_kwargs["password"] = password
            if database:
                conn_kwargs["db"] = database

            conn: AsyncpgConnection = await self._cloud_sql_connector.connect_async(**conn_kwargs)  # type: ignore[union-attr]
            return conn

        for key in ("dsn", "host", "port", "user", "password", "database"):
            config.pop(key, None)

        config["connect"] = get_conn

    def _setup_alloydb_connector(self, config: "dict[str, Any]") -> None:
        """Setup AlloyDB connector and configure pool for connection factory pattern.

        Args:
            config: Pool configuration dictionary to modify in-place.
        """
        from google.cloud.alloydb.connector import AsyncConnector  # type: ignore[import-untyped,unused-ignore]

        self._alloydb_connector = AsyncConnector()

        user = config.get("user")
        password = config.get("password")
        database = config.get("database")

        async def get_conn() -> "AsyncpgConnection":
            conn_kwargs: dict[str, Any] = {
                "instance_uri": self.driver_features["alloydb_instance_uri"],
                "driver": "asyncpg",
                "enable_iam_auth": self.driver_features.get("alloydb_enable_iam_auth", False),
                "ip_type": self.driver_features.get("alloydb_ip_type", "PRIVATE"),
            }

            if user:
                conn_kwargs["user"] = user
            if password:
                conn_kwargs["password"] = password
            if database:
                conn_kwargs["db"] = database

            conn: AsyncpgConnection = await self._alloydb_connector.connect(**conn_kwargs)  # type: ignore[union-attr]
            return conn

        for key in ("dsn", "host", "port", "user", "password", "database"):
            config.pop(key, None)

        config["connect"] = get_conn

    async def _create_pool(self) -> "Pool[Record]":
        """Create the actual async connection pool."""
        config = self._get_pool_config_dict()

        if self.driver_features.get("enable_cloud_sql", False):
            self._setup_cloud_sql_connector(config)
        elif self.driver_features.get("enable_alloydb", False):
            self._setup_alloydb_connector(config)

        config.setdefault("init", self._init_connection)

        return await asyncpg_create_pool(**config)

    async def _init_connection(self, connection: "AsyncpgConnection") -> None:
        """Initialize connection with JSON codecs and pgvector support.

        Args:
            connection: AsyncPG connection to initialize.
        """
        if self.driver_features.get("enable_json_codecs", True):
            await register_json_codecs(
                connection,
                encoder=self.driver_features.get("json_serializer", to_json),
                decoder=self.driver_features.get("json_deserializer", from_json),
            )

        if self.driver_features.get("enable_pgvector", False):
            if self._pgvector_available is None:
                try:
                    result = await connection.fetchval("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
                    self._pgvector_available = bool(result)
                except Exception:
                    # If we can't query extensions, assume false to be safe and avoid errors
                    self._pgvector_available = False

            if self._pgvector_available:
                await register_pgvector_support(connection)

    async def _close_pool(self) -> None:
        """Close the actual async connection pool and cleanup connectors."""
        if self.connection_instance:
            await self.connection_instance.close()

        if self._cloud_sql_connector is not None:
            await self._cloud_sql_connector.close_async()
            self._cloud_sql_connector = None

        if self._alloydb_connector is not None:
            await self._alloydb_connector.close()
            self._alloydb_connector = None

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def create_connection(self) -> "AsyncpgConnection":
        """Create a single async connection from the pool.

        Returns:
            An AsyncPG connection instance.
        """
        if self.connection_instance is None:
            self.connection_instance = await self._create_pool()
        return await self.connection_instance.acquire()

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AsyncpgConnectionContext":
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            An AsyncPG connection context manager.
        """
        return AsyncpgConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "AsyncpgSessionContext":
        """Provide an async driver session context manager.

        Args:
            *_args: Additional arguments.
            statement_config: Optional statement configuration override.
            **_kwargs: Additional keyword arguments.

        Returns:
            An AsyncPG driver session context manager.
        """
        connection_holder: dict[str, AsyncpgConnection] = {}

        async def acquire_connection() -> AsyncpgConnection:
            if self.connection_instance is None:
                self.connection_instance = await self._create_pool()
            connection = await self.connection_instance.acquire()
            connection_holder["conn"] = connection
            return connection

        async def release_connection(_conn: AsyncpgConnection) -> None:
            if "conn" in connection_holder and self.connection_instance is not None:
                await self.connection_instance.release(connection_holder["conn"])
                connection_holder.clear()

        return AsyncpgSessionContext(
            acquire_connection=acquire_connection,
            release_connection=release_connection,
            statement_config=statement_config or self.statement_config or asyncpg_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "Pool[Record]":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for AsyncPG types.

        This provides all AsyncPG-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({
            "Connection": Connection,
            "Pool": Pool,
            "PoolConnectionProxy": PoolConnectionProxy,
            "PoolConnectionProxyMeta": PoolConnectionProxyMeta,
            "ConnectionMeta": ConnectionMeta,
            "Record": Record,
            "AsyncpgConnection": AsyncpgConnection,
            "AsyncpgConnectionConfig": AsyncpgConnectionConfig,
            "AsyncpgConnectionContext": AsyncpgConnectionContext,
            "AsyncpgCursor": AsyncpgCursor,
            "AsyncpgDriver": AsyncpgDriver,
            "AsyncpgExceptionHandler": AsyncpgExceptionHandler,
            "AsyncpgPool": AsyncpgPool,
            "AsyncpgPoolConfig": AsyncpgPoolConfig,
            "AsyncpgPreparedStatement": AsyncpgPreparedStatement,
            "AsyncpgSessionContext": AsyncpgSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults for PostgreSQL queue fallback."""

        return EventRuntimeHints(poll_interval=0.5, select_for_update=True, skip_locked=True)
