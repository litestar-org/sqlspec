"""AsyncPG database configuration with direct field-based configuration."""

import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

from asyncpg import Connection, Record
from asyncpg import create_pool as asyncpg_create_pool
from asyncpg.connection import ConnectionMeta
from asyncpg.pool import Pool, PoolConnectionProxy, PoolConnectionProxyMeta
from typing_extensions import NotRequired

from sqlspec.adapters.asyncpg._types import AsyncpgConnection, AsyncpgPool
from sqlspec.adapters.asyncpg.driver import (
    AsyncpgCursor,
    AsyncpgDriver,
    AsyncpgExceptionHandler,
    asyncpg_statement_config,
    build_asyncpg_statement_config,
)
from sqlspec.config import ADKConfig, AsyncDatabaseConfig, FastAPIConfig, FlaskConfig, LitestarConfig, StarletteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import ALLOYDB_CONNECTOR_INSTALLED, CLOUD_SQL_CONNECTOR_INSTALLED, PGVECTOR_INSTALLED
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop
    from collections.abc import AsyncGenerator, Awaitable

    from sqlspec.core import StatementConfig


__all__ = ("AsyncpgConfig", "AsyncpgConnectionConfig", "AsyncpgDriverFeatures", "AsyncpgPoolConfig")

logger = logging.getLogger("sqlspec")


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
        When False, requires user/password in pool_config.
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
        pool_config: "AsyncpgPoolConfig | dict[str, Any] | None" = None,
        pool_instance: "Pool[Record] | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "AsyncpgDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "dict[str, dict[str, Any]] | LitestarConfig | FastAPIConfig | StarletteConfig | FlaskConfig | ADKConfig | None" = None,
    ) -> None:
        """Initialize AsyncPG configuration.

        Args:
            pool_config: Pool configuration parameters (TypedDict or dict)
            pool_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Statement configuration override
            driver_features: Driver features configuration (TypedDict or dict)
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
        """
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
            pool_config=dict(pool_config) if pool_config else {},
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=base_statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
        )

        self._cloud_sql_connector: Any | None = None
        self._alloydb_connector: Any | None = None

        self._validate_connector_config()

    def _validate_connector_config(self) -> None:
        """Validate Google Cloud connector configuration.

        Raises:
            ImproperConfigurationError: If configuration is invalid.
        """
        enable_cloud_sql = self.driver_features.get("enable_cloud_sql", False)
        enable_alloydb = self.driver_features.get("enable_alloydb", False)

        if enable_cloud_sql and enable_alloydb:
            msg = "Cannot enable both Cloud SQL and AlloyDB connectors simultaneously. Use separate configs for each database."
            raise ImproperConfigurationError(msg)

        if enable_cloud_sql:
            if not CLOUD_SQL_CONNECTOR_INSTALLED:
                msg = "cloud-sql-python-connector package not installed. Install with: pip install cloud-sql-python-connector"
                raise ImproperConfigurationError(msg)

            instance = self.driver_features.get("cloud_sql_instance")
            if not instance:
                msg = "cloud_sql_instance required when enable_cloud_sql is True. Format: 'project:region:instance'"
                raise ImproperConfigurationError(msg)

            cloud_sql_instance_parts_expected = 2
            if instance.count(":") != cloud_sql_instance_parts_expected:
                msg = f"Invalid Cloud SQL instance format: {instance}. Expected format: 'project:region:instance'"
                raise ImproperConfigurationError(msg)

        elif enable_alloydb:
            if not ALLOYDB_CONNECTOR_INSTALLED:
                msg = "cloud-alloydb-python-connector package not installed. Install with: pip install cloud-alloydb-python-connector"
                raise ImproperConfigurationError(msg)

            instance_uri = self.driver_features.get("alloydb_instance_uri")
            if not instance_uri:
                msg = "alloydb_instance_uri required when enable_alloydb is True. Format: 'projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE'"
                raise ImproperConfigurationError(msg)

            if not instance_uri.startswith("projects/"):
                msg = f"Invalid AlloyDB instance URI format: {instance_uri}. Expected format: 'projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE'"
                raise ImproperConfigurationError(msg)

    def _get_pool_config_dict(self) -> "dict[str, Any]":
        """Get pool configuration as plain dict for external library.

        Returns:
            Dictionary with pool parameters, filtering out None values.
        """
        config: dict[str, Any] = dict(self.pool_config)
        extras = config.pop("extra", {})
        config.update(extras)
        return {k: v for k, v in config.items() if v is not None}

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
            from sqlspec.adapters.asyncpg._type_handlers import register_json_codecs

            await register_json_codecs(
                connection,
                encoder=self.driver_features.get("json_serializer", to_json),
                decoder=self.driver_features.get("json_deserializer", from_json),
            )

        if self.driver_features.get("enable_pgvector", False):
            from sqlspec.adapters.asyncpg._type_handlers import register_pgvector_support

            await register_pgvector_support(connection)

    async def _close_pool(self) -> None:
        """Close the actual async connection pool and cleanup connectors."""
        if self.pool_instance:
            await self.pool_instance.close()

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
        if self.pool_instance is None:
            self.pool_instance = await self._create_pool()
        return await self.pool_instance.acquire()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[AsyncpgConnection, None]":
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            An AsyncPG connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self._create_pool()
        connection = None
        try:
            connection = await self.pool_instance.acquire()
            yield connection
        finally:
            if connection is not None:
                await self.pool_instance.release(connection)

    @asynccontextmanager
    async def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AsyncGenerator[AsyncpgDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            An AsyncpgDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as connection:
            final_statement_config = statement_config or self.statement_config or asyncpg_statement_config
            yield self.driver_type(
                connection=connection, statement_config=final_statement_config, driver_features=self.driver_features
            )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "Pool[Record]":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

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
            "AsyncpgCursor": AsyncpgCursor,
            "AsyncpgDriver": AsyncpgDriver,
            "AsyncpgExceptionHandler": AsyncpgExceptionHandler,
            "AsyncpgPool": AsyncpgPool,
            "AsyncpgPoolConfig": AsyncpgPoolConfig,
        })
        return namespace
