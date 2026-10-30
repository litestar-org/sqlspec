"""AsyncPG database configuration with direct field-based configuration."""

import asyncio
from collections.abc import Mapping
from contextlib import suppress
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, Self, TypedDict, cast

from mypy_extensions import mypyc_attr

import sqlspec.adapters.asyncpg._typing as _asyncpg_typing
from sqlspec.adapters.asyncpg._typing import (
    AsyncpgConnection,
    AsyncpgCursor,
    AsyncpgPool,
    AsyncpgPreparedStatement,
    AsyncpgSessionContext,
    asyncpg_connect,
    asyncpg_create_pool,
)
from sqlspec.adapters.asyncpg._typing import AsyncpgConnectionMeta as ConnectionMeta
from sqlspec.adapters.asyncpg._typing import AsyncpgNativePool as Pool
from sqlspec.adapters.asyncpg._typing import AsyncpgNativePoolConnectionProxy as PoolConnectionProxy
from sqlspec.adapters.asyncpg._typing import AsyncpgPoolConnectionProxyMeta as PoolConnectionProxyMeta
from sqlspec.adapters.asyncpg._typing import AsyncpgRawConnection as Connection
from sqlspec.adapters.asyncpg._typing import AsyncpgRecord as Record
from sqlspec.adapters.asyncpg.core import (
    apply_driver_features,
    build_connection_config,
    default_statement_config,
    register_json_codecs,
    register_pgvector_support,
)
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver, AsyncpgExceptionHandler
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.core.config_runtime import (
    build_postgres_extension_probe_names,
    is_postgres_extension_active,
    resolve_postgres_extension_state,
    resolve_runtime_statement_config,
)
from sqlspec.driver import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.typing import ALLOYDB_CONNECTOR_INSTALLED, CLOUD_SQL_CONNECTOR_INSTALLED, PGVECTOR_INSTALLED
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop
    from collections.abc import Awaitable, Callable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


__all__ = (
    "PGVECTOR_INSTALLED",
    "AsyncpgConfig",
    "AsyncpgConnectionConfig",
    "AsyncpgDriverFeatures",
    "AsyncpgGSSLib",
    "AsyncpgPoolConfig",
    "AsyncpgTargetSessionAttrs",
    "register_json_codecs",
    "register_pgvector_support",
)


_POOL_ONLY_CONFIG_KEYS: Final[frozenset[str]] = frozenset({
    "init",
    "max_inactive_connection_lifetime",
    "max_queries",
    "max_size",
    "min_size",
    "reset",
    "setup",
})
_CLOUD_SQL_INSTANCE_PARTS: Final[int] = 3
_ALLOYDB_URI_PARTS: Final[int] = 8

logger = get_logger(__name__)


AsyncpgTargetSessionAttrs = Literal["any", "primary", "standby", "read-write", "read-only", "prefer-standby"]
AsyncpgGSSLib = Literal["gssapi", "sspi"]


class AsyncpgConnectionConfig(TypedDict):
    """TypedDict for AsyncPG connection parameters."""

    dsn: NotRequired[str]
    """PostgreSQL connection URI (``postgresql://user:pass@host:port/dbname``)."""
    host: NotRequired[str]
    """Database server hostname or Unix domain socket directory."""
    port: NotRequired[int]
    """Database server TCP port (defaults to ``5432``)."""
    user: NotRequired[str]
    """PostgreSQL role name used for authentication."""
    password: NotRequired[str]
    """Password or dynamic token for ``user``."""
    database: NotRequired[str]
    """Target PostgreSQL database name."""
    ssl: NotRequired[Any]
    """TLS mode string, boolean flag, or ``ssl.SSLContext`` instance."""
    passfile: NotRequired[str]
    """Path to a ``pgpass`` password file."""
    service: NotRequired[str]
    """Service section name in ``pg_service.conf``."""
    servicefile: NotRequired[str]
    """Path to an alternate ``pg_service.conf`` file."""
    direct_tls: NotRequired[bool]
    """Initiate TLS negotiation directly without PostgreSQL ``SSLRequest`` startup."""
    timeout: NotRequired[float]
    """Connection establishment timeout in seconds (alias supported by ``asyncpg.connect``)."""
    connect_timeout: NotRequired[float]
    """Pool connection establishment timeout in seconds."""
    command_timeout: NotRequired[float]
    """Default per-command execution timeout in seconds."""
    statement_cache_size: NotRequired[int]
    """Maximum number of server-side prepared statements cached per connection (``0`` disables caching)."""
    pgbouncer: NotRequired[bool]
    """Disable prepared statement caching for transaction-pooling proxies such as PgBouncer."""
    max_cached_statement_lifetime: NotRequired[int]
    """Maximum age in seconds before cached prepared statements are evicted."""
    max_cacheable_statement_size: NotRequired[int]
    """Maximum SQL byte length eligible for prepared statement caching."""
    server_settings: NotRequired["dict[str, str]"]
    """Startup PostgreSQL run-time GUC settings (such as ``application_name`` or ``search_path``)."""
    target_session_attrs: NotRequired[AsyncpgTargetSessionAttrs]
    """Required server session role when connecting to multi-host targets (such as ``read-write``)."""
    krbsrvname: NotRequired[str]
    """Kerberos service principal name used for GSSAPI authentication."""
    gsslib: NotRequired[AsyncpgGSSLib]
    """GSSAPI library backend (``gssapi`` or ``sspi`` on Windows)."""


class AsyncpgPoolConfig(AsyncpgConnectionConfig):
    """TypedDict for AsyncPG pool parameters, inheriting connection parameters."""

    min_size: NotRequired[int]
    """Minimum number of connections maintained in the pool."""
    max_size: NotRequired[int]
    """Maximum number of concurrent connections allowed in the pool."""
    max_queries: NotRequired[int]
    """Number of executed queries after which a pooled connection is closed and replaced."""
    max_inactive_connection_lifetime: NotRequired[float]
    """Idle duration in seconds before unused connections above ``min_size`` are closed."""
    connect: NotRequired["Callable[..., Awaitable[AsyncpgConnection]]"]
    """Custom async callable invoked to establish raw ``asyncpg`` connections."""
    setup: NotRequired["Callable[[AsyncpgConnection], Awaitable[None]]"]
    """Async callback invoked before each connection acquisition from the pool."""
    init: NotRequired["Callable[[AsyncpgConnection], Awaitable[None]]"]
    """Async callback invoked once when a new physical connection is created."""
    reset: NotRequired["Callable[[AsyncpgConnection], Awaitable[None]]"]
    """Async callback invoked when a connection is released back to the pool."""
    loop: NotRequired["AbstractEventLoop"]
    """Explicit ``asyncio`` event loop passed to ``asyncpg.create_pool``."""
    connection_class: NotRequired[type["AsyncpgConnection"]]
    """Custom ``asyncpg.Connection`` subclass instantiated by the pool."""
    record_class: NotRequired[type[Record]]
    """Custom ``asyncpg.Record`` subclass returned by raw queries."""
    extra: NotRequired["dict[str, Any]"]
    """Additional keyword arguments forwarded directly to ``asyncpg.create_pool``."""


class AsyncpgDriverFeatures(TypedDict):
    """Optional driver feature flags, codecs, cloud connectors, and pooler settings for AsyncPG."""

    json_serializer: NotRequired["Callable[[Any], str]"]
    """Custom JSON serializer registered on ``json``/``jsonb`` codecs (defaults to ``to_json``)."""
    json_deserializer: NotRequired["Callable[[str], Any]"]
    """Custom JSON deserializer registered on ``json``/``jsonb`` codecs (defaults to ``from_json``)."""
    enable_json_codecs: NotRequired[bool]
    """Automatically register ``json`` and ``jsonb`` codecs on new connections (defaults to ``True``)."""
    enable_pgvector: NotRequired[bool]
    """Register ``pgvector`` binary codecs and promote the dialect when ``pgvector`` is installed."""
    enable_paradedb: NotRequired[bool]
    """Detect ``pg_search`` and promote the SQL dialect to ``paradedb`` (defaults to ``True``)."""
    enable_pg_textsearch: NotRequired[bool]
    """Detect ``pg_textsearch`` and promote the SQL dialect for BM25 ``<@>`` ranking (defaults to ``True``)."""
    enable_cloud_sql: NotRequired[bool]
    """Route connections through ``cloud-sql-python-connector`` (mutually exclusive with ``enable_alloydb``)."""
    cloud_sql_instance: NotRequired[str]
    """Cloud SQL instance connection string in ``project:region:instance`` format."""
    cloud_sql_enable_iam_auth: NotRequired[bool]
    """Use IAM database authentication with the Cloud SQL Python Connector."""
    cloud_sql_ip_type: NotRequired[str]
    """Cloud SQL network path (``PUBLIC``, ``PRIVATE``, or ``PSC``; defaults to ``PRIVATE``)."""
    enable_alloydb: NotRequired[bool]
    """Route connections through ``google-cloud-alloydb-connector`` (mutually exclusive with ``enable_cloud_sql``)."""
    alloydb_instance_uri: NotRequired[str]
    """AlloyDB instance resource URI (``projects/.../locations/.../clusters/.../instances/...``)."""
    enable_alloydb_iam_auth: NotRequired[bool]
    """Use IAM database authentication with the AlloyDB Python Connector."""
    alloydb_ip_type: NotRequired[str]
    """AlloyDB network path (``PUBLIC``, ``PRIVATE``, or ``PSC``; defaults to ``PRIVATE``)."""
    enable_alloydb_columnar: NotRequired[bool]
    """Enable ``google_columnar_engine.enabled=on`` at session startup or per transaction."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec event channel pub/sub support (defaults to ``True`` when ``events`` is configured)."""
    events_backend: NotRequired[Literal["notify", "notify_queue", "poll_queue"]]
    """Event transport backend (``notify`` for LISTEN/NOTIFY, ``notify_queue``, or ``poll_queue``)."""
    on_connection_create: NotRequired["Callable[[AsyncpgConnection], Awaitable[None]]"]
    """Async hook invoked with each raw ``asyncpg`` connection after internal codec setup."""
    pooler_mode: NotRequired[Literal["session", "transaction", "statement", "auto"] | None]
    """External pooler compatibility mode; sets ``statement_cache_size=0`` in ``transaction`` or ``statement`` mode."""
    pgbouncer: NotRequired[bool]
    """Alias for ``pooler_mode='transaction'`` disabling server-side prepared statements."""
    pgbouncer_mode: NotRequired[bool]
    """Alias for ``pooler_mode='transaction'`` disabling server-side prepared statements."""
    direct_dsn: NotRequired["str | Mapping[str, Any] | None"]
    """Direct PostgreSQL DSN or connection mapping bypassing the pooler for migrations and ``LISTEN/NOTIFY``."""
    type_codecs: NotRequired["list[dict[str, Any]]"]
    """Custom ``asyncpg`` type codec specifications registered on connection initialization."""


class _AsyncpgCloudSqlConnector:
    __slots__ = ("_config", "_database", "_password", "_user")

    def __init__(self, config: "AsyncpgConfig", user: str | None, password: str | None, database: str | None) -> None:
        self._config = config
        self._user = user
        self._password = password
        self._database = database

    async def __call__(self) -> "AsyncpgConnection":
        connector = self._config._get_cloud_sql_connector()
        if connector is None:
            msg = "Cloud SQL connector is not initialized"
            raise ImproperConfigurationError(msg)
        conn_kwargs: dict[str, Any] = {
            "instance_connection_string": self._config.driver_features["cloud_sql_instance"],
            "driver": "asyncpg",
            "enable_iam_auth": self._config.driver_features.get("cloud_sql_enable_iam_auth", False),
            "ip_type": self._config.driver_features.get("cloud_sql_ip_type", "PRIVATE"),
        }
        if self._user:
            conn_kwargs["user"] = self._user
        if self._password:
            conn_kwargs["password"] = self._password
        if self._database:
            conn_kwargs["db"] = self._database
        return cast("AsyncpgConnection", await connector.connect_async(**conn_kwargs))


class _AsyncpgAlloydbConnector:
    __slots__ = ("_config", "_database", "_password", "_user")

    def __init__(self, config: "AsyncpgConfig", user: str | None, password: str | None, database: str | None) -> None:
        self._config = config
        self._user = user
        self._password = password
        self._database = database

    async def __call__(self) -> "AsyncpgConnection":
        connector = self._config._get_alloydb_connector()
        if connector is None:
            msg = "AlloyDB connector is not initialized"
            raise ImproperConfigurationError(msg)
        conn_kwargs: dict[str, Any] = {
            "instance_uri": self._config.driver_features["alloydb_instance_uri"],
            "driver": "asyncpg",
            "enable_iam_auth": self._config.driver_features.get("enable_alloydb_iam_auth", False),
            "ip_type": self._config.driver_features.get("alloydb_ip_type", "PRIVATE"),
        }
        if self._user:
            conn_kwargs["user"] = self._user
        if self._password:
            conn_kwargs["password"] = self._password
        if self._database:
            conn_kwargs["db"] = self._database
        return cast("AsyncpgConnection", await connector.connect(**conn_kwargs))


class _AsyncpgSessionFactory(AsyncPoolSessionFactory):
    __slots__ = ()


class AsyncpgConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for AsyncPG connections."""

    __slots__ = ()


@mypyc_attr(native_class=False)
class AsyncpgConfig(AsyncDatabaseConfig[AsyncpgConnection, "Pool[Record]", AsyncpgDriver]):
    """Configuration for AsyncPG database connections using TypedDict."""

    driver_type: "ClassVar[type[AsyncpgDriver]]" = AsyncpgDriver
    connection_type: "ClassVar[type[AsyncpgConnection]]" = AsyncpgConnection  # type: ignore[assignment]
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_migration_schemas: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    supports_native_row_streaming: "ClassVar[bool]" = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="native"
    )
    _connection_context_class: "ClassVar[type[AsyncpgConnectionContext]]" = AsyncpgConnectionContext
    _session_factory_class: "ClassVar[type[_AsyncpgSessionFactory]]" = _AsyncpgSessionFactory
    _session_context_class: "ClassVar[type[AsyncpgSessionContext]]" = AsyncpgSessionContext
    _default_statement_config = default_statement_config

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
            extension_config: Extension-specific configuration
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments
        """
        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)
        features_dict = dict(driver_features)
        if connection_instance is None and "connection_instance" in features_dict:
            connection_instance = features_dict.pop("connection_instance")
        self._user_connection_hook: Callable[[AsyncpgConnection], Awaitable[None]] | None = features_dict.get(
            "on_connection_create"
        )
        raw_conn_cfg = normalize_connection_config(connection_config)
        self._explicit_statement_cache_size = "statement_cache_size" in raw_conn_cfg
        raw_server_settings = raw_conn_cfg.get("server_settings")
        self._explicit_server_settings: dict[str, str] | None = (
            dict(raw_server_settings) if isinstance(raw_server_settings, Mapping) else None
        )
        normalized_conn_cfg = build_connection_config(raw_conn_cfg)
        if raw_conn_cfg.get("pgbouncer"):
            features_dict["pgbouncer"] = True
            features_dict["pgbouncer_mode"] = True
            if features_dict.get("pooler_mode") in {None, "session", "auto"}:
                features_dict["pooler_mode"] = "transaction"
        if features_dict.get("pooler_mode") in {"transaction", "statement"} or bool(
            features_dict.get("pgbouncer") or features_dict.get("pgbouncer_mode")
        ):
            normalized_conn_cfg.setdefault("statement_cache_size", 0)
        if bool(features_dict.get("enable_alloydb_columnar", False)) and features_dict.get("pooler_mode") == "session":
            merged_settings = dict(self._explicit_server_settings) if self._explicit_server_settings is not None else {}
            merged_settings.setdefault("google_columnar_engine.enabled", "on")
            normalized_conn_cfg["server_settings"] = merged_settings
        self._custom_type_codecs: list[dict[str, Any]] = list(features_dict.get("type_codecs") or [])

        super().__init__(
            connection_config=normalized_conn_cfg,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

        self._cloud_sql_connector: Any | None = None
        self._alloydb_connector: Any | None = None
        self._pgvector_available: bool | None = None
        self._paradedb_available: bool | None = None
        self._pg_textsearch_available: bool | None = None
        self._pg_textsearch_probe_error: Exception | None = None
        self._extension_probe_lock = asyncio.Lock()

        self._validate_connector_config()

    @property
    def pg_textsearch_available(self) -> bool:
        """Return True if the pg_textsearch extension is available."""
        return bool(self._pg_textsearch_available)

    async def create_connection(self) -> "AsyncpgConnection":
        """Open a standalone connection owned by the caller.

        The connection carries the same connection parameters and init hook the
        pool applies, consumes no pool slot, and must be closed by the caller.

        Returns:
            An AsyncPG connection instance.
        """
        config = {
            key: value for key, value in build_connection_config(self.connection_config).items() if value is not None
        }
        for key in _POOL_ONLY_CONFIG_KEYS:
            config.pop(key, None)

        if self.driver_features.get("pgbouncer"):
            config["statement_cache_size"] = 0

        if self.driver_features.get("enable_cloud_sql", False):
            self._setup_cloud_sql_connector(config)
        elif self.driver_features.get("enable_alloydb", False):
            self._setup_alloydb_connector(config)

        connect = config.pop("connect", None)
        connection = await connect() if connect is not None else await asyncpg_connect(**config)
        user_init = self.connection_config.get("init")
        try:
            await self._init_connection(connection)
            if user_init is not None and user_init != self._init_connection:
                await user_init(connection)
        except BaseException:
            with suppress(Exception):
                await connection.close()
            raise
        return cast("AsyncpgConnection", connection)

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
        factory = _AsyncpgSessionFactory(self)
        return AsyncpgSessionContext(
            acquire_connection=factory.acquire_connection,
            release_connection=factory.release_connection,
            statement_config=statement_config
            or (lambda: resolve_runtime_statement_config(None, self.statement_config, default_statement_config)),
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

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

    def derive_direct_config(self) -> Self:
        """Derive a session-mode configuration using direct_dsn for DDL, migrations, or LISTEN/NOTIFY."""
        direct_dsn = self.driver_features.get("direct_dsn")
        if not direct_dsn:
            return self
        conn_cfg: dict[str, Any] = dict(self.connection_config)
        if not self._explicit_statement_cache_size and conn_cfg.get("statement_cache_size") == 0:
            conn_cfg.pop("statement_cache_size", None)
        if self._explicit_server_settings is not None:
            conn_cfg["server_settings"] = dict(self._explicit_server_settings)
        else:
            conn_cfg.pop("server_settings", None)
        if isinstance(direct_dsn, str):
            conn_cfg["dsn"] = direct_dsn
        elif isinstance(direct_dsn, Mapping):
            conn_cfg.update(build_connection_config(direct_dsn))
        features: dict[str, Any] = dict(self.driver_features)
        features["pooler_mode"] = "session"
        features["pgbouncer"] = False
        features["pgbouncer_mode"] = False
        features["direct_dsn"] = None
        if self._user_connection_hook is not None:
            features["on_connection_create"] = self._user_connection_hook
        return type(self)(
            connection_config=cast("AsyncpgPoolConfig", conn_cfg),
            connection_instance=None,
            migration_config=dict(self.migration_config),
            statement_config=self.statement_config,
            driver_features=cast("AsyncpgDriverFeatures", features),
            bind_key=self.bind_key,
            extension_config=cast("ExtensionConfigs", dict(self.extension_config)),
            observability_config=self.observability_config,
        )

    def _get_cloud_sql_connector(self) -> Any | None:
        """Return the configured Cloud SQL connector instance."""
        return self._cloud_sql_connector

    def _get_alloydb_connector(self) -> Any | None:
        """Return the configured AlloyDB connector instance."""
        return self._alloydb_connector

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
                if not instance or not isinstance(instance, str):
                    msg = "cloud_sql_instance required when enable_cloud_sql is True. Format: 'project:region:instance'"
                    raise ImproperConfigurationError(msg)

                parts = instance.split(":")
                if len(parts) != _CLOUD_SQL_INSTANCE_PARTS or not all(part.strip() for part in parts):
                    msg = f"Invalid Cloud SQL instance format: {instance}. Expected format: 'project:region:instance'"
                    raise ImproperConfigurationError(msg)
            case (False, True):
                if not ALLOYDB_CONNECTOR_INSTALLED:
                    raise MissingDependencyError(
                        package="google-cloud-alloydb-connector", install_package="google-cloud-alloydb-connector"
                    )

                instance_uri = self.driver_features.get("alloydb_instance_uri")
                if not instance_uri or not isinstance(instance_uri, str):
                    msg = "alloydb_instance_uri required when enable_alloydb is True. Format: 'projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE'"
                    raise ImproperConfigurationError(msg)

                uri_parts = instance_uri.split("/")
                if (
                    len(uri_parts) != _ALLOYDB_URI_PARTS
                    or uri_parts[0] != "projects"
                    or uri_parts[2] != "locations"
                    or uri_parts[4] != "clusters"
                    or uri_parts[6] != "instances"
                    or not all(part.strip() for part in uri_parts)
                ):
                    msg = f"Invalid AlloyDB instance URI format: {instance_uri}. Expected format: 'projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE'"
                    raise ImproperConfigurationError(msg)

    def _setup_cloud_sql_connector(self, config: "dict[str, Any]") -> None:
        """Setup Cloud SQL connector and configure pool for connection factory pattern.

        Args:
            config: Pool configuration dictionary to modify in-place.
        """
        if self._cloud_sql_connector is None:
            self._cloud_sql_connector = _asyncpg_typing.AsyncpgCloudSqlConnector()

        user = config.get("user")
        password = config.get("password")
        database = config.get("database")

        for key in ("dsn", "host", "port", "user", "password", "database"):
            config.pop(key, None)

        config["connect"] = _AsyncpgCloudSqlConnector(self, user, password, database)

    def _setup_alloydb_connector(self, config: "dict[str, Any]") -> None:
        """Setup AlloyDB connector and configure pool for connection factory pattern.

        Args:
            config: Pool configuration dictionary to modify in-place.
        """
        if self._alloydb_connector is None:
            self._alloydb_connector = _asyncpg_typing.AsyncpgAlloydbAsyncConnector()

        user = config.get("user")
        password = config.get("password")
        database = config.get("database")

        for key in ("dsn", "host", "port", "user", "password", "database"):
            config.pop(key, None)

        config["connect"] = _AsyncpgAlloydbConnector(self, user, password, database)

    def _build_chained_init(
        self, user_init: "Callable[[AsyncpgConnection], Awaitable[None]]"
    ) -> "Callable[[AsyncpgConnection], Awaitable[None]]":
        async def _chained_init(connection: "AsyncpgConnection") -> None:
            await self._init_connection(connection)
            await user_init(connection)

        return _chained_init

    async def _create_pool(self) -> "Pool[Record]":
        """Create the actual async connection pool."""
        config = {
            key: value for key, value in build_connection_config(self.connection_config).items() if value is not None
        }

        if self.connection_config.get("pgbouncer") or self.driver_features.get("pgbouncer"):
            config["statement_cache_size"] = 0

        if self.driver_features.get("enable_cloud_sql", False):
            self._setup_cloud_sql_connector(config)
        elif self.driver_features.get("enable_alloydb", False):
            self._setup_alloydb_connector(config)

        user_init = config.pop("init", None)
        if user_init is not None and user_init != self._init_connection:
            config["init"] = self._build_chained_init(user_init)
        else:
            config["init"] = self._init_connection

        try:
            return await asyncpg_create_pool(**config)
        except BaseException:
            if self._cloud_sql_connector is not None:
                with suppress(Exception):
                    await self._cloud_sql_connector.close_async()
                self._cloud_sql_connector = None
            if self._alloydb_connector is not None:
                with suppress(Exception):
                    await self._alloydb_connector.close()
                self._alloydb_connector = None
            raise

    async def _init_connection(self, connection: "AsyncpgConnection") -> None:
        """Initialize connection with JSON codecs, pgvector support, custom codecs, and user callback.

        Args:
            connection: AsyncPG connection to initialize.
        """
        if self.driver_features.get("enable_json_codecs", True):
            await register_json_codecs(
                connection,
                encoder=self.driver_features.get("json_serializer", to_json),
                decoder=self.driver_features.get("json_deserializer", from_json),
            )

        async with self._extension_probe_lock:
            if self._pgvector_available is None:
                detected_extensions: set[str] = set()
                probe_failed = False
                extensions = build_postgres_extension_probe_names(self.driver_features)
                if extensions:
                    try:
                        results = await connection.fetch(
                            "SELECT extname FROM pg_extension WHERE extname = ANY($1::text[])", extensions
                        )
                        detected_extensions = {r["extname"] for r in results}
                    except Exception as exc:
                        probe_failed = True
                        self._pg_textsearch_probe_error = exc
                        self._pg_textsearch_available = False
                if not probe_failed:
                    self._pg_textsearch_probe_error = None
                    self.statement_config, self._pgvector_available, self._paradedb_available = (
                        resolve_postgres_extension_state(
                            self.statement_config, self.driver_features, detected_extensions
                        )
                    )
                    self._pg_textsearch_available = is_postgres_extension_active(self.driver_features, "pg_textsearch")

        if self._pgvector_available:
            await register_pgvector_support(connection)

        for codec in self._custom_type_codecs:
            codec_kwargs: dict[str, Any] = {
                "schema": codec.get("schema", "public"),
                "format": codec.get("format", "text"),
            }
            codec_kwargs["encoder"] = codec["encoder"]
            codec_kwargs["decoder"] = codec["decoder"]
            await connection.set_type_codec(codec["typename"], **codec_kwargs)

        if self._user_connection_hook is not None:
            await self._user_connection_hook(connection)

    def _ensure_pg_textsearch_available(self) -> None:
        if self._pg_textsearch_available:
            return
        msg = "ADK memory enable_bm25 requires the pg_textsearch PostgreSQL extension"
        raise ImproperConfigurationError(msg) from self._pg_textsearch_probe_error

    async def _close_pool(self) -> None:
        """Close the actual async connection pool and cleanup connectors."""
        if self.connection_instance:
            await self.connection_instance.close()
            self.connection_instance = None

        if self._cloud_sql_connector is not None:
            await self._cloud_sql_connector.close_async()
            self._cloud_sql_connector = None

        if self._alloydb_connector is not None:
            await self._alloydb_connector.close()
            self._alloydb_connector = None
