"""Psycopg database configuration with direct field-based configuration."""

import inspect
from collections.abc import Mapping
from contextlib import suppress
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, Self, TypedDict, cast

from mypy_extensions import mypyc_attr
from psycopg.adapt import AdaptersMap
from psycopg.types.json import set_json_dumps, set_json_loads

import sqlspec.adapters.psycopg._typing as _psycopg_typing
from sqlspec.adapters.psycopg._typing import (
    PsycopgAsyncConnection,
    PsycopgAsyncCursor,
    PsycopgAsyncSessionContext,
    PsycopgConnection,
    PsycopgSyncConnection,
    PsycopgSyncCursor,
    PsycopgSyncSessionContext,
)
from sqlspec.adapters.psycopg._typing import PsycopgAsyncConnectionPool as AsyncConnectionPool
from sqlspec.adapters.psycopg._typing import PsycopgAsyncNullConnectionPool as AsyncNullConnectionPool
from sqlspec.adapters.psycopg._typing import PsycopgConnectionPool as ConnectionPool
from sqlspec.adapters.psycopg._typing import PsycopgNullConnectionPool as NullConnectionPool
from sqlspec.adapters.psycopg.core import apply_driver_features, default_statement_config
from sqlspec.adapters.psycopg.driver import (
    PsycopgAsyncDriver,
    PsycopgAsyncExceptionHandler,
    PsycopgSyncDriver,
    PsycopgSyncExceptionHandler,
)
from sqlspec.adapters.psycopg.type_converter import register_pgvector_async, register_pgvector_sync
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.core.config_runtime import (
    build_postgres_extension_probe_names,
    is_postgres_extension_active,
    resolve_postgres_extension_state,
    resolve_runtime_statement_config,
)
from sqlspec.driver import (
    AsyncPoolConnectionContext,
    AsyncPoolSessionFactory,
    SyncPoolConnectionContext,
    SyncPoolSessionFactory,
)
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.typing import ALLOYDB_CONNECTOR_INSTALLED
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from sqlspec.adapters.psycopg._typing import PsycopgAdaptContext as AdaptContext
    from sqlspec.adapters.psycopg._typing import PsycopgAsyncConnectFailedCB as AsyncConnectFailedCB
    from sqlspec.adapters.psycopg._typing import PsycopgAsyncConnectionCB as AsyncConnectionCB
    from sqlspec.adapters.psycopg._typing import PsycopgAsyncRowFactory as AsyncRowFactory
    from sqlspec.adapters.psycopg._typing import PsycopgConnectFailedCB as ConnectFailedCB
    from sqlspec.adapters.psycopg._typing import PsycopgConnection as Connection
    from sqlspec.adapters.psycopg._typing import PsycopgConnectionCB as ConnectionCB
    from sqlspec.adapters.psycopg._typing import PsycopgCursor as Cursor
    from sqlspec.adapters.psycopg._typing import PsycopgNativeAsyncConnection as AsyncConnection
    from sqlspec.adapters.psycopg._typing import PsycopgNativeAsyncCursor as AsyncCursor
    from sqlspec.adapters.psycopg._typing import PsycopgRowFactory as RowFactory
    from sqlspec.core import StatementConfig

__all__ = (
    "PsycopgAsyncConfig",
    "PsycopgAsyncCursor",
    "PsycopgConnectionParams",
    "PsycopgDriverFeatures",
    "PsycopgPoolParams",
    "PsycopgSyncConfig",
    "PsycopgSyncCursor",
    "build_connection_config",
)


PsycopgSSLMode = Literal["disable", "allow", "prefer", "require", "verify-ca", "verify-full"]
_ALLOYDB_DIRECT_CONNECTION_KEYS = ("conninfo", "host", "hostaddr", "port", "user", "password", "dbname", "db")
_POOL_ONLY_CONFIG_KEYS: Final[frozenset[str]] = frozenset({
    "auth_token_provider",
    "check",
    "close_returns",
    "configure",
    "max_idle",
    "max_lifetime",
    "max_size",
    "max_waiting",
    "min_size",
    "name",
    "num_workers",
    "null_pool",
    "open",
    "reconnect_failed",
    "reconnect_timeout",
    "reset",
    "timeout",
})
_ALLOYDB_CONNECTOR_PACKAGE = "google-cloud-alloydb-connector"
_DEFAULT_PSYCOPG_PREPARE_THRESHOLD: Final[int] = 5
_AUTH_TOKEN_DEFAULT_MAX_LIFETIME: Final[float] = 1800.0


class PsycopgConnectionParams(TypedDict):
    """Psycopg connection parameters."""

    conninfo: NotRequired[str]
    """PostgreSQL libpq connection URI or keyword/value conninfo string."""
    host: NotRequired[str]
    """Database server hostname or Unix socket directory."""
    port: NotRequired[int]
    """Database server TCP port (defaults to ``5432``)."""
    user: NotRequired[str]
    """PostgreSQL role name used for authentication."""
    password: NotRequired[str]
    """Password or token for ``user``."""
    auth_token_provider: NotRequired["Callable[[], str] | Callable[[], Awaitable[str]]"]
    """Callable returning a fresh authentication password or IAM token on each connection attempt."""
    dbname: NotRequired[str]
    """Target PostgreSQL database name."""
    connect_timeout: NotRequired[int]
    """Connection establishment timeout in seconds."""
    options: NotRequired[str]
    """Command-line startup GUC options sent to the PostgreSQL backend."""
    application_name: NotRequired[str]
    """Application identifier reported in ``pg_stat_activity``."""
    sslmode: NotRequired[PsycopgSSLMode]
    """libpq TLS verification mode (``disable``, ``allow``, ``prefer``, ``require``, ``verify-ca``, or ``verify-full``)."""
    sslcert: NotRequired[str]
    """Path to the client TLS certificate file."""
    sslkey: NotRequired[str]
    """Path to the client TLS private key file."""
    sslrootcert: NotRequired[str]
    """Path to the trusted root CA certificate file."""
    autocommit: NotRequired[bool]
    """Enable Psycopg autocommit mode outside explicit transactions."""
    prepare_threshold: NotRequired[int | None]
    """Executions before server-side statement preparation, or ``None`` to disable prepared statements."""
    context: NotRequired["AdaptContext | None"]
    """Custom Psycopg ``AdaptContext`` used for type adaptation."""
    row_factory: NotRequired["RowFactory[Any] | AsyncRowFactory[Any] | None"]
    """Row factory applied to raw Psycopg connections or cursors."""
    cursor_factory: NotRequired["type[Cursor[Any] | AsyncCursor[Any]] | None"]
    """Custom Psycopg ``Cursor`` or ``AsyncCursor`` class."""
    extra: NotRequired["dict[str, Any]"]
    """Additional libpq keyword parameters forwarded to ``connect``."""


class PsycopgPoolParams(PsycopgConnectionParams):
    """Psycopg pool parameters."""

    min_size: NotRequired[int]
    """Minimum number of connections kept open in ``ConnectionPool`` / ``AsyncConnectionPool``."""
    max_size: NotRequired[int]
    """Maximum number of connections allowed in the pool."""
    connection_class: NotRequired["type[Connection[Any] | AsyncConnection[Any]]"]
    """Custom Psycopg ``Connection`` or ``AsyncConnection`` subclass."""
    name: NotRequired[str]
    """Pool identifier used in logs and worker thread names."""
    timeout: NotRequired[float]
    """Maximum wait time in seconds when acquiring a connection from the pool."""
    max_waiting: NotRequired[int]
    """Maximum number of queued callers waiting for a connection (``0`` means no limit)."""
    max_lifetime: NotRequired[float]
    """Maximum connection age in seconds before recycling."""
    max_idle: NotRequired[float]
    """Maximum idle duration in seconds before shrinking connections above ``min_size``."""
    reconnect_timeout: NotRequired[float]
    """Seconds to retry failed connection attempts before invoking ``reconnect_failed``."""
    num_workers: NotRequired[int]
    """Number of background maintenance workers run by ``psycopg_pool``."""
    open: NotRequired[bool | None]
    """Whether to open the pool immediately upon instantiation."""
    configure: NotRequired["ConnectionCB[PsycopgSyncConnection] | AsyncConnectionCB[PsycopgAsyncConnection] | None"]
    """Callback run once on each new physical connection before entering the pool."""
    check: NotRequired["ConnectionCB[PsycopgSyncConnection] | AsyncConnectionCB[PsycopgAsyncConnection] | None"]
    """Health-check callback run before lending out a pooled connection."""
    reset: NotRequired["ConnectionCB[PsycopgSyncConnection] | AsyncConnectionCB[PsycopgAsyncConnection] | None"]
    """Reset callback run when returning a connection to the pool."""
    close_returns: NotRequired[bool]
    """Make ``conn.close()`` return the connection to the pool instead of closing the socket."""
    reconnect_failed: NotRequired["ConnectFailedCB | AsyncConnectFailedCB | None"]
    """Callback invoked when pool reconnection attempts exceed ``reconnect_timeout``."""
    kwargs: NotRequired["dict[str, Any]"]
    """Explicit connection keyword mapping passed to ``psycopg.connect``."""
    null_pool: NotRequired[bool]
    """Use ``NullConnectionPool`` / ``AsyncNullConnectionPool`` so idle connections are never retained."""


class PsycopgDriverFeatures(TypedDict):
    """Optional Psycopg driver feature flags, extension codecs, AlloyDB settings, and pooler modes."""

    enable_pgvector: NotRequired[bool]
    """Register ``pgvector`` adapters and promote the dialect when ``pgvector`` is installed."""
    enable_paradedb: NotRequired[bool]
    """Detect ``pg_search`` and promote the SQL dialect to ``paradedb`` (defaults to ``True``)."""
    enable_pg_textsearch: NotRequired[bool]
    """Detect ``pg_textsearch`` and promote the dialect for BM25 ``<@>`` ranking (defaults to ``True``)."""
    json_serializer: NotRequired["Callable[[Any], str]"]
    """Custom JSON serializer applied to statement parameters and Psycopg JSON dumpers."""
    json_deserializer: NotRequired["Callable[[str], Any]"]
    """Custom JSON deserializer applied to Psycopg JSON/JSONB loaders."""
    on_connection_create: NotRequired["Callable[..., Any]"]
    """Sync or async callback invoked with each raw Psycopg connection after internal setup."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec event channel pub/sub support (defaults to ``True`` when ``events`` is configured)."""
    events_backend: NotRequired[Literal["notify", "notify_queue", "poll_queue"]]
    """Event transport backend (``notify`` for LISTEN/NOTIFY, ``notify_queue``, or ``poll_queue``)."""
    enable_alloydb: NotRequired[bool]
    """Route sync connections through ``google-cloud-alloydb-connector`` (defaults to ``False``)."""
    alloydb_instance_uri: NotRequired[str]
    """AlloyDB instance resource URI (``projects/.../locations/.../clusters/.../instances/...``)."""
    enable_alloydb_iam_auth: NotRequired[bool]
    """Use IAM database authentication with the AlloyDB Python Connector."""
    alloydb_ip_type: NotRequired[str]
    """AlloyDB network path (``PUBLIC``, ``PRIVATE``, or ``PSC``; defaults to ``PRIVATE``)."""
    enable_alloydb_columnar: NotRequired[bool]
    """Enable ``google_columnar_engine.enable_columnar_scan=on`` via startup ``options`` or per-transaction ``SET LOCAL``."""
    pooler_mode: NotRequired[Literal["session", "transaction", "statement", "auto"]]
    """External pooler compatibility mode; sets ``prepare_threshold=None`` in ``transaction`` or ``statement`` mode."""
    pgbouncer: NotRequired[bool]
    """Alias for ``pooler_mode='transaction'`` disabling server-side prepared statements."""
    pgbouncer_mode: NotRequired[bool]
    """Alias for ``pooler_mode='transaction'`` disabling server-side prepared statements."""
    direct_dsn: NotRequired["str | Mapping[str, Any] | None"]
    """Direct PostgreSQL DSN or connection mapping bypassing the pooler for migrations and ``LISTEN/NOTIFY``."""
    null_pool: NotRequired[bool]
    """Enable ``NullConnectionPool`` / ``AsyncNullConnectionPool`` for serverless or external pooler deployments."""


def build_connection_config(connection_config: "PsycopgPoolParams | Mapping[str, Any] | None") -> dict[str, Any]:
    """Build normalized psycopg connection configuration, resolving aliases for libpq compatibility.

    Maps connection string aliases (dsn, url, connection_string) to conninfo, database aliases
    (database, db) to dbname, and user aliases (username) to user, while discarding redundant keys
    that libpq rejects.
    """
    config = normalize_connection_config(connection_config)
    conninfo = (
        config.pop("conninfo", None)
        or config.pop("dsn", None)
        or config.pop("url", None)
        or config.pop("connection_string", None)
    )
    if conninfo is not None:
        config["conninfo"] = conninfo
    dbname = config.pop("dbname", None) or config.pop("database", None) or config.pop("db", None)
    if dbname is not None:
        config["dbname"] = dbname
    user = config.pop("user", None) or config.pop("username", None)
    if user is not None:
        config["user"] = user
    return config


class PsycopgSyncConnectionContext(SyncPoolConnectionContext):
    """Context manager for Psycopg connections."""

    __slots__ = ()

    def __init__(self, config: "PsycopgSyncConfig") -> None:
        super().__init__(config)

    def __enter__(self) -> "PsycopgSyncConnection":
        self._ctx = self._config.provide_pool().connection()
        return cast("PsycopgSyncConnection", self._ctx.__enter__())

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._ctx is None:
            return None
        try:
            return cast("bool | None", self._ctx.__exit__(exc_type, exc_val, exc_tb))
        finally:
            self._ctx = None


class _PsycopgSyncSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()

    def acquire_connection(self) -> "PsycopgSyncConnection":
        self._ctx = self._config.provide_pool().connection()
        return cast("PsycopgSyncConnection", self._ctx.__enter__())

    def release_connection(self, _conn: "PsycopgSyncConnection", **kwargs: Any) -> None:
        if self._ctx is not None:
            self._ctx.__exit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))
            self._ctx = None


@mypyc_attr(native_class=False)
class PsycopgSyncConfig(SyncDatabaseConfig[PsycopgSyncConnection, ConnectionPool, PsycopgSyncDriver]):
    """Configuration for Psycopg synchronous database connections with direct field-based configuration."""

    driver_type: "ClassVar[type[PsycopgSyncDriver]]" = PsycopgSyncDriver
    connection_type: "ClassVar[type[PsycopgSyncConnection]]" = PsycopgSyncConnection
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
    _connection_context_class: "ClassVar[type[PsycopgSyncConnectionContext]]" = PsycopgSyncConnectionContext
    _session_factory_class: "ClassVar[type[_PsycopgSyncSessionConnectionHandler]]" = (
        _PsycopgSyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[PsycopgSyncSessionContext]]" = PsycopgSyncSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "PsycopgPoolParams | dict[str, Any] | None" = None,
        connection_instance: "ConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "PsycopgDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Psycopg synchronous configuration.

        Args:
            connection_config: Connection and pool configuration parameters (TypedDict or dict)
            connection_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Optional driver feature configuration
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration
            **kwargs: Additional keyword arguments
        """
        connection_config = build_connection_config(connection_config)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[PsycopgSyncConnection], None] | None = features_dict.get(
            "on_connection_create"
        )
        features_dict.setdefault("enable_alloydb", False)
        features_dict.setdefault("enable_alloydb_iam_auth", False)
        features_dict.setdefault("alloydb_ip_type", "PRIVATE")
        pooler_mode = features_dict.get("pooler_mode", "session")
        is_transaction_pooler = bool(
            pooler_mode in {"transaction", "statement"}
            or features_dict.get("pgbouncer")
            or features_dict.get("pgbouncer_mode")
        )
        if is_transaction_pooler:
            connection_config.setdefault("prepare_threshold", None)
        if features_dict.get("enable_alloydb_columnar") and not is_transaction_pooler:
            existing_options = connection_config.get("options")
            columnar_opt = "-c google_columnar_engine.enable_columnar_scan=on"
            if not existing_options:
                connection_config["options"] = columnar_opt
            elif columnar_opt not in str(existing_options):
                connection_config["options"] = f"{existing_options} {columnar_opt}"
        self._pgvector_available: bool | None = None
        self._paradedb_available: bool | None = None
        self._pg_textsearch_available: bool | None = None
        self._pg_textsearch_probe_error: Exception | None = None
        self._alloydb_connector: Any | None = None

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            **kwargs,
        )

        self._validate_alloydb_config()

    @property
    def pg_textsearch_available(self) -> bool:
        """Return True if the pg_textsearch extension is available."""
        return bool(self._pg_textsearch_available)

    def derive_direct_config(self) -> Self:
        """Derive a session-mode Psycopg sync configuration targeting direct_dsn when configured."""
        direct_dsn = self.driver_features.get("direct_dsn")
        if not direct_dsn:
            return self
        conn_cfg = dict(self.connection_config)
        if isinstance(direct_dsn, Mapping):
            conn_cfg.update(build_connection_config(direct_dsn))
        else:
            conn_cfg["conninfo"] = str(direct_dsn)
        is_pooled = bool(
            self.driver_features.get("pooler_mode") in {"transaction", "statement"}
            or self.driver_features.get("pgbouncer")
            or self.driver_features.get("pgbouncer_mode")
        )
        if is_pooled and conn_cfg.get("prepare_threshold") is None:
            conn_cfg.pop("prepare_threshold", None)
        features = dict(self.driver_features)
        features["pooler_mode"] = "session"
        features["pgbouncer"] = False
        features["pgbouncer_mode"] = False
        features["direct_dsn"] = None
        return type(self)(
            connection_config=conn_cfg,
            migration_config=dict(self.migration_config) if self.migration_config else None,
            statement_config=self.statement_config,
            driver_features=features,
            bind_key=self.bind_key,
            extension_config=dict(self.extension_config) if self.extension_config else None,
        )

    def create_connection(self) -> "PsycopgSyncConnection":
        """Open a standalone connection owned by the caller.

        The connection is not bound to the pool, so closing it actually closes
        it and consumes no pool slot.

        Returns:
            A psycopg Connection instance.
        """
        connection_class, conninfo, connection_kwargs = self._connection_kwargs()
        connection = connection_class.connect(conninfo, **connection_kwargs)
        user_configure = self.connection_config.get("configure")
        try:
            self._configure_connection(cast("PsycopgSyncConnection", connection))
            if user_configure is not None and user_configure is not self._configure_connection:
                user_configure(cast("PsycopgSyncConnection", connection))
        except BaseException:
            with suppress(Exception):
                connection.close()
            raise
        return cast("PsycopgSyncConnection", connection)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "PsycopgSyncSessionContext":
        """Provide a driver session context manager.

        Args:
            *_args: Additional arguments.
            statement_config: Optional statement configuration override.
            **_kwargs: Additional keyword arguments.

        Returns:
            A PsycopgSyncDriver session context manager.
        """
        handler = _PsycopgSyncSessionConnectionHandler(self)

        return PsycopgSyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config
            or (lambda: resolve_runtime_statement_config(None, self.statement_config, default_statement_config)),
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def provide_pool(self, *args: Any, **kwargs: Any) -> "ConnectionPool":
        """Provide pool instance.

        Returns:
            The connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for Psycopg types.

        This provides all Psycopg-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "PsycopgConnectionParams": PsycopgConnectionParams,
            "PsycopgPoolParams": PsycopgPoolParams,
            "PsycopgSyncConnectionContext": PsycopgSyncConnectionContext,
            "PsycopgSyncConnection": PsycopgSyncConnection,
            "PsycopgSyncCursor": PsycopgSyncCursor,
            "PsycopgSyncDriver": PsycopgSyncDriver,
            "PsycopgSyncExceptionHandler": PsycopgSyncExceptionHandler,
            "PsycopgSyncSessionContext": PsycopgSyncSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults for PostgreSQL queue fallback."""

        return EventRuntimeHints(poll_interval=0.5, select_for_update=True, skip_locked=True)

    def _validate_alloydb_config(self) -> None:
        """Validate sync-only AlloyDB connector configuration."""
        if not self.driver_features.get("enable_alloydb", False):
            return

        if not ALLOYDB_CONNECTOR_INSTALLED:
            raise MissingDependencyError(package=_ALLOYDB_CONNECTOR_PACKAGE, install_package=_ALLOYDB_CONNECTOR_PACKAGE)

        instance_uri = self.driver_features.get("alloydb_instance_uri")
        if not instance_uri:
            msg = (
                "alloydb_instance_uri required when enable_alloydb is True. "
                "Format: 'projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE'"
            )
            raise ImproperConfigurationError(msg)

        if not str(instance_uri).startswith("projects/"):
            msg = (
                f"Invalid AlloyDB instance URI format: {instance_uri}. "
                "Expected format: 'projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE'"
            )
            raise ImproperConfigurationError(msg)

    def _setup_alloydb_connector(
        self, config: "dict[str, Any]", pool_parameters: "dict[str, Any] | None" = None
    ) -> None:
        """Setup AlloyDB connector and configure psycopg-pool connection_class."""
        if self._alloydb_connector is None:
            self._alloydb_connector = _psycopg_typing.PsycopgAlloydbConnector()

        user = config.get("user")
        password = config.get("password")
        database = config.get("dbname") or config.get("db")
        prepare_threshold = config.pop("prepare_threshold", _DEFAULT_PSYCOPG_PREPARE_THRESHOLD)

        for key in _ALLOYDB_DIRECT_CONNECTION_KEYS:
            config.pop(key, None)

        if pool_parameters is not None:
            pool_parameters["connection_class"] = _make_alloydb_connection_class(
                connector=self._alloydb_connector,
                instance_uri=str(self.driver_features["alloydb_instance_uri"]),
                enable_iam_auth=bool(self.driver_features.get("enable_alloydb_iam_auth", False)),
                ip_type=str(self.driver_features.get("alloydb_ip_type", "PRIVATE")),
                user=cast("str | None", user),
                password=cast("str | None", password),
                database=cast("str | None", database),
                prepare_threshold=cast("int | None", prepare_threshold),
            )

    def _create_pool(self) -> "ConnectionPool":
        """Create the actual connection pool."""
        all_config = dict(self.connection_config)
        all_config.pop("null_pool", None)
        auth_token_provider = all_config.pop("auth_token_provider", None)
        default_max_lifetime = _AUTH_TOKEN_DEFAULT_MAX_LIFETIME if auth_token_provider is not None else 3600.0

        pool_parameters = {
            "connection_class": all_config.pop("connection_class", None),
            "min_size": all_config.pop("min_size", 4),
            "max_size": all_config.pop("max_size", None),
            "name": all_config.pop("name", None),
            "timeout": all_config.pop("timeout", 30.0),
            "max_waiting": all_config.pop("max_waiting", 0),
            "max_lifetime": all_config.pop("max_lifetime", default_max_lifetime),
            "max_idle": all_config.pop("max_idle", 600.0),
            "reconnect_timeout": all_config.pop("reconnect_timeout", 300.0),
            "reconnect_failed": all_config.pop("reconnect_failed", None),
            "num_workers": all_config.pop("num_workers", 3),
            "open": all_config.pop("open", True),
            "check": all_config.pop("check", None),
            "reset": all_config.pop("reset", None),
            "close_returns": all_config.pop("close_returns", False),
        }

        if callable(auth_token_provider) and not self.driver_features.get("enable_alloydb", False):
            base_conn_cls = cast(
                "type[PsycopgConnection[Any]]", pool_parameters.get("connection_class") or PsycopgConnection
            )
            pool_parameters["connection_class"] = _make_token_provider_sync_connection_class(
                base_conn_cls, cast("Callable[[], str]", auth_token_provider)
            )

        user_configure = all_config.pop("configure", None)
        if user_configure is not None and user_configure is not self._configure_connection:

            def _chained_configure(conn: "PsycopgSyncConnection") -> None:
                self._configure_connection(conn)
                user_configure(conn)

            pool_parameters["configure"] = _chained_configure
        else:
            pool_parameters["configure"] = self._configure_connection

        pool_parameters = {k: v for k, v in pool_parameters.items() if v is not None}

        conninfo = all_config.pop("conninfo", None)
        kwargs = all_config.pop("kwargs", {})
        all_config.update(kwargs)

        if self.driver_features.get("enable_alloydb", False):
            if conninfo is not None:
                all_config["conninfo"] = conninfo
            self._setup_alloydb_connector(all_config, pool_parameters)
            conninfo = None

        is_null_pool = bool(self.connection_config.get("null_pool") or self.driver_features.get("null_pool"))
        pool_cls = NullConnectionPool if is_null_pool else ConnectionPool
        if is_null_pool:
            pool_parameters.pop("min_size", None)

        if conninfo:
            pool = pool_cls(conninfo, kwargs=all_config, **pool_parameters)
        else:
            pool = pool_cls("", kwargs=all_config, **pool_parameters)

        return pool

    def _configure_connection(self, conn: "PsycopgSyncConnection") -> None:
        autocommit_setting = self.connection_config.get("autocommit")
        if autocommit_setting is not None:
            conn.autocommit = autocommit_setting

        serializer = self.driver_features.get("json_serializer", to_json)
        deserializer = self.driver_features.get("json_deserializer", from_json)
        if isinstance(getattr(conn, "adapters", None), AdaptersMap):
            set_json_dumps(serializer, conn)
            set_json_loads(deserializer, conn)

        if self._pgvector_available is None:
            detected_extensions: set[str] = set()
            extensions = build_postgres_extension_probe_names(self.driver_features)
            probe_succeeded = True
            if extensions:
                try:
                    cursor = conn.execute(
                        "SELECT extname FROM pg_extension WHERE extname = ANY(%s::text[])", (extensions,)
                    )
                    results = cursor.fetchall()
                    detected_extensions = {
                        cast("str", r["extname"] if isinstance(r, Mapping) else r[0]) for r in results
                    }
                except Exception as exc:
                    probe_succeeded = False
                    self._pg_textsearch_probe_error = exc
                    if not conn.autocommit:
                        with suppress(Exception):
                            conn.rollback()
            if probe_succeeded:
                self.statement_config, self._pgvector_available, self._paradedb_available = (
                    resolve_postgres_extension_state(self.statement_config, self.driver_features, detected_extensions)
                )
                self._pg_textsearch_available = is_postgres_extension_active(self.driver_features, "pg_textsearch")

        if self._pgvector_available:
            register_pgvector_sync(conn)

        if self._user_connection_hook is not None:
            self._user_connection_hook(conn)

        if not conn.autocommit:
            conn.rollback()

    def _ensure_pg_textsearch_available(self) -> None:
        if self._pg_textsearch_available:
            return
        msg = "ADK memory enable_bm25 requires the pg_textsearch PostgreSQL extension"
        raise ImproperConfigurationError(msg) from self._pg_textsearch_probe_error

    def _close_pool(self) -> None:
        """Close the actual connection pool and cleanup connectors."""
        try:
            if self.connection_instance:
                self.connection_instance.close()
        finally:
            self.connection_instance = None
            if self._alloydb_connector is not None:
                self._alloydb_connector.close()
                self._alloydb_connector = None

    def _connection_kwargs(self) -> "tuple[type[Any], str, dict[str, Any]]":
        """Split the pooled configuration into standalone connect arguments.

        Returns:
            The connection class, the conninfo string, and the connect kwargs.
        """
        all_config = {k: v for k, v in self.connection_config.items() if k not in _POOL_ONLY_CONFIG_KEYS}
        auth_token_provider = self.connection_config.get("auth_token_provider")
        if callable(auth_token_provider):
            all_config["password"] = str(auth_token_provider())
        connection_class = all_config.pop("connection_class", None) or PsycopgConnection
        conninfo = all_config.pop("conninfo", None)
        all_config.update(all_config.pop("kwargs", {}))
        if self.driver_features.get("enable_alloydb", False):
            if conninfo is not None:
                all_config["conninfo"] = conninfo
            alloydb_parameters: dict[str, Any] = {}
            self._setup_alloydb_connector(all_config, alloydb_parameters)
            connection_class = alloydb_parameters["connection_class"]
            conninfo = None
        return cast("type[Any]", connection_class), str(conninfo or ""), all_config


class PsycopgAsyncConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for Psycopg connections."""

    __slots__ = ("_ctx",)

    def __init__(self, config: "PsycopgAsyncConfig") -> None:
        super().__init__(config)
        self._ctx: Any = None

    async def __aenter__(self) -> "PsycopgAsyncConnection":
        pool = await self._config.provide_pool()
        self._ctx = pool.connection()
        return cast("PsycopgAsyncConnection", await self._ctx.__aenter__())

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._ctx:
            try:
                return cast("bool | None", await self._ctx.__aexit__(exc_type, exc_val, exc_tb))
            finally:
                self._ctx = None
        return None


class _PsycopgAsyncSessionConnectionHandler(AsyncPoolSessionFactory):
    __slots__ = ("_ctx",)

    def __init__(self, config: "PsycopgAsyncConfig") -> None:
        super().__init__(config)
        self._ctx: Any = None

    async def acquire_connection(self) -> "PsycopgAsyncConnection":
        pool = await self._config.provide_pool()
        self._ctx = pool.connection()
        return cast("PsycopgAsyncConnection", await self._ctx.__aenter__())

    async def release_connection(self, _conn: "PsycopgAsyncConnection", **kwargs: Any) -> None:
        if self._ctx is None:
            return
        try:
            await self._ctx.__aexit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))
        finally:
            self._ctx = None


@mypyc_attr(native_class=False)
class PsycopgAsyncConfig(AsyncDatabaseConfig[PsycopgAsyncConnection, AsyncConnectionPool, PsycopgAsyncDriver]):
    """Configuration for Psycopg asynchronous database connections with direct field-based configuration."""

    driver_type: ClassVar[type[PsycopgAsyncDriver]] = PsycopgAsyncDriver
    connection_type: "ClassVar[type[PsycopgAsyncConnection]]" = PsycopgAsyncConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_migration_schemas: "ClassVar[bool]" = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_row_streaming: ClassVar[bool] = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="native"
    )
    _connection_context_class: "ClassVar[type[PsycopgAsyncConnectionContext]]" = PsycopgAsyncConnectionContext
    _session_factory_class: "ClassVar[type[_PsycopgAsyncSessionConnectionHandler]]" = (
        _PsycopgAsyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[PsycopgAsyncSessionContext]]" = PsycopgAsyncSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "PsycopgPoolParams | dict[str, Any] | None" = None,
        connection_instance: "AsyncConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "PsycopgDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Psycopg asynchronous configuration.

        Args:
            connection_config: Connection and pool configuration parameters (TypedDict or dict)
            connection_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Optional driver feature configuration
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration
            **kwargs: Additional keyword arguments
        """
        connection_config = build_connection_config(connection_config)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        if features_dict.get("enable_alloydb", False):
            msg = "Google AlloyDB connector integration (enable_alloydb) is only supported on PsycopgSyncConfig"
            raise ImproperConfigurationError(msg)
        self._user_connection_hook: Callable[[PsycopgAsyncConnection], Awaitable[None]] | None = features_dict.get(
            "on_connection_create"
        )
        pooler_mode = features_dict.get("pooler_mode", "session")
        is_transaction_pooler = bool(
            pooler_mode in {"transaction", "statement"}
            or features_dict.get("pgbouncer")
            or features_dict.get("pgbouncer_mode")
        )
        if is_transaction_pooler:
            connection_config.setdefault("prepare_threshold", None)
        if features_dict.get("enable_alloydb_columnar") and not is_transaction_pooler:
            existing_options = connection_config.get("options")
            columnar_opt = "-c google_columnar_engine.enable_columnar_scan=on"
            if not existing_options:
                connection_config["options"] = columnar_opt
            elif columnar_opt not in str(existing_options):
                connection_config["options"] = f"{existing_options} {columnar_opt}"
        self._pgvector_available: bool | None = None
        self._paradedb_available: bool | None = None
        self._pg_textsearch_available: bool | None = None
        self._pg_textsearch_probe_error: Exception | None = None

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            **kwargs,
        )

    @property
    def pg_textsearch_available(self) -> bool:
        """Return True if the pg_textsearch extension is available."""
        return bool(self._pg_textsearch_available)

    def derive_direct_config(self) -> Self:
        """Derive a session-mode Psycopg async configuration targeting direct_dsn when configured."""
        direct_dsn = self.driver_features.get("direct_dsn")
        if not direct_dsn:
            return self
        conn_cfg = dict(self.connection_config)
        if isinstance(direct_dsn, Mapping):
            conn_cfg.update(build_connection_config(direct_dsn))
        else:
            conn_cfg["conninfo"] = str(direct_dsn)
        is_pooled = bool(
            self.driver_features.get("pooler_mode") in {"transaction", "statement"}
            or self.driver_features.get("pgbouncer")
            or self.driver_features.get("pgbouncer_mode")
        )
        if is_pooled and conn_cfg.get("prepare_threshold") is None:
            conn_cfg.pop("prepare_threshold", None)
        features = dict(self.driver_features)
        features["pooler_mode"] = "session"
        features["pgbouncer"] = False
        features["pgbouncer_mode"] = False
        features["direct_dsn"] = None
        return type(self)(
            connection_config=conn_cfg,
            migration_config=dict(self.migration_config) if self.migration_config else None,
            statement_config=self.statement_config,
            driver_features=features,
            bind_key=self.bind_key,
            extension_config=dict(self.extension_config) if self.extension_config else None,
        )

    async def create_connection(self) -> "PsycopgAsyncConnection":  # pyright: ignore
        """Open a standalone connection owned by the caller.

        The connection is not bound to the pool, so closing it actually closes
        it and consumes no pool slot.

        Returns:
            A psycopg AsyncConnection instance.
        """
        connection_class, conninfo, connection_kwargs = self._connection_kwargs()
        auth_token_provider = self.connection_config.get("auth_token_provider")
        if callable(auth_token_provider):
            token_result = auth_token_provider()
            if inspect.isawaitable(token_result):
                token_result = await token_result
            connection_kwargs["password"] = str(token_result)
        connection = await connection_class.connect(conninfo, **connection_kwargs)
        user_configure = self.connection_config.get("configure")
        try:
            await self._configure_async_connection(cast("PsycopgAsyncConnection", connection))
            if user_configure is not None and user_configure is not self._configure_async_connection:
                await user_configure(cast("PsycopgAsyncConnection", connection))
        except BaseException:
            with suppress(Exception):
                await connection.close()
            raise
        return cast("PsycopgAsyncConnection", connection)

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for PsycopgAsyncConfig types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "PsycopgAsyncConnectionContext": PsycopgAsyncConnectionContext,
            "PsycopgAsyncConnection": PsycopgAsyncConnection,
            "PsycopgAsyncCursor": PsycopgAsyncCursor,
            "PsycopgAsyncDriver": PsycopgAsyncDriver,
            "PsycopgAsyncExceptionHandler": PsycopgAsyncExceptionHandler,
            "PsycopgAsyncSessionContext": PsycopgAsyncSessionContext,
            "PsycopgConnectionParams": PsycopgConnectionParams,
            "PsycopgPoolParams": PsycopgPoolParams,
        })
        return namespace

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "PsycopgAsyncSessionContext":
        """Provide an async driver session context manager.

        Args:
            *_args: Additional arguments.
            statement_config: Optional statement configuration override.
            **_kwargs: Additional keyword arguments.

        Returns:
            A PsycopgAsyncDriver session context manager.
        """
        handler = _PsycopgAsyncSessionConnectionHandler(self)

        return PsycopgAsyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config
            or (lambda: resolve_runtime_statement_config(None, self.statement_config, default_statement_config)),
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "AsyncConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults for PostgreSQL queue fallback."""

        return EventRuntimeHints(poll_interval=0.5, select_for_update=True, skip_locked=True)

    async def _create_pool(self) -> "AsyncConnectionPool":
        """Create the actual async connection pool."""

        all_config = dict(self.connection_config)
        all_config.pop("null_pool", None)
        auth_token_provider = all_config.pop("auth_token_provider", None)
        default_max_lifetime = _AUTH_TOKEN_DEFAULT_MAX_LIFETIME if auth_token_provider is not None else 3600.0

        pool_parameters = {
            "connection_class": all_config.pop("connection_class", None),
            "min_size": all_config.pop("min_size", 4),
            "max_size": all_config.pop("max_size", None),
            "name": all_config.pop("name", None),
            "timeout": all_config.pop("timeout", 30.0),
            "max_waiting": all_config.pop("max_waiting", 0),
            "max_lifetime": all_config.pop("max_lifetime", default_max_lifetime),
            "max_idle": all_config.pop("max_idle", 600.0),
            "reconnect_timeout": all_config.pop("reconnect_timeout", 300.0),
            "reconnect_failed": all_config.pop("reconnect_failed", None),
            "num_workers": all_config.pop("num_workers", 3),
            "check": all_config.pop("check", None),
            "reset": all_config.pop("reset", None),
            "close_returns": all_config.pop("close_returns", False),
        }
        if callable(auth_token_provider):
            base_async_cls = cast(
                "type[PsycopgAsyncConnection]", pool_parameters.get("connection_class") or PsycopgAsyncConnection
            )
            pool_parameters["connection_class"] = _make_token_provider_async_connection_class(
                base_async_cls, cast("Callable[[], str | Awaitable[str]]", auth_token_provider)
            )
        open_pool = all_config.pop("open", True)
        pool_parameters["open"] = False if open_pool is True else open_pool

        user_configure = all_config.pop("configure", None)
        if user_configure is not None and user_configure is not self._configure_async_connection:

            async def _chained_async_configure(conn: "PsycopgAsyncConnection") -> None:
                await self._configure_async_connection(conn)
                await user_configure(conn)

            pool_parameters["configure"] = _chained_async_configure
        else:
            pool_parameters["configure"] = self._configure_async_connection

        pool_parameters = {k: v for k, v in pool_parameters.items() if v is not None}

        conninfo = all_config.pop("conninfo", None)
        kwargs = all_config.pop("kwargs", {})
        all_config.update(kwargs)

        is_null_pool = bool(self.connection_config.get("null_pool") or self.driver_features.get("null_pool"))
        pool_cls = AsyncNullConnectionPool if is_null_pool else AsyncConnectionPool
        if is_null_pool:
            pool_parameters.pop("min_size", None)

        if conninfo:
            pool = pool_cls(conninfo, kwargs=all_config, **pool_parameters)
        else:
            pool = pool_cls("", kwargs=all_config, **pool_parameters)

        if open_pool is True:
            await pool.open()

        return pool

    async def _configure_async_connection(self, conn: "PsycopgAsyncConnection") -> None:
        autocommit_setting = self.connection_config.get("autocommit")
        if autocommit_setting is not None:
            await conn.set_autocommit(autocommit_setting)

        serializer = self.driver_features.get("json_serializer", to_json)
        deserializer = self.driver_features.get("json_deserializer", from_json)
        if isinstance(getattr(conn, "adapters", None), AdaptersMap):
            set_json_dumps(serializer, conn)
            set_json_loads(deserializer, conn)

        if self._pgvector_available is None:
            detected_extensions: set[str] = set()
            extensions = build_postgres_extension_probe_names(self.driver_features)
            probe_succeeded = True
            if extensions:
                try:
                    cursor = await conn.execute(
                        "SELECT extname FROM pg_extension WHERE extname = ANY(%s::text[])", (extensions,)
                    )
                    results = await cursor.fetchall()
                    detected_extensions = {
                        cast("str", r["extname"] if isinstance(r, Mapping) else r[0]) for r in results
                    }
                except Exception as exc:
                    probe_succeeded = False
                    self._pg_textsearch_probe_error = exc
                    if not conn.autocommit:
                        with suppress(Exception):
                            await conn.rollback()
            if probe_succeeded:
                self.statement_config, self._pgvector_available, self._paradedb_available = (
                    resolve_postgres_extension_state(self.statement_config, self.driver_features, detected_extensions)
                )
                self._pg_textsearch_available = is_postgres_extension_active(self.driver_features, "pg_textsearch")

        if self._pgvector_available:
            await register_pgvector_async(conn)

        if self._user_connection_hook is not None:
            await self._user_connection_hook(conn)

        if not conn.autocommit:
            await conn.rollback()

    def _ensure_pg_textsearch_available(self) -> None:
        if self._pg_textsearch_available:
            return
        msg = "ADK memory enable_bm25 requires the pg_textsearch PostgreSQL extension"
        raise ImproperConfigurationError(msg) from self._pg_textsearch_probe_error

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if not self.connection_instance:
            return

        try:
            await self.connection_instance.close()
        finally:
            self.connection_instance = None

    def _connection_kwargs(self) -> "tuple[type[Any], str, dict[str, Any]]":
        """Split the pooled configuration into standalone connect arguments.

        Returns:
            The connection class, the conninfo string, and the connect kwargs.
        """
        all_config = {k: v for k, v in self.connection_config.items() if k not in _POOL_ONLY_CONFIG_KEYS}
        connection_class = all_config.pop("connection_class", None) or PsycopgAsyncConnection
        conninfo = all_config.pop("conninfo", None)
        all_config.update(all_config.pop("kwargs", {}))
        return cast("type[Any]", connection_class), str(conninfo or ""), all_config


def _make_alloydb_connection_class(
    *,
    connector: Any,
    instance_uri: str,
    enable_iam_auth: bool,
    ip_type: str,
    user: str | None,
    password: str | None,
    database: str | None,
    prepare_threshold: int | None = _DEFAULT_PSYCOPG_PREPARE_THRESHOLD,
) -> "type[PsycopgConnection[Any]]":
    """Build a psycopg connection class backed by the AlloyDB connector."""

    class _AlloyDBPsycopgConnection(PsycopgConnection[Any]):
        default_prepare_threshold: ClassVar[int | None] = prepare_threshold

        @classmethod
        def connect(
            cls,
            conninfo: str = "",
            *,
            autocommit: bool = False,
            prepare_threshold: int | None = prepare_threshold,
            context: "AdaptContext | None" = None,
            row_factory: "RowFactory[Any] | None" = None,
            cursor_factory: "type[Cursor[Any]] | None" = None,
            **kwargs: str | int | None,
        ) -> Self:
            _ = (cls, conninfo, context)
            connector_kwargs = dict(kwargs)
            connector_kwargs["enable_iam_auth"] = enable_iam_auth
            connector_kwargs["ip_type"] = ip_type
            if user is not None:
                connector_kwargs["user"] = user
            if password is not None:
                connector_kwargs["password"] = password
            if database is not None:
                connector_kwargs["db"] = database
            conn = connector.connect(instance_uri, "psycopg", **connector_kwargs)
            if autocommit:
                conn.autocommit = autocommit
            if prepare_threshold != _DEFAULT_PSYCOPG_PREPARE_THRESHOLD:
                conn.prepare_threshold = prepare_threshold
            if row_factory is not None:
                conn.row_factory = row_factory
            if cursor_factory is not None:
                conn.cursor_factory = cursor_factory
            return cast("Self", conn)

    return _AlloyDBPsycopgConnection


def _make_token_provider_sync_connection_class(
    base_cls: "type[PsycopgConnection[Any]]", provider: "Callable[[], str]"
) -> "type[PsycopgConnection[Any]]":
    """Build a Psycopg sync connection subclass that refreshes ``password`` from ``provider``."""

    class _TokenProviderPsycopgConnection(base_cls):  # type: ignore[valid-type,misc]
        @classmethod
        def connect(cls, conninfo: str = "", **kwargs: Any) -> Self:
            kwargs["password"] = str(provider())
            return cast("Self", super().connect(conninfo, **kwargs))

    return _TokenProviderPsycopgConnection


def _make_token_provider_async_connection_class(
    base_cls: "type[PsycopgAsyncConnection]", provider: "Callable[[], str | Awaitable[str]]"
) -> "type[PsycopgAsyncConnection]":
    """Build a Psycopg async connection subclass that refreshes ``password`` from ``provider``."""

    class _TokenProviderPsycopgAsyncConnection(base_cls):  # type: ignore[valid-type,misc]
        @classmethod
        async def connect(cls, conninfo: str = "", **kwargs: Any) -> Self:
            token_result = provider()
            if inspect.isawaitable(token_result):
                token_result = await token_result
            kwargs["password"] = str(token_result)
            return cast("Self", await super().connect(conninfo, **kwargs))

    return _TokenProviderPsycopgAsyncConnection
