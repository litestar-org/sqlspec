"""pymssql database configuration."""

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from sqlspec.adapters.pymssql._typing import PymssqlConnection, PymssqlCursor, PymssqlRawCursor, PymssqlSessionContext
from sqlspec.adapters.pymssql.core import apply_driver_features, build_connection_config, default_statement_config
from sqlspec.adapters.pymssql.driver import PymssqlDriver, PymssqlExceptionHandler
from sqlspec.adapters.pymssql.migrations import PymssqlSyncMigrationTracker
from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("PymssqlConfig", "PymssqlConnectionParams", "PymssqlDriverFeatures", "PymssqlPoolParams", "PymssqlTimeout")

PymssqlTimeout = int | float


class PymssqlConnectionParams(TypedDict):
    """pymssql connection parameters."""

    server: NotRequired[str]
    """SQL Server hostname, ``host:port``, or ``host\\instance`` target."""
    host: NotRequired[str]
    """Alias for ``server`` accepted by ``pymssql.connect``."""
    user: NotRequired[str]
    """SQL Server or Windows domain login (``DOMAIN\\user``)."""
    password: NotRequired[str]
    """Password for ``user``."""
    database: NotRequired[str]
    """Initial SQL Server database selected on connect."""
    port: NotRequired[int | str]
    """SQL Server TCP port (defaults to ``1433``)."""
    timeout: NotRequired[PymssqlTimeout]
    """Query response timeout in seconds (``0`` means wait indefinitely)."""
    login_timeout: NotRequired[PymssqlTimeout]
    """Connection and authentication timeout in seconds (defaults to ``60``)."""
    charset: NotRequired[str]
    """Character encoding communicated over FreeTDS (defaults to ``'UTF-8'``)."""
    as_dict: NotRequired[bool]
    """Return rows from raw ``pymssql`` cursors as dictionaries instead of tuples."""
    appname: NotRequired[str]
    """Application name visible in ``sys.dm_exec_sessions``."""
    conn_properties: NotRequired[str]
    """Semicolon-separated ``SET`` session statements run immediately upon connect."""
    autocommit: NotRequired[bool]
    """Enable autocommit mode on new ``pymssql`` connections."""
    tds_version: NotRequired[str]
    """Explicit FreeTDS protocol version (such as ``'7.4'``)."""
    encryption: NotRequired[Literal["off", "request", "require"]]
    """FreeTDS TLS transport encryption requirement (``off``, ``request``, or ``require``)."""
    use_datetime2: NotRequired[bool]
    """Bind Python ``datetime`` values using ``DATETIME2`` precision instead of legacy ``DATETIME``."""
    arraysize: NotRequired[int]
    """Default row batch count returned by ``cursor.fetchmany()``."""
    conv: NotRequired[Mapping[int | type[Any], Callable[..., Any]]]
    """Custom FreeTDS type serialization/deserialization mapping."""
    read_only: NotRequired[bool]
    """Set Always On ``ApplicationIntent=ReadOnly`` during login."""
    pool_recycle_seconds: NotRequired[int]
    """Maximum connection age in seconds before ``PymssqlConnectionPool`` recycles connections."""
    health_check_interval: NotRequired[float]
    """Minimum interval in seconds between connection health checks in ``PymssqlConnectionPool``."""
    extra: NotRequired["dict[str, Any]"]
    """Additional keyword arguments forwarded to ``pymssql.connect``."""


class PymssqlPoolParams(PymssqlConnectionParams):
    """pymssql pool parameters."""


class PymssqlDriverFeatures(TypedDict):
    """Optional pymssql driver feature flags for JSON serialization, connection hooks, and events."""

    json_serializer: NotRequired["Callable[[Any], str]"]
    """Custom JSON serializer applied to bound JSON parameters (defaults to ``to_json``)."""
    json_deserializer: NotRequired["Callable[[str], Any]"]
    """Custom JSON deserializer applied to JSON result columns (defaults to ``from_json``)."""
    on_connection_create: "NotRequired[Callable[[PymssqlConnection], None]]"
    """Synchronous hook invoked with each raw ``pymssql.Connection`` upon creation."""
    enable_events: NotRequired[bool]
    """Enable durable table-backed SQLSpec event queue support."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event channel backend (``poll_queue`` is the sole SQL Server backend)."""
    use_pool: NotRequired[bool]
    """Enable driver-level connection pooling."""
    enable_uuid_conversion: NotRequired[bool]
    """Enable automatic UUID string-to-object conversions."""


class PymssqlConnectionContext(SyncPoolConnectionContext):
    """Context manager for pymssql connections."""

    __slots__ = ()


class _PymssqlSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()


class PymssqlConfig(SyncDatabaseConfig[PymssqlConnection, PymssqlConnectionPool, PymssqlDriver]):
    """Configuration for pymssql synchronous connections."""

    __slots__ = ("_user_connection_hook",)

    driver_type: "ClassVar[type[PymssqlDriver]]" = PymssqlDriver
    connection_type: "ClassVar[type[PymssqlConnection]]" = cast("type[PymssqlConnection]", PymssqlConnection)
    migration_tracker_type: "ClassVar[type[PymssqlSyncMigrationTracker]]" = PymssqlSyncMigrationTracker
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_migration_schemas: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    supports_native_row_streaming: "ClassVar[bool]" = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
    _connection_context_class: "ClassVar[type[PymssqlConnectionContext]]" = PymssqlConnectionContext
    _session_factory_class: "ClassVar[type[_PymssqlSessionConnectionHandler]]" = _PymssqlSessionConnectionHandler
    _session_context_class: "ClassVar[type[PymssqlSessionContext]]" = PymssqlSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "PymssqlPoolParams | dict[str, Any] | None" = None,
        connection_instance: "PymssqlConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "PymssqlDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        connection_config = build_connection_config(normalize_connection_config(connection_config))

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[PymssqlConnection], None] | None = features_dict.pop(
            "on_connection_create", None
        )

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def create_connection(self) -> "PymssqlConnection":
        """Open a standalone connection owned by the caller.

        The connection carries the same parameters and creation hook the pool
        applies, but it is not the pool's thread-local connection, so closing it
        leaves the pool usable.

        Returns:
            PymssqlConnection: A newly opened connection.
        """
        return self.provide_pool().new_connection()

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "PymssqlConfig": PymssqlConfig,
            "PymssqlConnection": PymssqlConnection,
            "PymssqlConnectionContext": PymssqlConnectionContext,
            "PymssqlConnectionParams": PymssqlConnectionParams,
            "PymssqlConnectionPool": PymssqlConnectionPool,
            "PymssqlCursor": PymssqlCursor,
            "PymssqlDriver": PymssqlDriver,
            "PymssqlDriverFeatures": PymssqlDriverFeatures,
            "PymssqlExceptionHandler": PymssqlExceptionHandler,
            "PymssqlPoolParams": PymssqlPoolParams,
            "PymssqlRawCursor": PymssqlRawCursor,
            "PymssqlSessionContext": PymssqlSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return runtime hints for pymssql event channels."""
        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5)

    def _create_pool(self) -> "PymssqlConnectionPool":
        config = dict(self.connection_config)
        pool_recycle = config.pop("pool_recycle_seconds", 86400)
        health_check = config.pop("health_check_interval", 30.0)
        return PymssqlConnectionPool(
            config,
            recycle_seconds=pool_recycle,
            health_check_interval=health_check,
            on_connection_create=self._user_connection_hook,
        )

    def _close_pool(self) -> None:
        if self.connection_instance:
            self.connection_instance.close()
            self.connection_instance = None
