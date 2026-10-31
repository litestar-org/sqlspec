"""SAP SQL Anywhere database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from sqlspec.adapters.sqlanywhere._typing import (
    SqlanywhereConnection,
    SqlanywhereCursor,
    SqlanywhereRawCursor,
    SqlanywhereSessionContext,
)
from sqlspec.adapters.sqlanywhere.core import apply_driver_features, build_connection_config, default_statement_config
from sqlspec.adapters.sqlanywhere.driver import SqlanywhereDriver, SqlanywhereExceptionHandler
from sqlspec.adapters.sqlanywhere.migrations import SqlanywhereSyncMigrationTracker
from sqlspec.adapters.sqlanywhere.pool import SqlanywhereConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "SqlanywhereConfig",
    "SqlanywhereConnectionContext",
    "SqlanywhereConnectionParams",
    "SqlanywhereDriverFeatures",
    "SqlanywherePoolParams",
)


class SqlanywhereConnectionParams(TypedDict):
    """SAP SQL Anywhere connection parameters."""

    uid: NotRequired[str]
    """User ID / login name for authentication."""
    user: NotRequired[str]
    """Alias for ``uid``."""
    username: NotRequired[str]
    """Alias for ``uid``."""
    pwd: NotRequired[str]
    """Password for authentication."""
    password: NotRequired[str]
    """Alias for ``pwd``."""
    dbn: NotRequired[str]
    """Database name on the SQL Anywhere server."""
    database: NotRequired[str]
    """Alias for ``dbn``."""
    db: NotRequired[str]
    """Alias for ``dbn``."""
    eng: NotRequired[str]
    """SQL Anywhere database engine name."""
    server: NotRequired[str]
    """Alias for ``eng``."""
    engine: NotRequired[str]
    """Alias for ``eng``."""
    server_name: NotRequired[str]
    """Alias for ``eng``."""
    dbf: NotRequired[str]
    """Database file path for local or embedded engines."""
    database_file: NotRequired[str]
    """Alias for ``dbf``."""
    host: NotRequired[str]
    """TCP hostname or IP address of the server."""
    port: NotRequired[int | str]
    """TCP port number of the server."""
    dsn: NotRequired[str]
    """ODBC data source name."""
    links: NotRequired[str]
    """Network communications links protocol configuration."""
    cset: NotRequired[str]
    """Character set used for character data conversion."""
    extra: NotRequired["dict[str, Any]"]
    """Additional connection parameters passed to the driver."""


class SqlanywherePoolParams(SqlanywhereConnectionParams):
    """SAP SQL Anywhere pool parameters."""

    pool_min_size: NotRequired[int]
    """Minimum number of connections maintained in the pool."""
    pool_max_size: NotRequired[int]
    """Maximum number of connections allowed in the pool."""
    pool_timeout: NotRequired[float]
    """Seconds to wait for a connection before timing out."""
    pool_recycle_seconds: NotRequired[int]
    """Maximum age in seconds before a connection is recycled."""
    health_check_interval: NotRequired[float]
    """Interval in seconds between background health checks."""


class SqlanywhereDriverFeatures(TypedDict):
    """SAP SQL Anywhere driver feature flags."""

    json_serializer: NotRequired["Callable[[Any], str]"]
    """Custom callable for serializing objects to JSON strings."""
    json_deserializer: NotRequired["Callable[[str], Any]"]
    """Custom callable for deserializing JSON strings."""
    on_connection_create: "NotRequired[Callable[[SqlanywhereConnection], None]]"
    """Callback invoked immediately when a new connection is established."""
    enable_events: NotRequired[bool]
    """Enable database event listening."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Backend mechanism used for dispatching events."""


class SqlanywhereConnectionContext(SyncPoolConnectionContext):
    """Context manager for SAP SQL Anywhere connections."""

    __slots__ = ()


class _SqlanywhereSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()


class SqlanywhereConfig(SyncDatabaseConfig[SqlanywhereConnection, SqlanywhereConnectionPool, SqlanywhereDriver]):
    """Configuration for SAP SQL Anywhere synchronous connections over sqlanydb."""

    __slots__ = (
        "_health_check_interval",
        "_pool_max_size",
        "_pool_min_size",
        "_pool_recycle_seconds",
        "_pool_timeout",
        "_user_connection_hook",
    )

    driver_type: "ClassVar[type[SqlanywhereDriver]]" = SqlanywhereDriver
    connection_type: "ClassVar[type[SqlanywhereConnection]]" = cast(
        "type[SqlanywhereConnection]", SqlanywhereConnection
    )
    migration_tracker_type: "ClassVar[type[SqlanywhereSyncMigrationTracker]]" = SqlanywhereSyncMigrationTracker
    supports_transactional_ddl: "ClassVar[bool]" = False
    supports_migration_schemas: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    supports_native_row_streaming: "ClassVar[bool]" = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
    _connection_context_class: "ClassVar[type[SqlanywhereConnectionContext]]" = SqlanywhereConnectionContext
    _session_factory_class: "ClassVar[type[_SqlanywhereSessionConnectionHandler]]" = (
        _SqlanywhereSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[SqlanywhereSessionContext]]" = SqlanywhereSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "SqlanywherePoolParams | dict[str, Any] | None" = None,
        connection_instance: "SqlanywhereConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "SqlanywhereDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        raw_config = dict(normalize_connection_config(connection_config))
        self._pool_min_size = int(raw_config.pop("pool_min_size", 1))
        self._pool_max_size = int(raw_config.pop("pool_max_size", 10))
        self._pool_timeout = float(raw_config.pop("pool_timeout", 30.0))
        self._pool_recycle_seconds = int(raw_config.pop("pool_recycle_seconds", 86400))
        self._health_check_interval = float(raw_config.pop("health_check_interval", 30.0))
        normalized_connection_config = build_connection_config(raw_config)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[SqlanywhereConnection], None] | None = features_dict.pop(
            "on_connection_create", None
        )

        super().__init__(
            connection_config=normalized_connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def create_connection(self) -> "SqlanywhereConnection":
        """Open a standalone SAP SQL Anywhere connection owned by the caller."""
        return self.provide_pool().new_connection()

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "SqlanywhereConfig": SqlanywhereConfig,
            "SqlanywhereConnection": SqlanywhereConnection,
            "SqlanywhereConnectionContext": SqlanywhereConnectionContext,
            "SqlanywhereConnectionParams": SqlanywhereConnectionParams,
            "SqlanywhereConnectionPool": SqlanywhereConnectionPool,
            "SqlanywhereCursor": SqlanywhereCursor,
            "SqlanywhereDriver": SqlanywhereDriver,
            "SqlanywhereDriverFeatures": SqlanywhereDriverFeatures,
            "SqlanywhereExceptionHandler": SqlanywhereExceptionHandler,
            "SqlanywherePoolParams": SqlanywherePoolParams,
            "SqlanywhereRawCursor": SqlanywhereRawCursor,
            "SqlanywhereSessionContext": SqlanywhereSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.5, lease_seconds=30, select_for_update=False, skip_locked=False)

    def _create_pool(self) -> "SqlanywhereConnectionPool":
        return SqlanywhereConnectionPool(
            self.connection_config,
            min_size=self._pool_min_size,
            max_size=self._pool_max_size,
            timeout=self._pool_timeout,
            recycle_seconds=self._pool_recycle_seconds,
            health_check_interval=self._health_check_interval,
            on_connection_create=self._user_connection_hook,
        )

    def _close_pool(self) -> None:
        if self.connection_instance:
            self.connection_instance.close()
            self.connection_instance = None
