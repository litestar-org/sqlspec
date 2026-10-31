"""Sybase ASE database configuration."""

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from sqlspec.adapters.sybase._typing import SybaseConnection, SybaseCursor, SybaseRawCursor, SybaseSessionContext
from sqlspec.adapters.sybase.core import apply_driver_features, build_connection_config, default_statement_config
from sqlspec.adapters.sybase.driver import SybaseDriver, SybaseExceptionHandler
from sqlspec.adapters.sybase.migrations import SybaseSyncMigrationTracker
from sqlspec.adapters.sybase.pool import SybaseConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "SybaseConfig",
    "SybaseConnectionContext",
    "SybaseConnectionParams",
    "SybaseDriverFeatures",
    "SybasePoolParams",
    "SybaseTimeout",
)

SybaseTimeout = int | float


class SybaseConnectionParams(TypedDict):
    """Sybase ASE connection parameters."""

    server: NotRequired[str]
    host: NotRequired[str]
    freetds_server: NotRequired[str]
    server_alias: NotRequired[str]
    user: NotRequired[str]
    username: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    db: NotRequired[str]
    port: NotRequired[int | str]
    timeout: NotRequired[SybaseTimeout]
    login_timeout: NotRequired[SybaseTimeout]
    charset: NotRequired[str]
    as_dict: NotRequired[bool]
    appname: NotRequired[str]
    conn_properties: NotRequired[str]
    autocommit: NotRequired[bool]
    tds_version: NotRequired[str]
    encryption: NotRequired[Literal["off", "request", "require"]]
    arraysize: NotRequired[int]
    conv: NotRequired[Mapping[int | type[Any], Callable[..., Any]]]
    read_only: NotRequired[bool]
    pool_recycle_seconds: NotRequired[int]
    health_check_interval: NotRequired[float]
    extra: NotRequired["dict[str, Any]"]


class SybasePoolParams(SybaseConnectionParams):
    """Sybase ASE pool parameters."""


class SybaseDriverFeatures(TypedDict):
    """Sybase ASE driver feature flags."""

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    on_connection_create: "NotRequired[Callable[[SybaseConnection], None]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[Literal["poll_queue"]]


class SybaseConnectionContext(SyncPoolConnectionContext):
    """Context manager for Sybase ASE connections."""

    __slots__ = ()


class _SybaseSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()


class SybaseConfig(SyncDatabaseConfig[SybaseConnection, SybaseConnectionPool, SybaseDriver]):
    """Configuration for Sybase ASE synchronous connections over pymssql (FreeTDS TDS 5.0)."""

    __slots__ = ("_user_connection_hook",)

    driver_type: "ClassVar[type[SybaseDriver]]" = SybaseDriver
    connection_type: "ClassVar[type[SybaseConnection]]" = cast("type[SybaseConnection]", SybaseConnection)
    migration_tracker_type: "ClassVar[type[SybaseSyncMigrationTracker]]" = SybaseSyncMigrationTracker
    supports_transactional_ddl: "ClassVar[bool]" = False
    supports_migration_schemas: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    supports_native_row_streaming: "ClassVar[bool]" = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
    _connection_context_class: "ClassVar[type[SybaseConnectionContext]]" = SybaseConnectionContext
    _session_factory_class: "ClassVar[type[_SybaseSessionConnectionHandler]]" = _SybaseSessionConnectionHandler
    _session_context_class: "ClassVar[type[SybaseSessionContext]]" = SybaseSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "SybasePoolParams | dict[str, Any] | None" = None,
        connection_instance: "SybaseConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "SybaseDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        connection_config = build_connection_config(normalize_connection_config(connection_config))

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[SybaseConnection], None] | None = features_dict.pop(
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

    def create_connection(self) -> "SybaseConnection":
        """Open a standalone Sybase ASE connection owned by the caller."""
        return self.provide_pool().new_connection()

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "SybaseConfig": SybaseConfig,
            "SybaseConnection": SybaseConnection,
            "SybaseConnectionContext": SybaseConnectionContext,
            "SybaseConnectionParams": SybaseConnectionParams,
            "SybaseConnectionPool": SybaseConnectionPool,
            "SybaseCursor": SybaseCursor,
            "SybaseDriver": SybaseDriver,
            "SybaseDriverFeatures": SybaseDriverFeatures,
            "SybaseExceptionHandler": SybaseExceptionHandler,
            "SybasePoolParams": SybasePoolParams,
            "SybaseRawCursor": SybaseRawCursor,
            "SybaseSessionContext": SybaseSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return runtime hints for Sybase ASE event channels."""
        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5)

    def _create_pool(self) -> "SybaseConnectionPool":
        config = dict(self.connection_config)
        pool_recycle = config.pop("pool_recycle_seconds", 86400)
        health_check = config.pop("health_check_interval", 30.0)
        return SybaseConnectionPool(
            config,
            recycle_seconds=pool_recycle,
            health_check_interval=health_check,
            on_connection_create=self._user_connection_hook,
        )

    def _close_pool(self) -> None:
        if self.connection_instance:
            self.connection_instance.close()
            self.connection_instance = None
