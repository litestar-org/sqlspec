"""pymssql database configuration."""

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.pymssql._typing import PymssqlConnection, PymssqlCursor, PymssqlRawCursor, PymssqlSessionContext
from sqlspec.adapters.pymssql.core import apply_driver_features, default_statement_config
from sqlspec.adapters.pymssql.driver import PymssqlDriver, PymssqlExceptionHandler
from sqlspec.adapters.pymssql.migrations import PymssqlSyncMigrationTracker
from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.driver._sync import SyncPoolConnectionContext, SyncPoolSessionFactory
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
    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    port: NotRequired[int | str]
    timeout: NotRequired[PymssqlTimeout]
    login_timeout: NotRequired[PymssqlTimeout]
    charset: NotRequired[str]
    as_dict: NotRequired[bool]
    appname: NotRequired[str]
    conn_properties: NotRequired[str]
    autocommit: NotRequired[bool]
    tds_version: NotRequired[str]
    use_datetime2: NotRequired[bool]
    arraysize: NotRequired[int]
    conv: NotRequired[Mapping[int | type[Any], Callable[..., Any]]]
    read_only: NotRequired[bool]
    pool_recycle_seconds: NotRequired[int]
    health_check_interval: NotRequired[float]
    extra: NotRequired["dict[str, Any]"]


class PymssqlPoolParams(PymssqlConnectionParams):
    """pymssql pool parameters."""


class PymssqlDriverFeatures(TypedDict):
    """pymssql driver feature flags.

    json_serializer: Custom JSON serializer function.
     Defaults to sqlspec.utils.serializers.to_json.
    json_deserializer: Custom JSON deserializer function.
     Defaults to sqlspec.utils.serializers.from_json.
    on_connection_create: Callback executed when a connection is created.
     Receives the raw pymssql connection for low-level driver configuration.
     Runs after connection creation.
    enable_events: Enable database event channel support.
    events_backend: Event channel backend selection.
    """

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    on_connection_create: "NotRequired[Callable[[PymssqlConnection], None]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class PymssqlConnectionContext(SyncPoolConnectionContext):
    """Context manager for pymssql connections."""

    __slots__ = ()


class _PymssqlSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()


class PymssqlConfig(SyncDatabaseConfig[PymssqlConnection, PymssqlConnectionPool, PymssqlDriver]):
    """Configuration for pymssql synchronous connections."""

    driver_type: "ClassVar[type[PymssqlDriver]]" = PymssqlDriver
    connection_type: "ClassVar[type[PymssqlConnection]]" = cast("type[PymssqlConnection]", PymssqlConnection)
    migration_tracker_type: "ClassVar[type[PymssqlSyncMigrationTracker]]" = PymssqlSyncMigrationTracker
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = False
    supports_native_arrow_import: "ClassVar[bool]" = False
    supports_native_parquet_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    supports_native_row_streaming: "ClassVar[bool]" = True
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
        connection_config = normalize_connection_config(connection_config)
        connection_config.setdefault("server", connection_config.pop("host", "localhost"))
        connection_config.setdefault("port", 1433)

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

    def create_connection(self) -> "PymssqlConnection":
        pool = self.provide_pool()
        return pool.acquire()

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
        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5, select_for_update=True, skip_locked=True)
