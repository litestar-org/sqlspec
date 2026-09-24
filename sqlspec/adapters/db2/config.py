"""IBM Db2 database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, cast

from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.db2._typing import Db2RawCursor, Db2SyncConnection, Db2SyncCursor, Db2SyncSessionContext
from sqlspec.adapters.db2.core import (
    apply_driver_features,
    build_connection_config,
    build_dsn_string,
    default_statement_config,
)
from sqlspec.adapters.db2.driver import Db2SyncDriver, Db2SyncExceptionHandler
from sqlspec.adapters.db2.pool import Db2SyncConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.migrations.tracker import SyncMigrationTracker
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "Db2ConnectionParams",
    "Db2DriverFeatures",
    "Db2PoolParams",
    "Db2SyncConfig",
    "Db2SyncConnectionContext",
    "build_connection_config",
)


class Db2ConnectionParams(TypedDict):
    """IBM Db2 connection parameters."""

    database: NotRequired[str]
    db: NotRequired[str]
    hostname: NotRequired[str]
    host: NotRequired[str]
    server: NotRequired[str]
    port: NotRequired[int | str]
    protocol: NotRequired[str]
    username: NotRequired[str]
    user: NotRequired[str]
    uid: NotRequired[str]
    password: NotRequired[str]
    pwd: NotRequired[str]
    dsn: NotRequired[str]
    url: NotRequired[str]
    connection_string: NotRequired[str]
    pool_recycle_seconds: NotRequired[int]
    health_check_interval: NotRequired[float]
    extra: NotRequired["dict[str, Any]"]


class Db2PoolParams(Db2ConnectionParams):
    """IBM Db2 pool parameters."""


class Db2DriverFeatures(TypedDict):
    """IBM Db2 driver feature flags.

    json_serializer: Custom JSON serializer function.
     Defaults to sqlspec.utils.serializers.to_json.
    json_deserializer: Custom JSON deserializer function.
     Defaults to sqlspec.utils.serializers.from_json.
    on_connection_create: Callback executed when a connection is created.
     Receives the raw Db2 connection for low-level driver configuration.
     Runs after connection creation.
    connection_factory: Optional factory callable for custom connection instantiation.
    enable_events: Enable database event channel support.
    events_backend: Event channel backend selection.
    enable_lowercase_column_names: Normalize implicit uppercase column names to lowercase.
     Defaults to True.
    """

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    on_connection_create: "NotRequired[Callable[[Db2SyncConnection], None]]"
    connection_factory: "NotRequired[Callable[[], Db2SyncConnection]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[Literal["poll_queue"]]
    enable_lowercase_column_names: NotRequired[bool]


class Db2SyncConnectionContext(SyncPoolConnectionContext):
    """Context manager for IBM Db2 connections."""

    __slots__ = ()


class _Db2SyncSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()


@mypyc_attr(native_class=False)
class Db2SyncConfig(SyncDatabaseConfig[Db2SyncConnection, Db2SyncConnectionPool, Db2SyncDriver]):
    """Configuration for IBM Db2 synchronous connections."""

    driver_type: "ClassVar[type[Db2SyncDriver]]" = Db2SyncDriver
    connection_type: "ClassVar[type[Db2SyncConnection]]" = cast("type[Db2SyncConnection]", Db2SyncConnection)
    migration_tracker_type: "ClassVar[type[SyncMigrationTracker]]" = SyncMigrationTracker
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_migration_schemas: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = False
    supports_native_arrow_import: "ClassVar[bool]" = False
    supports_native_parquet_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    supports_native_row_streaming: "ClassVar[bool]" = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
    _connection_context_class: "ClassVar[type[Db2SyncConnectionContext]]" = Db2SyncConnectionContext
    _session_factory_class: "ClassVar[type[_Db2SyncSessionConnectionHandler]]" = _Db2SyncSessionConnectionHandler
    _session_context_class: "ClassVar[type[Db2SyncSessionContext]]" = Db2SyncSessionContext
    _default_statement_config = default_statement_config

    __slots__ = ("_connection_factory", "_user_connection_hook")

    def __init__(
        self,
        *,
        connection_config: "Db2PoolParams | dict[str, Any] | None" = None,
        connection_instance: "Db2SyncConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "Db2DriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Db2 configuration."""
        raw_config = normalize_connection_config(connection_config)
        normalized_connection_config = build_connection_config(raw_config)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[Db2SyncConnection], None] | None = features_dict.pop(
            "on_connection_create", None
        )
        self._connection_factory: Callable[[], Db2SyncConnection] | None = features_dict.pop("connection_factory", None)

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

    def _create_pool(self) -> "Db2SyncConnectionPool":
        """Create a new thread-local connection pool."""
        config = dict(self.connection_config)
        pool_recycle = config.pop("pool_recycle_seconds", 86400)
        health_check = config.pop("health_check_interval", 30.0)
        return Db2SyncConnectionPool(
            config,
            recycle_seconds=pool_recycle,
            health_check_interval=health_check,
            on_connection_create=self._user_connection_hook,
            connection_factory=self._connection_factory,
        )

    def _close_pool(self) -> None:
        """Close connection pool and release resources."""
        if self.connection_instance:
            self.connection_instance.close()
            self.connection_instance = None

    def create_connection(self) -> "Db2SyncConnection":
        """Open a standalone connection owned by the caller.

        Returns:
            Db2SyncConnection: A newly opened physical connection.
        """
        return self.provide_pool().new_connection()

    def get_connection_string(self) -> str:
        """Generate a valid IBM Db2 CLI DSN connection string."""
        return build_dsn_string(self.connection_config)

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get namespace for dependency injection resolution."""
        namespace = super().get_signature_namespace()
        namespace.update({
            "Db2SyncConfig": Db2SyncConfig,
            "Db2SyncConnection": Db2SyncConnection,
            "Db2SyncConnectionContext": Db2SyncConnectionContext,
            "Db2ConnectionParams": Db2ConnectionParams,
            "Db2SyncConnectionPool": Db2SyncConnectionPool,
            "Db2SyncCursor": Db2SyncCursor,
            "Db2SyncDriver": Db2SyncDriver,
            "Db2DriverFeatures": Db2DriverFeatures,
            "Db2SyncExceptionHandler": Db2SyncExceptionHandler,
            "Db2PoolParams": Db2PoolParams,
            "Db2RawCursor": Db2RawCursor,
            "Db2SyncSessionContext": Db2SyncSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return runtime hints for Db2 event channels."""
        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5)
