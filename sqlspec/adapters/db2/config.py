"""IBM Db2 database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, cast

from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.db2._typing import (
    Db2AsyncConnection,
    Db2AsyncCursor,
    Db2AsyncRawCursor,
    Db2AsyncSessionContext,
    Db2RawCursor,
    Db2SyncConnection,
    Db2SyncCursor,
    Db2SyncSessionContext,
)
from sqlspec.adapters.db2.core import (
    apply_driver_features,
    build_connection_config,
    build_dsn_string,
    default_statement_config,
)
from sqlspec.adapters.db2.driver import Db2AsyncDriver, Db2AsyncExceptionHandler, Db2SyncDriver, Db2SyncExceptionHandler
from sqlspec.adapters.db2.migrations import Db2AsyncMigrationTracker, Db2SyncMigrationTracker
from sqlspec.adapters.db2.pool import Db2AsyncConnectionPool, Db2SyncConnectionPool
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import (
    AsyncPoolConnectionContext,
    AsyncPoolSessionFactory,
    SyncPoolConnectionContext,
    SyncPoolSessionFactory,
)
from sqlspec.extensions.events import EventRuntimeHints

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "Db2AsyncConfig",
    "Db2AsyncConnectionContext",
    "Db2AsyncPoolParams",
    "Db2ConnectionParams",
    "Db2DriverFeatures",
    "Db2PoolParams",
    "Db2SyncConfig",
    "Db2SyncConnectionContext",
    "build_connection_config",
)

_ASYNC_POOL_KEYS = ("max_size", "acquire_timeout")


class Db2ConnectionParams(TypedDict):
    """IBM Db2 connection parameters.

    Each modeled parameter renders under one CLI keyword:

    database: ``DATABASE``. Required, either directly or through ``dsn``.
    hostname: ``HOSTNAME``. Omit it to connect to a cataloged database alias.
    port: ``PORT``. Defaults to 50000 when ``hostname`` is set.
    protocol: ``PROTOCOL``. Defaults to ``TCPIP`` when ``hostname`` is set.
    user: ``UID``.
    password: ``PWD``.
    current_schema: ``CURRENTSCHEMA``.
    security: ``SECURITY``, for example ``"SSL"``.
    ssl_server_certificate: ``SSLSERVERCERTIFICATE``.
    authentication: ``AUTHENTICATION``.
    connect_timeout: ``CONNECTTIMEOUT`` in seconds.
    autocommit: Autocommit mode new connections start in. Never rendered into the DSN.
    dsn: ``KEY=VALUE;...`` connection string or ``db2://user:password@host:port/database?Key=Value``
     URL. Explicit parameters override values parsed from it.
    extra: Additional CLI keywords rendered verbatim after the modeled parameters.
    """

    database: NotRequired[str]
    hostname: NotRequired[str]
    port: NotRequired[int]
    protocol: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    current_schema: NotRequired[str]
    security: NotRequired[str]
    ssl_server_certificate: NotRequired[str]
    authentication: NotRequired[str]
    connect_timeout: NotRequired[int]
    autocommit: NotRequired[bool]
    dsn: NotRequired[str]
    extra: NotRequired["dict[str, str | int | bool]"]


class Db2PoolParams(Db2ConnectionParams):
    """IBM Db2 pool parameters.

    pool_recycle_seconds: Seconds after which a pooled connection is replaced. Defaults to 86400.
    health_check_interval: Idle seconds after which a pooled connection is pinged before reuse.
     Defaults to 30.0.
    """

    pool_recycle_seconds: NotRequired[int]
    health_check_interval: NotRequired[float]


class Db2AsyncPoolParams(Db2PoolParams):
    """IBM Db2 async pool parameters.

    max_size: Maximum number of connections checked out or being opened at once. Defaults to 10.
    acquire_timeout: Seconds to wait for a free connection before raising
     ``ConnectionTimeoutError``. Defaults to 30.0.
    """

    max_size: NotRequired[int]
    acquire_timeout: NotRequired[float]


class Db2DriverFeatures(TypedDict):
    """IBM Db2 driver feature flags.

    json_serializer: Custom JSON serializer function.
     Defaults to sqlspec.utils.serializers.to_json.
    json_deserializer: Custom JSON deserializer function.
     Defaults to sqlspec.utils.serializers.from_json.
    on_connection_create: Callback executed when a connection is created.
     Receives the raw Db2 connection (``ibm_db_dbi.Connection`` for ``Db2SyncConfig``,
     ``ibm_db_dbi.AsyncConnection`` for ``Db2AsyncConfig``) for low-level driver configuration.
     Runs after connection creation; ``Db2AsyncConfig`` awaits an awaitable result.
    enable_events: Enable database event channel support.
    events_backend: Event channel backend selection.
    enable_lowercase_column_names: Normalize implicit uppercase column names to lowercase.
     Defaults to True.
    """

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    on_connection_create: "NotRequired[Callable[[Any], Any]]"
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
    migration_tracker_type: "ClassVar[type[Db2SyncMigrationTracker]]" = Db2SyncMigrationTracker
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

    __slots__ = ("_user_connection_hook",)

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
        if connection_config is None:
            connection_config = {"database": "SAMPLE"}
        normalized_connection_config = build_connection_config(connection_config)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[Db2SyncConnection], None] | None = features_dict.pop(
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
        )

    def _provide_session_impl(
        self, *args: Any, statement_config: "StatementConfig | None" = None, transaction: bool = False, **kwargs: Any
    ) -> "Db2SyncSessionContext":
        """Build a session context that restores the connection's autocommit baseline on exit.

        Args:
            *args: Unused positional arguments.
            statement_config: Statement configuration override for the session.
            transaction: Begin a transaction before the session driver is yielded.
            **kwargs: Unused keyword arguments.

        Returns:
            Db2SyncSessionContext: The session context manager.
        """
        handler = self._session_factory_class(self)
        return Db2SyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or self._default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
            autocommit_baseline=bool(self.connection_config.get("autocommit", True)),
            begin_transaction=transaction,
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


class Db2AsyncConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for IBM Db2 connections."""

    __slots__ = ()


class _Db2AsyncSessionConnectionHandler(AsyncPoolSessionFactory):
    __slots__ = ()


@mypyc_attr(native_class=False)
class Db2AsyncConfig(AsyncDatabaseConfig[Db2AsyncConnection, Db2AsyncConnectionPool, Db2AsyncDriver]):
    """Configuration for IBM Db2 asynchronous connections over ``ibm_db_dbi.AsyncConnection``."""

    driver_type: "ClassVar[type[Db2AsyncDriver]]" = Db2AsyncDriver
    connection_type: "ClassVar[type[Db2AsyncConnection]]" = cast("type[Db2AsyncConnection]", Db2AsyncConnection)
    migration_tracker_type: "ClassVar[type[Db2AsyncMigrationTracker]]" = Db2AsyncMigrationTracker
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
    _connection_context_class: "ClassVar[type[Db2AsyncConnectionContext]]" = Db2AsyncConnectionContext
    _session_factory_class: "ClassVar[type[_Db2AsyncSessionConnectionHandler]]" = _Db2AsyncSessionConnectionHandler
    _session_context_class: "ClassVar[type[Db2AsyncSessionContext]]" = Db2AsyncSessionContext
    _default_statement_config = default_statement_config

    __slots__ = ("_user_connection_hook",)

    def __init__(
        self,
        *,
        connection_config: "Db2AsyncPoolParams | dict[str, Any] | None" = None,
        connection_instance: "Db2AsyncConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "Db2DriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Db2 async configuration.

        ``max_size`` and ``acquire_timeout`` are kept for the pool; every other key is validated
        and normalized like the sync configuration's connection parameters.
        """
        if connection_config is None:
            connection_config = {"database": "SAMPLE"}
        raw_connection_config = dict(connection_config)
        pool_options = {
            key: raw_connection_config.pop(key)
            for key in _ASYNC_POOL_KEYS
            if raw_connection_config.get(key) is not None
        }
        normalized_connection_config = build_connection_config(raw_connection_config)
        normalized_connection_config.update(pool_options)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[Db2AsyncConnection], Awaitable[None] | None] | None = features_dict.pop(
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

    async def _create_pool(self) -> "Db2AsyncConnectionPool":
        """Create a new bounded asyncio connection pool."""
        config = dict(self.connection_config)
        max_size = config.pop("max_size", 10)
        acquire_timeout = config.pop("acquire_timeout", 30.0)
        pool_recycle = config.pop("pool_recycle_seconds", 86400)
        health_check = config.pop("health_check_interval", 30.0)
        return Db2AsyncConnectionPool(
            config,
            max_size=max_size,
            acquire_timeout=acquire_timeout,
            recycle_seconds=pool_recycle,
            health_check_interval=health_check,
            on_connection_create=self._user_connection_hook,
        )

    def _provide_session_impl(
        self, *args: Any, statement_config: "StatementConfig | None" = None, transaction: bool = False, **kwargs: Any
    ) -> "Db2AsyncSessionContext":
        """Build a session context that restores the connection's autocommit baseline on exit.

        Args:
            *args: Unused positional arguments.
            statement_config: Statement configuration override for the session.
            transaction: Begin a transaction before the session driver is yielded.
            **kwargs: Unused keyword arguments.

        Returns:
            Db2AsyncSessionContext: The session context manager.
        """
        handler = self._session_factory_class(self)
        return Db2AsyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or self._default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
            autocommit_baseline=bool(self.connection_config.get("autocommit", True)),
            begin_transaction=transaction,
        )

    async def _close_pool(self) -> None:
        """Close the connection pool and release its idle connections."""
        if self.connection_instance:
            await self.connection_instance.close()
            self.connection_instance = None

    async def create_connection(self) -> "Db2AsyncConnection":
        """Open a standalone connection owned by the caller.

        Returns:
            Db2AsyncConnection: A newly opened ``ibm_db_dbi.AsyncConnection``.
        """
        pool = await self.provide_pool()
        return cast("Db2AsyncConnection", await pool.new_connection())

    def get_connection_string(self) -> str:
        """Generate a valid IBM Db2 CLI DSN connection string."""
        return build_dsn_string(self.connection_config)

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get namespace for dependency injection resolution."""
        namespace = super().get_signature_namespace()
        namespace.update({
            "Db2AsyncConfig": Db2AsyncConfig,
            "Db2AsyncConnection": Db2AsyncConnection,
            "Db2AsyncConnectionContext": Db2AsyncConnectionContext,
            "Db2AsyncConnectionPool": Db2AsyncConnectionPool,
            "Db2AsyncCursor": Db2AsyncCursor,
            "Db2AsyncDriver": Db2AsyncDriver,
            "Db2AsyncExceptionHandler": Db2AsyncExceptionHandler,
            "Db2AsyncPoolParams": Db2AsyncPoolParams,
            "Db2AsyncRawCursor": Db2AsyncRawCursor,
            "Db2AsyncSessionContext": Db2AsyncSessionContext,
            "Db2ConnectionParams": Db2ConnectionParams,
            "Db2DriverFeatures": Db2DriverFeatures,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return runtime hints for Db2 event channels."""
        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5)
