"""aiomysql database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast
from weakref import WeakSet

import aiomysql  # pyright: ignore
from aiomysql import Pool as AiomysqlPool  # pyright: ignore
from aiomysql.cursors import Cursor, DictCursor  # pyright: ignore
from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.aiomysql._typing import AiomysqlConnection, AiomysqlCursor, AiomysqlSessionContext
from sqlspec.adapters.aiomysql.core import apply_driver_features, default_statement_config
from sqlspec.adapters.aiomysql.driver import AiomysqlDriver, AiomysqlExceptionHandler
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.driver._async import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from aiomysql import Pool  # pyright: ignore
    from aiomysql.cursors import Cursor, DictCursor  # pyright: ignore

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


__all__ = ("AiomysqlConfig", "AiomysqlConnectionParams", "AiomysqlDriverFeatures", "AiomysqlPoolParams")


class AiomysqlConnectionParams(TypedDict):
    """aiomysql connection parameters."""

    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    db: NotRequired[str]
    port: NotRequired[int]
    unix_socket: NotRequired[str]
    charset: NotRequired[str]
    connect_timeout: NotRequired[int]
    read_default_file: NotRequired[str]
    read_default_group: NotRequired[str]
    autocommit: NotRequired[bool]
    local_infile: NotRequired[bool]
    ssl: NotRequired[Any]
    sql_mode: NotRequired[str]
    init_command: NotRequired[str]
    cursor_class: NotRequired[type["Cursor"] | type["DictCursor"]]


class AiomysqlPoolParams(AiomysqlConnectionParams):
    """aiomysql pool parameters."""

    minsize: NotRequired[int]
    maxsize: NotRequired[int]
    echo: NotRequired[bool]
    pool_recycle: NotRequired[int]


class AiomysqlDriverFeatures(TypedDict):
    """aiomysql driver feature flags.

    MySQL/MariaDB handle JSON natively, but custom serializers can be provided
    for specialized use cases (e.g., orjson for performance, msgspec for type safety).

    json_serializer: Custom JSON serializer function.
        Defaults to sqlspec.utils.serializers.to_json.
        Use for performance (orjson) or custom encoding.
    json_deserializer: Custom JSON deserializer function.
        Defaults to sqlspec.utils.serializers.from_json.
        Use for performance (orjson) or custom decoding.
    on_connection_create: Async callback executed when a connection is acquired from pool.
        Receives the raw aiomysql connection for low-level driver configuration.
        Called exactly once per physical connection using WeakSet tracking.
    enable_events: Enable database event channel support.
        Defaults to True when extension_config["events"] is configured.
        Provides pub/sub capabilities via table-backed queue (MySQL/MariaDB have no native pub/sub).
        Requires extension_config["events"] for migration setup.
    events_backend: Event channel backend selection.
        Only option: "table_queue" (durable table-backed queue with retries and exactly-once delivery).
        MySQL/MariaDB do not have native pub/sub, so table_queue is the only backend.
        Defaults to "table_queue".
    """

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    on_connection_create: "NotRequired[Callable[[AiomysqlConnection], Awaitable[None]]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class _AiomysqlSessionFactory(AsyncPoolSessionFactory):
    __slots__ = ("_ctx",)

    def __init__(self, config: "AiomysqlConfig") -> None:
        super().__init__(config)
        self._ctx: Any | None = None

    async def acquire_connection(self) -> "AiomysqlConnection":
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.acquire()
        self._ctx = ctx
        connection = cast("AiomysqlConnection", await ctx.__aenter__())
        await self._config._ensure_connection_initialized(connection)  # pyright: ignore[reportPrivateUsage]
        return connection

    async def release_connection(self, _conn: "AiomysqlConnection", **kwargs: Any) -> None:
        if self._ctx is not None:
            await self._ctx.__aexit__(None, None, None)
            self._ctx = None


class AiomysqlConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for aiomysql connections."""

    __slots__ = ("_ctx",)

    def __init__(self, config: "AiomysqlConfig") -> None:
        super().__init__(config)
        self._ctx: Any = None

    async def __aenter__(self) -> AiomysqlConnection:
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.acquire()
        self._ctx = ctx
        connection = cast("AiomysqlConnection", await ctx.__aenter__())
        await self._config._ensure_connection_initialized(connection)  # pyright: ignore[reportPrivateUsage]
        return connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._ctx:
            return cast("bool | None", await self._ctx.__aexit__(exc_type, exc_val, exc_tb))
        return None


@mypyc_attr(native_class=False)
class AiomysqlConfig(AsyncDatabaseConfig[AiomysqlConnection, "aiomysql.Pool", AiomysqlDriver]):  # pyright: ignore
    """Configuration for aiomysql database connections.

    Example::

        config = AiomysqlConfig(
            connection_config=AiomysqlPoolParams(
                host="localhost", user="root", db="mydb"
            )
        )
    """

    driver_type: ClassVar[type[AiomysqlDriver]] = AiomysqlDriver
    connection_type: "ClassVar[type[Any]]" = cast("type[Any]", AiomysqlConnection)
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    _connection_context_class: "ClassVar[type[AiomysqlConnectionContext]]" = AiomysqlConnectionContext
    _session_factory_class: "ClassVar[type[_AiomysqlSessionFactory]]" = _AiomysqlSessionFactory
    _session_context_class: "ClassVar[type[AiomysqlSessionContext]]" = AiomysqlSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "AiomysqlPoolParams | dict[str, Any] | None" = None,
        connection_instance: "aiomysql.Pool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "AiomysqlDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize aiomysql configuration.

        Args:
            connection_config: Connection and pool configuration parameters
            connection_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Statement configuration override
            driver_features: Driver feature configuration (TypedDict or dict)
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments
        """
        connection_config = normalize_connection_config(connection_config)

        connection_config.setdefault("host", "localhost")
        connection_config.setdefault("port", 3306)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        # Extract user connection hook before storing driver_features
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[AiomysqlConnection], Awaitable[None]] | None = features_dict.pop(
            "on_connection_create", None
        )
        # Track initialized connections to ensure callback runs exactly once per physical connection
        self._initialized_connections: WeakSet[Any] = WeakSet()

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

    async def _create_pool(self) -> "aiomysql.Pool":
        """Create the actual async connection pool.

        MySQL/MariaDB handle JSON types natively without requiring connection-level
        type handlers. JSON serialization is handled via type_coercion_map in the
        driver's statement_config (see driver.py).
        """
        return cast("aiomysql.Pool", await aiomysql.create_pool(**dict(self.connection_config)))

    async def _ensure_connection_initialized(self, connection: "AiomysqlConnection") -> None:
        """Ensure connection callback has been called exactly once for this connection.

        Uses WeakSet tracking to ensure the callback runs once per physical connection.
        """
        if self._user_connection_hook is None:
            return
        if connection not in self._initialized_connections:
            await self._user_connection_hook(connection)
            self._initialized_connections.add(connection)

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.connection_instance:
            self.connection_instance.close()
            await self.connection_instance.wait_closed()
            self.connection_instance = None

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def create_connection(self) -> AiomysqlConnection:
        """Create a single async connection (not from pool).

        Returns:
            An aiomysql connection instance.
        """
        pool = self.connection_instance
        if pool is None:
            pool = await self.create_pool()
            self.connection_instance = pool
        connection = cast("AiomysqlConnection", await pool.acquire())
        await self._ensure_connection_initialized(connection)
        return connection

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "Pool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for aiomysql types.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({
            "AiomysqlConnectionContext": AiomysqlConnectionContext,
            "AiomysqlConnection": AiomysqlConnection,
            "AiomysqlConnectionParams": AiomysqlConnectionParams,
            "AiomysqlCursor": AiomysqlCursor,
            "AiomysqlDriver": AiomysqlDriver,
            "AiomysqlDriverFeatures": AiomysqlDriverFeatures,
            "AiomysqlExceptionHandler": AiomysqlExceptionHandler,
            "AiomysqlPool": AiomysqlPool,
            "AiomysqlPoolParams": AiomysqlPoolParams,
            "AiomysqlSessionContext": AiomysqlSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return queue polling defaults for aiomysql adapters."""

        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5, select_for_update=True, skip_locked=True)
