"""Asyncmy database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

import asyncmy
from asyncmy.cursors import Cursor, DictCursor  # pyright: ignore
from asyncmy.pool import Pool as AsyncmyPool  # pyright: ignore
from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.asyncmy._typing import AsyncmyConnection
from sqlspec.adapters.asyncmy.core import apply_driver_features, default_statement_config
from sqlspec.adapters.asyncmy.driver import AsyncmyCursor, AsyncmyDriver, AsyncmyExceptionHandler, AsyncmySessionContext
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config, reject_pool_aliases

if TYPE_CHECKING:
    from collections.abc import Callable

    from asyncmy.cursors import Cursor, DictCursor  # pyright: ignore
    from asyncmy.pool import Pool  # pyright: ignore

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


__all__ = ("AsyncmyConfig", "AsyncmyConnectionParams", "AsyncmyDriverFeatures", "AsyncmyPoolParams")


class AsyncmyConnectionParams(TypedDict):
    """Asyncmy connection parameters."""

    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
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
    extra: NotRequired["dict[str, Any]"]


class AsyncmyPoolParams(AsyncmyConnectionParams):
    """Asyncmy pool parameters."""

    minsize: NotRequired[int]
    maxsize: NotRequired[int]
    echo: NotRequired[bool]
    pool_recycle: NotRequired[int]


class AsyncmyDriverFeatures(TypedDict):
    """Asyncmy driver feature flags.

    MySQL/MariaDB handle JSON natively, but custom serializers can be provided
    for specialized use cases (e.g., orjson for performance, msgspec for type safety).

    json_serializer: Custom JSON serializer function.
        Defaults to sqlspec.utils.serializers.to_json.
        Use for performance (orjson) or custom encoding.
    json_deserializer: Custom JSON deserializer function.
        Defaults to sqlspec.utils.serializers.from_json.
        Use for performance (orjson) or custom decoding.
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
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class _AsyncmySessionFactory:
    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "AsyncmyConfig") -> None:
        self._config = config
        self._ctx: Any | None = None

    async def acquire_connection(self) -> "AsyncmyConnection":
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.acquire()
        self._ctx = ctx
        return cast("AsyncmyConnection", await ctx.__aenter__())

    async def release_connection(self, _conn: "AsyncmyConnection") -> None:
        if self._ctx is not None:
            await self._ctx.__aexit__(None, None, None)
            self._ctx = None


class AsyncmyConnectionContext:
    """Async context manager for Asyncmy connections."""

    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "AsyncmyConfig") -> None:
        self._config = config
        self._ctx: Any = None

    async def __aenter__(self) -> AsyncmyConnection:
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.acquire()
        self._ctx = ctx
        return cast("AsyncmyConnection", await ctx.__aenter__())

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._ctx:
            return cast("bool | None", await self._ctx.__aexit__(exc_type, exc_val, exc_tb))
        return None


@mypyc_attr(native_class=False)
class AsyncmyConfig(AsyncDatabaseConfig[AsyncmyConnection, "AsyncmyPool", AsyncmyDriver]):  # pyright: ignore
    """Configuration for Asyncmy database connections."""

    driver_type: ClassVar[type[AsyncmyDriver]] = AsyncmyDriver
    connection_type: "ClassVar[type[Any]]" = cast("type[Any]", AsyncmyConnection)
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True

    def __init__(
        self,
        *,
        connection_config: "AsyncmyPoolParams | dict[str, Any] | None" = None,
        connection_instance: "AsyncmyPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "AsyncmyDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Asyncmy configuration.

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
        reject_pool_aliases(kwargs)

        connection_config = normalize_connection_config(connection_config)

        connection_config.setdefault("host", "localhost")
        connection_config.setdefault("port", 3306)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    async def _create_pool(self) -> "AsyncmyPool":
        """Create the actual async connection pool.

        MySQL/MariaDB handle JSON types natively without requiring connection-level
        type handlers. JSON serialization is handled via type_coercion_map in the
        driver's statement_config (see driver.py).

        Future driver_features can be added here if needed (e.g., custom connection
        initialization, specialized type handling).
        """
        return cast("AsyncmyPool", await asyncmy.create_pool(**dict(self.connection_config)))

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.connection_instance:
            self.connection_instance.close()
            await self.connection_instance.wait_closed()
            self.connection_instance = None

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def create_connection(self) -> AsyncmyConnection:
        """Create a single async connection (not from pool).

        Returns:
            An Asyncmy connection instance.
        """
        pool = self.connection_instance
        if pool is None:
            pool = await self.create_pool()
            self.connection_instance = pool
        return cast("AsyncmyConnection", await pool.acquire())

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AsyncmyConnectionContext":
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            An Asyncmy connection context manager.
        """
        return AsyncmyConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "AsyncmySessionContext":
        """Provide an async driver session context manager.

        Args:
            *_args: Additional arguments.
            statement_config: Optional statement configuration override.
            **_kwargs: Additional keyword arguments.

        Returns:
            An Asyncmy driver session context manager.
        """
        factory = _AsyncmySessionFactory(self)
        return AsyncmySessionContext(
            acquire_connection=factory.acquire_connection,
            release_connection=factory.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "Pool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for Asyncmy types.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({
            "AsyncmyConnectionContext": AsyncmyConnectionContext,
            "AsyncmyConnection": AsyncmyConnection,
            "AsyncmyConnectionParams": AsyncmyConnectionParams,
            "AsyncmyCursor": AsyncmyCursor,
            "AsyncmyDriver": AsyncmyDriver,
            "AsyncmyDriverFeatures": AsyncmyDriverFeatures,
            "AsyncmyExceptionHandler": AsyncmyExceptionHandler,
            "AsyncmyPool": AsyncmyPool,
            "AsyncmyPoolParams": AsyncmyPoolParams,
            "AsyncmySessionContext": AsyncmySessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return queue polling defaults for Asyncmy adapters."""

        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5, select_for_update=True, skip_locked=True)
