"""Asyncmy database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

import asyncmy
from asyncmy.cursors import Cursor, DictCursor  # pyright: ignore
from asyncmy.pool import Pool as AsyncmyPool  # pyright: ignore
from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.asyncmy._typing import AsyncmyConnection
from sqlspec.adapters.asyncmy.driver import (
    AsyncmyCursor,
    AsyncmyDriver,
    AsyncmyExceptionHandler,
    AsyncmySessionContext,
    asyncmy_statement_config,
    build_asyncmy_statement_config,
)
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.extensions.events._hints import EventRuntimeHints
from sqlspec.utils.config_normalization import apply_pool_deprecations, normalize_connection_config
from sqlspec.utils.serializers import from_json, to_json

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
    extra: "NotRequired[dict[str", Any]]


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


class AsyncmyConnectionContext:
    """Async context manager for Asyncmy connections."""

    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "AsyncmyConfig") -> None:
        self._config = config
        self._ctx: Any = None

    async def __aenter__(self) -> AsyncmyConnection:  # pyright: ignore
        if self._config.connection_instance is None:
            self._config.connection_instance = await self._config.create_pool()
        # asyncmy pool.acquire() returns a context manager that is also awaitable?
        # Based on existing code: async with ...acquire() as connection:
        self._ctx = self._config.connection_instance.acquire()  # pyright: ignore
        return await self._ctx.__aenter__()

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._ctx:
            return await self._ctx.__aexit__(exc_type, exc_val, exc_tb)  # type: ignore[no-any-return]
        return None


@mypyc_attr(native_class=False)
class AsyncmyConfig(AsyncDatabaseConfig[AsyncmyConnection, "AsyncmyPool", AsyncmyDriver]):  # pyright: ignore
    """Configuration for Asyncmy database connections."""

    driver_type: ClassVar[type[AsyncmyDriver]] = AsyncmyDriver
    connection_type: "ClassVar[type[AsyncmyConnection]]" = AsyncmyConnection  # pyright: ignore
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
        migration_config: "dict[str", Any] | None = None,
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
            **kwargs: Additional keyword arguments (handles deprecated pool_config/pool_instance)
        """
        connection_config, connection_instance = apply_pool_deprecations(
            kwargs=kwargs, connection_config=connection_config, connection_instance=connection_instance
        )

        processed_connection_config = normalize_connection_config(connection_config)

        processed_connection_config.setdefault("host", "localhost")
        processed_connection_config.setdefault("port", 3306)

        processed_driver_features: "dict[str", Any] = dict(driver_features) if driver_features else {}
        serializer = processed_driver_features.setdefault("json_serializer", to_json)
        deserializer = processed_driver_features.setdefault("json_deserializer", from_json)

        base_statement_config = statement_config or build_asyncmy_statement_config(
            json_serializer=serializer, json_deserializer=deserializer
        )

        super().__init__(
            connection_config=processed_connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=base_statement_config,
            driver_features=processed_driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    async def _create_pool(self) -> "AsyncmyPool":  # pyright: ignore
        """Create the actual async connection pool.

        MySQL/MariaDB handle JSON types natively without requiring connection-level
        type handlers. JSON serialization is handled via type_coercion_map in the
        driver's statement_config (see driver.py).

        Future driver_features can be added here if needed (e.g., custom connection
        initialization, specialized type handling).
        """
        return await asyncmy.create_pool(**dict(self.connection_config))  # pyright: ignore

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.connection_instance:
            self.connection_instance.close()

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def create_connection(self) -> AsyncmyConnection:  # pyright: ignore
        """Create a single async connection (not from pool).

        Returns:
            An Asyncmy connection instance.
        """
        if self.connection_instance is None:
            self.connection_instance = await self.create_pool()
        return await self.connection_instance.acquire()  # pyright: ignore

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
        acquire_ctx_holder: "dict[str", Any] = {}

        async def acquire_connection() -> AsyncmyConnection:
            pool = self.connection_instance
            if pool is None:
                pool = await self.create_pool()
                self.connection_instance = pool
            ctx = pool.acquire()
            acquire_ctx_holder["ctx"] = ctx
            return await ctx.__aenter__()

        async def release_connection(_conn: AsyncmyConnection) -> None:
            if "ctx" in acquire_ctx_holder:
                await acquire_ctx_holder["ctx"].__aexit__(None, None, None)
                acquire_ctx_holder.clear()

        return AsyncmySessionContext(
            acquire_connection=acquire_connection,
            release_connection=release_connection,
            statement_config=statement_config or self.statement_config or asyncmy_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "Pool":  # pyright: ignore
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
