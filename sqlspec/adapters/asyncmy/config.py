"""Asyncmy database configuration."""

import inspect
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast
from weakref import WeakSet

import asyncmy
from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.asyncmy._typing import (
    AsyncmyConnection,
    AsyncmyCursor,
    AsyncmyDictCursor,
    AsyncmyPool,
    AsyncmyRawCursor,
    AsyncmySessionContext,
)
from sqlspec.adapters.asyncmy.core import apply_driver_features, default_statement_config
from sqlspec.adapters.asyncmy.driver import AsyncmyDriver, AsyncmyExceptionHandler
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.driver._async import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    import ssl
    from collections.abc import Awaitable, Callable, Mapping
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


__all__ = ("AsyncmyConfig", "AsyncmyConnectionParams", "AsyncmyDriverFeatures", "AsyncmyPoolParams", "AsyncmySSLParams")


_ASYNCMY_POOL_ONLY_KEYS = frozenset(("minsize", "maxsize", "pool_recycle"))
_ASYNCMY_POOL_KEYS = _ASYNCMY_POOL_ONLY_KEYS | {"echo"}
_ASYNCMY_LOCAL_INFILE_GATE = "allow_local_infile"


def _get_asyncmy_connect_parameter_names() -> "frozenset[str]":
    try:
        return frozenset(inspect.signature(asyncmy.connect).parameters)
    except (TypeError, ValueError):
        return frozenset()


_ASYNCMY_CONNECT_PARAMETER_NAMES = _get_asyncmy_connect_parameter_names()


class AsyncmySSLParams(TypedDict):
    """Asyncmy TLS parameters."""

    ca: NotRequired[str]
    capath: NotRequired[str]
    cert: NotRequired[str]
    key: NotRequired[str]
    cipher: NotRequired[str]
    check_hostname: NotRequired[bool]
    verify_mode: NotRequired[bool | int | str]


class AsyncmyConnectionParams(TypedDict):
    """Asyncmy connection parameters."""

    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    db: NotRequired[str]
    port: NotRequired[int]
    unix_socket: NotRequired[str]
    charset: NotRequired[str]
    connect_timeout: NotRequired[int | float]
    read_default_file: NotRequired[str]
    read_default_group: NotRequired[str]
    autocommit: NotRequired[bool]
    allow_local_infile: NotRequired[bool]
    local_infile: NotRequired[bool]
    ssl: NotRequired["AsyncmySSLParams | ssl.SSLContext | dict[str, Any]"]
    sql_mode: NotRequired[str]
    init_command: NotRequired[str]
    auth_plugin_map: NotRequired["dict[str | bytes, type[Any]]"]
    binary_prefix: NotRequired[bool]
    client_flag: NotRequired[int]
    conv: NotRequired["dict[Any, Any]"]
    cursor_class: NotRequired[type["AsyncmyRawCursor"] | type["AsyncmyDictCursor"]]
    cursor_cls: NotRequired[type["AsyncmyRawCursor"] | type["AsyncmyDictCursor"]]
    max_allowed_packet: NotRequired[int]
    program_name: NotRequired[str]
    read_timeout: NotRequired[int | float]
    server_public_key: NotRequired[str | bytes]
    use_unicode: NotRequired[bool]
    write_timeout: NotRequired[int | float]
    extra: NotRequired["dict[str, Any]"]


class AsyncmyPoolParams(AsyncmyConnectionParams):
    """Asyncmy pool parameters."""

    minsize: NotRequired[int]
    maxsize: NotRequired[int]
    echo: NotRequired[bool]
    pool_recycle: NotRequired[int]


def _normalize_asyncmy_connection_config(connection_config: "Mapping[str, Any] | None") -> "dict[str, Any]":
    """Normalize SQLSpec asyncmy config keys before storing them."""
    config = normalize_connection_config(connection_config)

    if "cursor_class" in config:
        cursor_class = config.pop("cursor_class")
        existing_cursor_cls = config.get("cursor_cls")
        if existing_cursor_cls is not None and existing_cursor_cls is not cursor_class:
            msg = "Asyncmy connection_config received conflicting 'cursor_cls' and legacy 'cursor_class' values."
            raise ImproperConfigurationError(msg)
        config["cursor_cls"] = cursor_class

    allow_local_infile = bool(config.pop(_ASYNCMY_LOCAL_INFILE_GATE, False))
    local_infile = bool(config.get("local_infile", False))
    if local_infile and not allow_local_infile:
        msg = "Asyncmy local_infile=True requires allow_local_infile=True because LOAD DATA LOCAL INFILE can read client files."
        raise ImproperConfigurationError(msg)
    config["local_infile"] = bool(local_infile and allow_local_infile)

    return config


def _split_asyncmy_pool_config(connection_config: "Mapping[str, Any]") -> "tuple[dict[str, Any], dict[str, Any]]":
    """Split pool constructor settings from connection settings."""
    pool_kwargs: dict[str, Any] = {}
    connection_kwargs: dict[str, Any] = {}

    for key, value in connection_config.items():
        if value is None:
            continue
        if key in _ASYNCMY_POOL_KEYS:
            pool_kwargs[key] = value
            continue
        if key == "write_timeout" and key not in _ASYNCMY_CONNECT_PARAMETER_NAMES:
            continue
        connection_kwargs[key] = value

    return pool_kwargs, connection_kwargs


def _build_asyncmy_pool_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    pool_kwargs, connection_kwargs = _split_asyncmy_pool_config(connection_config)
    return {**connection_kwargs, **pool_kwargs}


class AsyncmyDriverFeatures(TypedDict):
    """Asyncmy driver feature flags.

    MySQL/MariaDB handle JSON natively, but custom serializers can be provided
    for specialized use cases.

    json_serializer: Custom JSON serializer function.
     Defaults to sqlspec.utils.serializers.to_json.
     Use for performance (orjson) or custom encoding.
    json_deserializer: Custom JSON deserializer function.
     Defaults to sqlspec.utils.serializers.from_json.
     Use for performance (orjson) or custom decoding.
    on_connection_create: Async callback executed when a connection is acquired from pool.
     Receives the raw asyncmy connection for low-level driver configuration.
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
    on_connection_create: "NotRequired[Callable[[AsyncmyConnection], Awaitable[None]]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class _AsyncmySessionFactory(AsyncPoolSessionFactory):
    __slots__ = ("_ctx",)

    def __init__(self, config: "AsyncmyConfig") -> None:
        super().__init__(config)
        self._ctx: Any | None = None

    async def acquire_connection(self) -> "AsyncmyConnection":
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.acquire()
        self._ctx = ctx
        connection = cast("AsyncmyConnection", await ctx.__aenter__())
        await self._config._ensure_connection_initialized(connection)  # pyright: ignore[reportPrivateUsage]
        return connection

    async def release_connection(self, _conn: "AsyncmyConnection", **kwargs: Any) -> None:
        if self._ctx is not None:
            await self._ctx.__aexit__(None, None, None)
            self._ctx = None


class AsyncmyConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for Asyncmy connections."""

    __slots__ = ("_ctx",)

    def __init__(self, config: "AsyncmyConfig") -> None:
        super().__init__(config)
        self._ctx: Any = None

    async def __aenter__(self) -> AsyncmyConnection:
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.acquire()
        self._ctx = ctx
        connection = cast("AsyncmyConnection", await ctx.__aenter__())
        await self._config._ensure_connection_initialized(connection)  # pyright: ignore[reportPrivateUsage]
        return connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
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
    supports_native_row_streaming: ClassVar[bool] = True
    _connection_context_class: "ClassVar[type[AsyncmyConnectionContext]]" = AsyncmyConnectionContext
    _session_factory_class: "ClassVar[type[_AsyncmySessionFactory]]" = _AsyncmySessionFactory
    _session_context_class: "ClassVar[type[AsyncmySessionContext]]" = AsyncmySessionContext
    _default_statement_config = default_statement_config

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
            extension_config: Extension-specific configuration
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments
        """
        connection_config = _normalize_asyncmy_connection_config(connection_config)

        connection_config.setdefault("host", "localhost")
        connection_config.setdefault("port", 3306)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        # Extract user connection hook before storing driver_features
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[AsyncmyConnection], Awaitable[None]] | None = features_dict.pop(
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

    async def _create_pool(self) -> "AsyncmyPool":
        """Create the actual async connection pool.

        MySQL/MariaDB handle JSON types natively without requiring connection-level
        type handlers. JSON serialization is handled via type_coercion_map in the
        driver's statement_config (see driver.py).

        Future driver_features can be added here if needed.
        """
        return cast("AsyncmyPool", await asyncmy.create_pool(**_build_asyncmy_pool_config(self.connection_config)))

    async def _ensure_connection_initialized(self, connection: "AsyncmyConnection") -> None:
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

    async def create_connection(self) -> AsyncmyConnection:
        """Create a single async connection (not from pool).

        Returns:
            An Asyncmy connection instance.
        """
        pool = self.connection_instance
        if pool is None:
            pool = await self.create_pool()
            self.connection_instance = pool
        connection = cast("AsyncmyConnection", await pool.acquire())
        await self._ensure_connection_initialized(connection)
        return connection

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "AsyncmyPool":
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
            "AsyncmyDictCursor": AsyncmyDictCursor,
            "AsyncmyDriver": AsyncmyDriver,
            "AsyncmyDriverFeatures": AsyncmyDriverFeatures,
            "AsyncmyExceptionHandler": AsyncmyExceptionHandler,
            "AsyncmyPool": AsyncmyPool,
            "AsyncmyPoolParams": AsyncmyPoolParams,
            "AsyncmyRawCursor": AsyncmyRawCursor,
            "AsyncmySessionContext": AsyncmySessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return queue polling defaults for Asyncmy adapters."""

        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5, select_for_update=True, skip_locked=True)
