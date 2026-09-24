"""Asyncmy database configuration."""

import asyncio
import contextlib
import inspect
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, cast
from weakref import WeakSet

from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.asyncmy._typing import (
    AsyncmyConnection,
    AsyncmyCursor,
    AsyncmyDictCursor,
    AsyncmyModule,
    AsyncmyPool,
    AsyncmyRawCursor,
    AsyncmySessionContext,
)
from sqlspec.adapters.asyncmy.core import apply_driver_features, default_statement_config
from sqlspec.adapters.asyncmy.driver import AsyncmyDriver, AsyncmyExceptionHandler
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config, parse_mysql_dsn

if TYPE_CHECKING:
    import ssl
    from collections.abc import Awaitable, Callable, Mapping
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


__all__ = (
    "AsyncmyConfig",
    "AsyncmyConnectionParams",
    "AsyncmyDriverFeatures",
    "AsyncmyPoolParams",
    "AsyncmySSLParams",
    "build_connection_config",
)


_ASYNCMY_POOL_ONLY_KEYS = frozenset(("minsize", "maxsize", "pool_recycle"))
_ASYNCMY_POOL_KEYS = _ASYNCMY_POOL_ONLY_KEYS | {"echo"}
_ASYNCMY_SHARED_POOL_KEYS = frozenset(("echo",))
asyncmy: "AsyncmyModule" = cast("AsyncmyModule", AsyncmyModule)


def _connect_parameter_names() -> "frozenset[str]":
    try:
        return frozenset(inspect.signature(asyncmy.connect).parameters)
    except (TypeError, ValueError):
        return frozenset()


_ASYNCMY_CONNECT_PARAMETER_NAMES = _connect_parameter_names()


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

    dsn: NotRequired[str]
    url: NotRequired[str]
    connection_string: NotRequired[str]
    host: NotRequired[str]
    user: NotRequired[str]
    username: NotRequired[str]
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
    stmt_cache_size: NotRequired[int]
    use_unicode: NotRequired[bool]
    write_timeout: NotRequired[int | float]
    extra: NotRequired["dict[str, Any]"]


class AsyncmyPoolParams(AsyncmyConnectionParams):
    """Asyncmy pool parameters."""

    minsize: NotRequired[int]
    maxsize: NotRequired[int]
    echo: NotRequired[bool]
    pool_recycle: NotRequired[int]


def _normalize_connection_config(connection_config: "Mapping[str, Any] | None") -> "dict[str, Any]":
    """Normalize SQLSpec asyncmy config keys before storing them."""
    config = normalize_connection_config(connection_config)

    if "cursor_class" in config:
        cursor_class = config.pop("cursor_class")
        existing_cursor_cls = config.get("cursor_cls")
        if existing_cursor_cls is not None and existing_cursor_cls is not cursor_class:
            msg = "Asyncmy connection_config received conflicting 'cursor_cls' and legacy 'cursor_class' values."
            raise ImproperConfigurationError(msg)
        config["cursor_cls"] = cursor_class

    allow_local_infile = bool(config.pop("allow_local_infile", False))
    config["local_infile"] = bool(config.get("local_infile", False) or allow_local_infile)

    return config


def _split_pool_config(connection_config: "Mapping[str, Any]") -> "tuple[dict[str, Any], dict[str, Any]]":
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


def _pool_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    pool_kwargs, connection_kwargs = _split_pool_config(connection_config)
    return {**connection_kwargs, **pool_kwargs}


def build_connection_config(
    connection_config: "AsyncmyPoolParams | dict[str, Any] | Mapping[str, Any] | None",
) -> dict[str, Any]:
    """Normalize asyncmy connection configuration, parsing DSN and mapping aliases."""
    config = _normalize_connection_config(connection_config)
    dsn = config.pop("dsn", None) or config.pop("url", None) or config.pop("connection_string", None)
    user_alias = config.pop("username", None)
    if user_alias is not None and "user" not in config:
        config["user"] = user_alias
    if dsn is not None and isinstance(dsn, str):
        dsn_params = parse_mysql_dsn(dsn)
        for key, value in dsn_params.items():
            if key == "database" and "db" in config:
                continue
            config.setdefault(key, value)
    config.setdefault("host", "localhost")
    config.setdefault("port", 3306)
    config.setdefault("charset", "utf8mb4")
    config.setdefault("stmt_cache_size", 128)
    return config


class AsyncmyDriverFeatures(TypedDict):
    """Asyncmy driver feature flags.

    MySQL/MariaDB handle JSON natively, but custom serializers can be provided
    for specialized use cases.

    enable_local_infile_bulk_load: Use native LOCAL INFILE for eligible Arrow rows.
     Defaults to the connection's local_infile or allow_local_infile opt-in.
     Set False to force executemany on an opted-in connection.
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
     Only option: "poll_queue" (durable table-backed queue with lease-based retries and acknowledgements).
     MySQL/MariaDB do not have native pub/sub, so poll_queue is the only backend.
     Defaults to "poll_queue".
    """

    enable_local_infile_bulk_load: NotRequired[bool]
    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    on_connection_create: "NotRequired[Callable[[AsyncmyConnection], Awaitable[None]]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[Literal["poll_queue"]]


class _AsyncmySessionFactory(AsyncPoolSessionFactory):
    __slots__ = ("_contexts",)

    def __init__(self, config: "AsyncmyConfig") -> None:
        super().__init__(config)
        self._contexts: dict[int, Any] = {}

    async def acquire_connection(self) -> "AsyncmyConnection":
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.acquire()
        connection = cast("AsyncmyConnection", await ctx.__aenter__())
        self._contexts[id(connection)] = ctx
        try:
            ensure_conn = self._config._ensure_connection
            await ensure_conn(connection)
        except Exception:
            self._contexts.pop(id(connection), None)
            with contextlib.suppress(Exception):
                await ctx.__aexit__(None, None, None)
            raise
        return connection

    async def release_connection(self, _conn: "AsyncmyConnection", **kwargs: Any) -> None:
        ctx = self._contexts.pop(id(_conn), None)
        if ctx is not None:
            await ctx.__aexit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))


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
        self._connection = connection
        try:
            ensure_conn = self._config._ensure_connection
            await ensure_conn(connection)
        except Exception:
            self._connection = None
            self._ctx = None
            with contextlib.suppress(Exception):
                await ctx.__aexit__(None, None, None)
            raise
        return connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        self._connection = None
        if self._ctx:
            ctx = self._ctx
            self._ctx = None
            return cast("bool | None", await ctx.__aexit__(exc_type, exc_val, exc_tb))
        return None


@mypyc_attr(native_class=False)
class AsyncmyConfig(AsyncDatabaseConfig[AsyncmyConnection, "AsyncmyPool", AsyncmyDriver]):
    """Configuration for Asyncmy database connections."""

    driver_type: ClassVar[type[AsyncmyDriver]] = AsyncmyDriver
    connection_type: "ClassVar[type[Any]]" = cast("type[Any]", AsyncmyConnection)
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_row_streaming: ClassVar[bool] = True
    type_coercion_capabilities: ClassVar[TypeCoercionCapabilities] = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
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
        connection_config = build_connection_config(connection_config)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[AsyncmyConnection], Awaitable[None]] | None = features_dict.pop(
            "on_connection_create", None
        )
        self._initialized_connections: WeakSet[Any] = WeakSet()

        features_dict.setdefault("enable_local_infile_bulk_load", connection_config["local_infile"])
        if features_dict.get("enable_local_infile_bulk_load") and not connection_config.get("local_infile"):
            msg = "enable_local_infile_bulk_load requires local_infile=True or allow_local_infile=True in connection_config."
            raise ImproperConfigurationError(msg)

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
        return await asyncmy.create_pool(**_pool_config(self.connection_config))

    async def _ensure_connection(self, connection: "AsyncmyConnection") -> None:
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
            with contextlib.suppress(Exception):
                try:
                    await asyncio.wait_for(self.connection_instance.wait_closed(), timeout=5.0)
                except (TimeoutError, asyncio.TimeoutError):
                    if hasattr(self.connection_instance, "terminate"):
                        self.connection_instance.terminate()
            self.connection_instance = None

    async def create_connection(self) -> AsyncmyConnection:
        """Open a standalone connection owned by the caller.

        The connection carries the same connection settings and creation hook
        the pool applies, consumes no pool slot, and must be closed by the caller.

        Returns:
            An Asyncmy connection instance.
        """
        pool_kwargs, connection_kwargs = _split_pool_config(self.connection_config)
        connection_kwargs.update({key: value for key, value in pool_kwargs.items() if key in _ASYNCMY_SHARED_POOL_KEYS})
        connection = await asyncmy.connect(**connection_kwargs)
        await self._ensure_connection(connection)
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
