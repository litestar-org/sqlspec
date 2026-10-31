"""Asyncmy database configuration."""

import contextlib
import inspect
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, TypedDict, cast
from weakref import WeakSet

from mypy_extensions import mypyc_attr

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


class AsyncmySSLParams(TypedDict):
    """Asyncmy TLS parameters."""

    ca: NotRequired[str]
    """Path to the PEM CA bundle file used to verify server certificates."""
    capath: NotRequired[str]
    """Directory path containing hashed CA certificate files."""
    cert: NotRequired[str]
    """Path to the client TLS certificate file."""
    key: NotRequired[str]
    """Path to the client TLS private key file."""
    cipher: NotRequired[str]
    """OpenSSL cipher suite list string."""
    check_hostname: NotRequired[bool]
    """Verify that the server certificate matches the target hostname."""
    verify_mode: NotRequired[bool | int | str]
    """Certificate verification policy (``ssl.VerifyMode``, boolean, or mode name)."""


class AsyncmyConnectionParams(TypedDict):
    """Asyncmy connection parameters."""

    dsn: NotRequired[str]
    """MySQL/MariaDB connection URI (``mysql://user:pass@host:port/db``)."""
    url: NotRequired[str]
    """Connection URL alias parsed identically to ``dsn``."""
    connection_string: NotRequired[str]
    """Connection string alias parsed identically to ``dsn``."""
    host: NotRequired[str]
    """MySQL or MariaDB server hostname (defaults to ``'localhost'``)."""
    user: NotRequired[str]
    """Database account username."""
    username: NotRequired[str]
    """Alias normalized to ``user`` when ``user`` is omitted."""
    password: NotRequired[str]
    """Password for the database user."""
    database: NotRequired[str]
    """Default schema/database selected upon connecting."""
    db: NotRequired[str]
    """Alias for ``database`` accepted by ``asyncmy``."""
    port: NotRequired[int]
    """MySQL/MariaDB TCP port (defaults to ``3306``)."""
    unix_socket: NotRequired[str]
    """Filesystem path to a local MySQL Unix domain socket."""
    charset: NotRequired[str]
    """Connection character set (defaults to ``'utf8mb4'``)."""
    connect_timeout: NotRequired[int | float]
    """TCP handshake and authentication timeout in seconds."""
    read_default_file: NotRequired[str]
    """Path to a MySQL ``my.cnf`` option file."""
    read_default_group: NotRequired[str]
    """Option group name read from ``read_default_file``."""
    autocommit: NotRequired[bool]
    """Enable MySQL autocommit mode on new connections."""
    allow_local_infile: NotRequired[bool]
    """Permit client-side ``LOAD DATA LOCAL INFILE`` transfers."""
    local_infile: NotRequired[bool]
    """Alias enabling ``LOAD DATA LOCAL INFILE`` support."""
    ssl: NotRequired["AsyncmySSLParams | ssl.SSLContext | dict[str, Any]"]
    """TLS configuration mapping or ``ssl.SSLContext`` instance."""
    sql_mode: NotRequired[str]
    """Session ``sql_mode`` string applied when the connection opens."""
    init_command: NotRequired[str]
    """SQL statement executed immediately after authentication."""
    auth_plugin_map: NotRequired["dict[str | bytes, type[Any]]"]
    """Mapping from MySQL auth plugin names to custom handler classes."""
    binary_prefix: NotRequired[bool]
    """Emit ``_binary`` introducer prefixes when escaping ``bytes`` literals."""
    client_flag: NotRequired[int]
    """Bitmask of MySQL ``CLIENT_*`` protocol capability flags."""
    conv: NotRequired["dict[Any, Any]"]
    """Custom MySQL field-type conversion table."""
    cursor_class: NotRequired[type["AsyncmyRawCursor"] | type["AsyncmyDictCursor"]]
    """Cursor class alias normalized for ``asyncmy`` connections."""
    cursor_cls: NotRequired[type["AsyncmyRawCursor"] | type["AsyncmyDictCursor"]]
    """Default ``asyncmy`` cursor class used by raw connections."""
    max_allowed_packet: NotRequired[int]
    """Maximum client packet size in bytes (defaults to ``16 MiB`` in ``asyncmy``)."""
    program_name: NotRequired[str]
    """Client program name reported in ``performance_schema.session_connect_attrs``."""
    read_timeout: NotRequired[int | float]
    """Socket read timeout in seconds for query responses."""
    server_public_key: NotRequired[str | bytes]
    """RSA public key PEM used for ``caching_sha2_password`` / ``sha256_password`` exchange."""
    stmt_cache_size: NotRequired[int]
    """Maximum prepared statements cached per ``asyncmy`` connection."""
    use_unicode: NotRequired[bool]
    """Decode string columns into Python ``str`` values (defaults to ``True``)."""
    write_timeout: NotRequired[int | float]
    """Socket write timeout in seconds for outgoing packets."""
    extra: NotRequired["dict[str, Any]"]
    """Additional keyword arguments forwarded to ``asyncmy.connect`` / ``create_pool``."""


class AsyncmyPoolParams(AsyncmyConnectionParams):
    """Asyncmy pool parameters."""

    minsize: NotRequired[int]
    """Minimum number of open connections maintained in ``asyncmy.Pool``."""
    maxsize: NotRequired[int]
    """Maximum number of concurrent connections allowed in ``asyncmy.Pool``."""
    echo: NotRequired[bool]
    """Log executed SQL statements on ``asyncmy`` connections and pools."""
    pool_recycle: NotRequired[int]
    """Maximum connection age in seconds before ``asyncmy`` recycles pooled connections."""


def build_connection_config(
    connection_config: "AsyncmyPoolParams | dict[str, Any] | Mapping[str, Any] | None",
) -> dict[str, Any]:
    """Normalize asyncmy connection configuration, parsing DSN and mapping aliases."""
    config = _normalize_connection_config(connection_config)
    dsn = config.pop("dsn", None) or config.pop("url", None) or config.pop("connection_string", None)
    user_alias = config.pop("username", None)
    if user_alias is not None and "user" not in config:
        config["user"] = user_alias
    if "database" in config and "db" in config:
        config.pop("db", None)
    if dsn is not None and isinstance(dsn, str):
        dsn_params = parse_mysql_dsn(dsn)
        for key, value in dsn_params.items():
            if key == "database" and "db" in config:
                continue
            config.setdefault(key, value)
    config.setdefault("host", "localhost")
    config.setdefault("port", 3306)
    config.setdefault("charset", "utf8mb4")
    return config


class AsyncmyDriverFeatures(TypedDict):
    """Optional Asyncmy driver feature flags for JSON serialization, bulk loading, and events."""

    enable_local_infile_bulk_load: NotRequired[bool]
    """Use ``LOAD DATA LOCAL INFILE`` for ``load_from_arrow`` when local infile is enabled on the connection."""
    json_serializer: NotRequired["Callable[[Any], str]"]
    """Custom JSON serializer applied to bound JSON parameters (defaults to ``to_json``)."""
    json_deserializer: NotRequired["Callable[[str], Any]"]
    """Custom JSON deserializer applied to JSON result columns (defaults to ``from_json``)."""
    on_connection_create: "NotRequired[Callable[[AsyncmyConnection], Awaitable[None]]]"
    """Async callback invoked once per physical ``asyncmy`` connection acquired from the pool."""
    enable_events: NotRequired[bool]
    """Enable durable table-backed SQLSpec event queue support."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event channel backend (``poll_queue`` is the sole MySQL/MariaDB backend)."""


class _AsyncmySessionFactory(AsyncPoolSessionFactory):
    __slots__ = ("_contexts",)

    def __init__(self, config: "AsyncmyConfig") -> None:
        super().__init__(config)
        self._contexts: dict[int, Any] = {}

    async def acquire_connection(self) -> "AsyncmyConnection":
        pool = await self._config.provide_pool()
        ctx = pool.acquire()
        connection = cast("AsyncmyConnection", await ctx.__aenter__())
        self._contexts[id(connection)] = ctx
        try:
            ensure_conn = self._config._ensure_connection
            await ensure_conn(connection)
        except BaseException:
            self._contexts.pop(id(connection), None)
            with contextlib.suppress(Exception):
                await ctx.__aexit__(None, None, None)
            raise
        return connection

    async def release_connection(self, _conn: "AsyncmyConnection", **kwargs: Any) -> None:
        ctx = self._contexts.pop(id(_conn), None)
        if ctx is not None:
            if _conn is not None and bool(getattr(_conn, "server_status", 0) & 1):
                with contextlib.suppress(Exception):
                    await _conn.rollback()
            await ctx.__aexit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))


class AsyncmyConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for Asyncmy connections."""

    __slots__ = ("_ctx",)

    def __init__(self, config: "AsyncmyConfig") -> None:
        super().__init__(config)
        self._ctx: Any = None

    async def __aenter__(self) -> AsyncmyConnection:
        pool = await self._config.provide_pool()
        ctx = pool.acquire()
        self._ctx = ctx
        connection = cast("AsyncmyConnection", await ctx.__aenter__())
        self._connection = connection
        try:
            ensure_conn = self._config._ensure_connection
            await ensure_conn(connection)
        except BaseException:
            self._connection = None
            self._ctx = None
            with contextlib.suppress(Exception):
                await ctx.__aexit__(None, None, None)
            raise
        return connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        conn = self._connection
        self._connection = None
        if conn is not None and bool(getattr(conn, "server_status", 0) & 1):
            with contextlib.suppress(Exception):
                await conn.rollback()
        if self._ctx:
            ctx = self._ctx
            self._ctx = None
            return cast("bool | None", await ctx.__aexit__(exc_type, exc_val, exc_tb))
        return None


@mypyc_attr(native_class=False)
class AsyncmyConfig(AsyncDatabaseConfig[AsyncmyConnection, "AsyncmyPool", AsyncmyDriver]):
    """Configuration for Asyncmy database connections."""

    __slots__ = ("_initialized_connections", "_user_connection_hook")

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
        statement_config, features_dict = apply_driver_features(statement_config, driver_features)
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
            await self.connection_instance.wait_closed()
            self.connection_instance = None


_FALLBACK_ASYNCMY_CONNECT_PARAMS: Final[frozenset[str]] = frozenset({
    "host",
    "user",
    "password",
    "database",
    "db",
    "port",
    "unix_socket",
    "charset",
    "sql_mode",
    "read_default_file",
    "conv",
    "use_unicode",
    "client_flag",
    "cursor_cls",
    "init_command",
    "connect_timeout",
    "read_default_group",
    "autocommit",
    "local_infile",
    "max_allowed_packet",
    "auth_plugin_map",
    "read_timeout",
    "binary_prefix",
    "program_name",
    "server_public_key",
    "ssl",
    "stmt_cache_size",
    "echo",
})


def _connect_parameter_names() -> "frozenset[str]":
    try:
        names = frozenset(inspect.signature(asyncmy.connect).parameters)
    except (TypeError, ValueError):
        return _FALLBACK_ASYNCMY_CONNECT_PARAMS
    return names or _FALLBACK_ASYNCMY_CONNECT_PARAMS


_ASYNCMY_CONNECT_PARAMETER_NAMES = _connect_parameter_names()


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
