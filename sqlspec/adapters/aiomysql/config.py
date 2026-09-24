"""aiomysql database configuration."""

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, cast
from weakref import WeakSet

from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.aiomysql._typing import (
    AiomysqlConnection,
    AiomysqlCursor,
    AiomysqlDictCursor,
    AiomysqlModule,
    AiomysqlPool,
    AiomysqlRawCursor,
    AiomysqlSessionContext,
)
from sqlspec.adapters.aiomysql.core import apply_driver_features, default_statement_config
from sqlspec.adapters.aiomysql.driver import AiomysqlDriver, AiomysqlExceptionHandler
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config, parse_mysql_dsn

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from ssl import SSLContext
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


__all__ = (
    "AiomysqlConfig",
    "AiomysqlConnectionParams",
    "AiomysqlDriverFeatures",
    "AiomysqlPoolParams",
    "build_connection_config",
)

_POOL_ONLY_CONFIG_KEYS = frozenset({"maxsize", "minsize", "pool_recycle"})
aiomysql: "AiomysqlModule" = cast("AiomysqlModule", AiomysqlModule)


class AiomysqlConnectionParams(TypedDict):
    """aiomysql connection parameters.

    PyMySQL-only flat TLS and read/write timeout kwargs are intentionally excluded
    until aiomysql accepts them at runtime.
    """

    dsn: NotRequired[str]
    url: NotRequired[str]
    connection_string: NotRequired[str]
    host: NotRequired[str]
    user: NotRequired[str]
    username: NotRequired[str]
    password: NotRequired[str]
    passwd: NotRequired[str]
    db: NotRequired[str]
    database: NotRequired[str]
    port: NotRequired[int]
    unix_socket: NotRequired[str]
    charset: NotRequired[str]
    connect_timeout: NotRequired[int | float | None]
    read_default_file: NotRequired[str]
    read_default_group: NotRequired[str]
    autocommit: NotRequired[bool | None]
    allow_local_infile: NotRequired[bool]
    echo: NotRequired[bool]
    local_infile: NotRequired[bool]
    ssl: NotRequired["SSLContext"]
    sql_mode: NotRequired[str]
    init_command: NotRequired[str]
    conv: NotRequired["dict[int, Callable[[bytes], Any]] | dict[int, type[Any]] | Mapping[int, Any]"]
    use_unicode: NotRequired[bool | None]
    client_flag: NotRequired[int]
    cursorclass: NotRequired[type["AiomysqlRawCursor"] | type["AiomysqlDictCursor"]]
    cursor_class: NotRequired[type["AiomysqlRawCursor"] | type["AiomysqlDictCursor"]]
    auth_plugin: NotRequired[str]
    program_name: NotRequired[str]
    server_public_key: NotRequired[str | bytes]
    loop: NotRequired[Any]


class AiomysqlPoolParams(AiomysqlConnectionParams):
    """aiomysql pool parameters."""

    minsize: NotRequired[int]
    maxsize: NotRequired[int]
    pool_recycle: NotRequired[int]


def _normalize_local_infile(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Normalize aiomysql local-infile aliases to the native connection flag."""
    config = dict(connection_config)

    config.pop("enable_local_infile", None)
    allow_local_infile = bool(config.pop("allow_local_infile", False))
    config["local_infile"] = bool(config.get("local_infile", False) or allow_local_infile)
    return config


def _normalize_connection_kwargs(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build aiomysql.connect-compatible kwargs from SQLSpec connection config."""
    config = _normalize_local_infile(connection_config)

    for key in _POOL_ONLY_CONFIG_KEYS:
        config.pop(key, None)

    if "cursor_class" in config and "cursorclass" not in config:
        config["cursorclass"] = config["cursor_class"]
    config.pop("cursor_class", None)

    if "database" in config and "db" not in config:
        config["db"] = config["database"]
    config.pop("database", None)

    if "passwd" in config and "password" not in config:
        config["password"] = config["passwd"]
    config.pop("passwd", None)

    return config


class AiomysqlDriverFeatures(TypedDict):
    """aiomysql driver feature flags.

    MySQL/MariaDB handle JSON natively, but custom serializers can be provided
    for specialized use cases.

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
     Only option: "poll_queue" (durable table-backed queue with lease-based retries and acknowledgements).
     MySQL/MariaDB do not have native pub/sub, so poll_queue is the only backend.
     Defaults to "poll_queue".
    """

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    on_connection_create: "NotRequired[Callable[[AiomysqlConnection], Awaitable[None]]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[Literal["poll_queue"]]
    enable_local_infile_bulk_load: NotRequired[bool]


def build_connection_config(
    connection_config: "AiomysqlPoolParams | dict[str, Any] | Mapping[str, Any] | None",
) -> dict[str, Any]:
    """Normalize aiomysql connection configuration, parsing DSN and mapping aliases."""
    config = normalize_connection_config(connection_config)
    dsn = config.pop("dsn", None) or config.pop("url", None) or config.pop("connection_string", None)
    user_alias = config.pop("username", None)
    if user_alias is not None and "user" not in config:
        config["user"] = user_alias
    db_alias = config.pop("database", None)
    if db_alias is not None and "db" not in config:
        config["db"] = db_alias
    if dsn is not None and isinstance(dsn, str):
        dsn_params = parse_mysql_dsn(dsn)
        if "database" in dsn_params and "db" not in dsn_params:
            dsn_params["db"] = dsn_params.pop("database")
        for key, value in dsn_params.items():
            config.setdefault(key, value)
    config.setdefault("host", "localhost")
    config.setdefault("port", 3306)
    config.setdefault("charset", "utf8mb4")
    config.setdefault("pool_recycle", 300)
    return _normalize_local_infile(config)


class _AiomysqlSessionFactory(AsyncPoolSessionFactory):
    __slots__ = ("_contexts",)

    def __init__(self, config: "AiomysqlConfig") -> None:
        super().__init__(config)
        self._contexts: dict[int, Any] = {}

    async def acquire_connection(self) -> "AiomysqlConnection":
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.acquire()
        connection = cast("AiomysqlConnection", await ctx.__aenter__())
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

    async def release_connection(self, _conn: "AiomysqlConnection", **kwargs: Any) -> None:
        ctx = self._contexts.pop(id(_conn), None)
        if ctx is not None:
            if hasattr(_conn, "get_transaction_status") and _conn.get_transaction_status():
                with contextlib.suppress(Exception):
                    await _conn.rollback()
            await ctx.__aexit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))


class AiomysqlConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for aiomysql connections."""

    __slots__ = ("_ctx",)

    def __init__(self, config: "AiomysqlConfig") -> None:
        super().__init__(config)
        self._ctx: Any = None
        self._connection: AiomysqlConnection | None = None

    async def __aenter__(self) -> AiomysqlConnection:
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.acquire()
        self._ctx = ctx
        connection = cast("AiomysqlConnection", await ctx.__aenter__())
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
        conn = self._connection
        self._connection = None
        if conn is not None and hasattr(conn, "get_transaction_status") and conn.get_transaction_status():
            with contextlib.suppress(Exception):
                await conn.rollback()
        if self._ctx:
            ctx = self._ctx
            self._ctx = None
            return cast("bool | None", await ctx.__aexit__(exc_type, exc_val, exc_tb))
        return None


@mypyc_attr(native_class=False)
class AiomysqlConfig(AsyncDatabaseConfig[AiomysqlConnection, "AiomysqlPool", AiomysqlDriver]):
    """Configuration for aiomysql database connections."""

    driver_type: ClassVar[type[AiomysqlDriver]] = AiomysqlDriver
    connection_type: "ClassVar[type[Any]]" = cast("type[Any]", AiomysqlConnection)
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_row_streaming: ClassVar[bool] = True
    type_coercion_capabilities: ClassVar[TypeCoercionCapabilities] = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
    _connection_context_class: "ClassVar[type[AiomysqlConnectionContext]]" = AiomysqlConnectionContext
    _session_factory_class: "ClassVar[type[_AiomysqlSessionFactory]]" = _AiomysqlSessionFactory
    _session_context_class: "ClassVar[type[AiomysqlSessionContext]]" = AiomysqlSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "AiomysqlPoolParams | dict[str, Any] | None" = None,
        connection_instance: "AiomysqlPool | None" = None,
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
            extension_config: Extension-specific configuration
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments
        """
        connection_config = build_connection_config(connection_config)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[AiomysqlConnection], Awaitable[None]] | None = features_dict.pop(
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

    async def _create_pool(self) -> "AiomysqlPool":
        """Create the actual async connection pool.

        MySQL/MariaDB handle JSON types natively without requiring connection-level
        type handlers. JSON serialization is handled via type_coercion_map in the
        driver's statement_config (see driver.py).
        """
        return cast("AiomysqlPool", await aiomysql.create_pool(**self._pool_kwargs()))

    def _connection_kwargs(self) -> "dict[str, Any]":
        """Return aiomysql.connect-compatible kwargs without pool-only settings."""
        return _normalize_connection_kwargs(self.connection_config)

    def _pool_kwargs(self) -> "dict[str, Any]":
        """Return aiomysql.create_pool kwargs with normalized connection settings."""
        pool_kwargs = self._connection_kwargs()
        for key in _POOL_ONLY_CONFIG_KEYS:
            if key in self.connection_config:
                pool_kwargs[key] = self.connection_config[key]
        return pool_kwargs

    async def _ensure_connection(self, connection: "AiomysqlConnection") -> None:
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
                await asyncio.wait_for(self.connection_instance.wait_closed(), timeout=5.0)
            self.connection_instance = None

    async def create_connection(self) -> AiomysqlConnection:
        """Open a standalone connection owned by the caller.

        The connection carries the same connection settings and creation hook
        the pool applies, consumes no pool slot, and must be closed by the caller.

        Returns:
            An aiomysql connection instance.
        """
        connection = await aiomysql.connect(**self._connection_kwargs())
        await self._ensure_connection(connection)
        return connection

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "AiomysqlPool":
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
