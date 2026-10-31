"""Aiosqlite database configuration."""

from collections.abc import Mapping, Sequence
from os import PathLike
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.aiosqlite._typing import (
    AiosqliteConnection,
    AiosqliteConnectionFactory,
    AiosqliteCursor,
    AiosqliteSessionContext,
)
from sqlspec.adapters.aiosqlite.core import (
    apply_driver_features,
    build_connection_config,
    default_statement_config,
    render_pragmas,
)
from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver, AiosqliteExceptionHandler
from sqlspec.adapters.aiosqlite.pool import (
    AiosqliteConnectionPool,
    AiosqlitePoolConnection,
    AiosqlitePoolConnectionContext,
)
from sqlspec.adapters.aiosqlite.type_converter import register_type_handlers
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.driver import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.logging import get_logger
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "AiosqliteAggregateConfig",
    "AiosqliteCollationConfig",
    "AiosqliteConfig",
    "AiosqliteConnectionParams",
    "AiosqliteDriverFeatures",
    "AiosqliteFunctionConfig",
    "AiosqlitePoolParams",
    "AiosqliteWindowFunctionConfig",
)

logger = get_logger("sqlspec.adapters.aiosqlite")

SQLiteIsolationLevel = Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"] | None
SQLiteAutocommitMode = bool | Literal[-1]


class AiosqliteConnectionParams(TypedDict):
    """TypedDict for aiosqlite connection parameters."""

    database: NotRequired[str | PathLike[str]]
    """Filesystem path, ``':memory:'``, or ``file:...`` URI for the SQLite database."""
    timeout: NotRequired[float]
    """Busy lock wait timeout in seconds passed to ``aiosqlite.connect``."""
    detect_types: NotRequired[int]
    """Bitmask of ``sqlite3.PARSE_DECLTYPES`` and ``sqlite3.PARSE_COLNAMES`` flags."""
    isolation_level: NotRequired[SQLiteIsolationLevel]
    """Implicit transaction begin mode, or ``None`` for manual transaction management."""
    check_same_thread: NotRequired[bool]
    """Restrict underlying ``sqlite3.Connection`` access to the ``aiosqlite`` worker thread."""
    factory: "NotRequired[AiosqliteConnectionFactory | None]"
    """Custom ``sqlite3.Connection`` subclass instantiated on the worker thread."""
    cached_statements: NotRequired[int]
    """Number of compiled SQLite VM statements cached per connection."""
    uri: NotRequired[bool]
    """Interpret ``database`` as a SQLite ``file:`` URI supporting query parameters."""
    iter_chunk_size: NotRequired[int]
    """Row batch size fetched per worker-thread hop when iterating ``aiosqlite`` cursors."""
    autocommit: NotRequired[SQLiteAutocommitMode]
    """PEP 249 / Python 3.12+ autocommit flag or ``sqlite3.LEGACY_TRANSACTION_CONTROL`` (``-1``)."""


class AiosqlitePoolParams(AiosqliteConnectionParams):
    """TypedDict for aiosqlite pool parameters, inheriting connection parameters."""

    pool_size: NotRequired[int]
    """Maximum number of worker-backed SQLite connections managed by ``AiosqliteConnectionPool``."""
    min_size: NotRequired[int]
    """Minimum number of warm connections pre-opened in the pool."""
    connect_timeout: NotRequired[float]
    """Maximum wait time in seconds when acquiring a connection from the pool."""
    idle_timeout: NotRequired[float]
    """Idle duration in seconds before recycling unused pooled connections."""
    operation_timeout: NotRequired[float]
    """Maximum duration in seconds allowed for pool lifecycle operations."""
    health_check_interval: NotRequired[float]
    """Interval in seconds between background or checkout connection health checks."""
    enable_optimizations: NotRequired[bool]
    """Apply WAL journal mode, ``synchronous=NORMAL``, and busy-timeout PRAGMAs on connect."""
    enable_foreign_keys: NotRequired[bool]
    """Execute ``PRAGMA foreign_keys = ON`` on every pooled connection (defaults to ``False``)."""
    extra: NotRequired["dict[str, Any]"]
    """Additional keyword arguments forwarded directly to ``aiosqlite.connect``."""


class AiosqliteFunctionConfig(TypedDict):
    """User-defined aiosqlite function registration."""

    name: str
    """SQL scalar function name registered on the worker-thread SQLite connection."""
    narg: int
    """Expected positional argument count (use ``-1`` for variadic functions)."""
    func: "Callable[..., Any]"
    """Synchronous Python callable executed on the ``aiosqlite`` worker thread."""
    deterministic: NotRequired[bool]
    """Mark the function deterministic so SQLite can inline it in indexes and expressions."""


class AiosqliteCollationConfig(TypedDict):
    """User-defined aiosqlite collation registration."""

    name: str
    """SQL collation name usable in ``COLLATE <name>`` clauses."""
    func: "Callable[[str, str], int]"
    """Synchronous comparator callable returning negative, zero, or positive ordering."""


class AiosqliteAggregateConfig(TypedDict):
    """User-defined aiosqlite aggregate registration."""

    name: str
    """SQL aggregate function name registered on each connection."""
    narg: int
    """Expected argument count passed to ``step()`` (use ``-1`` for variadic aggregates)."""
    aggregate_class: "type[Any]"
    """Class implementing ``step(...)`` and ``finalize()`` on the worker thread."""


class AiosqliteWindowFunctionConfig(TypedDict):
    """User-defined aiosqlite window function registration."""

    name: str
    """SQL window function name registered on each connection."""
    narg: int
    """Expected argument count passed to ``step()``/``inverse()`` (use ``-1`` for variadic functions)."""
    window_class: "type[Any]"
    """Class implementing ``step()``, ``inverse()``, ``value()``, and ``finalize()``."""


class AiosqliteDriverFeatures(TypedDict):
    """Optional aiosqlite driver feature flags, UDF registrations, PRAGMAs, and async hooks."""

    enable_custom_adapters: NotRequired[bool]
    """Register global ``sqlite3`` module type adapters/converters (defaults to ``False``)."""
    json_serializer: "NotRequired[Callable[[Any], str]]"
    """Custom JSON serializer used for bound parameters (defaults to ``to_json``)."""
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    """Custom JSON deserializer used for JSON column decoding (defaults to ``from_json``)."""
    on_connection_create: "NotRequired[Callable[[AiosqliteConnection], Awaitable[None]]]"
    """Async hook invoked with each ``aiosqlite.Connection`` after PRAGMA initialization."""
    enable_events: NotRequired[bool]
    """Enable durable table-backed SQLSpec event queue support."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event channel backend (``poll_queue`` is the sole SQLite backend)."""
    custom_functions: "NotRequired[Sequence[AiosqliteFunctionConfig]]"
    """Scalar SQL functions registered on the ``aiosqlite`` worker thread."""
    custom_collations: "NotRequired[Sequence[AiosqliteCollationConfig]]"
    """Custom SQL collations registered on the ``aiosqlite`` worker thread."""
    custom_aggregates: "NotRequired[Sequence[AiosqliteAggregateConfig]]"
    """Custom SQL aggregate classes registered on each connection."""
    custom_window_functions: "NotRequired[Sequence[AiosqliteWindowFunctionConfig]]"
    """Custom SQL window function classes registered on each connection."""
    default_transaction_mode: NotRequired[Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]]
    """Default ``BEGIN`` locking mode used by ``begin()`` (``DEFERRED``, ``IMMEDIATE``, or ``EXCLUSIVE``)."""
    authorizer_callback: "NotRequired[Callable[[int, str | None, str | None, str | None, str | None], int]]"
    """SQLite compile-time authorizer callback executed on the worker thread."""
    trace_callback: "NotRequired[Callable[[str], None]]"
    """Statement trace callback executed on the worker thread."""
    progress_handler: "NotRequired[Callable[[], int | None]]"
    """SQLite VM progress hook invoked every ``progress_handler_interval`` opcodes."""
    progress_handler_interval: NotRequired[int]
    """Positive VM opcode count between ``progress_handler`` invocations."""
    row_factory: "NotRequired[Literal['row', 'dict', 'tuple'] | Callable[..., Any]]"
    """Raw connection row factory selector (``'row'``, ``'dict'``, ``'tuple'``) or custom callable."""
    text_factory: "NotRequired[Callable[[bytes], Any]]"
    """Decoder callable assigned to ``conn.text_factory`` for ``TEXT`` columns."""
    pragmas: "NotRequired[Mapping[str, str | int | bool]]"
    """Additional ``PRAGMA`` key/value overrides applied after built-in optimizations."""
    extensions: "NotRequired[Sequence[str]]"
    """Shared-library SQLite extension paths loaded on each connection."""


_ROW_FACTORY_LITERALS = frozenset({"dict", "row", "tuple"})
_TRANSACTION_MODE_LITERALS = frozenset({"DEFERRED", "IMMEDIATE", "EXCLUSIVE"})
_RUNTIME_FEATURE_KEYS = (
    "authorizer_callback",
    "custom_aggregates",
    "custom_collations",
    "custom_functions",
    "custom_window_functions",
    "extensions",
    "pragmas",
    "progress_handler",
    "progress_handler_interval",
    "row_factory",
    "text_factory",
    "trace_callback",
)


class _AiosqliteSessionFactory(AsyncPoolSessionFactory):
    __slots__ = ("_pool_conn",)

    def __init__(self, config: "AiosqliteConfig") -> None:
        super().__init__(config)
        self._pool_conn: AiosqlitePoolConnection | None = None

    async def acquire_connection(self) -> "AiosqliteConnection":
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        pool_conn = await pool.acquire()
        self._pool_conn = pool_conn
        return cast("AiosqliteConnection", pool_conn.connection)

    async def release_connection(self, _conn: "AiosqliteConnection", **kwargs: Any) -> None:
        if self._pool_conn is not None and self._config.connection_instance is not None:
            await self._config.connection_instance.release(self._pool_conn)
            self._pool_conn = None


class AiosqliteConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for AioSQLite connections."""

    __slots__ = ("_ctx",)

    def __init__(self, config: "AiosqliteConfig") -> None:
        super().__init__(config)
        self._ctx: AiosqlitePoolConnectionContext | None = None

    async def __aenter__(self) -> AiosqliteConnection:
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        self._ctx = pool.get_connection()
        assert self._ctx is not None
        return cast("AiosqliteConnection", await self._ctx.__aenter__())

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._ctx:
            return await self._ctx.__aexit__(exc_type, exc_val, exc_tb)
        return None


@mypyc_attr(native_class=False)
class AiosqliteConfig(AsyncDatabaseConfig["AiosqliteConnection", AiosqliteConnectionPool, AiosqliteDriver]):
    """Database configuration for AioSQLite engine."""

    driver_type: "ClassVar[type[AiosqliteDriver]]" = AiosqliteDriver
    connection_type: "ClassVar[type[AiosqliteConnection]]" = AiosqliteConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    supports_native_row_streaming: "ClassVar[bool]" = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="iso_text", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
    _connection_context_class: "ClassVar[type[AiosqliteConnectionContext]]" = AiosqliteConnectionContext
    _session_factory_class: "ClassVar[type[_AiosqliteSessionFactory]]" = _AiosqliteSessionFactory
    _session_context_class: "ClassVar[type[AiosqliteSessionContext]]" = AiosqliteSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "AiosqlitePoolParams | dict[str, Any] | None" = None,
        connection_instance: "AiosqliteConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "AiosqliteDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize AioSQLite configuration.

        Args:
            connection_config: Connection and pool configuration parameters (TypedDict or dict)
            connection_instance: Optional pre-configured connection pool instance.
            migration_config: Optional migration configuration.
            statement_config: Optional statement configuration.
            driver_features: Optional driver feature configuration.
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments passed to the base configuration.
        """
        config_dict: dict[str, Any] = dict(connection_config) if connection_config else {}
        database = (
            config_dict.pop("database", None)
            or config_dict.pop("db", None)
            or config_dict.pop("path", None)
            or config_dict.pop("file", None)
        )
        if database is not None:
            config_dict["database"] = database

        if "database" not in config_dict or config_dict["database"] == ":memory:":
            config_dict["database"] = f"file:memory_{uuid4().hex}?mode=memory&cache=shared"
            config_dict["uri"] = True
        elif "database" in config_dict:
            database_path = str(config_dict["database"])
            if database_path.startswith("file:") and not config_dict.get("uri"):
                logger.debug(
                    "Database URI detected (%s) but uri=True not set. "
                    "Auto-enabling URI mode to prevent physical file creation.",
                    database_path,
                )
                config_dict["uri"] = True

        config_dict = normalize_connection_config(config_dict)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[AiosqliteConnection], Awaitable[None]] | None = features_dict.pop(
            "on_connection_create", None
        )
        self._runtime_setup: dict[str, Any] | None = _build_runtime_setup(features_dict)

        super().__init__(
            connection_config=config_dict,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for AiosqliteConfig types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "AiosqliteAggregateConfig": AiosqliteAggregateConfig,
            "AiosqliteCollationConfig": AiosqliteCollationConfig,
            "AiosqliteConnectionContext": AiosqliteConnectionContext,
            "AiosqliteConnection": AiosqliteConnection,
            "AiosqliteConnectionFactory": AiosqliteConnectionFactory,
            "AiosqliteConnectionParams": AiosqliteConnectionParams,
            "AiosqliteConnectionPool": AiosqliteConnectionPool,
            "AiosqliteCursor": AiosqliteCursor,
            "AiosqliteDriver": AiosqliteDriver,
            "AiosqliteDriverFeatures": AiosqliteDriverFeatures,
            "AiosqliteExceptionHandler": AiosqliteExceptionHandler,
            "AiosqliteFunctionConfig": AiosqliteFunctionConfig,
            "AiosqlitePoolParams": AiosqlitePoolParams,
            "AiosqliteSessionContext": AiosqliteSessionContext,
            "AiosqliteWindowFunctionConfig": AiosqliteWindowFunctionConfig,
            "Literal": Literal,
            "PathLike": PathLike,
        })
        return namespace

    async def create_connection(self) -> "AiosqliteConnection":
        """Open a standalone connection owned by the caller.

        The connection carries the same parameters, PRAGMAs, and runtime setup
        the pool applies, consumes no pool slot, and must be closed by the caller.

        Returns:
            A newly opened aiosqlite connection.
        """
        pool = await self.provide_pool()
        return await pool.new_connection()

    async def provide_pool(self) -> AiosqliteConnectionPool:
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    async def _create_pool(self) -> AiosqliteConnectionPool:
        """Create the connection pool instance.

        Returns:
            AiosqliteConnectionPool: The connection pool instance.
        """
        pool_size = self.connection_config.get("pool_size")
        if pool_size is None:
            pool_size = 5
        min_size = self.connection_config.get("min_size")
        if min_size is None:
            min_size = 0
        connect_timeout = self.connection_config.get("connect_timeout")
        if connect_timeout is None:
            connect_timeout = 30.0
        idle_timeout = self.connection_config.get("idle_timeout")
        if idle_timeout is None:
            idle_timeout = 24 * 60 * 60
        operation_timeout = self.connection_config.get("operation_timeout")
        if operation_timeout is None:
            operation_timeout = 10.0
        health_check_interval = self.connection_config.get("health_check_interval")
        if health_check_interval is None:
            health_check_interval = 30.0
        pool_kwargs: dict[str, Any] = {}
        enable_optimizations = self.connection_config.get("enable_optimizations")
        if enable_optimizations is not None:
            pool_kwargs["enable_optimizations"] = enable_optimizations
        enable_foreign_keys = self.connection_config.get("enable_foreign_keys")
        if enable_foreign_keys is not None:
            pool_kwargs["enable_foreign_keys"] = enable_foreign_keys

        pool = AiosqliteConnectionPool(
            connection_parameters=build_connection_config(self.connection_config),
            pool_size=pool_size,
            min_size=min_size,
            connect_timeout=connect_timeout,
            idle_timeout=idle_timeout,
            operation_timeout=operation_timeout,
            health_check_interval=health_check_interval,
            on_connection_create=self._user_connection_hook,
            runtime_setup=self._runtime_setup,
            **pool_kwargs,
        )

        if self.driver_features.get("enable_custom_adapters", False):
            self._register_type_adapters()

        return pool

    def _register_type_adapters(self) -> None:
        """Register custom type adapters and converters for SQLite.

        Called once during pool creation if enable_custom_adapters is True.
        Registers JSON serialization handlers if configured.
        """
        register_type_handlers(
            json_serializer=self.driver_features.get("json_serializer"),
            json_deserializer=self.driver_features.get("json_deserializer"),
        )

    async def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.connection_instance and not self.connection_instance.is_closed:
            await self.connection_instance.close()
            self.connection_instance = None


def _validate_entries(entries: Any, required_keys: "tuple[str, ...]", feature_name: str) -> None:
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        msg = f"driver_features['{feature_name}'] must be a sequence of mappings"
        raise ImproperConfigurationError(msg)
    for entry in entries:
        if not isinstance(entry, Mapping):
            msg = f"driver_features['{feature_name}'] entry must be a mapping"
            raise ImproperConfigurationError(msg)
        for required_key in required_keys:
            if required_key not in entry:
                msg = f"driver_features['{feature_name}'] entry is missing required key {required_key!r}"
                raise ImproperConfigurationError(msg)


def _build_runtime_setup(features: "dict[str, Any]") -> "dict[str, Any] | None":
    if "default_transaction_mode" in features and features["default_transaction_mode"] is not None:
        mode = features["default_transaction_mode"]
        if not isinstance(mode, str) or mode.upper() not in _TRANSACTION_MODE_LITERALS:
            msg = f"driver_features['default_transaction_mode'] must be DEFERRED, IMMEDIATE, or EXCLUSIVE; got {mode!r}"
            raise ImproperConfigurationError(msg)
        features["default_transaction_mode"] = mode.upper()

    runtime_setup: dict[str, Any] = {}
    for key in _RUNTIME_FEATURE_KEYS:
        if key in features:
            runtime_setup[key] = features.pop(key)
    if not runtime_setup:
        return None

    if "pragmas" in runtime_setup and runtime_setup["pragmas"] is not None:
        if not isinstance(runtime_setup["pragmas"], Mapping):
            msg = "driver_features['pragmas'] must be a mapping of PRAGMA names to values"
            raise ImproperConfigurationError(msg)
        runtime_setup["pragmas"] = render_pragmas(runtime_setup["pragmas"])

    if "extensions" in runtime_setup and runtime_setup["extensions"] is not None:
        extensions = runtime_setup["extensions"]
        if not isinstance(extensions, Sequence) or isinstance(extensions, (str, bytes)):
            msg = "driver_features['extensions'] must be a sequence of extension paths"
            raise ImproperConfigurationError(msg)

    row_factory = runtime_setup.get("row_factory")
    if row_factory is not None and not isinstance(row_factory, str) and not callable(row_factory):
        msg = f"driver_features['row_factory'] must be 'row', 'dict', 'tuple', or a callable; got {row_factory!r}"
        raise ImproperConfigurationError(msg)
    if isinstance(row_factory, str) and row_factory not in _ROW_FACTORY_LITERALS:
        msg = f"driver_features['row_factory'] must be 'row', 'dict', 'tuple', or a callable; got {row_factory!r}"
        raise ImproperConfigurationError(msg)

    _validate_entries(runtime_setup.get("custom_functions", ()), ("name", "narg", "func"), "custom_functions")
    _validate_entries(runtime_setup.get("custom_collations", ()), ("name", "func"), "custom_collations")
    _validate_entries(
        runtime_setup.get("custom_aggregates", ()), ("name", "narg", "aggregate_class"), "custom_aggregates"
    )
    window_functions = runtime_setup.get("custom_window_functions", ())
    _validate_entries(window_functions, ("name", "narg", "window_class"), "custom_window_functions")

    interval = runtime_setup.get("progress_handler_interval")
    if interval is not None and (not isinstance(interval, int) or isinstance(interval, bool) or interval < 1):
        msg = f"driver_features['progress_handler_interval'] must be a positive int; got {interval!r}"
        raise ImproperConfigurationError(msg)

    return runtime_setup
