"""SQLite database configuration with thread-local connections."""

from collections.abc import Mapping, Sequence
from os import PathLike
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict

from sqlspec.adapters.sqlite._typing import (
    SqliteConnection,
    SqliteConnectionFactory,
    SqliteCursor,
    SqliteSessionContext,
)
from sqlspec.adapters.sqlite.core import (
    apply_driver_features,
    build_connection_config,
    default_statement_config,
    render_pragmas,
    require_python_version,
)
from sqlspec.adapters.sqlite.driver import SqliteDriver, SqliteExceptionHandler
from sqlspec.adapters.sqlite.pool import SqliteConnectionPool
from sqlspec.adapters.sqlite.type_converter import register_type_handlers
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "SqliteAggregateConfig",
    "SqliteCollationConfig",
    "SqliteConfig",
    "SqliteConnectionParams",
    "SqliteDriverFeatures",
    "SqliteFunctionConfig",
    "SqliteWindowFunctionConfig",
)

logger = get_logger("sqlspec.adapters.sqlite")

SQLiteAutocommitMode = bool | Literal[-1]


class SqliteConnectionParams(TypedDict):
    """SQLite connection parameters."""

    database: NotRequired[str | PathLike[str]]
    """Filesystem path, ``':memory:'``, or ``file:...`` URI for the SQLite database."""
    timeout: NotRequired[float]
    """Busy lock wait timeout in seconds passed to ``sqlite3.connect``."""
    detect_types: NotRequired[int]
    """Bitmask of ``sqlite3.PARSE_DECLTYPES`` and ``sqlite3.PARSE_COLNAMES`` flags."""
    isolation_level: NotRequired[Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"] | None]
    """Implicit transaction begin mode, or ``None`` for manual/autocommit control."""
    check_same_thread: NotRequired[bool]
    """Restrict raw ``sqlite3.Connection`` usage to the creating OS thread."""
    factory: "NotRequired[SqliteConnectionFactory | None]"
    """Custom ``sqlite3.Connection`` subclass passed to ``sqlite3.connect``."""
    cached_statements: NotRequired[int]
    """Number of compiled SQLite VM statements cached per connection by ``sqlite3``."""
    uri: NotRequired[bool]
    """Interpret ``database`` as a SQLite ``file:`` URI supporting query parameters."""
    autocommit: NotRequired[SQLiteAutocommitMode]
    """PEP 249 / Python 3.12+ autocommit control flag or ``sqlite3.LEGACY_TRANSACTION_CONTROL`` (``-1``)."""
    pool_recycle_seconds: NotRequired[int]
    """Maximum connection age in seconds before recycling pooled SQLite connections."""
    health_check_interval: NotRequired[float]
    """Minimum interval in seconds between connection liveness probes in ``SqliteConnectionPool``."""
    enable_optimizations: NotRequired[bool]
    """Apply recommended SQLite ``PRAGMA`` settings (WAL journal mode, synchronous=NORMAL, busy_timeout) on connect."""
    enable_foreign_keys: NotRequired[bool]
    """Execute ``PRAGMA foreign_keys = ON`` when initializing each connection (defaults to ``True``)."""
    extra: NotRequired[dict[str, Any]]
    """Additional keyword arguments forwarded to ``sqlite3.connect``."""


class SqliteFunctionConfig(TypedDict):
    """User-defined SQLite function registration."""

    name: str
    """SQL function name registered on each SQLite connection."""
    narg: int
    """Expected positional argument count (use ``-1`` for variadic functions)."""
    func: "Callable[..., Any]"
    """Python callable invoked synchronously when the SQL function executes."""
    deterministic: NotRequired[bool]
    """Mark the function deterministic so SQLite can inline it in indexes and generated columns."""


class SqliteCollationConfig(TypedDict):
    """User-defined SQLite collation registration."""

    name: str
    """SQL collation name usable in ``COLLATE <name>`` clauses."""
    func: "Callable[[str, str], int]"
    """Comparator callable returning a negative, zero, or positive integer for two strings."""


class SqliteAggregateConfig(TypedDict):
    """User-defined SQLite aggregate registration."""

    name: str
    """SQL aggregate function name registered on each connection."""
    narg: int
    """Expected argument count passed to ``step()`` (use ``-1`` for variadic aggregates)."""
    aggregate_class: "type[Any]"
    """Class implementing ``step(...)`` and ``finalize()`` methods."""


class SqliteWindowFunctionConfig(TypedDict):
    """User-defined SQLite window function registration."""

    name: str
    """SQL window function name registered on each connection."""
    narg: int
    """Expected argument count passed to ``step()``/``inverse()`` (use ``-1`` for variadic functions)."""
    window_class: "type[Any]"
    """Class implementing ``step()``, ``inverse()``, ``value()``, and ``finalize()`` methods."""


class SqliteDriverFeatures(TypedDict):
    """Optional SQLite driver feature flags, UDF registrations, PRAGMAs, and connection hooks."""

    enable_custom_adapters: NotRequired[bool]
    """Register global ``sqlite3`` module type adapters/converters (defaults to ``False``)."""
    json_serializer: "NotRequired[Callable[[Any], str]]"
    """Custom JSON serializer used for bound parameters (defaults to ``to_json``)."""
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    """Custom JSON deserializer used for JSON column decoding (defaults to ``from_json``)."""
    on_connection_create: "NotRequired[Callable[[SqliteConnection], None]]"
    """Synchronous hook called with each raw ``sqlite3.Connection`` after PRAGMA setup."""
    enable_events: NotRequired[bool]
    """Enable durable table-backed SQLSpec event queue support."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event channel backend (``poll_queue`` is the sole SQLite backend)."""
    custom_functions: "NotRequired[Sequence[SqliteFunctionConfig]]"
    """Scalar SQL functions registered via ``conn.create_function`` on each connection."""
    custom_collations: "NotRequired[Sequence[SqliteCollationConfig]]"
    """Custom string collations registered via ``conn.create_collation`` on each connection."""
    custom_aggregates: "NotRequired[Sequence[SqliteAggregateConfig]]"
    """Custom SQL aggregate classes registered via ``conn.create_aggregate``."""
    custom_window_functions: "NotRequired[Sequence[SqliteWindowFunctionConfig]]"
    """Custom SQL window function classes registered via ``conn.create_window_function``."""
    default_transaction_mode: NotRequired[Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]]
    """Default ``BEGIN`` locking mode used by ``begin()`` (``DEFERRED``, ``IMMEDIATE``, or ``EXCLUSIVE``)."""
    authorizer_callback: "NotRequired[Callable[[int, str | None, str | None, str | None, str | None], int]]"
    """SQLite compile-time access control hook registered via ``conn.set_authorizer``."""
    trace_callback: "NotRequired[Callable[[str], None]]"
    """Statement trace callback registered via ``conn.set_trace_callback``."""
    progress_handler: "NotRequired[Callable[[], int | None]]"
    """SQLite VM progress hook invoked every ``progress_handler_interval`` opcodes."""
    progress_handler_interval: NotRequired[int]
    """Positive VM opcode count between ``progress_handler`` invocations."""
    row_factory: "NotRequired[Literal['row', 'dict', 'tuple'] | Callable[..., Any]]"
    """Raw connection row factory selector (``'row'``, ``'dict'``, ``'tuple'``) or custom callable."""
    text_factory: "NotRequired[Callable[[bytes], Any]]"
    """Decoder callable assigned to ``conn.text_factory`` for ``TEXT`` columns."""
    pragmas: "NotRequired[Mapping[str, str | int | bool]]"
    """Additional ``PRAGMA`` key/value overrides applied after built-in connection optimizations."""
    extensions: "NotRequired[Sequence[str]]"
    """Shared-library SQLite extension paths loaded via ``conn.load_extension``."""


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


class SqliteConnectionContext(SyncPoolConnectionContext):
    """Context manager for Sqlite connections."""

    __slots__ = ()


class _SqliteSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()


class SqliteConfig(SyncDatabaseConfig[SqliteConnection, SqliteConnectionPool, SqliteDriver]):
    """SQLite configuration with thread-local connections."""

    driver_type: "ClassVar[type[SqliteDriver]]" = SqliteDriver
    connection_type: "ClassVar[type[SqliteConnection]]" = SqliteConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    supports_native_row_streaming: "ClassVar[bool]" = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="iso_text", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
    _connection_context_class: "ClassVar[type[SqliteConnectionContext]]" = SqliteConnectionContext
    _session_factory_class: "ClassVar[type[_SqliteSessionConnectionHandler]]" = _SqliteSessionConnectionHandler
    _session_context_class: "ClassVar[type[SqliteSessionContext]]" = SqliteSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "SqliteConnectionParams | dict[str, Any] | None" = None,
        connection_instance: "SqliteConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "SqliteDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SQLite configuration.

        Args:
            connection_config: Configuration parameters including connection settings
            connection_instance: Pre-created pool instance
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Optional driver feature configuration
            bind_key: Optional bind key for the configuration
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
            config_dict["database"] = f"file:memory_{uuid4().hex}?mode=memory&cache=private"
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

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[SqliteConnection], None] | None = features_dict.pop(
            "on_connection_create", None
        )
        self._runtime_setup: dict[str, Any] | None = _build_runtime_setup(features_dict)

        super().__init__(
            bind_key=bind_key,
            connection_instance=connection_instance,
            connection_config=config_dict,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

    def create_connection(self) -> SqliteConnection:
        """Open a standalone connection owned by the caller.

        The connection carries the same parameters, PRAGMAs, and runtime setup
        the pool applies, but it is not the pool's thread-local connection, so
        closing it leaves the pool usable.

        Returns:
            SqliteConnection: A newly opened connection.
        """
        return self.provide_pool().new_connection()

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for SQLite types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "SqliteAggregateConfig": SqliteAggregateConfig,
            "SqliteCollationConfig": SqliteCollationConfig,
            "PathLike": PathLike,
            "Literal": Literal,
            "SqliteConnectionContext": SqliteConnectionContext,
            "SqliteConnection": SqliteConnection,
            "SqliteConnectionFactory": SqliteConnectionFactory,
            "SqliteConnectionParams": SqliteConnectionParams,
            "SqliteConnectionPool": SqliteConnectionPool,
            "SqliteCursor": SqliteCursor,
            "SqliteDriver": SqliteDriver,
            "SqliteDriverFeatures": SqliteDriverFeatures,
            "SqliteExceptionHandler": SqliteExceptionHandler,
            "SqliteFunctionConfig": SqliteFunctionConfig,
            "SqliteSessionContext": SqliteSessionContext,
            "SqliteWindowFunctionConfig": SqliteWindowFunctionConfig,
        })
        return namespace

    def _create_pool(self) -> SqliteConnectionPool:
        """Create connection pool from configuration."""
        config_dict = build_connection_config(self.connection_config)

        pool_kwargs: dict[str, Any] = {}
        recycle_seconds = self.connection_config.get("pool_recycle_seconds")
        if recycle_seconds is not None:
            pool_kwargs["recycle_seconds"] = recycle_seconds

        health_check_interval = self.connection_config.get("health_check_interval")
        if health_check_interval is not None:
            pool_kwargs["health_check_interval"] = health_check_interval

        enable_optimizations = self.connection_config.get("enable_optimizations")
        if enable_optimizations is not None:
            pool_kwargs["enable_optimizations"] = enable_optimizations

        enable_foreign_keys = self.connection_config.get("enable_foreign_keys")
        if enable_foreign_keys is not None:
            pool_kwargs["enable_foreign_keys"] = enable_foreign_keys

        pool = SqliteConnectionPool(
            connection_parameters=config_dict,
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

    def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.connection_instance:
            self.connection_instance.close()


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
    if window_functions:
        require_python_version("Custom SQLite window functions", (3, 11))

    interval = runtime_setup.get("progress_handler_interval")
    if interval is not None and (not isinstance(interval, int) or isinstance(interval, bool) or interval < 1):
        msg = f"driver_features['progress_handler_interval'] must be a positive int; got {interval!r}"
        raise ImproperConfigurationError(msg)

    return runtime_setup
