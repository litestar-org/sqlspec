"""Aiosqlite database configuration."""

import re
import uuid
from os import PathLike
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, cast

from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.aiosqlite._typing import (
    AiosqliteConnection,
    AiosqliteConnectionFactory,
    AiosqliteCursor,
    AiosqliteSessionContext,
)
from sqlspec.adapters.aiosqlite.core import apply_driver_features, build_connection_config, default_statement_config
from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver, AiosqliteExceptionHandler
from sqlspec.adapters.aiosqlite.pool import (
    AiosqliteConnectionPool,
    AiosqlitePoolConnection,
    AiosqlitePoolConnectionContext,
)
from sqlspec.adapters.aiosqlite.type_converter import register_type_handlers
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.driver._async import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
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
)

logger = get_logger("sqlspec.adapters.aiosqlite")

SQLiteIsolationLevel = Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"] | None
SQLiteAutocommitMode = bool | Literal[-1]


class AiosqliteConnectionParams(TypedDict):
    """TypedDict for aiosqlite connection parameters."""

    database: NotRequired[str | PathLike[str]]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[SQLiteIsolationLevel]
    check_same_thread: NotRequired[bool]
    factory: "NotRequired[AiosqliteConnectionFactory | None]"
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]
    iter_chunk_size: NotRequired[int]
    autocommit: NotRequired[SQLiteAutocommitMode]


class AiosqlitePoolParams(AiosqliteConnectionParams):
    """TypedDict for aiosqlite pool parameters, inheriting connection parameters."""

    pool_size: NotRequired[int]
    min_size: NotRequired[int]
    connect_timeout: NotRequired[float]
    idle_timeout: NotRequired[float]
    operation_timeout: NotRequired[float]
    health_check_interval: NotRequired[float]
    extra: NotRequired["dict[str, Any]"]


class AiosqliteFunctionConfig(TypedDict):
    """User-defined aiosqlite function registration."""

    name: str
    narg: int
    func: "Callable[..., Any]"
    deterministic: NotRequired[bool]


class AiosqliteCollationConfig(TypedDict):
    """User-defined aiosqlite collation registration."""

    name: str
    func: "Callable[[str, str], int]"


class AiosqliteAggregateConfig(TypedDict):
    """User-defined aiosqlite aggregate registration."""

    name: str
    narg: int
    aggregate_class: "type[Any]"


class AiosqliteDriverFeatures(TypedDict):
    """Aiosqlite driver feature configuration.

    Controls optional type handling and serialization features for SQLite connections.

    enable_custom_adapters: Enable custom type adapters for JSON/UUID/datetime conversion.
     Defaults to True for enhanced Python type support.
     Set to False only if you need pure SQLite behavior without type conversions.
    json_serializer: Custom JSON serializer function.
     Defaults to sqlspec.utils.serializers.to_json.
    json_deserializer: Custom JSON deserializer function.
     Defaults to sqlspec.utils.serializers.from_json.
    on_connection_create: Async callback executed when a connection is created.
     Receives the raw aiosqlite connection for low-level driver configuration.
     Runs after internal setup (PRAGMA optimizations).
    enable_events: Enable database event channel support.
     Defaults to True when extension_config["events"] is configured.
     Provides pub/sub capabilities via table-backed queue (SQLite has no native pub/sub).
     Requires extension_config["events"] for migration setup.
    events_backend: Event channel backend selection.
     Only option: "poll_queue" (durable table-backed queue with retries and exactly-once delivery).
     SQLite does not have native pub/sub, so poll_queue is the only backend.
     Defaults to "poll_queue".
    custom_functions: Register SQL functions that run on the aiosqlite worker thread.
     Each entry must include name, narg, and func. Callable values are plain sync callables.
    custom_collations: Register SQL collations that compare two string values.
     Each entry must include name and func. Callable values are plain sync callables.
    custom_aggregates: Register SQL aggregates with step/finalize classes.
     Each entry must include name, narg, and aggregate_class.
    authorizer_callback: sqlite3 authorizer hook run during statement compilation on the worker thread.
    trace_callback: sqlite3 trace hook run for executed statements on the worker thread.
    progress_handler: sqlite3 progress hook run every progress_handler_interval VM opcodes.
    progress_handler_interval: Progress callback interval in SQLite virtual machine opcodes.
     Must be a positive integer when provided.
    row_factory: Row factory selector or callable used for raw aiosqlite connections.
     "row" maps to sqlite3.Row, "dict" maps to a dict row adapter, "tuple" keeps tuple rows.
     "dict" and custom callables can change raw connection result shapes seen by callers.
    text_factory: Text factory used for raw aiosqlite connections.
    pragmas: Additional PRAGMA settings applied after built-in optimization PRAGMAs.
     User values override built-in defaults when the same PRAGMA appears in both places.
    extensions: Shared-library extension paths loaded on each connection.
    """

    enable_custom_adapters: NotRequired[bool]
    json_serializer: "NotRequired[Callable[[Any], str]]"
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    on_connection_create: "NotRequired[Callable[[AiosqliteConnection], Awaitable[None]]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]
    custom_functions: "NotRequired[Sequence[AiosqliteFunctionConfig]]"
    custom_collations: "NotRequired[Sequence[AiosqliteCollationConfig]]"
    custom_aggregates: "NotRequired[Sequence[AiosqliteAggregateConfig]]"
    authorizer_callback: "NotRequired[Callable[[int, str | None, str | None, str | None, str | None], int]]"
    trace_callback: "NotRequired[Callable[[str], None]]"
    progress_handler: "NotRequired[Callable[[], int | None]]"
    progress_handler_interval: NotRequired[int]
    row_factory: "NotRequired[Literal['row', 'dict', 'tuple'] | Callable[..., Any]]"
    text_factory: "NotRequired[Callable[[bytes], Any]]"
    pragmas: "NotRequired[Mapping[str, str | int | bool]]"
    extensions: "NotRequired[Sequence[str]]"


_PRAGMA_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PRAGMA_VALUE_PATTERN = re.compile(r"^[A-Za-z0-9_.\-]+$")
_ROW_FACTORY_LITERALS = frozenset({"dict", "row", "tuple"})
_RUNTIME_FEATURE_KEYS = (
    "authorizer_callback",
    "custom_aggregates",
    "custom_collations",
    "custom_functions",
    "extensions",
    "pragmas",
    "progress_handler",
    "progress_handler_interval",
    "row_factory",
    "text_factory",
    "trace_callback",
)


def _render_pragmas(pragmas: "Mapping[str, Any]") -> "list[tuple[str, str]]":
    rendered: list[tuple[str, str]] = []
    for pragma_name, pragma_value in pragmas.items():
        if not isinstance(pragma_name, str) or _PRAGMA_NAME_PATTERN.match(pragma_name) is None:
            msg = f"Invalid PRAGMA name in driver_features['pragmas']: {pragma_name!r}"
            raise ImproperConfigurationError(msg)
        if isinstance(pragma_value, bool):
            rendered_value = "1" if pragma_value else "0"
        elif isinstance(pragma_value, int):
            rendered_value = str(pragma_value)
        elif isinstance(pragma_value, str) and _PRAGMA_VALUE_PATTERN.match(pragma_value) is not None:
            rendered_value = pragma_value
        else:
            msg = f"Invalid PRAGMA value for {pragma_name!r} in driver_features['pragmas']: {pragma_value!r}"
            raise ImproperConfigurationError(msg)
        rendered.append((pragma_name, rendered_value))
    return rendered


def _validate_entries(entries: Any, required_keys: "tuple[str, ...]", feature_name: str) -> None:
    for entry in entries:
        for required_key in required_keys:
            if required_key not in entry:
                msg = f"driver_features['{feature_name}'] entry is missing required key {required_key!r}"
                raise ImproperConfigurationError(msg)


def _build_runtime_setup(features: "dict[str, Any]") -> "dict[str, Any] | None":
    runtime_setup: dict[str, Any] = {}
    for key in _RUNTIME_FEATURE_KEYS:
        if key in features:
            runtime_setup[key] = features.pop(key)
    if not runtime_setup:
        return None

    if "pragmas" in runtime_setup:
        runtime_setup["pragmas"] = _render_pragmas(runtime_setup["pragmas"])

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

    interval = runtime_setup.get("progress_handler_interval")
    if interval is not None and (not isinstance(interval, int) or isinstance(interval, bool) or interval < 1):
        msg = f"driver_features['progress_handler_interval'] must be a positive int; got {interval!r}"
        raise ImproperConfigurationError(msg)

    return runtime_setup


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

        if "database" not in config_dict or config_dict["database"] == ":memory:":
            config_dict["database"] = f"file:memory_{uuid.uuid4().hex}?mode=memory&cache=shared"
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

        # Extract user connection hook before storing driver_features
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

    async def _create_pool(self) -> AiosqliteConnectionPool:
        """Create the connection pool instance.

        Returns:
            AiosqliteConnectionPool: The connection pool instance.
        """
        pool_size = self.connection_config.get("pool_size") or 5
        min_size = self.connection_config.get("min_size")
        if min_size is None:
            min_size = 0
        connect_timeout = self.connection_config.get("connect_timeout") or 30.0
        idle_timeout = self.connection_config.get("idle_timeout") or 24 * 60 * 60
        operation_timeout = self.connection_config.get("operation_timeout") or 10.0
        health_check_interval = self.connection_config.get("health_check_interval")
        if health_check_interval is None:
            health_check_interval = 30.0

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
            "Literal": Literal,
            "PathLike": PathLike,
        })
        return namespace

    async def create_connection(self) -> "AiosqliteConnection":
        """Create a single async connection from the pool.

        Returns:
            An aiosqlite connection instance.
        """
        pool = self.connection_instance
        if pool is None:
            pool = await self.create_pool()
            self.connection_instance = pool
        pool_connection = await pool.acquire()
        return pool_connection.connection

    async def provide_pool(self) -> AiosqliteConnectionPool:
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    async def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.connection_instance and not self.connection_instance.is_closed:
            await self.connection_instance.close()
            self.connection_instance = None
