"""SQLite database configuration with thread-local connections."""

import re
import uuid
from os import PathLike
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite._typing import (
    SqliteConnection,
    SqliteConnectionFactory,
    SqliteCursor,
    SqliteSessionContext,
)
from sqlspec.adapters.sqlite.core import apply_driver_features, build_connection_config, default_statement_config
from sqlspec.adapters.sqlite.driver import SqliteDriver, SqliteExceptionHandler
from sqlspec.adapters.sqlite.pool import SqliteConnectionPool
from sqlspec.adapters.sqlite.type_converter import register_type_handlers
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.driver._sync import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "SqliteAggregateConfig",
    "SqliteCollationConfig",
    "SqliteConfig",
    "SqliteConnectionParams",
    "SqliteDriverFeatures",
    "SqliteFunctionConfig",
)

logger = get_logger("sqlspec.adapters.sqlite")


class SqliteConnectionParams(TypedDict):
    """SQLite connection parameters."""

    database: NotRequired[str | PathLike[str]]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"] | None]
    check_same_thread: NotRequired[bool]
    factory: "NotRequired[SqliteConnectionFactory | None]"
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]
    autocommit: NotRequired[bool]
    pool_recycle_seconds: NotRequired[int]
    health_check_interval: NotRequired[float]
    enable_optimizations: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


class SqliteFunctionConfig(TypedDict):
    """User-defined SQLite function registration."""

    name: str
    narg: int
    func: "Callable[..., Any]"
    deterministic: NotRequired[bool]


class SqliteCollationConfig(TypedDict):
    """User-defined SQLite collation registration."""

    name: str
    func: "Callable[[str, str], int]"


class SqliteAggregateConfig(TypedDict):
    """User-defined SQLite aggregate registration."""

    name: str
    narg: int
    aggregate_class: "type[Any]"


class SqliteDriverFeatures(TypedDict):
    """SQLite driver feature configuration.

    Controls optional type handling and serialization features for SQLite connections.

    enable_custom_adapters: Enable custom type adapters for JSON/UUID/datetime conversion.
     Defaults to True for enhanced Python type support.
     Set to False only if you need pure SQLite behavior without type conversions.
    json_serializer: Custom JSON serializer function.
     Defaults to sqlspec.utils.serializers.to_json.
    json_deserializer: Custom JSON deserializer function.
     Defaults to sqlspec.utils.serializers.from_json.
    on_connection_create: Callback executed when a connection is created.
     Receives the raw sqlite3 connection for low-level driver configuration.
     Runs after internal setup (PRAGMA optimizations).
    enable_events: Enable database event channel support.
     Defaults to True when extension_config["events"] is configured.
     Provides pub/sub capabilities via table-backed queue (SQLite has no native pub/sub).
     Requires extension_config["events"] for migration setup.
    events_backend: Event channel backend selection.
     Only option: "poll_queue" (durable table-backed queue with retries and exactly-once delivery).
     SQLite does not have native pub/sub, so poll_queue is the only backend.
     Defaults to "poll_queue".
    custom_functions: Register SQL functions that run on the connection thread.
     Each entry must include name, narg, and func.
    custom_collations: Register SQL collations that compare two string values.
     Each entry must include name and func.
    custom_aggregates: Register SQL aggregates with step/finalize classes.
     Each entry must include name, narg, and aggregate_class.
    authorizer_callback: sqlite3 authorizer hook run during statement compilation.
    trace_callback: sqlite3 trace hook run for executed statements.
    progress_handler: sqlite3 progress hook run every progress_handler_interval VM opcodes.
    progress_handler_interval: Progress callback interval in SQLite virtual machine opcodes.
     Must be a positive integer when provided.
    row_factory: Row factory selector or callable used for raw sqlite3 connections.
     "row" maps to sqlite3.Row, "dict" maps to a dict row adapter, "tuple" keeps tuple rows.
     "dict" and custom callables can change raw connection result shapes seen by callers.
    text_factory: Text factory used for raw sqlite3 connections.
    pragmas: Additional PRAGMA settings applied after built-in optimization PRAGMAs.
     User values override built-in defaults when the same PRAGMA appears in both places.
    extensions: Shared-library extension paths loaded on each connection.
    """

    enable_custom_adapters: NotRequired[bool]
    json_serializer: "NotRequired[Callable[[Any], str]]"
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    on_connection_create: "NotRequired[Callable[[SqliteConnection], None]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[Literal["poll_queue"]]
    custom_functions: "NotRequired[Sequence[SqliteFunctionConfig]]"
    custom_collations: "NotRequired[Sequence[SqliteCollationConfig]]"
    custom_aggregates: "NotRequired[Sequence[SqliteAggregateConfig]]"
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
        if "database" not in config_dict or config_dict["database"] == ":memory:":
            config_dict["database"] = f"file:memory_{uuid.uuid4().hex}?mode=memory&cache=private"
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

        # Extract user connection hook before storing driver_features
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

    def create_connection(self) -> SqliteConnection:
        """Get a SQLite connection from the pool.

        Returns:
            SqliteConnection: A connection from the pool
        """
        pool = self.provide_pool()
        return pool.acquire()

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
        })
        return namespace
