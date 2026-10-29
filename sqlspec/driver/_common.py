"""Common driver attributes and utilities.

``SyncExceptionHandler`` and ``AsyncExceptionHandler`` record mapped exceptions on
``pending_exception`` rather than raising inside ``__exit__`` or ``__aexit__`` so
compiled mypyc callers re-raise outside the context-manager ABI boundary.
"""

import hashlib
import logging
import re
from collections.abc import Mapping
from time import perf_counter
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NamedTuple, NoReturn, Protocol, Self, cast, overload

import sqlglot
from mypy_extensions import mypyc_attr, trait
from sqlglot import exp

from sqlspec.builder import QueryBuilder
from sqlspec.core import (
    SQL,
    CachedStatement,
    OperationProfile,
    ParameterDeclaration,
    ParameterStyle,
    SQLResult,
    Statement,
    StatementConfig,
    TypedParameter,
    get_cache,
    get_cache_config,
    matches_param_type,
    split_sql_script,
)
from sqlspec.core._pool import get_processed_state_pool, get_sql_pool
from sqlspec.core.filters import find_filter as _find_filter_impl
from sqlspec.core.metrics import StackExecutionMetrics
from sqlspec.core.parameters import (
    ParameterProcessor,
    apply_type_coercion,
    structural_fingerprint,
    type_coercion_dispatcher,
    type_coercion_fallbacks,
    value_fingerprint,
)
from sqlspec.core.result._base import RowFormat
from sqlspec.core.sqlcommenter import format_wire_sqlcomment, strip_sqlcomment
from sqlspec.core.statement import ProcessedState
from sqlspec.data_dictionary import (
    ForeignKeyMetadata,
    ObjectIdentity,
    VersionCacheResult,
    VersionInfo,
    dependency_edges_from_foreign_keys,
    get_data_dictionary_loader,
    sort_dependencies,
)
from sqlspec.data_dictionary._registry import get_dialect_config
from sqlspec.driver._query_cache import CachedQuery, QueryCache, StatementCacheHolder, resolve_statement_cache_size
from sqlspec.driver._storage_helpers import (
    CAPABILITY_HINTS,
    arrow_table_needs_parameter_preparation,
    arrow_table_to_rows,
    attach_partition_telemetry,
    build_ingest_telemetry,
    coerce_arrow_table,
    create_storage_job,
    records_to_arrow_table,
)
from sqlspec.exceptions import (
    ImproperConfigurationError,
    NotFoundError,
    SQLFileNotFoundError,
    SQLSpecError,
    StorageCapabilityError,
    TransactionError,
)
from sqlspec.observability import ObservabilityRuntime, get_trace_context, resolve_db_system
from sqlspec.protocols import StatementProtocol
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.schema import to_schema as _to_schema_impl
from sqlspec.utils.text import normalize_identifier
from sqlspec.utils.type_guards import (
    has_asdict_method,
    has_cursor_metadata,
    has_statement_type,
    is_dict_row,
    is_mapping_like,
    is_statement_filter,
)

if TYPE_CHECKING:
    from collections import OrderedDict, abc
    from collections.abc import Awaitable
    from types import TracebackType

    from sqlspec.core import ArrowResult, FilterTypeT, OperationType, StatementFilter
    from sqlspec.core.parameters._types import ConvertedParameters
    from sqlspec.core.stack import StatementStack
    from sqlspec.data_dictionary._types import DialectConfig
    from sqlspec.storage import (
        AsyncStoragePipeline,
        StorageBridgeJob,
        StorageCapabilities,
        StorageTelemetry,
        SyncStoragePipeline,
    )
    from sqlspec.typing import ArrowTable, SchemaT, StatementParameters


__all__ = (
    "VERSION_GROUPS_MIN_FOR_MINOR",
    "VERSION_GROUPS_MIN_FOR_PATCH",
    "AsyncExceptionHandler",
    "CachedQuery",
    "CommonDriverAttributesMixin",
    "DataDictionaryDialectMixin",
    "DataDictionaryMixin",
    "ExecutionResult",
    "StackExecutionObserver",
    "SyncExceptionHandler",
    "describe_stack_statement",
    "handle_single_row_error",
    "hash_stack_operations",
    "parameter_value_needs_processing",
    "parameter_values_need_processing",
    "type_coercion_fallbacks",
    "validate_savepoint_name",
)


logger = get_logger("sqlspec.driver")

_SAVEPOINT_NAME_PATTERN: Final["re.Pattern[str]"] = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

VERSION_GROUPS_MIN_FOR_MINOR = 1
VERSION_GROUPS_MIN_FOR_PATCH = 2

_DEFAULT_DML_METADATA: Final = {"status_message": "OK"}
_EMPTY_DML_DATA: Final[tuple[()]] = ()

_CACHED_NAMED_STYLES: Final[frozenset[str]] = frozenset((
    ParameterStyle.NAMED_COLON.value,
    ParameterStyle.NAMED_AT.value,
    ParameterStyle.NAMED_DOLLAR.value,
    ParameterStyle.NAMED_PYFORMAT.value,
))


class SyncExceptionHandler(Protocol):
    """Protocol for synchronous context managers that map driver errors to SQLSpec exceptions.

    Implementations suppress recognized driver exceptions in ``__exit__`` and expose the
    mapped SQLSpec exception on ``pending_exception`` for the caller to raise.
    """

    def __enter__(self) -> Self: ...
    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: "TracebackType | None"
    ) -> bool: ...
    @property
    def pending_exception(self) -> Exception | None: ...


class AsyncExceptionHandler(Protocol):
    """Protocol for asynchronous context managers that map driver errors to SQLSpec exceptions.

    Implementations suppress recognized driver exceptions in ``__aexit__`` and expose the
    mapped SQLSpec exception on ``pending_exception`` for the caller to raise.
    """

    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: "TracebackType | None"
    ) -> bool: ...
    @property
    def pending_exception(self) -> Exception | None: ...


class ExecutionResult(NamedTuple):
    """Execution result containing all data needed for SQLResult building."""

    cursor_result: Any
    rowcount_override: int | None
    special_data: Any
    selected_data: list[Any] | None
    column_names: list[str] | None
    data_row_count: int | None
    statement_count: int | None
    successful_statements: int | None
    is_script_result: bool
    is_select_result: bool
    is_many_result: bool
    row_format: RowFormat = "dict"
    last_inserted_id: int | str | None = None
    column_types: dict[str, str] | None = None


def describe_stack_statement(statement: "StatementProtocol | str") -> str:
    """Return a readable representation of a stack statement for diagnostics."""
    if isinstance(statement, str):
        return statement
    if isinstance(statement, StatementProtocol):
        return statement.raw_sql or repr(statement)
    return repr(statement)


def validate_savepoint_name(name: str) -> str:
    """Return the savepoint name when it is a safe SQL identifier."""
    if not _SAVEPOINT_NAME_PATTERN.fullmatch(name):
        msg = f"Invalid savepoint name: {name!r}"
        raise TransactionError(msg)
    return name


def handle_single_row_error(error: ValueError) -> "NoReturn":
    """Normalize single-row selection errors to SQLSpec exceptions."""
    message = str(error)
    if message.startswith("No result found"):
        msg = "No rows found"
        raise NotFoundError(msg) from error
    raise error


def hash_stack_operations(stack: "StatementStack") -> "tuple[str, ...]":
    """Return SHA256 fingerprints for statements contained in the stack."""
    hashes: list[str] = []
    for operation in stack.operations:
        summary = describe_stack_statement(operation.statement)
        if not isinstance(summary, str):
            summary = str(summary)
        digest = hashlib.sha256(summary.encode("utf-8")).hexdigest()
        hashes.append(digest[:16])
    return tuple(hashes)


@mypyc_attr(allow_interpreted_subclasses=False)
class StackExecutionObserver:
    """Context manager that aggregates telemetry for stack execution."""

    __slots__ = (
        "continue_on_error",
        "driver",
        "hashed_operations",
        "metrics",
        "native_pipeline",
        "runtime",
        "span",
        "stack",
        "started",
    )

    def __init__(
        self,
        driver: "CommonDriverAttributesMixin",
        stack: "StatementStack",
        continue_on_error: bool,
        native_pipeline: bool,
    ) -> None:
        self.driver = driver
        self.stack = stack
        self.continue_on_error = continue_on_error
        self.native_pipeline = native_pipeline
        self.runtime = driver.observability
        self.metrics = StackExecutionMetrics(
            adapter=type(driver).__name__,
            statement_count=len(stack.operations),
            continue_on_error=continue_on_error,
            native_pipeline=native_pipeline,
            forced_disable=driver.stack_native_disabled,
        )
        self.hashed_operations = hash_stack_operations(stack)
        self.span: Any | None = None
        self.started = 0.0

    def __enter__(self) -> Self:
        self.started = perf_counter()
        trace_id, span_id = get_trace_context()
        attributes = {
            "sqlspec.stack.statement_count": len(self.stack.operations),
            "sqlspec.stack.continue_on_error": self.continue_on_error,
            "sqlspec.stack.native_pipeline": self.native_pipeline,
            "sqlspec.stack.forced_disable": self.driver.stack_native_disabled,
        }
        self.span = self.runtime.start_span("sqlspec.stack.execute", attributes=attributes)
        log_with_context(
            logger,
            logging.DEBUG,
            "stack.execute.start",
            driver=type(self.driver).__name__,
            db_system=resolve_db_system(type(self.driver).__name__),
            stack_size=len(self.stack.operations),
            continue_on_error=self.continue_on_error,
            native_pipeline=self.native_pipeline,
            forced_disable=self.driver.stack_native_disabled,
            hashed_operations=self.hashed_operations,
            trace_id=trace_id,
            span_id=span_id,
        )
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: "TracebackType | None"
    ) -> Literal[False]:
        duration = perf_counter() - self.started
        self.metrics.record_duration(duration)
        if isinstance(exc_val, Exception):
            self.metrics.record_error(exc_val)
        self.runtime.span_manager.end_span(self.span, error=exc_val if exc_val is not None else None)
        self.metrics.emit(self.runtime)
        level = logging.ERROR if exc_val is not None else logging.DEBUG
        trace_id, span_id = get_trace_context()
        log_with_context(
            logger,
            level,
            "stack.execute.failed" if exc_val is not None else "stack.execute.complete",
            driver=type(self.driver).__name__,
            db_system=resolve_db_system(type(self.driver).__name__),
            stack_size=len(self.stack.operations),
            continue_on_error=self.continue_on_error,
            native_pipeline=self.native_pipeline,
            forced_disable=self.driver.stack_native_disabled,
            hashed_operations=self.hashed_operations,
            duration_ms=duration * 1000,
            error_type=type(exc_val).__name__ if exc_val is not None else None,
            trace_id=trace_id,
            span_id=span_id,
        )
        return False

    def record_operation_error(self, error: Exception) -> None:
        """Record an operation error when continue-on-error is enabled."""
        self.metrics.record_operation_error(error)


@mypyc_attr(allow_interpreted_subclasses=True)
@trait
class DataDictionaryDialectMixin:
    """Mixin providing dialect SQL helpers for data dictionaries."""

    __slots__ = ()

    dialect: "ClassVar[str]"

    def get_dialect_config(self) -> "DialectConfig":
        """Return the dialect configuration for this data dictionary."""
        return get_dialect_config(type(self).dialect)

    def get_query(self, domain: str, operation: str, *, mode: str | None = None) -> "SQL":
        """Return an exact domain query for this dialect."""
        loader = get_data_dictionary_loader()
        query = loader.get_domain_query(type(self).dialect, domain, operation, mode=mode)
        if query.sql is None:
            msg = f"No data-dictionary query found for {type(self).dialect}/{domain}/{operation}"
            raise SQLFileNotFoundError(msg)
        return query.sql

    def get_query_text(self, domain: str, operation: str, *, mode: str | None = None) -> str:
        """Return raw SQL text for an exact domain query."""
        return self.get_query(domain, operation, mode=mode).raw_sql

    def get_query_text_or_none(self, domain: str, operation: str, *, mode: str | None = None) -> "str | None":
        """Return raw SQL text for a named query or None if missing."""
        try:
            return self.get_query_text(domain, operation, mode=mode)
        except SQLFileNotFoundError:
            return None

    def resolve_schema(self, schema: "str | None") -> "str | None":
        """Return a schema name using dialect defaults when missing."""
        config = self.get_dialect_config()
        if schema is not None:
            return normalize_identifier(schema, config.name)
        if config.default_schema is None:
            return None
        return normalize_identifier(config.default_schema, config.name)

    def resolve_identifier(self, identifier: str) -> str:
        """Return a dialect-normalized identifier value."""
        return normalize_identifier(identifier, self.get_dialect_config().name)

    def resolve_feature_flag(self, feature: str, version: "VersionInfo | None") -> bool:
        """Resolve a feature flag using dialect config and version info."""
        config = self.get_dialect_config()
        flag = config.get_feature_flag(feature)
        if flag is not None:
            return flag
        required_version = config.get_feature_version(feature)
        if required_version is None or version is None:
            return False
        return bool(version >= required_version)

    def get_default_features(self) -> "list[str]":
        """Get default feature flags for this dialect."""
        return []

    def list_available_features(self) -> "list[str]":
        """List all features that can be checked via get_feature_flag.

        Returns:
            List of feature names this data dictionary supports
        """
        config = self.get_dialect_config()
        features = set(self.get_default_features())
        features.update(config.feature_flags.keys())
        features.update(config.feature_versions.keys())
        return sorted(features)


@mypyc_attr(allow_interpreted_subclasses=True)
@trait
class DataDictionaryMixin:
    """Mixin providing common data dictionary functionality.

    Includes version caching to avoid repeated database queries when checking
    feature flags or optimal types.
    """

    __slots__ = ()

    dialect: "ClassVar[str]"

    _version_cache: "dict[int, VersionInfo | None]"
    _version_fetch_attempted: "set[int]"

    def get_cached_version(self, driver_id: int) -> "VersionCacheResult":
        """Get cached version info for a driver.

        Args:
            driver_id: The id() of the driver instance.

        Returns:
            Tuple of (was_cached, version_info). If was_cached is False,
            the caller should fetch the version and call cache_version().
        """
        if driver_id in self._version_fetch_attempted:
            return True, self._version_cache.get(driver_id)
        return False, None

    def cache_version(self, driver_id: int, version: "VersionInfo | None") -> None:
        """Cache version info for a driver.

        Args:
            driver_id: The id() of the driver instance.
            version: The version info to cache (can be None if detection failed).
        """
        self._version_fetch_attempted.add(driver_id)
        if version is not None:
            self._version_cache[driver_id] = version

    def parse_version_with_pattern(self, pattern: "re.Pattern[str]", version_str: str) -> "VersionInfo | None":
        """Parse version string using a specific regex pattern.

        Args:
            pattern: Compiled regex pattern for the version format
            version_str: Raw version string from database

        Returns:
            VersionInfo instance or None if parsing fails
        """
        match = pattern.search(version_str)
        if not match:
            return None

        groups = match.groups()
        if not groups:
            return None

        major = int(groups[0])
        minor = int(groups[1]) if len(groups) > VERSION_GROUPS_MIN_FOR_MINOR and groups[1] else 0
        patch = int(groups[2]) if len(groups) > VERSION_GROUPS_MIN_FOR_PATCH and groups[2] else 0
        return VersionInfo(major, minor, patch)

    def get_default_type_mapping(self) -> "dict[str, str]":
        """Get default type mappings for common categories.

        Returns:
            Dictionary mapping type categories to generic SQL types
        """
        return {
            "json": "TEXT",
            "uuid": "VARCHAR(36)",
            "boolean": "INTEGER",
            "timestamp": "TIMESTAMP",
            "text": "TEXT",
            "blob": "BLOB",
        }

    def sort_tables_topologically(self, tables: "list[str]", foreign_keys: "list[ForeignKeyMetadata]") -> "list[str]":
        """Sort tables topologically based on typed foreign key dependency edges.

        Args:
            tables: List of table names.
            foreign_keys: List of foreign key metadata.

        Returns:
            List of table names in topological order (dependencies first).
        """
        table_schemas: dict[str, str | None] = {}
        for fk in foreign_keys:
            table_schemas.setdefault(fk.table_name, fk.schema)
            table_schemas.setdefault(fk.referenced_table, fk.referenced_schema or fk.schema)
        table_objects = tuple(ObjectIdentity(table, "table", schema=table_schemas.get(table)) for table in tables)
        result = sort_dependencies(table_objects, dependency_edges_from_foreign_keys(foreign_keys, source="unknown"))
        result.raise_for_cycles()
        return [identity.name for identity in result.ordered]

    def _log_version_detected(self, adapter: str, version: VersionInfo) -> None:
        """Log detected database version with db.system context."""
        logger.debug(
            "Detected database version", extra={"db.system": resolve_db_system(adapter), "db.version": str(version)}
        )

    def _log_version_unavailable(self, adapter: str, reason: str) -> None:
        """Log that database version could not be determined."""
        logger.debug("Database version unavailable", extra={"db.system": resolve_db_system(adapter), "reason": reason})

    def _log_schema_introspect(
        self, driver: Any, *, schema_name: "str | None", table_name: "str | None", operation: str
    ) -> None:
        """Log schema-level introspection activity."""
        log_with_context(
            logger,
            logging.DEBUG,
            "schema.introspect",
            db_system=resolve_db_system(type(driver).__name__),
            schema_name=schema_name,
            table_name=table_name,
            operation=operation,
        )

    def _log_table_describe(self, driver: Any, *, schema_name: "str | None", table_name: str, operation: str) -> None:
        """Log table-level introspection activity."""
        log_with_context(
            logger,
            logging.DEBUG,
            "table.describe",
            db_system=resolve_db_system(type(driver).__name__),
            schema_name=schema_name,
            table_name=table_name,
            operation=operation,
        )


@mypyc_attr(allow_interpreted_subclasses=True)
class CommonDriverAttributesMixin:
    """Common attributes and methods for driver adapters."""

    __slots__ = (
        "_cache_holder",
        "_observability",
        "_pending_statement_profile",
        "_processed_state_pool",
        "_statement_cache",
        "_statement_pool",
        "_stmt_cache",
        "_stmt_cache_enabled",
        "_stmt_cache_max_size",
        "_stmt_cache_rebind_processor",
        "_transaction_depth",
        "connection",
        "driver_features",
        "statement_config",
    )
    connection: "Any"
    statement_config: "StatementConfig"
    driver_features: "dict[str, Any]"

    storage_pipeline_factory: "ClassVar[type[SyncStoragePipeline | AsyncStoragePipeline] | None]" = None

    def __init__(
        self,
        connection: "Any",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any] | None" = None,
        observability: "ObservabilityRuntime | None" = None,
    ) -> None:
        """Initialize driver adapter with connection and configuration.

        Args:
            connection: Database connection instance
            statement_config: Statement configuration for the driver
            driver_features: Driver-specific features like extensions, secrets, and connection callbacks
            observability: Optional runtime handling lifecycle hooks, observers, and spans
        """
        self.connection = connection
        statement_config.freeze()
        self.statement_config = statement_config
        self.driver_features = driver_features or {}
        self._observability = observability
        self._pending_statement_profile: dict[str, Any] | None = None
        self._transaction_depth = 0
        self._stmt_cache_max_size = self._statement_cache_size()
        shared_holder = self.driver_features.get("_statement_cache_holder")
        if (
            isinstance(shared_holder, StatementCacheHolder)
            and shared_holder.statement_config is self.statement_config
            and shared_holder.max_size == self._stmt_cache_max_size
        ):
            self._cache_holder = shared_holder
        else:
            self._cache_holder = StatementCacheHolder(self.statement_config, self._stmt_cache_max_size)
        self._statement_cache: OrderedDict[str, SQL] = self._cache_holder._statement_cache
        self._stmt_cache: QueryCache = self._cache_holder.query_cache
        self._stmt_cache_rebind_processor: ParameterProcessor = self._cache_holder.rebind_processor
        self._stmt_cache_enabled = False
        self._statement_pool = get_sql_pool()
        self._processed_state_pool = get_processed_state_pool()
        self._refresh_statement_cache_state()

    @property
    def observability(self) -> "ObservabilityRuntime":
        """Return the observability runtime, creating a disabled instance when absent."""
        if self._observability is None:
            self._observability = ObservabilityRuntime(config_name=type(self).__name__)
        return self._observability

    @property
    def is_async(self) -> bool:
        """Return whether the driver executes asynchronously.

        Returns:
            False for sync drivers.
        """
        return False

    @property
    def stack_native_disabled(self) -> bool:
        """Return True when native stack execution is disabled for this driver."""
        return bool(self.driver_features.get("stack_native_disabled", False))

    def attach_observability(self, runtime: "ObservabilityRuntime") -> None:
        """Attach or replace the observability runtime."""
        self._observability = runtime
        self._refresh_statement_cache_state()

    def storage_capabilities(self) -> "StorageCapabilities":
        """Return cached storage capabilities for the active driver.

        Returns:
            StorageCapabilities dict with capability flags.

        Raises:
            StorageCapabilityError: If storage capabilities are not configured.
        """
        capabilities = self.driver_features.get("storage_capabilities")
        if capabilities is None:
            msg = "Storage capabilities are not configured for this driver."
            raise StorageCapabilityError(msg, capability="storage_capabilities")
        return cast("StorageCapabilities", dict(capabilities))

    def stmt_cache_rebind(
        self, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "ConvertedParameters":
        """Rebind parameters for a cached query."""
        config = self.statement_config.parameter_config
        if (
            not cached.input_named_parameters
            and not cached.applied_wrap_types
            and not config.type_coercion_map
            and not cached.needs_style_remap
        ):
            return params
        return self._stmt_cache_rebind_processor.transform_cached_parameters(
            params,
            cached.parameter_profile,
            config,
            input_named_parameters=cached.input_named_parameters,
            is_many=False,
            apply_wrap_types=cached.applied_wrap_types,
        )

    @overload
    @staticmethod
    def to_schema(data: "list[dict[str, Any]]", *, schema_type: "type[SchemaT]") -> "list[SchemaT]": ...
    @overload
    @staticmethod
    def to_schema(data: "list[dict[str, Any]]", *, schema_type: None = None) -> "list[dict[str, Any]]": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: "type[SchemaT]") -> "SchemaT": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: None = None) -> "dict[str, Any]": ...
    @overload
    @staticmethod
    def to_schema(data: Any, *, schema_type: "type[SchemaT]") -> Any: ...
    @overload
    @staticmethod
    def to_schema(data: Any, *, schema_type: None = None) -> Any: ...
    @staticmethod
    def to_schema(data: Any, *, schema_type: "type[Any] | None" = None) -> Any:
        """Convert data to a specified schema type.

        Supports transformation to various schema types including:
            - TypedDict
            - dataclasses
            - msgspec Structs
            - Pydantic models
            - attrs classes

        Args:
            data: Input data to convert (dict, list of dicts, or other).
            schema_type: Target schema type for conversion. If None, returns data unchanged.

        Returns:
            Converted data in the specified schema type, or original data if schema_type is None.
        """
        return _to_schema_impl(data, schema_type=schema_type)

    def create_execution_result(
        self,
        cursor_result: Any,
        *,
        rowcount_override: int | None = None,
        special_data: Any = None,
        selected_data: "list[Any] | None" = None,
        column_names: "list[str] | None" = None,
        data_row_count: int | None = None,
        statement_count: int | None = None,
        successful_statements: int | None = None,
        is_script_result: bool = False,
        is_select_result: bool = False,
        is_many_result: bool = False,
        row_format: "RowFormat" = "dict",
        last_inserted_id: int | str | None = None,
        column_types: "dict[str, str] | None" = None,
    ) -> ExecutionResult:
        """Create ExecutionResult with all necessary data for any operation type.

        Args:
            cursor_result: The raw result returned by the database cursor/driver
            rowcount_override: Optional override for the number of affected rows
            special_data: Any special metadata or additional information
            selected_data: For SELECT operations, the extracted row data (raw driver-native format)
            column_names: For SELECT operations, the column names
            data_row_count: For SELECT operations, the number of rows returned
            statement_count: For script operations, total number of statements
            successful_statements: For script operations, number of successful statements
            is_script_result: Whether this result is from script execution
            is_select_result: Whether this result is from a SELECT operation
            is_many_result: Whether this result is from an execute_many operation
            row_format: Format of raw rows - "tuple", "dict", or "record"
            last_inserted_id: The ID of the last inserted row (if applicable)
            column_types: For SELECT operations, a column name to neutral type-token map
                describing declared column types, used to type all-NULL Arrow columns

        Returns:
            ExecutionResult configured for the specified operation type
        """
        return ExecutionResult(
            cursor_result,
            rowcount_override,
            special_data,
            selected_data,
            column_names,
            data_row_count,
            statement_count,
            successful_statements,
            is_script_result,
            is_select_result,
            is_many_result,
            row_format,
            last_inserted_id,
            column_types,
        )

    def build_statement_result(self, statement: "SQL", execution_result: ExecutionResult) -> "SQLResult":
        """Build and return the SQLResult from ExecutionResult data.

        Args:
            statement: SQL statement that was executed
            execution_result: ExecutionResult containing all necessary data

        Returns:
            SQLResult with complete execution data
        """
        statement._pooled = False
        if execution_result.is_script_result:
            return SQLResult(
                statement=statement,
                data=[],
                rows_affected=execution_result.rowcount_override or 0,
                operation_type="SCRIPT",
                total_statements=execution_result.statement_count or 0,
                successful_statements=execution_result.successful_statements or 0,
                metadata=execution_result.special_data or _DEFAULT_DML_METADATA,
            )

        if execution_result.is_select_result:
            select_metadata = execution_result.special_data or {}
            if execution_result.column_types:
                select_metadata = {**select_metadata, "column_types": execution_result.column_types}
            return SQLResult(
                statement=statement,
                data=execution_result.selected_data or [],
                column_names=execution_result.column_names or [],
                rows_affected=execution_result.data_row_count or 0,
                operation_type="SELECT",
                metadata=select_metadata,
                row_format=execution_result.row_format,
                last_inserted_id=execution_result.last_inserted_id,
            )

        return SQLResult(
            statement=statement,
            data=_EMPTY_DML_DATA,
            rows_affected=execution_result.rowcount_override or 0,
            operation_type=statement.operation_type,
            last_inserted_id=execution_result.last_inserted_id,
            metadata=execution_result.special_data or _DEFAULT_DML_METADATA,
        )

    def prepare_statement(
        self,
        statement: "Statement | QueryBuilder",
        parameters: "tuple[StatementParameters | StatementFilter, ...]" = (),
        *,
        statement_config: "StatementConfig | None" = None,
        kwargs: "dict[str, Any] | None" = None,
    ) -> "SQL":
        """Build SQL statement from various input types.

        Ensures dialect is set and preserves existing state when rebuilding SQL objects.

        Args:
            statement: SQL statement or QueryBuilder to prepare
            parameters: Parameters for the SQL statement
            statement_config: Optional statement configuration override.
            kwargs: Additional keyword arguments

        Returns:
            Prepared SQL statement
        """
        if statement_config is None:
            statement_config = self.statement_config

        if isinstance(statement, str):
            cached_sql = self._cache_holder.get_statement(statement)
            if cached_sql is not None and not kwargs:
                has_filters = any(is_statement_filter(p) for p in parameters)
                if not has_filters:
                    return cached_sql.copy(parameters=parameters)

        kwargs = kwargs or {}
        filters, data_parameters = self._split_parameters(parameters)

        if isinstance(statement, QueryBuilder):
            sql_statement = self._prepare_from_builder(statement, data_parameters, statement_config, kwargs)
        elif isinstance(statement, SQL):
            pending_filters = statement.filters
            sql_statement = self._prepare_from_sql(statement, data_parameters, statement_config, kwargs)
            sql_statement, _ = sql_statement._take_pending_filters()
            filters = [*pending_filters, *filters]
        else:
            sql_statement = self._prepare_from_string(statement, data_parameters, statement_config, kwargs)
            if not filters and not kwargs and isinstance(statement, str) and self._stmt_cache_max_size > 0:
                self._cache_holder.set_statement(statement, sql_statement, max_size=self._stmt_cache_max_size)

        _check_declared_parameters(sql_statement)
        return self._apply_filters(sql_statement, filters)

    def split_script_statements(
        self, script: str, statement_config: "StatementConfig", strip_trailing_semicolon: bool = False
    ) -> "list[str]":
        """Split a SQL script into individual statements.

        Uses a lexer-driven state machine to handle multi-statement scripts,
        including complex constructs like PL/SQL blocks, T-SQL batches, and nested blocks.

        Args:
            script: The SQL script to split
            statement_config: Statement configuration containing dialect information
            strip_trailing_semicolon: If True, remove trailing semicolons from statements

        Returns:
            A list of individual SQL statements
        """
        return [
            sql_script.strip()
            for sql_script in split_sql_script(
                script, dialect=str(statement_config.dialect), strip_trailing_terminator=strip_trailing_semicolon
            )
            if sql_script.strip()
        ]

    def prepare_driver_parameters(
        self,
        parameters: "StatementParameters | list[StatementParameters] | tuple[StatementParameters, ...]",
        statement_config: "StatementConfig",
        is_many: bool = False,
        prepared_statement: Any | None = None,
    ) -> "ConvertedParameters":
        """Prepare parameters for database driver consumption.

        Normalizes parameter structure and unwraps TypedParameter objects
        to their underlying values, which database drivers expect.

        Args:
            parameters: Parameters in any format (dict, list, tuple, scalar, TypedParameter)
            statement_config: Statement configuration for parameter style detection
            is_many: If True, handle as executemany parameter sequence
            prepared_statement: Optional prepared statement containing metadata for parameter processing

        Returns:
            Parameters with TypedParameter objects unwrapped to primitive values
        """
        if parameters is None and statement_config.parameter_config.needs_static_script_compilation:
            return None

        if not parameters:
            return []

        if is_many:
            if isinstance(parameters, list):
                type_coercion_map = statement_config.parameter_config.type_coercion_map
                fallback_items = type_coercion_fallbacks(type_coercion_map)
                needs_transform = False
                for param_set in parameters:
                    if isinstance(param_set, dict):
                        for value in param_set.values():
                            if parameter_value_needs_processing(value, type_coercion_map, fallback_items):
                                needs_transform = True
                                break
                    elif isinstance(param_set, (list, tuple)):
                        for value in param_set:
                            if parameter_value_needs_processing(value, type_coercion_map, fallback_items):
                                needs_transform = True
                                break
                    elif parameter_value_needs_processing(param_set, type_coercion_map, fallback_items):
                        needs_transform = True
                    if needs_transform:
                        break

                if not needs_transform:
                    return parameters

                formatted_many: list[Any] | None = None
                for idx, param_set in enumerate(parameters):
                    formatted = self._batch_parameters(param_set, statement_config)
                    if formatted_many is None:
                        if formatted is param_set:
                            continue
                        formatted_many = list(parameters[:idx])
                    formatted_many.append(formatted)
                if formatted_many is None:
                    return parameters
                return formatted_many
            return [self._batch_parameters(parameters, statement_config)]
        return self._driver_parameters(parameters, statement_config)

    @staticmethod
    def find_filter(
        filter_type: "type[FilterTypeT]",
        filters: "abc.Sequence[StatementFilter | StatementParameters] | abc.Sequence[StatementFilter]",
    ) -> "FilterTypeT | None":
        """Get the filter specified by filter type from the filters.

        Args:
            filter_type: The type of filter to find.
            filters: filter types to apply to the query

        Returns:
            The match filter instance or None
        """
        return _find_filter_impl(filter_type, filters)

    def build_cached_select_result(
        self,
        sql: str,
        params: "tuple[Any, ...] | list[Any] | dict[str, Any]",
        cached: "CachedQuery",
        *,
        selected_data: "list[Any]",
        column_names: "list[str]",
        data_row_count: int,
        row_format: "RowFormat" = "dict",
        last_inserted_id: int | str | None = None,
        compiled_sql: str | None = None,
    ) -> "SQLResult":
        """Build a fast-path SELECT result with deferred statement materialization."""
        return SQLResult.create_cached_select(
            sql=sql,
            config=self.statement_config,
            cached_state=cached.processed_state,
            compiled_sql=cached.compiled_sql if compiled_sql is None else compiled_sql,
            params=params,
            data=selected_data,
            column_names=column_names,
            rows_affected=data_row_count,
            row_format=row_format,
            last_inserted_id=last_inserted_id,
        )

    def _format_wire_sql(self, sql: str) -> str:
        """Apply wire-level sqlcommenter formatting when enabled on statement_config."""
        if not self.statement_config.enable_sqlcommenter:
            return sql
        return format_wire_sqlcomment(sql, self.statement_config)

    def _statement_cache_size(self) -> int:
        """Return the configured statement-cache capacity."""
        return resolve_statement_cache_size(self.driver_features)

    def _refresh_statement_cache_state(self) -> None:
        self._stmt_cache_enabled = bool(self._stmt_cache_max_size > 0 and not self.statement_config._has_transformers)

    def _require_capability(self, capability_flag: str) -> None:
        """Check that a storage capability is enabled.

        Args:
            capability_flag: The capability flag to check.

        Raises:
            StorageCapabilityError: If the capability is not available.
        """
        capabilities = self.storage_capabilities()
        if capabilities.get(capability_flag, False):
            return
        human_label = CAPABILITY_HINTS.get(capability_flag, capability_flag)
        remediation = "Check adapter supports this capability or stage artifacts via storage pipeline."
        msg = f"{human_label} is not available for this adapter"
        raise StorageCapabilityError(msg, capability=capability_flag, remediation=remediation)

    def _raise_storage_not_implemented(self, capability: str) -> NoReturn:
        """Raise NotImplementedError for storage operations.

        Args:
            capability: The capability that is not implemented.

        Raises:
            StorageCapabilityError: Always raised.
        """
        msg = f"{capability} is not implemented for this driver"
        remediation = "Override storage methods on the adapter to enable this capability."
        raise StorageCapabilityError(msg, capability=capability, remediation=remediation)

    def _release_pooled_statement(self, statement: "SQL") -> None:
        if statement._pooled:
            self._statement_pool.release(statement)

    def _cached_statement(
        self,
        sql: str,
        _params: "tuple[Any, ...] | list[Any] | dict[str, Any]",
        cached: "CachedQuery",
        execution_parameters: "ConvertedParameters",
        *,
        params_are_simple: bool = False,
        compiled_sql: str | None = None,
    ) -> "SQL":
        if params_are_simple:
            prepared_parameters = execution_parameters
        else:
            prepared_parameters = self.prepare_driver_parameters(
                execution_parameters, self.statement_config, is_many=False
            )
        effective_compiled_sql = cached.compiled_sql if compiled_sql is None else compiled_sql
        cached_state = cached.processed_state
        direct_state = ProcessedState(
            compiled_sql=effective_compiled_sql,
            execution_parameters=prepared_parameters,
            parsed_expression=cached_state.parsed_expression,
            operation_type=cached_state.operation_type,
            input_named_parameters=cached_state.input_named_parameters,
            applied_wrap_types=cached_state.applied_wrap_types,
            filter_hash=cached_state.filter_hash,
            parameter_fingerprint=cached_state.parameter_fingerprint,
            parameter_casts=cached_state.parameter_casts,
            validation_errors=cached_state.validation_errors,
            parameter_profile=cached_state.parameter_profile,
            operation_profile=cached_state.operation_profile,
            is_many=False,
            expression_is_shared=True,
        )
        return SQL._create_cached_direct(sql, self.statement_config, direct_state, pooled=False)

    def _sub_statement(self, sql: str, execution_parameters: "ConvertedParameters") -> "SQL":
        direct_state = self._processed_state_pool.acquire()
        ProcessedState.__init__(direct_state, compiled_sql=sql, execution_parameters=execution_parameters)
        return SQL._create_cached_direct(sql, self.statement_config, direct_state)

    def _emit_script_statement_event(
        self,
        *,
        sql: str,
        parameters: Any,
        operation: "OperationType",
        rows_affected: int | None,
        duration_s: float,
        started_at: float | None,
        profile: "dict[str, Any] | None",
        script_ordinal: int,
        script_statement_count: int,
    ) -> None:
        """Emit a per-statement StatementEvent during multi-statement script execution."""
        runtime = self._observability
        if runtime is None or not runtime.has_statement_observers:
            return
        runtime.emit_statement_event(
            sql=sql,
            parameters=parameters,
            driver=type(self).__name__,
            operation=operation,
            execution_mode=self.statement_config.execution_mode,
            is_many=False,
            is_script=True,
            rows_affected=rows_affected,
            duration_s=duration_s,
            storage_backend=None,
            started_at=started_at,
            profile=profile,
            script_ordinal=script_ordinal,
            script_statement_index=script_ordinal,
            script_statement_count=script_statement_count,
        )

    def _cached_execution(
        self, statement: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]"
    ) -> "SQLResult | Awaitable[SQLResult | None] | None":
        """Attempt cached execution for query.

        Args:
            statement: Raw SQL string.
            params: Query parameters.

        Returns:
            SQLResult (sync) or Awaitable[SQLResult] (async) if cache hit,
            None if cache miss.
        """
        if not self._stmt_cache_enabled:
            return None
        cached = self._stmt_cache.get(statement)
        if cached is None or cached.param_count != len(params):
            return None

        config = self.statement_config
        param_config = config.parameter_config
        if param_config.ast_transformer is not None:
            param_values = _cache_param_values(params)
            if any(p is None for p in param_values):
                return None
        else:
            param_values = None

        needs_rebind = bool(cached.input_named_parameters or cached.applied_wrap_types or cached.needs_style_remap)
        if not needs_rebind and params:
            if param_values is None:
                param_values = _cache_param_values(params)
            needs_rebind = parameter_values_need_processing(param_values, param_config.type_coercion_map)

        if not needs_rebind and not config._has_output_transformer:
            effective_cached = cached
            if config.enable_sqlcommenter:
                wire_sql = self._format_wire_sql(cached.compiled_sql)
                if wire_sql != cached.compiled_sql:
                    effective_cached = CachedQuery(
                        compiled_sql=wire_sql,
                        parameter_profile=cached.parameter_profile,
                        input_named_parameters=cached.input_named_parameters,
                        applied_wrap_types=cached.applied_wrap_types,
                        parameter_casts=cached.parameter_casts,
                        operation_type=cached.operation_type,
                        operation_profile=cached.operation_profile,
                        param_count=cached.param_count,
                        processed_state=cached.processed_state,
                        needs_style_remap=cached.needs_style_remap,
                    )
            runtime = self._observability
            if runtime is not None and not runtime.is_idle:
                return self._execute_cache_hit_observed(statement, params, effective_cached)
            return self._execute_cache_hit(statement, params, effective_cached)

        if needs_rebind:
            rebound_params = self.stmt_cache_rebind(params, cached)
            params_are_simple = False
        else:
            rebound_params = params
            params_are_simple = True

        compiled_sql = self._format_wire_sql(cached.compiled_sql)
        output_transformer = config.output_transformer
        if output_transformer is not None:
            compiled_sql, rebound_params = output_transformer(compiled_sql, rebound_params)
            params_are_simple = False

        prepared = self._cached_statement(
            statement, params, cached, rebound_params, params_are_simple=params_are_simple, compiled_sql=compiled_sql
        )
        runtime = self._observability
        if runtime is not None and not runtime.is_idle:
            return self._execute_cached_statement_observed(
                prepared, compiled_sql, rebound_params, cached.operation_type
            )
        return self._execute_cached_statement(prepared)

    def _execute_cached_statement(self, statement: "SQL") -> "SQLResult | Awaitable[SQLResult]":
        raise NotImplementedError

    def _execute_cached_statement_observed(
        self, statement: "SQL", compiled_sql: str, execution_parameters: Any, operation: "OperationType"
    ) -> "SQLResult | Awaitable[SQLResult]":
        return self._execute_cached_statement(statement)

    def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult | Awaitable[SQLResult]":
        """Execute pre-compiled query via ultra-fast path (no rebind needed).

        Default implementation falls back to building a direct SQL object and
        routing through _execute_cached_statement. Subclasses may override with adapter-specific
        implementations that bypass SQL object construction entirely.
        """
        prepared = self._cached_statement(
            sql, params, cached, params, params_are_simple=True, compiled_sql=self._format_wire_sql(cached.compiled_sql)
        )
        return self._execute_cached_statement(prepared)

    def _execute_cache_hit_observed(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult | Awaitable[SQLResult]":
        return self._execute_cache_hit(sql, params, cached)

    def _cache_statement(self, statement: "SQL") -> None:
        """Store statement in cache if eligible.

        All eligibility validation happens here (executed once per unique query).
        This keeps the hot lookup path (qc_prepare) minimal - just a flag check
        and cache lookup.

        Ineligible queries:
            - QC disabled or config mismatch
            - Scripts or execute-many (multiple statements/param sets)
            - Raw expressions (dynamic SQL)
            - Static script compilation (parameters embedded in SQL)
            - Filtered statements (dynamic WHERE clauses)
            - Unprocessed statements (no compiled metadata)
        """
        if not self._stmt_cache_enabled:
            return
        if statement.statement_config is not self.statement_config:
            return
        if statement.is_script or statement.is_many:
            return
        if statement.raw_expression is not None:
            return
        if not statement.raw_sql:
            return
        if statement.statement_config.parameter_config.needs_static_script_compilation:
            return
        if len(statement.get_filters_view()) > 0:
            return
        if not statement.is_processed:
            return
        if statement.statement_config.parameter_config.ast_transformer is not None:
            params = statement.positional_parameters
            if any(p is None for p in params):
                return
        if self._stmt_cache.get(statement.raw_sql) is not None:
            return

        processed = cast("ProcessedState", statement.get_processed_state())
        if processed.parsed_expression is None or processed.operation_type in {"COMMAND", "UNKNOWN"}:
            return
        param_profile = processed.parameter_profile
        operation_profile = processed.operation_profile
        if not operation_profile.returns_rows and statement.returns_rows():
            operation_profile = OperationProfile(returns_rows=True, modifies_rows=operation_profile.modifies_rows)
        clean_sql = (
            strip_sqlcomment(processed.compiled_sql)
            if self.statement_config.enable_sqlcommenter
            else processed.compiled_sql
        )
        cached_state = _clone_processed_state(processed, operation_profile, compiled_sql=clean_sql)
        cached = CachedQuery(
            compiled_sql=clean_sql,
            parameter_profile=param_profile,
            input_named_parameters=processed.input_named_parameters,
            applied_wrap_types=processed.applied_wrap_types,
            parameter_casts=cached_state.parameter_casts,
            operation_type=processed.operation_type,
            operation_profile=operation_profile,
            param_count=param_profile.total_count,
            processed_state=cached_state,
            needs_style_remap=bool(_CACHED_NAMED_STYLES.intersection(param_profile.styles)),
        )
        self._stmt_cache.set(statement.raw_sql, cached)

    def _should_force_select(self, statement: "SQL", cursor: object) -> bool:
        """Determine if a statement with unknown type should be treated as SELECT.

        Uses driver metadata (statement_type, description/schema) as a safety net when
        the compiler cannot classify the operation. This remains conservative by only
        triggering when the operation type is "COMMAND" (generic fallback).

        Args:
            statement: SQL statement being executed.
            cursor: Database cursor/job object that may expose metadata.

        Returns:
            True when cursor metadata indicates a row-returning operation despite a
            generic command operation type; otherwise False.
        """
        if statement.operation_type != "COMMAND":
            return False

        if has_statement_type(cursor) and isinstance(cursor.statement_type, str):
            statement_type = cursor.statement_type
        else:
            statement_type = None
        if isinstance(statement_type, str) and statement_type.upper() == "SELECT":
            return True

        if has_cursor_metadata(cursor):
            return bool(cursor.description)
        return False

    def _split_parameters(
        self, parameters: "tuple[StatementParameters | StatementFilter, ...]"
    ) -> "tuple[list[StatementFilter], list[StatementParameters]]":
        filters: list[StatementFilter] = []
        data_parameters: list[StatementParameters] = []
        for param in parameters:
            if is_statement_filter(param):
                filters.append(param)
            else:
                data_parameters.append(param)
        return filters, data_parameters

    def _prepare_from_builder(
        self,
        builder: "QueryBuilder",
        data_parameters: "list[StatementParameters]",
        statement_config: "StatementConfig",
        kwargs: "dict[str, Any]",
    ) -> "SQL":
        sql_statement = builder.to_statement(statement_config)
        if data_parameters or kwargs:
            merged_parameters = (
                (*sql_statement.positional_parameters, *tuple(data_parameters))
                if data_parameters
                else sql_statement.positional_parameters
            )
            statement_seed = sql_statement.raw_expression or sql_statement.raw_sql
            return SQL(statement_seed, *merged_parameters, statement_config=statement_config, **kwargs)
        return sql_statement

    def _prepare_from_sql(
        self,
        sql_statement: "SQL",
        data_parameters: "list[StatementParameters]",
        statement_config: "StatementConfig",
        kwargs: "dict[str, Any]",
    ) -> "SQL":
        declared = sql_statement.declared_parameters
        if data_parameters or kwargs:
            bound_named = sql_statement.named_parameters
            extra_mapping = data_parameters[0] if len(data_parameters) == 1 else None
            if (
                bound_named
                and not sql_statement.positional_parameters
                and (not data_parameters or isinstance(extra_mapping, Mapping))
            ):
                merged_named = dict(bound_named)
                if isinstance(extra_mapping, Mapping):
                    merged_named.update(extra_mapping)
                return SQL(
                    sql_statement.raw_expression or sql_statement.raw_sql,
                    merged_named,
                    statement_config=statement_config,
                    declared_parameters=declared,
                    **kwargs,
                )
            merged_parameters = (
                (*sql_statement.positional_parameters, *tuple(data_parameters))
                if data_parameters
                else sql_statement.positional_parameters
            )
            statement_seed = sql_statement.raw_expression or sql_statement.raw_sql
            merged = SQL(
                statement_seed,
                *merged_parameters,
                statement_config=statement_config,
                declared_parameters=declared,
                **kwargs,
            )
            merged._named_parameters.update({
                key: value for key, value in bound_named.items() if key not in merged._named_parameters
            })
            return merged

        needs_rebuild = False
        if statement_config.dialect and (
            not sql_statement.statement_config.dialect
            or sql_statement.statement_config.dialect != statement_config.dialect
        ):
            needs_rebuild = True

        if (
            sql_statement.statement_config.parameter_config.default_execution_parameter_style
            != statement_config.parameter_config.default_execution_parameter_style
        ):
            needs_rebuild = True

        if needs_rebuild:
            statement_seed = sql_statement.raw_expression or sql_statement.raw_sql
            if sql_statement.is_many and sql_statement.parameters:
                return SQL(
                    statement_seed,
                    sql_statement.parameters,
                    statement_config=statement_config,
                    is_many=True,
                    declared_parameters=declared,
                )
            rebuilt = SQL(
                statement_seed,
                *sql_statement.positional_parameters,
                statement_config=statement_config,
                declared_parameters=declared,
            )
            rebuilt._named_parameters.update(sql_statement.named_parameters)
            return rebuilt
        if declared:
            return sql_statement.copy()
        return sql_statement

    def _prepare_from_string(
        self,
        statement: "Statement",
        data_parameters: "list[StatementParameters]",
        statement_config: "StatementConfig",
        kwargs: "dict[str, Any]",
    ) -> "SQL":
        return SQL(statement, *tuple(data_parameters), statement_config=statement_config, **kwargs)

    def _apply_filters(self, sql_statement: "SQL", filters: "list[StatementFilter]") -> "SQL":
        for filter_obj in filters:
            sql_statement = filter_obj.append_to_statement(sql_statement)
        return sql_statement

    def _batch_parameters(
        self, parameters: "StatementParameters", statement_config: "StatementConfig"
    ) -> "ConvertedParameters":
        """Prepare a single parameter set for execute_many operations.

        Handles parameter sets without converting the structure to array format,
        applying type coercion to individual values while preserving structure.

        Args:
            parameters: Single parameter set (tuple, list, or dict)
            statement_config: Statement configuration for parameter style detection

        Returns:
            Processed parameter set with individual values coerced but structure preserved
        """
        if not parameters:
            return []

        type_coercion_map = statement_config.parameter_config.type_coercion_map
        fallback_items = type_coercion_fallbacks(type_coercion_map)

        if not isinstance(parameters, (dict, list, tuple)):
            return [apply_type_coercion(parameters, type_coercion_map, fallback_items)]

        if isinstance(parameters, dict):
            result_mapping: dict[str, Any] | None = None
            for key, value in parameters.items():
                coerced_value = apply_type_coercion(value, type_coercion_map, fallback_items)
                if result_mapping is None:
                    if coerced_value is value:
                        continue
                    result_mapping = dict(parameters)
                result_mapping[key] = coerced_value
            if result_mapping is None:
                return parameters
            return result_mapping

        updated_params: list[Any] | None = None
        for idx, value in enumerate(parameters):
            coerced_value = apply_type_coercion(value, type_coercion_map, fallback_items)
            if updated_params is None:
                if coerced_value is value:
                    continue
                updated_params = list(parameters[:idx])
            updated_params.append(coerced_value)

        if updated_params is None:
            return parameters
        return tuple(updated_params) if isinstance(parameters, tuple) else updated_params

    def _driver_parameters(
        self, parameters: "StatementParameters", statement_config: "StatementConfig"
    ) -> "ConvertedParameters":
        """Prepare a single parameter set for database driver consumption.

        Args:
            parameters: Single parameter set in any format
            statement_config: Statement configuration for parameter style detection

        Returns:
            Processed parameter set with TypedParameter objects unwrapped and type coercion applied
        """
        if not parameters:
            return []

        type_coercion_map = statement_config.parameter_config.type_coercion_map
        fallback_items = type_coercion_fallbacks(type_coercion_map)
        coerce_value = apply_type_coercion

        if not isinstance(parameters, (dict, list, tuple)):
            return [coerce_value(parameters, type_coercion_map, fallback_items)]

        if isinstance(parameters, dict):
            if statement_config.parameter_config.supported_execution_parameter_styles and (
                ParameterStyle.NAMED_PYFORMAT in statement_config.parameter_config.supported_execution_parameter_styles
                or ParameterStyle.NAMED_COLON in statement_config.parameter_config.supported_execution_parameter_styles
            ):
                return _lazy_copy_coerce_dict(parameters, coerce_value, type_coercion_map, fallback_items)
            if statement_config.parameter_config.default_parameter_style in {
                ParameterStyle.NUMERIC,
                ParameterStyle.QMARK,
                ParameterStyle.POSITIONAL_PYFORMAT,
            }:
                sorted_items = sorted(parameters.items(), key=_parameter_sort_key)
                return [coerce_value(value, type_coercion_map, fallback_items) for _, value in sorted_items]

            return _lazy_copy_coerce_dict(parameters, coerce_value, type_coercion_map, fallback_items)

        updated_params: list[Any] | None = None
        for idx, value in enumerate(parameters):
            coerced_value = coerce_value(value, type_coercion_map, fallback_items)
            if updated_params is None:
                if coerced_value is value:
                    continue
                updated_params = list(parameters[:idx])
            updated_params.append(coerced_value)

        if updated_params is None:
            if isinstance(parameters, tuple) and statement_config.parameter_config.preserve_parameter_format:
                return parameters
            return list(parameters) if isinstance(parameters, tuple) else parameters

        if statement_config.parameter_config.preserve_parameter_format and isinstance(parameters, tuple):
            return tuple(updated_params)
        return updated_params

    def _compiled_sql(
        self, statement: "SQL", statement_config: "StatementConfig", flatten_single_parameters: bool = False
    ) -> "tuple[str, object]":
        """Get compiled SQL with parameter style conversion and caching.

        Compiles the SQL statement and applies parameter style conversion.
        Results are cached when caching is enabled.

        Args:
            statement: SQL statement to compile
            statement_config: Statement configuration including parameter config and dialect
            flatten_single_parameters: If True, flatten single-element lists for scalar parameters

        Returns:
            Tuple of (compiled_sql, parameters)
        """
        compiled_statement, prepared_parameters = self._compiled_statement(
            statement, statement_config, flatten_single_parameters=flatten_single_parameters
        )
        return compiled_statement.compiled_sql, prepared_parameters

    def _compiled_statement(
        self, statement: "SQL", statement_config: "StatementConfig", flatten_single_parameters: bool = False
    ) -> "tuple[CachedStatement, object]":
        """Compile SQL and return cached statement metadata plus prepared parameters.

        FAST PATH: If the statement is already processed (compiled), we reuse
        its ProcessedState directly. This eliminates redundant compilation when
        dispatch_statement_execution() has already triggered compile().
        """
        if statement.is_processed:
            if statement._compiled_from_cache:
                compiled_sql, execution_parameters = statement.compile()
                processed = statement.get_processed_state()
                if (
                    statement.is_many
                    and isinstance(execution_parameters, list)
                    and type(self).prepare_driver_parameters is CommonDriverAttributesMixin.prepare_driver_parameters
                    and _many_parameters_already_prepared(
                        execution_parameters, statement_config, processed.applied_wrap_types
                    )
                ):
                    prepared_parameters: Any = execution_parameters
                else:
                    prepared_parameters = self.prepare_driver_parameters(
                        execution_parameters, statement_config, is_many=statement.is_many, prepared_statement=statement
                    )
                cached_statement = CachedStatement(
                    compiled_sql=compiled_sql,
                    parameters=cast("tuple[Any, ...] | list[Any] | dict[str, Any] | None", prepared_parameters),
                    expression=statement._peek_expression(),
                    input_named_parameters=processed.input_named_parameters,
                    parameter_profile=processed.parameter_profile,
                    applied_wrap_types=processed.applied_wrap_types,
                )
                self._cache_statement(statement)
                return cached_statement, prepared_parameters

            processed = statement.get_processed_state()
            if statement._is_cache_direct:
                prepared_parameters = processed.execution_parameters
                cached_statement = CachedStatement(
                    compiled_sql=processed.compiled_sql,
                    parameters=prepared_parameters,
                    expression=processed.parsed_expression,
                    input_named_parameters=processed.input_named_parameters,
                    parameter_profile=processed.parameter_profile,
                    applied_wrap_types=processed.applied_wrap_types,
                )
                return cached_statement, prepared_parameters

            if (
                statement.is_many
                and isinstance(processed.execution_parameters, list)
                and type(self).prepare_driver_parameters is CommonDriverAttributesMixin.prepare_driver_parameters
                and _many_parameters_already_prepared(
                    processed.execution_parameters, statement_config, processed.applied_wrap_types
                )
            ):
                prepared_parameters = processed.execution_parameters
            else:
                prepared_parameters = self.prepare_driver_parameters(
                    processed.execution_parameters,
                    statement_config,
                    is_many=statement.is_many,
                    prepared_statement=statement,
                )
            cached_statement = CachedStatement(
                compiled_sql=processed.compiled_sql,
                parameters=cast("tuple[Any, ...] | list[Any] | dict[str, Any] | None", prepared_parameters),
                expression=processed.parsed_expression,
                input_named_parameters=processed.input_named_parameters,
                parameter_profile=processed.parameter_profile,
                applied_wrap_types=processed.applied_wrap_types,
            )
            self._cache_statement(statement)
            return cached_statement, prepared_parameters

        params = statement.parameters
        if params is not None and not isinstance(params, (list, tuple, dict)):
            try:
                materialized = list(params)
                statement = statement.copy(parameters=materialized)
            except TypeError:
                pass

        cache_config = get_cache_config()
        dialect_key = str(statement.dialect) if statement.dialect else None
        cache_key = None
        cache = None
        if cache_config.compiled_cache_enabled and statement_config.enable_caching:
            cache_key = self._compile_cache_key(statement, statement_config, flatten_single_parameters)
            cache = get_cache()
            cached_result = cache.get_statement(cache_key, dialect_key)
            if cached_result is not None:
                if __debug__:
                    assert isinstance(cached_result, CachedStatement), (
                        f"Statement cache corruption: expected CachedStatement, got {type(cached_result)!r}"
                    )
                if not isinstance(cached_result, CachedStatement):
                    cached_result = None
            if cached_result is not None:
                parameter_profile = cached_result.parameter_profile
                if parameter_profile is not None:
                    prepared_parameters = self._stmt_cache_rebind_processor.transform_cached_parameters(
                        statement.parameters,
                        parameter_profile,
                        statement_config.parameter_config,
                        input_named_parameters=cached_result.input_named_parameters,
                        is_many=statement.is_many,
                        apply_wrap_types=cached_result.applied_wrap_types,
                    )
                else:
                    prepared_parameters = self.prepare_driver_parameters(
                        statement.parameters, statement_config, is_many=statement.is_many, prepared_statement=statement
                    )
                wire_sql = (
                    format_wire_sqlcomment(cached_result.compiled_sql, statement_config)
                    if statement_config.enable_sqlcommenter
                    else cached_result.compiled_sql
                )
                updated_cached = CachedStatement(
                    compiled_sql=wire_sql,
                    parameters=cast("tuple[Any, ...] | list[Any] | dict[str, Any] | None", prepared_parameters),
                    expression=cached_result.expression,
                    input_named_parameters=cached_result.input_named_parameters,
                    parameter_profile=parameter_profile,
                    applied_wrap_types=cached_result.applied_wrap_types,
                )
                return updated_cached, prepared_parameters

        compiled_sql, execution_parameters = statement.compile()
        processed = statement.get_processed_state()

        if (
            statement.is_many
            and isinstance(execution_parameters, list)
            and type(self).prepare_driver_parameters is CommonDriverAttributesMixin.prepare_driver_parameters
            and _many_parameters_already_prepared(execution_parameters, statement_config, processed.applied_wrap_types)
        ):
            prepared_parameters = execution_parameters
        else:
            prepared_parameters = self.prepare_driver_parameters(
                execution_parameters, statement_config, is_many=statement.is_many, prepared_statement=statement
            )

        cached_parameters = tuple(prepared_parameters) if isinstance(prepared_parameters, list) else prepared_parameters
        stored_sql = strip_sqlcomment(compiled_sql) if statement_config.enable_sqlcommenter else compiled_sql
        cached_statement = CachedStatement(
            compiled_sql=compiled_sql,
            parameters=cast("tuple[Any, ...] | list[Any] | dict[str, Any] | None", cached_parameters),
            expression=statement._peek_expression(),
            input_named_parameters=processed.input_named_parameters,
            parameter_profile=processed.parameter_profile,
            applied_wrap_types=processed.applied_wrap_types,
        )

        if cache_key is not None and cache is not None:
            stored_statement = (
                CachedStatement(
                    compiled_sql=stored_sql,
                    parameters=cached_statement.parameters,
                    expression=cached_statement.expression,
                    input_named_parameters=cached_statement.input_named_parameters,
                    parameter_profile=cached_statement.parameter_profile,
                    applied_wrap_types=cached_statement.applied_wrap_types,
                )
                if stored_sql != compiled_sql
                else cached_statement
            )
            cache.put_statement(cache_key, stored_statement, dialect_key)

        self._cache_statement(statement)
        return cached_statement, prepared_parameters

    def _compile_cache_key(self, statement: "SQL", config: "StatementConfig", flatten_single_parameters: bool) -> str:
        """Generate cache key that includes all compilation context.

        Creates a deterministic cache key that includes all factors that affect SQL compilation,
        preventing cache contamination between different compilation contexts.
        """
        statement_transformers = (
            tuple(_callable_cache_key(transformer) for transformer in config.statement_transformers)
            if config.statement_transformers
            else ()
        )
        context_hash = hash((
            hash(config.parameter_config),
            config.dialect,
            statement.is_script,
            statement.is_many,
            flatten_single_parameters,
            _callable_cache_key(config.output_transformer),
            statement_transformers,
            _callable_cache_key(config.parameter_config.output_transformer),
            _callable_cache_key(config.parameter_config.ast_transformer),
            bool(config.parameter_config.needs_static_script_compilation),
        ))

        params = statement.parameters

        if params is None or (isinstance(params, (list, tuple, dict)) and not params):
            return f"compiled:{hash(statement.raw_sql)}:{context_hash}"

        if isinstance(params, tuple) and all(isinstance(p, (int, str, bytes, bool, type(None))) for p in params):
            try:
                return (
                    f"compiled:{hash((statement.raw_sql, params, statement.is_many, statement.is_script))}:"
                    f"{context_hash}"
                )
            except TypeError:
                pass

        if config.parameter_config.needs_static_script_compilation:
            params_fingerprint = value_fingerprint(params)
        else:
            params_fingerprint = structural_fingerprint(params)
        base_hash = hash((statement.raw_sql, params_fingerprint, statement.is_many, statement.is_script))
        return f"compiled:{base_hash}:{context_hash}"

    def _count_query(self, original_sql: "SQL") -> "SQL":
        """Create a COUNT query from the original SQL statement.

        Transforms the original SELECT statement to count total rows while preserving
        WHERE, HAVING, and GROUP BY clauses but removing ORDER BY, LIMIT, OFFSET, and FETCH.
        Copies any existing ``WITH`` clause (sqlglot stores it under ``with_``).
        When GROUP BY, HAVING, JOINs, DISTINCT, QUALIFY, LATERALs, window/aggregate
        projections, or a WITH clause exist, wraps the payload in a subquery before counting.
        """
        if not original_sql.expression:
            original_sql.compile()

        if not original_sql.expression:
            msg = "Cannot create COUNT query from empty SQL expression"
            raise ImproperConfigurationError(msg)

        expr = original_sql.expression
        cte: exp.Expr | None = None
        if isinstance(expr, exp.Expression):
            cte = expr.args.get("with_")
            if cte is not None:
                expr = expr.copy()
                expr.set("with_", None)

        if isinstance(expr, exp.Select):
            from_clause = expr.args.get("from_")
            if from_clause is None:
                msg = (
                    "Cannot create COUNT query: SELECT statement missing FROM clause. "
                    "COUNT queries require a FROM clause to determine which table to count rows from."
                )
                raise ImproperConfigurationError(msg)

            has_group = bool(expr.args.get("group"))
            has_joins = bool(expr.args.get("joins"))
            has_distinct = bool(expr.args.get("distinct"))
            has_qualify = bool(expr.args.get("qualify"))
            has_laterals = bool(expr.args.get("laterals"))
            has_having = bool(expr.args.get("having"))
            has_agg_or_window = any(
                isinstance(node, (exp.AggFunc, exp.Window))
                for select_expr in expr.expressions
                for node in select_expr.walk()
            )
            needs_subquery = (
                has_group
                or has_joins
                or cte is not None
                or has_distinct
                or has_qualify
                or has_laterals
                or has_having
                or has_agg_or_window
            )
            leading_stripped = (
                0
                if needs_subquery
                else sum(_count_anonymous_placeholders(select_expr) for select_expr in expr.expressions)
            )
            trailing_stripped = sum(
                _count_anonymous_placeholders(expr.args.get(clause)) for clause in ("order", "limit", "offset", "fetch")
            )
            pagination_params = _pagination_names(original_sql, include_select_expressions=not needs_subquery)
            if needs_subquery:
                subquery_expr = expr.copy()
                subquery_expr.set("order", None)
                subquery_expr.set("limit", None)
                subquery_expr.set("offset", None)
                subquery_expr.set("fetch", None)
                subquery = subquery_expr.subquery(alias="grouped_data", copy=False)
                count_expr = exp.select(exp.Count(this=exp.Star())).from_(subquery, copy=False)
            else:
                count_expr = exp.select(exp.Count(this=exp.Star()))
                count_expr.set("from_", from_clause.copy())
                if expr.args.get("where"):
                    count_expr = count_expr.where(cast("exp.Expression", expr.args.get("where")).copy(), copy=False)

            if cte is not None:
                count_expr.set("with_", cte.copy())
            return _build_rewritten_sql(
                count_expr,
                original_sql,
                pagination_params=pagination_params,
                leading_stripped=leading_stripped,
                trailing_stripped=trailing_stripped,
            )

        leading_stripped = 0
        trailing_stripped = sum(
            _count_anonymous_placeholders(expr.args.get(clause)) for clause in ("order", "limit", "offset", "fetch")
        )
        pagination_params = _pagination_names(original_sql, include_select_expressions=False)
        count_source = cast("exp.Query", expr.copy())
        count_source.set("order", None)
        count_source.set("limit", None)
        count_source.set("offset", None)
        count_source.set("fetch", None)
        subquery = count_source.subquery(alias="total_query", copy=False)
        count_expr = exp.select(exp.Count(this=exp.Star())).from_(subquery, copy=False)
        if cte is not None:
            count_expr.set("with_", cte.copy())
        return _build_rewritten_sql(
            count_expr,
            original_sql,
            pagination_params=pagination_params,
            leading_stripped=leading_stripped,
            trailing_stripped=trailing_stripped,
        )

    def _with_total_count(self, original_sql: "SQL", alias: str = "_total_count") -> "SQL":
        """Add a COUNT(*) OVER() column to the SELECT statement for inline total counts.

        This method modifies the SELECT to include a window function that returns
        the total count of rows matching the WHERE clause, before LIMIT/OFFSET is applied.
        Useful for pagination where both data and total count are needed in a single query.

        Args:
            original_sql: The original SQL statement (must be a SELECT).
            alias: Column alias for the count (default "_total_count").

        Returns:
            New SQL object with the COUNT(*) OVER() column added.

        Raises:
            ImproperConfigurationError: If the SQL is not a SELECT statement.
        """
        if not original_sql.expression:
            original_sql.compile()

        if not original_sql.expression:
            msg = "Cannot add COUNT OVER to empty SQL expression"
            raise ImproperConfigurationError(msg)

        expr = original_sql.expression
        if isinstance(expr, exp.SetOperation):
            cte = expr.args.get("with_")
            expr_copy = expr.copy()
            if cte:
                expr_copy.set("with_", None)
            subquery = expr_copy.subquery(alias="__set_op_subq", copy=False)
            modified_expr = exp.select(exp.Column(this=exp.Star())).from_(subquery, copy=False)
            count_window = exp.Window(this=exp.Count(this=exp.Star()))
            aliased_count = exp.alias_(count_window, alias)
            modified_expr = modified_expr.select(aliased_count, copy=False)
            if cte:
                modified_expr.set("with_", cte.copy())
        elif isinstance(expr, exp.Select):
            modified_expr = expr.copy()
            count_window = exp.Window(this=exp.Count(this=exp.Star()))
            aliased_count = exp.alias_(count_window, alias)
            modified_expr = modified_expr.select(aliased_count, copy=False)
        else:
            msg = "COUNT(*) OVER() can only be added to SELECT or set operation statements"
            raise ImproperConfigurationError(msg)

        return _build_rewritten_sql(
            modified_expr, original_sql, pagination_params=set(), leading_stripped=0, trailing_stripped=0
        )

    def _extract_total_from_rows(self, rows: "list[Any]") -> "tuple[list[dict[str, Any]], int]":
        """Extract window-count totals and remove the synthetic total column from rows."""
        if not rows:
            return ([], 0)

        clean_rows: list[dict[str, Any]] = []
        total = 0
        total_found = False
        for row in rows:
            if is_dict_row(row):
                row_dict: dict[str, Any] = row
            elif is_mapping_like(row):
                row_dict = dict(row)
            elif has_asdict_method(row):
                row_dict = row._asdict()
            else:
                clean_rows.append({})
                continue
            if not total_found:
                total_val = row_dict.get("_total_count")
                if total_val is None:
                    total_val = row_dict.get("_TOTAL_COUNT")
                if total_val is not None:
                    total = int(total_val)
                    total_found = True
            clean_rows.append({k: v for k, v in row_dict.items() if k not in {"_total_count", "_TOTAL_COUNT"}})
        return (clean_rows, total)

    def _coerce_arrow_table(self, source: "ArrowResult | Any") -> "ArrowTable":
        """Coerce various sources to a PyArrow Table."""
        return coerce_arrow_table(source)

    @staticmethod
    def _arrow_table_to_rows(
        table: "ArrowTable", columns: "list[str] | None" = None
    ) -> "tuple[list[str], list[tuple[Any, ...]]]":
        """Convert Arrow table to column names and row tuples."""
        return arrow_table_to_rows(table, columns)

    @staticmethod
    def _arrow_rows_need_preparation(table: "ArrowTable") -> bool:
        """Return whether Arrow rows may contain nested values needing preparation."""
        return arrow_table_needs_parameter_preparation(table)

    def _prepare_records_for_arrow(
        self, records: "abc.Sequence[abc.Mapping[str, Any]] | abc.Sequence[abc.Sequence[Any]]"
    ) -> "abc.Sequence[abc.Mapping[str, Any]] | abc.Sequence[abc.Sequence[Any]]":
        """Prepare records before Arrow schema inference."""
        return records

    @staticmethod
    def _ingest_telemetry(table: "ArrowTable", *, format_label: str = "arrow") -> "StorageTelemetry":
        """Build telemetry dict from Arrow table statistics."""
        return build_ingest_telemetry(table, format_label=format_label)

    @staticmethod
    def _records_to_arrow_table(
        records: "abc.Sequence[abc.Mapping[str, Any]] | abc.Sequence[abc.Sequence[Any]]", columns: "list[str] | None"
    ) -> "ArrowTable":
        """Normalize dict or positional records into a PyArrow table for ingest."""
        return records_to_arrow_table(records, columns)

    def _attach_partition_telemetry(
        self, telemetry: "StorageTelemetry", partitioner: "dict[str, object] | None"
    ) -> None:
        """Attach partitioner info to telemetry dict."""
        attach_partition_telemetry(telemetry, partitioner)

    def _storage_job(
        self, produced: "StorageTelemetry", provided: "StorageTelemetry | None" = None, *, status: str = "completed"
    ) -> "StorageBridgeJob":
        """Create a StorageBridgeJob from telemetry data."""
        return create_storage_job(produced, provided, status=status)


def parameter_values_need_processing(
    values: "tuple[Any, ...] | list[Any]",
    type_coercion_map: "dict[type, Any] | None",
    fallback_items: "tuple[tuple[type, Any], ...] | None" = None,
) -> bool:
    if not values:
        return False
    if not type_coercion_map:
        if len(values) == 1:
            return type(values[0]) is TypedParameter
        return any(type(value) is TypedParameter for value in values)
    if fallback_items is None:
        fallback_items = type_coercion_fallbacks(type_coercion_map)
    dispatcher = type_coercion_dispatcher(fallback_items)
    for value in values:
        value_type = type(value)
        if value_type is TypedParameter or value_type in type_coercion_map:
            return True
        if dispatcher.resolve_type(value_type) is not None:
            return True
    return False


def parameter_value_needs_processing(
    value: object,
    type_coercion_map: "dict[type, Any] | None",
    fallback_items: "tuple[tuple[type, Any], ...] | None" = None,
) -> bool:
    if type(value) is TypedParameter:
        return True
    if not type_coercion_map:
        return False

    value_type = type(value)
    if value_type in type_coercion_map:
        return True
    if fallback_items is None:
        fallback_items = type_coercion_fallbacks(type_coercion_map)
    return type_coercion_dispatcher(fallback_items).resolve_type(value_type) is not None


def _resolve_fast_params(
    parameters: "tuple[Any, ...]", kwargs: "dict[str, Any]"
) -> "tuple[Any, ...] | list[Any] | dict[str, Any] | None":
    """Normalize execute(*parameters, **kwargs) arguments for fast-path cache lookup."""
    if kwargs:
        if not parameters:
            return kwargs
        return None
    param_len = len(parameters)
    if param_len == 0:
        return ()
    if param_len == 1:
        first = parameters[0]
        if isinstance(first, (tuple, list, dict)):
            return first
        if is_statement_filter(first):
            return None
        return parameters
    for item in parameters:
        if is_statement_filter(item) or isinstance(item, (tuple, list, dict)):
            return None
    return parameters


def _raise_database_exception(
    exc_handler: "AsyncExceptionHandler | SyncExceptionHandler", exc: Exception | None
) -> None:
    """Raise any mapped database exception captured by a deferred exception handler."""
    pending_exception = exc_handler.pending_exception
    if pending_exception is not None:
        if exc is not None:
            raise pending_exception from exc
        if pending_exception.__cause__ is not None:
            raise pending_exception
        raise pending_exception from None
    if exc is not None:
        raise exc


def _parameter_sort_key(item: "tuple[str, object]") -> float:
    key = item[0]
    if key.isdigit():
        return float(int(key))
    if key.startswith("param_"):
        suffix = key[6:]
        if suffix.isdigit():
            return float(int(suffix))
    return float("inf")


def _lazy_copy_coerce_dict(
    parameters: "dict[str, Any]",
    coerce_value: Any,
    type_coercion_map: Any,
    fallback_items: "tuple[tuple[type, Any], ...]",
) -> "dict[str, Any]":
    result: dict[str, Any] | None = None
    for key, value in parameters.items():
        coerced_value = coerce_value(value, type_coercion_map, fallback_items)
        if result is None:
            if coerced_value is value:
                continue
            result = dict(parameters)
        result[key] = coerced_value
    if result is None:
        return parameters
    return result


def _cache_param_values(params: "tuple[Any, ...] | list[Any] | dict[str, Any]") -> "tuple[Any, ...] | list[Any]":
    if isinstance(params, dict):
        return tuple(params.values())
    return params


def _clone_processed_state(
    processed: "ProcessedState", operation_profile: "OperationProfile | None" = None, compiled_sql: str | None = None
) -> "ProcessedState":
    return ProcessedState(
        compiled_sql=processed.compiled_sql if compiled_sql is None else compiled_sql,
        execution_parameters=processed.execution_parameters,
        parsed_expression=processed.parsed_expression,
        operation_type=processed.operation_type,
        input_named_parameters=processed.input_named_parameters,
        applied_wrap_types=processed.applied_wrap_types,
        filter_hash=processed.filter_hash,
        parameter_fingerprint=processed.parameter_fingerprint,
        parameter_casts=dict(processed.parameter_casts),
        validation_errors=processed.validation_errors.copy(),
        parameter_profile=processed.parameter_profile,
        operation_profile=operation_profile if operation_profile is not None else processed.operation_profile,
        is_many=False,
    )


def _count_anonymous_placeholders(node: "exp.Expr | None") -> int:
    """Count anonymous positional placeholders within a SQLGlot expression subtree."""
    if node is None:
        return 0
    return sum(1 for child in node.walk() if isinstance(child, exp.Placeholder) and child.this is None)


def _pagination_names_in_expression(expression: "exp.Expr", *, include_select_expressions: bool = False) -> "set[str]":
    """Extract named placeholder names from stripped clauses of an expression that do not appear in retained clauses.

    Args:
        expression: A SQLGlot expression to scan.
        include_select_expressions: Whether top-level SELECT expressions are also stripped.

    Returns:
        Set of placeholder names that only appear in stripped clauses.
    """
    stripped_placeholders: set[str] = set()
    stripped_nodes: list[exp.Expr] = []

    if include_select_expressions and isinstance(expression, exp.Select):
        stripped_nodes.extend(expression.expressions)

    for clause_name in ("order", "limit", "offset", "fetch"):
        clause = expression.args.get(clause_name)
        if isinstance(clause, exp.Expr):
            stripped_nodes.append(clause)

    for clause_node in stripped_nodes:
        for node in clause_node.walk():
            if isinstance(node, exp.Placeholder) and node.this is not None:
                stripped_placeholders.add(str(node.this))

    if not stripped_placeholders:
        return set()

    retained_expr = expression.copy()
    if include_select_expressions and isinstance(retained_expr, exp.Select):
        retained_expr.set("expressions", [])
    for clause_name in ("order", "limit", "offset", "fetch"):
        retained_expr.set(clause_name, None)

    retained_placeholders = {
        str(node.this) for node in retained_expr.walk() if isinstance(node, exp.Placeholder) and node.this is not None
    }
    return stripped_placeholders - retained_placeholders


def _pagination_names(original_sql: "SQL", *, include_select_expressions: bool = False) -> "set[str]":
    """Extract placeholder names from stripped pagination/projection clauses.

    Args:
        original_sql: The SQL object to extract pagination placeholders from.
        include_select_expressions: Whether top-level SELECT expressions are also stripped.

    Returns:
        Set of placeholder names found exclusively in stripped clauses.
    """
    stmt_expr = original_sql.statement_expression
    if stmt_expr is not None:
        placeholders = _pagination_names_in_expression(stmt_expr, include_select_expressions=include_select_expressions)
        if placeholders:
            return placeholders
        has_named = any(isinstance(n, exp.Placeholder) and n.this is not None for n in stmt_expr.walk())
        if has_named:
            return set()

    raw_sql = original_sql.raw_sql
    if not raw_sql:
        return set()

    try:
        dialect = str(original_sql.statement_config.dialect) if original_sql.statement_config.dialect else None
        parsed = sqlglot.parse_one(raw_sql, read=dialect)
        return _pagination_names_in_expression(parsed, include_select_expressions=include_select_expressions)
    except Exception:
        return set()


def _parameters_for_repeated_names(original_sql: "SQL", excluded_names: "set[str]") -> "tuple[Any, ...] | None":
    """Return occurrence-ordered parameters when a rewritten expression uses repeated named parameters."""
    processed_state = original_sql._processed_state  # pyright: ignore[reportPrivateUsage]
    if not isinstance(processed_state, ProcessedState):
        return None

    input_names = processed_state.input_named_parameters
    if not input_names:
        return None

    retained_names = tuple(name for name in input_names if name not in excluded_names)
    if len(retained_names) == len(set(retained_names)):
        return None

    execution_parameters = processed_state.execution_parameters
    if not isinstance(execution_parameters, (list, tuple)) or len(execution_parameters) != len(input_names):
        return None

    return tuple(
        value for name, value in zip(input_names, execution_parameters, strict=False) if name not in excluded_names
    )


def _build_rewritten_sql(
    rewritten_expr: "exp.Expr",
    original_sql: "SQL",
    *,
    pagination_params: "set[str]",
    leading_stripped: int,
    trailing_stripped: int,
) -> "SQL":
    """Construct a rewritten SQL object with stripped positional or named parameters."""
    positional = original_sql.positional_parameters
    retained_positional = positional
    if positional and (leading_stripped or trailing_stripped):
        end_idx = len(positional) - trailing_stripped if trailing_stripped else len(positional)
        retained_positional = positional[leading_stripped:end_idx]

    if original_sql.named_parameters:
        materialized_parameters = _parameters_for_repeated_names(original_sql, pagination_params)
        if materialized_parameters is not None:
            return SQL(rewritten_expr, *materialized_parameters, statement_config=original_sql.statement_config)
        processed_state = original_sql._processed_state  # pyright: ignore[reportPrivateUsage]
        input_names = processed_state.input_named_parameters if isinstance(processed_state, ProcessedState) else ()
        filtered_named_params: dict[str, Any] = {}
        for name in input_names:
            if (
                name not in pagination_params
                and name in original_sql.named_parameters
                and name not in filtered_named_params
            ):
                filtered_named_params[name] = original_sql.named_parameters[name]
        for key, value in original_sql.named_parameters.items():
            if key not in pagination_params and key not in filtered_named_params:
                filtered_named_params[key] = value
        return SQL(
            rewritten_expr,
            *retained_positional,
            statement_config=original_sql.statement_config,
            **filtered_named_params,
        )

    return SQL(rewritten_expr, *retained_positional, statement_config=original_sql.statement_config)


def _callable_cache_key(func: Any) -> Any:
    """Return a stable cache key component for callables.

    Args:
        func: Callable or None.

    Returns:
        Tuple identifying the callable, or None for missing callables.
    """
    if func is None:
        return None
    module = getattr(func, "__module__", None)
    qualname = getattr(func, "__qualname__", type(func).__name__)
    return (module, qualname, id(func))


def _apply_declared_optional_defaults(declared: "tuple[ParameterDeclaration, ...]", supplied: "dict[str, Any]") -> None:
    """Bind missing optional named params as SQL NULL."""
    for declaration in declared:
        if not declaration.required and declaration.name not in supplied:
            supplied[declaration.name] = None


def _check_declared_named_row(declared: "tuple[ParameterDeclaration, ...]", supplied: "dict[str, Any]") -> None:
    """Validate a single named-parameter mapping against declared params.

    Required params must be present. Missing optional params are bound as
    ``None`` so SQL receives ``NULL``. A present non-``None`` value whose
    declared type resolves via the registry must satisfy that matcher.
    Unresolved types are documentation-only. Extra keys are ignored.
    """
    _apply_declared_optional_defaults(declared, supplied)
    for declaration in declared:
        name = declaration.name
        if name not in supplied:
            msg = f"Missing required parameter '{name}' for declared SQL statement."
            raise SQLSpecError(msg)
        value = supplied[name]
        if value is None:
            continue
        if not matches_param_type(declaration.type_str, value):
            msg = f"Parameter '{name}' expected type '{declaration.type_str}' but got {type(value).__name__}."
            raise SQLSpecError(msg)


def _many_parameters_already_prepared(
    parameters: "list[Any]", statement_config: "StatementConfig", applied_wrap_types: bool
) -> bool:
    """Return True when execute_many parameters were already coerced during compile()."""
    if statement_config.parameter_config.type_coercion_map:
        return True
    if applied_wrap_types:
        return False
    if not parameters:
        return True
    first = parameters[0]
    if isinstance(first, (tuple, list)):
        return not any(type(v) is TypedParameter for v in first)
    if isinstance(first, dict):
        return not any(type(v) is TypedParameter for v in first.values())
    return type(first) is not TypedParameter


def _row_needs_declared_default(declared: "tuple[ParameterDeclaration, ...]", supplied: "dict[str, Any]") -> bool:
    """Return True when any optional declared parameter is missing from supplied."""
    return any(not declaration.required and declaration.name not in supplied for declaration in declared)


def _check_declared_parameters(sql_statement: "SQL") -> None:
    """Enforce declared-parameter contracts on a copy of the user params.

    No-op unless the statement carries declarations and is not a script. Runs before
    driver style conversion, so declared names and raw values are intact. Named binding
    is validated for presence and type across all rows without mutating caller dicts;
    positional binding is skipped (arity is validated at load time).
    """
    declared = sql_statement.declared_parameters
    if not declared or sql_statement.is_script:
        return
    if sql_statement.is_many:
        rows = sql_statement.positional_parameters
        if rows and isinstance(rows[0], dict):
            copied_rows: list[Any] | None = None
            for idx, row in enumerate(rows):
                if isinstance(row, dict):
                    if _row_needs_declared_default(declared, row):
                        row_copy = dict(row)
                        _check_declared_named_row(declared, row_copy)
                        if copied_rows is None:
                            copied_rows = list(rows[:idx])
                        copied_rows.append(row_copy)
                        continue
                    _check_declared_named_row(declared, row)
                if copied_rows is not None:
                    copied_rows.append(row)
            if copied_rows is not None:
                sql_statement._positional_parameters = copied_rows
        return
    if sql_statement.positional_parameters:
        return
    named_params = sql_statement.named_parameters
    if _row_needs_declared_default(declared, named_params):
        named_copy = dict(named_params)
        _check_declared_named_row(declared, named_copy)
        sql_statement._named_parameters = named_copy
    else:
        _check_declared_named_row(declared, named_params)
