"""Common driver attributes and utilities."""

import graphlib
import hashlib
import logging
import re
from contextlib import suppress
from time import perf_counter
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Final,
    Literal,
    NamedTuple,
    NoReturn,
    Optional,
    Protocol,
    TypeVar,
    cast,
    overload,
)

from mypy_extensions import mypyc_attr, trait
from sqlglot import exp

from sqlspec.builder import QueryBuilder
from sqlspec.core import (
    SQL,
    CachedStatement,
    ParameterStyle,
    SQLResult,
    Statement,
    StatementConfig,
    TypedParameter,
    get_cache,
    get_cache_config,
    split_sql_script,
)
from sqlspec.core.metrics import StackExecutionMetrics
from sqlspec.driver._storage_helpers import CAPABILITY_HINTS
from sqlspec.exceptions import ImproperConfigurationError, NotFoundError, StorageCapabilityError
from sqlspec.observability import ObservabilityRuntime
from sqlspec.protocols import StatementProtocol
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.schema import to_schema as _to_schema_impl
from sqlspec.utils.type_guards import (
    has_array_interface,
    has_cursor_metadata,
    has_dtype_str,
    has_statement_type,
    has_typecode,
    has_typecode_and_len,
    is_statement_filter,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.core import FilterTypeT, StatementFilter
    from sqlspec.core.stack import StatementStack
    from sqlspec.storage import AsyncStoragePipeline, StorageCapabilities, SyncStoragePipeline
    from sqlspec.typing import SchemaT, StatementParameters


__all__ = (
    "DEFAULT_EXECUTION_RESULT",
    "EXEC_CURSOR_RESULT",
    "EXEC_ROWCOUNT_OVERRIDE",
    "EXEC_SPECIAL_DATA",
    "AsyncExceptionHandler",
    "ColumnMetadata",
    "CommonDriverAttributesMixin",
    "DataDictionaryMixin",
    "ExecutionResult",
    "ForeignKeyMetadata",
    "IndexMetadata",
    "ScriptExecutionResult",
    "StackExecutionObserver",
    "SyncExceptionHandler",
    "VersionInfo",
    "describe_stack_statement",
    "handle_single_row_error",
    "hash_stack_operations",
    "make_cache_key_hashable",
)


class SyncExceptionHandler(Protocol):
    """Protocol for synchronous exception handlers with deferred exception pattern.

    Exception handlers implement this protocol to avoid ABI boundary violations
    with mypyc-compiled code. Instead of raising exceptions from __exit__,
    handlers store mapped exceptions in pending_exception for the caller to raise.
    """

    pending_exception: Exception | None

    def __enter__(self) -> "SyncExceptionHandler": ...

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool: ...


class AsyncExceptionHandler(Protocol):
    """Protocol for asynchronous exception handlers with deferred exception pattern.

    Exception handlers implement this protocol to avoid ABI boundary violations
    with mypyc-compiled code. Instead of raising exceptions from __aexit__,
    handlers store mapped exceptions in pending_exception for the caller to raise.
    """

    pending_exception: Exception | None

    async def __aenter__(self) -> "AsyncExceptionHandler": ...

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool: ...


logger = get_logger("driver")

DriverT = TypeVar("DriverT")
VERSION_GROUPS_MIN_FOR_MINOR = 1
VERSION_GROUPS_MIN_FOR_PATCH = 2


class ForeignKeyMetadata:
    """Metadata for a foreign key constraint."""

    __slots__ = (
        "column_name",
        "constraint_name",
        "referenced_column",
        "referenced_schema",
        "referenced_table",
        "schema",
        "table_name",
    )

    def __init__(
        self,
        table_name: str,
        column_name: str,
        referenced_table: str,
        referenced_column: str,
        constraint_name: str | None = None,
        schema: str | None = None,
        referenced_schema: str | None = None,
    ) -> None:
        self.table_name = table_name
        self.column_name = column_name
        self.referenced_table = referenced_table
        self.referenced_column = referenced_column
        self.constraint_name = constraint_name
        self.schema = schema
        self.referenced_schema = referenced_schema

    def __repr__(self) -> str:
        return (
            f"ForeignKeyMetadata(table_name={self.table_name!r}, column_name={self.column_name!r}, "
            f"referenced_table={self.referenced_table!r}, referenced_column={self.referenced_column!r}, "
            f"constraint_name={self.constraint_name!r}, schema={self.schema!r}, "
            f"referenced_schema={self.referenced_schema!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ForeignKeyMetadata):
            return NotImplemented
        return (
            self.table_name == other.table_name
            and self.column_name == other.column_name
            and self.referenced_table == other.referenced_table
            and self.referenced_column == other.referenced_column
            and self.constraint_name == other.constraint_name
            and self.schema == other.schema
            and self.referenced_schema == other.referenced_schema
        )

    def __hash__(self) -> int:
        return hash((
            self.table_name,
            self.column_name,
            self.referenced_table,
            self.referenced_column,
            self.constraint_name,
            self.schema,
            self.referenced_schema,
        ))


class ColumnMetadata:
    """Metadata for a database column."""

    __slots__ = ("data_type", "default_value", "max_length", "name", "nullable", "precision", "primary_key", "scale")

    def __init__(
        self,
        name: str,
        data_type: str,
        nullable: bool,
        default_value: str | None = None,
        primary_key: bool = False,
        max_length: int | None = None,
        precision: int | None = None,
        scale: int | None = None,
    ) -> None:
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
        self.default_value = default_value
        self.primary_key = primary_key
        self.max_length = max_length
        self.precision = precision
        self.scale = scale

    def __repr__(self) -> str:
        return (
            f"ColumnMetadata(name={self.name!r}, data_type={self.data_type!r}, nullable={self.nullable!r}, "
            f"default_value={self.default_value!r}, primary_key={self.primary_key!r}, max_length={self.max_length!r}, "
            f"precision={self.precision!r}, scale={self.scale!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ColumnMetadata):
            return NotImplemented
        return (
            self.name == other.name
            and self.data_type == other.data_type
            and self.nullable == other.nullable
            and self.default_value == other.default_value
            and self.primary_key == other.primary_key
            and self.max_length == other.max_length
            and self.precision == other.precision
            and self.scale == other.scale
        )

    def __hash__(self) -> int:
        return hash((
            self.name,
            self.data_type,
            self.nullable,
            self.default_value,
            self.primary_key,
            self.max_length,
            self.precision,
            self.scale,
        ))


class IndexMetadata:
    """Metadata for a database index."""

    __slots__ = ("columns", "name", "primary", "table_name", "unique")

    def __init__(
        self, name: str, table_name: str, columns: list[str], unique: bool = False, primary: bool = False
    ) -> None:
        self.name = name
        self.table_name = table_name
        self.columns = columns
        self.unique = unique
        self.primary = primary

    def __repr__(self) -> str:
        return (
            f"IndexMetadata(name={self.name!r}, table_name={self.table_name!r}, columns={self.columns!r}, "
            f"unique={self.unique!r}, primary={self.primary!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IndexMetadata):
            return NotImplemented
        return (
            self.name == other.name
            and self.table_name == other.table_name
            and self.columns == other.columns
            and self.unique == other.unique
            and self.primary == other.primary
        )

    def __hash__(self) -> int:
        return hash((self.name, self.table_name, tuple(self.columns), self.unique, self.primary))


_CONVERT_TO_TUPLE = object()
_CONVERT_TO_FROZENSET = object()


def make_cache_key_hashable(obj: Any) -> Any:
    """Recursively convert unhashable types to hashable ones for cache keys.

    Uses an iterative stack-based approach to avoid C-stack recursion limits
    in mypyc-compiled code.

    For array-like objects (NumPy arrays, Python arrays, etc.), we use structural
    info (dtype + shape or typecode + length) rather than content for cache keys.

    Args:
        obj: Object to make hashable.

    Returns:
        A hashable representation of the object. Collections become tuples,
        arrays become structural tuples like ("ndarray", dtype, shape).
    """
    # Fast path for common immutable scalar types
    if isinstance(obj, (int, str, bytes, bool, float, type(None))):
        return obj

    # Stack contains tuples of (object, parent_list, index_in_parent)
    # We build the result in-place in temporary lists, then convert to tuples/sets
    # A placeholder list is used as the root "parent"
    root: list[Any] = [obj]
    stack = [(obj, root, 0)]

    while stack:
        current_obj, parent, idx = stack.pop()

        # Post-processing markers
        if current_obj is _CONVERT_TO_TUPLE:
            parent[idx] = tuple(parent[idx])
            continue

        if current_obj is _CONVERT_TO_FROZENSET:
            parent[idx] = frozenset(parent[idx])
            continue

        # Handle structural types (arrays) - these are terminal nodes
        if has_typecode_and_len(current_obj):
            parent[idx] = ("array", current_obj.typecode, len(current_obj))
            continue
        if has_typecode(current_obj):
            parent[idx] = ("array", current_obj.typecode)
            continue
        if has_array_interface(current_obj):
            try:
                dtype_str = current_obj.dtype.str if has_dtype_str(current_obj.dtype) else str(type(current_obj))
                shape = tuple(int(s) for s in current_obj.shape)
                parent[idx] = ("ndarray", dtype_str, shape)
            except (AttributeError, TypeError):
                try:
                    length = len(current_obj)
                    parent[idx] = ("array_like", type(current_obj).__name__, length)
                except (AttributeError, TypeError):
                    parent[idx] = ("array_like", type(current_obj).__name__)
            continue

        # Handle collections
        if isinstance(current_obj, (list, tuple)):
            # Create a new list for transformed items
            new_list = [None] * len(current_obj)
            parent[idx] = new_list  # Placeholder, will be converted to tuple later

            # Push marker first so it is processed LAST (LIFO)
            stack.append((_CONVERT_TO_TUPLE, parent, idx))

            # Push items in reverse order
            stack.extend((current_obj[i], new_list, i) for i in range(len(current_obj) - 1, -1, -1))
            continue

        if isinstance(current_obj, dict):
            # Sort items by key for deterministic caching
            try:
                sorted_items = sorted(current_obj.items())
            except TypeError:
                sorted_items = list(current_obj.items())

            items_list = []
            for k, v in sorted_items:
                items_list.append([k, v])  # Temporary list [k, v]

            parent[idx] = items_list  # Will become tuple(tuple(k, v')...)

            # Push marker first
            stack.append((_CONVERT_TO_TUPLE, parent, idx))  # Convert items_list to tuple of tuples

            # Push children
            for i in range(len(items_list) - 1, -1, -1):
                # items_list[i] is [k, v]. We want to transform items_list[i][1].
                # items_list[i] needs to become (k, v').
                stack.append((_CONVERT_TO_TUPLE, items_list, i))  # Convert [k, v'] to (k, v')
                stack.append((items_list[i][1], items_list[i], 1))  # Transform v

            continue

        if isinstance(current_obj, set):
            # Convert to list, sort if possible
            try:
                sorted_list = sorted(current_obj)
            except TypeError:
                sorted_list = list(current_obj)

            new_list = [None] * len(sorted_list)
            parent[idx] = new_list

            # Push marker first
            stack.append((_CONVERT_TO_FROZENSET, parent, idx))

            stack.extend((sorted_list[i], new_list, i) for i in range(len(sorted_list) - 1, -1, -1))
            continue

        # Base case: Object is likely hashable or unknown
        parent[idx] = current_obj

    return root[0]


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

    def __enter__(self) -> "StackExecutionObserver":
        self.started = perf_counter()
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
            stack_size=len(self.stack.operations),
            continue_on_error=self.continue_on_error,
            native_pipeline=self.native_pipeline,
            forced_disable=self.driver.stack_native_disabled,
            hashed_operations=self.hashed_operations,
        )
        return self

    def __exit__(self, exc_type: Any, exc: Exception | None, exc_tb: Any) -> Literal[False]:
        duration = perf_counter() - self.started
        self.metrics.record_duration(duration)
        if exc is not None:
            self.metrics.record_error(exc)
        self.runtime.span_manager.end_span(self.span, error=exc if exc is not None else None)
        self.metrics.emit(self.runtime)
        level = logging.ERROR if exc is not None else logging.DEBUG
        log_with_context(
            logger,
            level,
            "stack.execute.failed" if exc is not None else "stack.execute.complete",
            driver=type(self.driver).__name__,
            stack_size=len(self.stack.operations),
            continue_on_error=self.continue_on_error,
            native_pipeline=self.native_pipeline,
            forced_disable=self.driver.stack_native_disabled,
            hashed_operations=self.hashed_operations,
            duration_s=duration,
            error_type=type(exc).__name__ if exc is not None else None,
        )
        return False

    def record_operation_error(self, error: Exception) -> None:
        """Record an operation error when continue-on-error is enabled."""
        self.metrics.record_operation_error(error)


def describe_stack_statement(statement: "StatementProtocol | str") -> str:
    """Return a readable representation of a stack statement for diagnostics."""
    if isinstance(statement, str):
        return statement
    if isinstance(statement, StatementProtocol):
        return statement.raw_sql or statement.sql
    return repr(statement)


def handle_single_row_error(error: ValueError) -> "NoReturn":
    """Normalize single-row selection errors to SQLSpec exceptions."""
    message = str(error)
    if message.startswith("No result found"):
        msg = "No rows found"
        raise NotFoundError(msg) from error
    raise error


@mypyc_attr(allow_interpreted_subclasses=True)
class VersionInfo:
    """Database version information."""

    def __init__(self, major: int, minor: int = 0, patch: int = 0) -> None:
        """Initialize version info.

        Args:
            major: Major version number
            minor: Minor version number
            patch: Patch version number

        """
        self.major = major
        self.minor = minor
        self.patch = patch

    @property
    def version_tuple(self) -> "tuple[int, int, int]":
        """Get version as tuple for comparison."""
        return (self.major, self.minor, self.patch)

    def __str__(self) -> str:
        """String representation of version info."""
        return f"{self.major}.{self.minor}.{self.patch}"

    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"VersionInfo({self.major}, {self.minor}, {self.patch})"

    def __eq__(self, other: object) -> bool:
        """Check version equality."""
        if not isinstance(other, VersionInfo):
            return NotImplemented
        return self.version_tuple == other.version_tuple

    def __lt__(self, other: "VersionInfo") -> bool:
        """Check if this version is less than another."""
        return self.version_tuple < other.version_tuple

    def __le__(self, other: "VersionInfo") -> bool:
        """Check if this version is less than or equal to another."""
        return self.version_tuple <= other.version_tuple

    def __gt__(self, other: "VersionInfo") -> bool:
        """Check if this version is greater than another."""
        return self.version_tuple > other.version_tuple

    def __ge__(self, other: "VersionInfo") -> bool:
        """Check if this version is greater than or equal to another."""
        return self.version_tuple >= other.version_tuple

    def __hash__(self) -> int:
        """Make VersionInfo hashable based on version tuple."""
        return hash(self.version_tuple)


@mypyc_attr(allow_interpreted_subclasses=True)
@trait
class DataDictionaryMixin:
    """Mixin providing common data dictionary functionality.

    Includes version caching to avoid repeated database queries when checking
    feature flags or optimal types.
    """

    __slots__ = ("_version_cache", "_version_fetch_attempted")

    _version_cache: "dict[int, VersionInfo | None]"
    _version_fetch_attempted: "set[int]"

    def __init__(self) -> None:
        self._version_cache = {}
        self._version_fetch_attempted = set()

    def get_cached_version(self, driver_id: int) -> "tuple[bool, VersionInfo | None]":
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

    def parse_version_string(self, version_str: str) -> "VersionInfo | None":
        """Parse version string into VersionInfo.

        Args:
            version_str: Raw version string from database

        Returns:
            VersionInfo instance or None if parsing fails

        """
        patterns = [r"(\d+)\.(\d+)\.(\d+)", r"(\d+)\.(\d+)", r"(\d+)"]

        for pattern in patterns:
            match = re.search(pattern, version_str)
            if match:
                groups = match.groups()

                major = int(groups[0])
                minor = int(groups[1]) if len(groups) > VERSION_GROUPS_MIN_FOR_MINOR else 0
                patch = int(groups[2]) if len(groups) > VERSION_GROUPS_MIN_FOR_PATCH else 0
                return VersionInfo(major, minor, patch)

        return None

    def detect_version_with_queries(self, driver: Any, queries: "list[str]") -> "VersionInfo | None":
        """Try multiple version queries to detect database version.

        Args:
            driver: Database driver instance
            queries: List of SQL queries to try

        Returns:
            Version information or None if detection fails

        """
        for query in queries:
            with suppress(Exception):
                result = driver.execute(query)
                if result.data:
                    version_str = str(result.data[0])
                    if isinstance(result.data[0], dict):
                        version_str = str(next(iter(result.data[0].values())))
                    elif isinstance(result.data[0], (list, tuple)):
                        version_str = str(result.data[0][0])

                    parsed_version = self.parse_version_string(version_str)
                    if parsed_version:
                        logger.debug("Detected database version: %s", parsed_version)
                        return parsed_version

        logger.warning("Could not detect database version")
        return None

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

    def get_default_features(self) -> "list[str]":
        """Get default feature flags supported by most databases.

        Returns:
            List of commonly supported feature names

        """
        return ["supports_transactions", "supports_prepared_statements"]

    def sort_tables_topologically(self, tables: "list[str]", foreign_keys: "list[ForeignKeyMetadata]") -> "list[str]":
        """Sort tables topologically based on foreign key dependencies using Python.

        Args:
            tables: List of table names.
            foreign_keys: List of foreign key metadata.

        Returns:
            List of table names in topological order (dependencies first).

        Raises:
            CycleError: If a dependency cycle is detected.

        """
        sorter: graphlib.TopologicalSorter[str] = graphlib.TopologicalSorter()
        for table in tables:
            sorter.add(table)

        for fk in foreign_keys:
            # If self-referencing, ignore for sorting purposes to avoid simple cycles
            if fk.table_name == fk.referenced_table:
                continue
            # table_name depends on referenced_table
            sorter.add(fk.table_name, fk.referenced_table)

        return list(sorter.static_order())


class ScriptExecutionResult(NamedTuple):
    """Result from script execution with statement count information."""

    cursor_result: Any
    rowcount_override: int | None
    special_data: Any
    statement_count: int
    successful_statements: int


class ExecutionResult(NamedTuple):
    """Execution result containing all data needed for SQLResult building."""

    cursor_result: Any
    rowcount_override: int | None
    special_data: Any
    selected_data: Optional["list[dict[str, Any]]"]
    column_names: Optional["list[str]"]
    data_row_count: int | None
    statement_count: int | None
    successful_statements: int | None
    is_script_result: bool
    is_select_result: bool
    is_many_result: bool
    last_inserted_id: int | str | None = None


EXEC_CURSOR_RESULT: Final[int] = 0
EXEC_ROWCOUNT_OVERRIDE: Final[int] = 1
EXEC_SPECIAL_DATA: Final[int] = 2
DEFAULT_EXECUTION_RESULT: Final[tuple[Any, int | None, Any]] = (None, None, None)


@trait
@mypyc_attr(allow_interpreted_subclasses=True)
class CommonDriverAttributesMixin:
    """Common attributes and methods for driver adapters."""

    __slots__ = ("_observability", "connection", "driver_features", "statement_config")
    connection: "Any"
    statement_config: "StatementConfig"
    driver_features: "dict[str, Any]"

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
        self.statement_config = statement_config
        self.driver_features = driver_features or {}
        self._observability = observability

    def attach_observability(self, runtime: "ObservabilityRuntime") -> None:
        """Attach or replace the observability runtime."""
        self._observability = runtime

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

    storage_pipeline_factory: "ClassVar[type[SyncStoragePipeline | AsyncStoragePipeline] | None]" = None

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

    def _raise_storage_not_implemented(self, capability: str) -> None:
        """Raise NotImplementedError for storage operations.

        Args:
            capability: The capability that is not implemented.

        Raises:
            StorageCapabilityError: Always raised.

        """
        msg = f"{capability} is not implemented for this driver"
        remediation = "Override storage methods on the adapter to enable this capability."
        raise StorageCapabilityError(msg, capability=capability, remediation=remediation)

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
        selected_data: Optional["list[dict[str, Any]]"] = None,
        column_names: Optional["list[str]"] = None,
        data_row_count: int | None = None,
        statement_count: int | None = None,
        successful_statements: int | None = None,
        is_script_result: bool = False,
        is_select_result: bool = False,
        is_many_result: bool = False,
        last_inserted_id: int | str | None = None,
    ) -> ExecutionResult:
        """Create ExecutionResult with all necessary data for any operation type.

        Args:
            cursor_result: The raw result returned by the database cursor/driver
            rowcount_override: Optional override for the number of affected rows
            special_data: Any special metadata or additional information
            selected_data: For SELECT operations, the extracted row data
            column_names: For SELECT operations, the column names
            data_row_count: For SELECT operations, the number of rows returned
            statement_count: For script operations, total number of statements
            successful_statements: For script operations, number of successful statements
            is_script_result: Whether this result is from script execution
            is_select_result: Whether this result is from a SELECT operation
            is_many_result: Whether this result is from an execute_many operation
            last_inserted_id: The ID of the last inserted row (if applicable)

        Returns:
            ExecutionResult configured for the specified operation type

        """
        return ExecutionResult(
            cursor_result=cursor_result,
            rowcount_override=rowcount_override,
            special_data=special_data,
            selected_data=selected_data,
            column_names=column_names,
            data_row_count=data_row_count,
            statement_count=statement_count,
            successful_statements=successful_statements,
            is_script_result=is_script_result,
            is_select_result=is_select_result,
            is_many_result=is_many_result,
            last_inserted_id=last_inserted_id,
        )

    def build_statement_result(self, statement: "SQL", execution_result: ExecutionResult) -> "SQLResult":
        """Build and return the SQLResult from ExecutionResult data.

        Args:
            statement: SQL statement that was executed
            execution_result: ExecutionResult containing all necessary data

        Returns:
            SQLResult with complete execution data

        """
        if execution_result.is_script_result:
            return SQLResult(
                statement=statement,
                data=[],
                rows_affected=execution_result.rowcount_override or 0,
                operation_type="SCRIPT",
                total_statements=execution_result.statement_count or 0,
                successful_statements=execution_result.successful_statements or 0,
                metadata=execution_result.special_data or {"status_message": "OK"},
            )

        if execution_result.is_select_result:
            return SQLResult(
                statement=statement,
                data=execution_result.selected_data or [],
                column_names=execution_result.column_names or [],
                rows_affected=execution_result.data_row_count or 0,
                operation_type="SELECT",
                metadata=execution_result.special_data or {},
            )

        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=execution_result.rowcount_override or 0,
            operation_type=statement.operation_type,
            last_inserted_id=execution_result.last_inserted_id,
            metadata=execution_result.special_data or {"status_message": "OK"},
        )

    def _should_force_select(self, statement: "SQL", cursor: Any) -> bool:
        """Determine if a statement with unknown type should be treated as SELECT.

        Uses driver metadata (statement_type, description/schema) as a safety net when
        the compiler cannot classify the operation. This remains conservative by only
        triggering when the operation type is "UNKNOWN".

        Args:
            statement: SQL statement being executed.
            cursor: Database cursor/job object that may expose metadata.

        Returns:
            True when cursor metadata indicates a row-returning operation despite an
            unknown operation type; otherwise False.

        """
        if statement.operation_type != "UNKNOWN":
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
        kwargs = kwargs or {}
        filters, data_parameters = self._split_parameters(parameters)

        if isinstance(statement, QueryBuilder):
            sql_statement = self._prepare_from_builder(statement, data_parameters, statement_config, kwargs)
        elif isinstance(statement, SQL):
            sql_statement = self._prepare_from_sql(statement, data_parameters, statement_config, kwargs)
        else:
            sql_statement = self._prepare_from_string(statement, data_parameters, statement_config, kwargs)

        return self._apply_filters(sql_statement, filters)

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
            return SQL(sql_statement.sql, *merged_parameters, statement_config=statement_config, **kwargs)
        return sql_statement

    def _prepare_from_sql(
        self,
        sql_statement: "SQL",
        data_parameters: "list[StatementParameters]",
        statement_config: "StatementConfig",
        kwargs: "dict[str, Any]",
    ) -> "SQL":
        if data_parameters or kwargs:
            merged_parameters = (
                (*sql_statement.positional_parameters, *tuple(data_parameters))
                if data_parameters
                else sql_statement.positional_parameters
            )
            return SQL(sql_statement.sql, *merged_parameters, statement_config=statement_config, **kwargs)

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
            sql_text = sql_statement.raw_sql or sql_statement.sql
            if sql_statement.is_many and sql_statement.parameters:
                return SQL(sql_text, sql_statement.parameters, statement_config=statement_config, is_many=True)
            if sql_statement.named_parameters:
                return SQL(sql_text, statement_config=statement_config, **sql_statement.named_parameters)
            return SQL(sql_text, *sql_statement.positional_parameters, statement_config=statement_config)
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

    def split_script_statements(
        self, script: str, statement_config: "StatementConfig", strip_trailing_semicolon: bool = False
    ) -> list[str]:
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
        parameters: Any,
        statement_config: "StatementConfig",
        is_many: bool = False,
        prepared_statement: Any | None = None,  # pyright: ignore[reportUnusedParameter]
    ) -> Any:
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
                return [self._format_parameter_set_for_many(param_set, statement_config) for param_set in parameters]
            return [self._format_parameter_set_for_many(parameters, statement_config)]
        return self._format_parameter_set(parameters, statement_config)

    def _apply_coercion(self, value: Any, statement_config: "StatementConfig") -> Any:
        """Apply type coercion to a single value.

        Args:
            value: Value to coerce (may be TypedParameter or raw value)
            statement_config: Statement configuration for type coercion map

        Returns:
            Coerced value with TypedParameter unwrapped

        """
        unwrapped_value = value.value if isinstance(value, TypedParameter) else value
        if statement_config.parameter_config.type_coercion_map:
            for type_check, converter in statement_config.parameter_config.type_coercion_map.items():
                if isinstance(unwrapped_value, type_check):
                    return converter(unwrapped_value)
        return unwrapped_value

    def _format_parameter_set_for_many(self, parameters: Any, statement_config: "StatementConfig") -> Any:
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

        if not isinstance(parameters, (dict, list, tuple)):
            return self._apply_coercion(parameters, statement_config)

        if isinstance(parameters, dict):
            return {k: self._apply_coercion(v, statement_config) for k, v in parameters.items()}

        coerced_params = [self._apply_coercion(p, statement_config) for p in parameters]
        return tuple(coerced_params) if isinstance(parameters, tuple) else coerced_params

    def _format_parameter_set(self, parameters: Any, statement_config: "StatementConfig") -> Any:
        """Prepare a single parameter set for database driver consumption.

        Args:
            parameters: Single parameter set in any format
            statement_config: Statement configuration for parameter style detection

        Returns:
            Processed parameter set with TypedParameter objects unwrapped and type coercion applied

        """
        if not parameters:
            return []

        if not isinstance(parameters, (dict, list, tuple)):
            return [self._apply_coercion(parameters, statement_config)]

        if isinstance(parameters, dict):
            if statement_config.parameter_config.supported_execution_parameter_styles and (
                ParameterStyle.NAMED_PYFORMAT in statement_config.parameter_config.supported_execution_parameter_styles
                or ParameterStyle.NAMED_COLON in statement_config.parameter_config.supported_execution_parameter_styles
            ):
                return {k: self._apply_coercion(v, statement_config) for k, v in parameters.items()}
            if statement_config.parameter_config.default_parameter_style in {
                ParameterStyle.NUMERIC,
                ParameterStyle.QMARK,
                ParameterStyle.POSITIONAL_PYFORMAT,
            }:
                sorted_items = sorted(
                    parameters.items(),
                    key=lambda item: (
                        int(item[0])
                        if item[0].isdigit()
                        else (
                            int(item[0][6:]) if item[0].startswith("param_") and item[0][6:].isdigit() else float("inf")
                        )
                    ),
                )
                return [self._apply_coercion(value, statement_config) for _, value in sorted_items]

            return {k: self._apply_coercion(v, statement_config) for k, v in parameters.items()}

        coerced_params = [self._apply_coercion(p, statement_config) for p in parameters]
        if statement_config.parameter_config.preserve_parameter_format and isinstance(parameters, tuple):
            return tuple(coerced_params)
        return coerced_params

    def _get_compiled_sql(
        self, statement: "SQL", statement_config: "StatementConfig", flatten_single_parameters: bool = False
    ) -> tuple[str, Any]:
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
        cache_config = get_cache_config()
        cache_key = None
        if cache_config.compiled_cache_enabled and statement_config.enable_caching:
            cache_key = self._generate_compilation_cache_key(statement, statement_config, flatten_single_parameters)
            cache = get_cache()
            cached_result = cache.get("statement", cache_key, str(statement.dialect) if statement.dialect else None)
            if cached_result is not None and isinstance(cached_result, CachedStatement):
                return cached_result.compiled_sql, cached_result.parameters

        prepared_statement = self.prepare_statement(statement, statement_config=statement_config)
        compiled_sql, execution_parameters = prepared_statement.compile()

        prepared_parameters = self.prepare_driver_parameters(
            execution_parameters,
            statement_config,
            is_many=prepared_statement.is_many,
            prepared_statement=prepared_statement,
        )

        if statement_config.parameter_config.output_transformer:
            compiled_sql, prepared_parameters = statement_config.parameter_config.output_transformer(
                compiled_sql, prepared_parameters
            )

        if cache_key is not None:
            cache = get_cache()
            cached_statement = CachedStatement(
                compiled_sql=compiled_sql,
                parameters=tuple(prepared_parameters)
                if isinstance(prepared_parameters, list)
                else (
                    prepared_parameters
                    if prepared_parameters is None or isinstance(prepared_parameters, dict)
                    else (
                        tuple(prepared_parameters)
                        if not isinstance(prepared_parameters, tuple)
                        else prepared_parameters
                    )
                ),
                expression=prepared_statement.expression,
            )
            cache.put("statement", cache_key, cached_statement, str(statement.dialect) if statement.dialect else None)

        return compiled_sql, prepared_parameters

    def _generate_compilation_cache_key(
        self, statement: "SQL", config: "StatementConfig", flatten_single_parameters: bool
    ) -> str:
        """Generate cache key that includes all compilation context.

        Creates a deterministic cache key that includes all factors that affect SQL compilation,
        preventing cache contamination between different compilation contexts.
        """
        context_hash = hash((
            config.parameter_config.hash(),
            config.dialect,
            statement.is_script,
            statement.is_many,
            flatten_single_parameters,
            bool(config.parameter_config.output_transformer),
            bool(config.parameter_config.ast_transformer),
            bool(config.parameter_config.needs_static_script_compilation),
        ))

        params = statement.parameters

        if params is None or (isinstance(params, (list, tuple, dict)) and not params):
            return f"compiled:{hash(statement.sql)}:{context_hash}"

        if isinstance(params, tuple) and all(isinstance(p, (int, str, bytes, bool, type(None))) for p in params):
            try:
                return (
                    f"compiled:{hash((statement.sql, params, statement.is_many, statement.is_script))}:{context_hash}"
                )
            except TypeError:
                pass

        try:
            if isinstance(params, dict):
                params_key = make_cache_key_hashable(params)
            elif isinstance(params, (list, tuple)) and params:
                if isinstance(params[0], dict):
                    params_key = tuple(make_cache_key_hashable(d) for d in params)
                else:
                    params_key = make_cache_key_hashable(params)
            elif isinstance(params, (list, tuple)):
                params_key = ()
            else:
                params_key = params
        except (TypeError, AttributeError):
            params_key = str(params)

        base_hash = hash((statement.sql, params_key, statement.is_many, statement.is_script))
        return f"compiled:{base_hash}:{context_hash}"

    def _get_dominant_parameter_style(self, parameters: "list[Any]") -> "ParameterStyle | None":
        """Determine the dominant parameter style from parameter info list.

        Args:
            parameters: List of ParameterInfo objects from validator.extract_parameters()

        Returns:
            The dominant parameter style, or None if no parameters

        """
        if not parameters:
            return None

        style_counts: dict[ParameterStyle, int] = {}
        for param in parameters:
            style_counts[param.style] = style_counts.get(param.style, 0) + 1

        precedence = {
            ParameterStyle.QMARK: 1,
            ParameterStyle.NUMERIC: 2,
            ParameterStyle.POSITIONAL_COLON: 3,
            ParameterStyle.POSITIONAL_PYFORMAT: 4,
            ParameterStyle.NAMED_AT: 5,
            ParameterStyle.NAMED_DOLLAR: 6,
            ParameterStyle.NAMED_COLON: 7,
            ParameterStyle.NAMED_PYFORMAT: 8,
        }

        return max(style_counts.keys(), key=lambda style: (style_counts[style], -precedence.get(style, 99)))

    @staticmethod
    def find_filter(
        filter_type: "type[FilterTypeT]",
        filters: "Sequence[StatementFilter | StatementParameters] | Sequence[StatementFilter]",
    ) -> "FilterTypeT | None":
        """Get the filter specified by filter type from the filters.

        Args:
            filter_type: The type of filter to find.
            filters: filter types to apply to the query

        Returns:
            The match filter instance or None

        """
        for filter_ in filters:
            if isinstance(filter_, filter_type):
                return filter_
        return None

    def _create_count_query(self, original_sql: "SQL") -> "SQL":
        """Create a COUNT query from the original SQL statement.

        Transforms the original SELECT statement to count total rows while preserving
        WHERE, HAVING, and GROUP BY clauses but removing ORDER BY, LIMIT, and OFFSET.
        """
        if not original_sql.expression:
            original_sql.compile()

        if not original_sql.expression:
            msg = "Cannot create COUNT query from empty SQL expression"
            raise ImproperConfigurationError(msg)

        expr = original_sql.expression

        if isinstance(expr, exp.Select):
            from_clause = expr.args.get("from")
            if from_clause is None:
                # Fallback: try alternate keys or inferred tables
                from_clause = expr.args.get("froms")
            if from_clause is None:
                tables = list(expr.find_all(exp.Table))
                if tables:
                    first_table = tables[0]
                    from_clause = exp.from_(first_table)
            if from_clause is None:
                msg = (
                    "Cannot create COUNT query: SELECT statement missing FROM clause. "
                    "COUNT queries require a FROM clause to determine which table to count rows from."
                )
                raise ImproperConfigurationError(msg)

            if expr.args.get("group"):
                subquery = expr.subquery(alias="grouped_data")
                count_expr = exp.select(exp.Count(this=exp.Star())).from_(subquery)
            else:
                source_from = cast("exp.Expression", from_clause)
                count_expr = exp.select(exp.Count(this=exp.Star())).from_(source_from, copy=False)
                if expr.args.get("where"):
                    count_expr = count_expr.where(cast("exp.Expression", expr.args.get("where")), copy=False)
                if expr.args.get("having"):
                    count_expr = count_expr.having(cast("exp.Expression", expr.args.get("having")), copy=False)

            count_expr.set("order", None)
            count_expr.set("limit", None)
            count_expr.set("offset", None)

            return SQL(count_expr, *original_sql.positional_parameters, statement_config=original_sql.statement_config)

        subquery = cast("exp.Select", expr).subquery(alias="total_query")
        count_expr = exp.select(exp.Count(this=exp.Star())).from_(subquery)
        return SQL(count_expr, *original_sql.positional_parameters, statement_config=original_sql.statement_config)
