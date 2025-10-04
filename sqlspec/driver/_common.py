"""Common driver attributes and utilities."""

import re
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Final, NamedTuple, Optional, TypeVar, Union, cast

from mypy_extensions import trait
from sqlglot import exp

from sqlspec.builder import QueryBuilder
from sqlspec.core import SQL, ParameterStyle, SQLResult, Statement, StatementConfig, TypedParameter
from sqlspec.core.cache import CachedStatement, get_cache, get_cache_config
from sqlspec.core.splitter import split_sql_script
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.core.filters import FilterTypeT, StatementFilter
    from sqlspec.typing import StatementParameters


__all__ = (
    "DEFAULT_EXECUTION_RESULT",
    "EXEC_CURSOR_RESULT",
    "EXEC_ROWCOUNT_OVERRIDE",
    "EXEC_SPECIAL_DATA",
    "CommonDriverAttributesMixin",
    "DataDictionaryMixin",
    "ExecutionResult",
    "ScriptExecutionResult",
    "VersionInfo",
)


logger = get_logger("driver")

DriverT = TypeVar("DriverT")
VERSION_GROUPS_MIN_FOR_MINOR = 1
VERSION_GROUPS_MIN_FOR_PATCH = 2


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


@trait
class DataDictionaryMixin:
    """Mixin providing common data dictionary functionality."""

    def get_dialect(self, driver: Any) -> str:
        """Get database dialect name.

        Args:
            driver: Database driver instance

        Returns:
            Dialect name (e.g., 'postgres', 'sqlite', 'mysql')
        """
        return str(driver.dialect)

    def parse_version_string(self, version_str: str) -> "Optional[VersionInfo]":
        """Parse version string into VersionInfo.

        Args:
            version_str: Raw version string from database

        Returns:
            VersionInfo instance or None if parsing fails
        """
        # Try common version patterns
        patterns = [
            r"(\d+)\.(\d+)\.(\d+)",  # x.y.z
            r"(\d+)\.(\d+)",  # x.y
            r"(\d+)",  # x
        ]

        for pattern in patterns:
            match = re.search(pattern, version_str)
            if match:
                groups = match.groups()

                major = int(groups[0])
                minor = int(groups[1]) if len(groups) > VERSION_GROUPS_MIN_FOR_MINOR else 0
                patch = int(groups[2]) if len(groups) > VERSION_GROUPS_MIN_FOR_PATCH else 0
                return VersionInfo(major, minor, patch)

        return None

    def detect_version_with_queries(self, driver: Any, queries: "list[str]") -> "Optional[VersionInfo]":
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


class ScriptExecutionResult(NamedTuple):
    """Result from script execution with statement count information."""

    cursor_result: Any
    rowcount_override: Optional[int]
    special_data: Any
    statement_count: int
    successful_statements: int


class ExecutionResult(NamedTuple):
    """Execution result containing all data needed for SQLResult building."""

    cursor_result: Any
    rowcount_override: Optional[int]
    special_data: Any
    selected_data: Optional["list[dict[str, Any]]"]
    column_names: Optional["list[str]"]
    data_row_count: Optional[int]
    statement_count: Optional[int]
    successful_statements: Optional[int]
    is_script_result: bool
    is_select_result: bool
    is_many_result: bool
    last_inserted_id: Optional[Union[int, str]] = None


EXEC_CURSOR_RESULT: Final[int] = 0
EXEC_ROWCOUNT_OVERRIDE: Final[int] = 1
EXEC_SPECIAL_DATA: Final[int] = 2
DEFAULT_EXECUTION_RESULT: Final[tuple[Any, Optional[int], Any]] = (None, None, None)


@trait
class CommonDriverAttributesMixin:
    """Common attributes and methods for driver adapters."""

    __slots__ = ("connection", "driver_features", "statement_config")
    connection: "Any"
    statement_config: "StatementConfig"
    driver_features: "dict[str, Any]"

    def __init__(
        self, connection: "Any", statement_config: "StatementConfig", driver_features: "Optional[dict[str, Any]]" = None
    ) -> None:
        """Initialize driver adapter with connection and configuration.

        Args:
            connection: Database connection instance
            statement_config: Statement configuration for the driver
            driver_features: Driver-specific features like extensions, secrets, and connection callbacks
        """
        self.connection = connection
        self.statement_config = statement_config
        self.driver_features = driver_features or {}

    def create_execution_result(
        self,
        cursor_result: Any,
        *,
        rowcount_override: Optional[int] = None,
        special_data: Any = None,
        selected_data: Optional["list[dict[str, Any]]"] = None,
        column_names: Optional["list[str]"] = None,
        data_row_count: Optional[int] = None,
        statement_count: Optional[int] = None,
        successful_statements: Optional[int] = None,
        is_script_result: bool = False,
        is_select_result: bool = False,
        is_many_result: bool = False,
        last_inserted_id: Optional[Union[int, str]] = None,
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

    def prepare_statement(
        self,
        statement: "Union[Statement, QueryBuilder]",
        parameters: "tuple[Union[StatementParameters, StatementFilter], ...]" = (),
        *,
        statement_config: "StatementConfig",
        kwargs: "Optional[dict[str, Any]]" = None,
    ) -> "SQL":
        """Build SQL statement from various input types.

        Ensures dialect is set and preserves existing state when rebuilding SQL objects.

        Args:
            statement: SQL statement or QueryBuilder to prepare
            parameters: Parameters for the SQL statement
            statement_config: Statement configuration
            kwargs: Additional keyword arguments

        Returns:
            Prepared SQL statement
        """
        kwargs = kwargs or {}

        if isinstance(statement, QueryBuilder):
            sql_statement = statement.to_statement(statement_config)
            if parameters or kwargs:
                merged_parameters = (
                    (*sql_statement.positional_parameters, *parameters)
                    if parameters
                    else sql_statement.positional_parameters
                )
                return SQL(sql_statement.sql, *merged_parameters, statement_config=statement_config, **kwargs)
            return sql_statement
        if isinstance(statement, SQL):
            if parameters or kwargs:
                merged_parameters = (
                    (*statement.positional_parameters, *parameters) if parameters else statement.positional_parameters
                )
                return SQL(statement.sql, *merged_parameters, statement_config=statement_config, **kwargs)
            needs_rebuild = False

            if statement_config.dialect and (
                not statement.statement_config.dialect or statement.statement_config.dialect != statement_config.dialect
            ):
                needs_rebuild = True

            if (
                statement.statement_config.parameter_config.default_execution_parameter_style
                != statement_config.parameter_config.default_execution_parameter_style
            ):
                needs_rebuild = True

            if needs_rebuild:
                sql_text = statement.raw_sql or statement.sql

                if statement.is_many and statement.parameters:
                    new_sql = SQL(sql_text, statement.parameters, statement_config=statement_config, is_many=True)
                elif statement.named_parameters:
                    new_sql = SQL(sql_text, statement_config=statement_config, **statement.named_parameters)
                else:
                    new_sql = SQL(sql_text, *statement.positional_parameters, statement_config=statement_config)

                return new_sql
            return statement
        return SQL(statement, *parameters, statement_config=statement_config, **kwargs)

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
        prepared_statement: Optional[Any] = None,  # pyright: ignore[reportUnusedParameter]
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
                    key=lambda item: int(item[0])
                    if item[0].isdigit()
                    else (int(item[0][6:]) if item[0].startswith("param_") and item[0][6:].isdigit() else float("inf")),
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
            execution_parameters, statement_config, is_many=statement.is_many, prepared_statement=statement
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
                expression=statement.expression,
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
        context_hash = hash(
            (
                config.parameter_config.hash(),
                config.dialect,
                statement.is_script,
                statement.is_many,
                flatten_single_parameters,
                bool(config.parameter_config.output_transformer),
                bool(config.parameter_config.ast_transformer),
                bool(config.parameter_config.needs_static_script_compilation),
            )
        )

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

        params_key: Any

        def make_hashable(obj: Any) -> Any:
            """Recursively convert unhashable types to hashable ones."""
            if isinstance(obj, (list, tuple)):
                return tuple(make_hashable(item) for item in obj)
            if isinstance(obj, dict):
                return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
            if isinstance(obj, set):
                return frozenset(make_hashable(item) for item in obj)
            return obj

        try:
            if isinstance(params, dict):
                params_key = make_hashable(params)
            elif isinstance(params, (list, tuple)) and params:
                if isinstance(params[0], dict):
                    params_key = tuple(make_hashable(d) for d in params)
                else:
                    params_key = make_hashable(params)
            elif isinstance(params, (list, tuple)):
                params_key = ()
            else:
                params_key = params
        except (TypeError, AttributeError):
            params_key = str(params)

        base_hash = hash((statement.sql, params_key, statement.is_many, statement.is_script))
        return f"compiled:{base_hash}:{context_hash}"

    def _get_dominant_parameter_style(self, parameters: "list[Any]") -> "Optional[ParameterStyle]":
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
    ) -> "Optional[FilterTypeT]":
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
            msg = "Cannot create COUNT query from empty SQL expression"
            raise ImproperConfigurationError(msg)
        expr = original_sql.expression

        if isinstance(expr, exp.Select):
            if expr.args.get("group"):
                subquery = expr.subquery(alias="grouped_data")
                count_expr = exp.select(exp.Count(this=exp.Star())).from_(subquery)
            else:
                count_expr = exp.select(exp.Count(this=exp.Star())).from_(
                    cast("exp.Expression", expr.args.get("from")), copy=False
                )
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
