"""Common driver attributes and utilities."""

from typing import TYPE_CHECKING, Any, Final, NamedTuple, Optional, Union, cast

from mypy_extensions import trait
from sqlglot import exp

from sqlspec.builder import QueryBuilder
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.parameters import ParameterStyle, TypedParameter
from sqlspec.statement import SQLResult, Statement, StatementFilter
from sqlspec.statement.cache import get_cache_config, sql_cache
from sqlspec.statement.result import OperationType
from sqlspec.statement.splitter import split_sql_script
from sqlspec.statement.sql import SQL, StatementConfig
from sqlspec.utils.logging import get_logger
from sqlspec.utils.statement_hashing import hash_sql_statement

if TYPE_CHECKING:
    from sqlspec.typing import StatementParameters


__all__ = (
    "DEFAULT_EXECUTION_RESULT",
    "EXEC_CURSOR_RESULT",
    "EXEC_ROWCOUNT_OVERRIDE",
    "EXEC_SPECIAL_DATA",
    "CommonDriverAttributesMixin",
    "ExecutionResult",
    "ScriptExecutionResult",
)


logger = get_logger("driver")


class ScriptExecutionResult(NamedTuple):
    """Result from script execution with statement count information.

    This named tuple eliminates the need for redundant script splitting
    by providing statement count information during execution rather than
    requiring re-parsing after execution.

    Attributes:
        cursor_result: The result returned by the database cursor/driver
        rowcount_override: Optional override for the number of affected rows
        special_data: Any special metadata or additional information
        statement_count: Total number of statements in the script
        successful_statements: Number of statements that executed successfully
    """

    cursor_result: Any
    rowcount_override: Optional[int]
    special_data: Any
    statement_count: int
    successful_statements: int


class ExecutionResult(NamedTuple):
    """Comprehensive execution result containing all data needed for SQLResult building.

    This named tuple consolidates all execution result data to eliminate the need
    for additional data extraction calls and script re-parsing in build_statement_result.

    Attributes:
        cursor_result: The raw result returned by the database cursor/driver
        rowcount_override: Optional override for the number of affected rows
        special_data: Any special metadata or additional information from execution
        selected_data: For SELECT operations, the extracted row data
        column_names: For SELECT operations, the column names
        data_row_count: For SELECT operations, the number of rows returned
        statement_count: For script operations, total number of statements
        successful_statements: For script operations, number of successful statements
        is_script_result: Whether this result is from script execution
        is_select_result: Whether this result is from a SELECT operation
        is_many_result: Whether this result is from an execute_many operation
    """

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


EXEC_CURSOR_RESULT = 0
EXEC_ROWCOUNT_OVERRIDE = 1
EXEC_SPECIAL_DATA = 2
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
        """Initialize driver adapter with connection and caching support.

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
    ) -> ExecutionResult:
        """Create ExecutionResult with all necessary data for any operation type.

        This consolidated method replaces multiple specialized creation methods.
        Pass only the parameters relevant to your operation type.

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

        Returns:
            ExecutionResult configured for the specified operation type

        Examples:
            # SELECT operation
            create_execution_result(cursor, selected_data=data, column_names=cols,
                                  data_row_count=len(data), is_select_result=True)

            # Script operation
            create_execution_result(cursor, statement_count=5, successful_statements=5,
                                  is_script_result=True)

            # Regular execute operation
            create_execution_result(cursor, rowcount_override=1)

            # Execute many operation
            create_execution_result(cursor, rowcount_override=10, is_many_result=True)
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
        )

    def build_statement_result(self, statement: "SQL", execution_result: ExecutionResult) -> "SQLResult":
        """Build and return the SQLResult from consolidated execution data.

        This method creates SQLResult objects from ExecutionResult data without requiring
        additional data extraction calls or script re-parsing, significantly improving
        performance and simplifying the execution flow.

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
            operation_type=self._determine_operation_type(statement),
            metadata=execution_result.special_data or {"status_message": "OK"},
        )

    def _determine_operation_type(self, statement: "Any") -> OperationType:
        """Determine operation type from SQL statement expression.

        Examines the statement's expression type to determine if it's
        INSERT, UPDATE, DELETE, SELECT, SCRIPT, or generic EXECUTE.

        Args:
            statement: SQL statement object with expression attribute

        Returns:
            OperationType literal value
        """
        if statement.is_script:
            return "SCRIPT"

        try:
            expression = statement.expression
        except AttributeError:
            return "EXECUTE"

        if not expression:
            return "EXECUTE"

        expr_type = type(expression).__name__.upper()

        # Handle Anonymous expressions that might be unparseable scripts
        if "ANONYMOUS" in expr_type and statement.is_script:
            return "SCRIPT"

        if "INSERT" in expr_type:
            return "INSERT"
        if "UPDATE" in expr_type:
            return "UPDATE"
        if "DELETE" in expr_type:
            return "DELETE"
        if "SELECT" in expr_type:
            return "SELECT"
        if "COPY" in expr_type:
            return "COPY"
        return "EXECUTE"

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
        """
        kwargs = kwargs or {}

        if isinstance(statement, QueryBuilder):
            return statement.to_statement(statement_config)
        if isinstance(statement, SQL):
            if parameters or kwargs:
                merged_parameters = (
                    (*statement._positional_parameters, *parameters) if parameters else statement._positional_parameters
                )
                return SQL(statement.sql, *merged_parameters, statement_config=statement_config, **kwargs)
            if self.statement_config.dialect and (
                not statement.statement_config.dialect
                or statement.statement_config.dialect != self.statement_config.dialect
            ):
                new_config = statement.statement_config.replace(dialect=self.statement_config.dialect)
                return statement.copy(statement_config=new_config, dialect=self.statement_config.dialect)
            return statement
        return SQL(statement, *parameters, statement_config=statement_config, **kwargs)

    def split_script_statements(
        self, script: str, statement_config: "StatementConfig", strip_trailing_semicolon: bool = False
    ) -> list[str]:
        """Split a SQL script into individual statements.

        Uses a robust lexer-driven state machine to handle multi-statement scripts,
        including complex constructs like PL/SQL blocks, T-SQL batches, and nested blocks.
        Particularly useful for databases that don't natively support multi-statement
        execution (e.g., Oracle, some async drivers).

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
        self, parameters: Any, statement_config: "StatementConfig", is_many: bool = False
    ) -> Any:
        """Prepare parameters for database driver consumption.

        Normalizes parameter structure and unwraps TypedParameter objects
        to their underlying values, which database drivers expect.
        Consolidates both single and many parameter handling.

        Args:
            parameters: Parameters in any format (dict, list, tuple, scalar, TypedParameter)
            statement_config: Statement configuration for parameter style detection
            is_many: If True, handle as executemany parameter sequence

        Returns:
            Parameters with TypedParameter objects unwrapped to primitive values
        """
        if not parameters:
            return []
        return (
            [self._format_parameter_set(parameters, statement_config) for parameters in parameters]
            if is_many
            else self._format_parameter_set(parameters, statement_config)
        )

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

        def apply_type_coercion(value: Any) -> Any:
            """Apply type coercion to a single value."""
            unwrapped_value = value.value if isinstance(value, TypedParameter) else value

            if statement_config.parameter_config.type_coercion_map:
                for type_check, converter in statement_config.parameter_config.type_coercion_map.items():
                    if isinstance(unwrapped_value, type_check):
                        return converter(unwrapped_value)

            return unwrapped_value

        if isinstance(parameters, dict):
            if not parameters:
                return []
            if (
                statement_config.parameter_config.supported_execution_parameter_styles
                and ParameterStyle.NAMED_PYFORMAT
                in statement_config.parameter_config.supported_execution_parameter_styles
            ):
                return {k: apply_type_coercion(v) for k, v in parameters.items()}
            if statement_config.parameter_config.default_parameter_style in {
                ParameterStyle.NUMERIC,
                ParameterStyle.QMARK,
                ParameterStyle.POSITIONAL_PYFORMAT,
            }:
                ordered_parameters = []
                sorted_items = sorted(
                    parameters.items(),
                    key=lambda item: int(item[0])
                    if item[0].isdigit()
                    else (int(item[0][6:]) if item[0].startswith("param_") and item[0][6:].isdigit() else float("inf")),
                )
                for _, value in sorted_items:
                    ordered_parameters.append(apply_type_coercion(value))
                return ordered_parameters

            return {k: apply_type_coercion(v) for k, v in parameters.items()}

        if isinstance(parameters, (list, tuple)):
            return [apply_type_coercion(p) for p in parameters]

        return [apply_type_coercion(parameters)]

    def _get_compiled_sql(
        self, statement: "SQL", statement_config: "StatementConfig", flatten_single_parameters: bool = False
    ) -> tuple[str, Any]:
        """Get compiled SQL with optimal parameter style and caching support.

        Args:
            statement: SQL statement to compile
            statement_config: Complete statement configuration including parameter config, dialect, etc.
            flatten_single_parameters: If True, flatten single-element lists for scalar parameters

        Returns:
            Tuple of (compiled_sql, parameters)
        """
        cache_config = get_cache_config()
        cache_key = None
        if cache_config.compiled_cache_enabled and statement_config.enable_caching:
            cache_key = self._generate_compilation_cache_key(statement, statement_config, flatten_single_parameters)
            cached_result = sql_cache.get(cache_key)
            if cached_result is not None:
                sql, parameters = cached_result
                prepared_parameters = self.prepare_driver_parameters(
                    parameters, statement_config, is_many=statement.is_many
                )
                if statement_config.parameter_config.output_transformer:
                    sql, prepared_parameters = statement_config.parameter_config.output_transformer(
                        sql, prepared_parameters
                    )
                return sql, prepared_parameters
        if statement.is_script and not statement_config.parameter_config.needs_static_script_compilation:
            target_style = ParameterStyle.STATIC
        elif statement_config.parameter_config.supported_execution_parameter_styles is not None:
            current_style = statement.detect_parameter_style()
            if (
                current_style
                and current_style in statement_config.parameter_config.supported_execution_parameter_styles
            ):
                target_style = None
            else:
                target_style = statement_config.parameter_config.default_execution_parameter_style
        else:
            target_style = statement_config.parameter_config.default_parameter_style

        sql, parameters = statement.compile(
            placeholder_style=target_style, flatten_single_parameters=flatten_single_parameters
        )

        if cache_key is not None:
            sql_cache.set(cache_key, (sql, parameters))

        prepared_parameters = self.prepare_driver_parameters(parameters, statement_config, is_many=statement.is_many)

        if statement_config.parameter_config.output_transformer:
            sql, prepared_parameters = statement_config.parameter_config.output_transformer(sql, prepared_parameters)

        return sql, prepared_parameters

    def _generate_compilation_cache_key(
        self, statement: "SQL", config: "StatementConfig", flatten_single_parameters: bool
    ) -> str:
        """Generate cache key that includes all compilation context.

        This method creates a deterministic cache key that includes all factors
        that affect SQL compilation, preventing cache contamination between
        different compilation contexts.
        """
        context_hash = hash(
            (
                config.parameter_config.hash(),
                config.dialect,
                statement.is_script,
                statement.is_many,
                flatten_single_parameters,
                bool(config.parameter_config.output_transformer),
                bool(config.parameter_config.needs_static_script_compilation),
            )
        )

        base_hash = hash_sql_statement(statement)
        return f"compiled:{base_hash}:{context_hash}"

    def _create_count_query(self, original_sql: "SQL") -> "SQL":
        """Create a COUNT query from the original SQL statement.

        Transforms the original SELECT statement to count total rows while preserving
        WHERE, HAVING, and GROUP BY clauses but removing ORDER BY, LIMIT, and OFFSET.

        For queries with GROUP BY, wraps the query in a subquery to count groups correctly.
        """
        if not original_sql.expression:
            msg = "Cannot create COUNT query from empty SQL expression"
            raise ImproperConfigurationError(msg)
        expr = original_sql.expression.copy()

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

            return SQL(count_expr, *original_sql._positional_parameters, statement_config=original_sql.statement_config)

        subquery = cast("exp.Select", expr).subquery(alias="total_query")
        count_expr = exp.select(exp.Count(this=exp.Star())).from_(subquery)
        return SQL(count_expr, *original_sql._positional_parameters, statement_config=original_sql.statement_config)
