"""Common driver attributes and utilities."""

import contextlib
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union

import sqlglot
from mypy_extensions import trait
from sqlglot import exp
from sqlglot.tokens import TokenType

from sqlspec.exceptions import NotFoundError
from sqlspec.parameters import ParameterStyle, ParameterValidator
from sqlspec.parameters.types import TypedParameter
from sqlspec.statement import SQLResult, Statement, StatementFilter
from sqlspec.statement.builder import QueryBuilder
from sqlspec.statement.cache import anonymous_returns_rows_cache
from sqlspec.statement.pipeline import SQLTransformContext, create_pipeline_from_config
from sqlspec.statement.splitter import split_sql_script
from sqlspec.statement.sql import SQL, StatementConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.typing import StatementParameters, T


__all__ = ("CommonDriverAttributesMixin",)


logger = get_logger("driver")

ROW_RETURNING_TOKENS = {
    TokenType.SELECT,
    TokenType.WITH,
    TokenType.VALUES,
    TokenType.TABLE,
    TokenType.SHOW,
    TokenType.DESCRIBE,
    TokenType.PRAGMA,
}


@trait
class CommonDriverAttributesMixin:
    """Common attributes and methods for driver adapters."""

    __slots__ = ("connection", "driver_features", "statement_config")
    connection: "Any"
    statement_config: "StatementConfig"
    driver_features: "dict[str, Any]"

    @property
    @abstractmethod
    def dialect(self) -> "Optional[str]":
        """Database dialect for this driver."""

    # ================================================================================
    # Initialization
    # ================================================================================

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

    # ================================================================================
    # SQL Analysis & Detection Methods
    # ================================================================================

    def returns_rows(self, expression: "Optional[exp.Expression]") -> bool:
        """Check if the SQL expression is expected to return rows.

        Args:
            expression: The SQL expression.

        Returns:
            True if the expression is a SELECT, VALUES, WITH (not CTE definition),
            INSERT/UPDATE/DELETE with RETURNING, or certain command types.
        """
        if expression is None:
            return False
        if isinstance(expression, (exp.Select, exp.Values, exp.Table, exp.Show, exp.Describe, exp.Pragma, exp.Command)):
            return True
        if isinstance(expression, exp.With) and expression.expressions:
            return self.returns_rows(expression.expressions[-1])
        if isinstance(expression, (exp.Insert, exp.Update, exp.Delete)):
            return bool(expression.find(exp.Returning))
        if isinstance(expression, exp.Anonymous):
            sql_text = str(expression.this) if expression.this else ""
            if not sql_text.strip():
                return False
            cache_key = f"returns_rows:{hash(sql_text)}"
            cached_result = anonymous_returns_rows_cache.get(cache_key)
            if cached_result is not None:
                return bool(cached_result)
            result = self._check_anonymous_returns_rows(sql_text)
            anonymous_returns_rows_cache.set(cache_key, result)

            return result
        return False

    def _build_select_result_from_data(
        self, statement: "SQL", data: "list[dict[str, Any]]", column_names: "list[str]", row_count: int
    ) -> "SQLResult":
        """Build SQLResult for SELECT operations from extracted data."""
        return SQLResult(
            statement=statement, data=data, column_names=column_names, rows_affected=row_count, operation_type="SELECT"
        )

    def _build_execute_result_from_data(
        self, statement: "SQL", row_count: int, metadata: "Optional[dict[str, Any]]" = None
    ) -> "SQLResult":
        """Build SQLResult for non-SELECT operations from extracted data."""
        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=row_count,
            operation_type="EXECUTE",
            metadata=metadata or {"status_message": "OK"},
        )

    def prepare_statement(
        self,
        statement: "Union[Statement, QueryBuilder]",
        *parameters: "Union[StatementParameters, StatementFilter]",
        statement_config: "StatementConfig",
        **kwargs: Any,
    ) -> "SQL":
        """Build SQL statement from various input types.

        Ensures dialect is set and preserves existing state when rebuilding SQL objects.
        """
        if isinstance(statement, QueryBuilder):
            return statement.to_statement(config=statement_config)
        if isinstance(statement, SQL):
            if parameters or kwargs:
                new_config = statement_config
                if self.dialect and new_config and not new_config.dialect:
                    new_config = new_config.replace(dialect=self.dialect)
                return statement.copy(
                    parameters=(*statement._positional_params, *parameters)
                    if parameters
                    else statement._positional_params,
                    config=new_config,
                    **kwargs,
                )
            if self.dialect and (
                not statement.statement_config.dialect or statement.statement_config.dialect != self.dialect
            ):
                new_config = statement.statement_config.replace(dialect=self.dialect)
                if statement.parameters:
                    return statement.copy(config=new_config, dialect=self.dialect)
                return statement.copy(config=new_config, dialect=self.dialect)
            return statement
        if self.dialect and statement_config and not statement_config.dialect:
            statement_config = statement_config.replace(dialect=self.dialect)
        return SQL(statement, *parameters, config=statement_config, **kwargs)

    def _check_anonymous_returns_rows(self, sql_text: str) -> bool:
        """Uncached implementation of anonymous expression checking."""
        with contextlib.suppress(Exception):
            parsed = sqlglot.parse_one(sql_text, read=None)
            if isinstance(parsed, (exp.Select, exp.Values, exp.Table, exp.Show, exp.Describe, exp.Pragma, exp.Command)):
                return True
            if isinstance(parsed, exp.With) and parsed.expressions:
                return self.returns_rows(parsed.expressions[-1])
            if isinstance(parsed, (exp.Insert, exp.Update, exp.Delete)):
                return bool(parsed.find(exp.Returning))

        try:
            tokens = list(sqlglot.tokenize(sql_text, read=None))
            for token in tokens:
                if token.token_type in {TokenType.COMMENT, TokenType.SEMICOLON}:
                    continue
                return token.token_type in ROW_RETURNING_TOKENS
        except Exception:
            return False

        return False

    def has_parameters(self, expression: "Union[exp.Expression, str]") -> bool:
        """Check if a SQL expression contains any parameter placeholders using AST analysis.

        This method uses SQLGlot's AST to detect parameter placeholders rather than
        naive string searching. It supports all parameter styles including:
        - Placeholder expressions (?, :name, $1, etc.)
        - Parameter expressions ({param})
        - Any other parameterized constructs

        Args:
            expression: SQLGlot expression or SQL string to analyze

        Returns:
            True if the expression contains parameter placeholders
        """
        # Parse string to expression if needed
        if isinstance(expression, str):
            original_sql = expression
            try:
                expression = sqlglot.maybe_parse(expression, read=self.dialect or None)
            except Exception:
                # If parsing fails, fall back to token analysis
                return self._has_parameters_by_tokens(original_sql)

        # Walk the AST looking for placeholder nodes
        return any(isinstance(node, (exp.Placeholder, exp.Parameter)) for node in expression.walk())

    def _has_parameters_by_tokens(self, sql_text: str) -> bool:
        """Fallback parameter detection using token analysis.

        Used when AST parsing fails, this method tokenizes the SQL and looks
        for parameter-related tokens.

        Args:
            sql_text: SQL string to analyze

        Returns:
            True if parameter tokens are found
        """
        try:
            tokens = list(sqlglot.tokenize(sql_text, read=self.dialect or None))
            return any(token.token_type in {TokenType.PLACEHOLDER, TokenType.PARAMETER} for token in tokens)
        except Exception:
            # Last resort: look for common parameter patterns
            # But only as absolute fallback when tokenization fails
            return any(marker in sql_text for marker in ["?", "$", ":"])

    def _select_parameter_style(self, statement: "Union[SQL, exp.Expression]") -> "ParameterStyle":
        """Select the best parameter style based on detected styles in SQL.

        This method examines the SQL statement for existing parameter placeholders
        and selects an appropriate style that the driver supports. If mixed or
        unsupported styles are detected, it falls back to the default style.

        Args:
            statement: SQL statement to analyze

        Returns:
            The selected parameter style to use for this statement
        """
        sql_str = statement.to_sql(placeholder_style=None) if isinstance(statement, SQL) else str(statement)
        validator = ParameterValidator()
        param_infos = validator.extract_parameters(sql_str)

        parameter_config = self.statement_config.get_parameter_config()

        if not param_infos:
            return parameter_config.default_parameter_style

        detected_styles = {p.style for p in param_infos}
        if len(detected_styles) > 1:
            return parameter_config.default_parameter_style
        detected_style = next(iter(detected_styles))
        return detected_style or parameter_config.default_parameter_style

    @staticmethod
    def check_not_found(item_or_none: "Optional[T]" = None) -> "T":
        """Raise :exc:`sqlspec.exceptions.NotFoundError` if ``item_or_none`` is ``None``.

        Args:
            item_or_none: Item to be tested for existence.

        Raises:
            NotFoundError: If ``item_or_none`` is ``None``

        Returns:
            The item, if it exists.
        """
        if item_or_none is None:
            msg = "No result found when one was expected"
            raise NotFoundError(msg)
        return item_or_none

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
                script, dialect=str(statement_config.dialect), strip_trailing_semicolon=strip_trailing_semicolon
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

        if is_many:
            return [self._format_parameter_set(param_set, statement_config) for param_set in parameters]

        return self._format_parameter_set(parameters, statement_config)

    def _format_parameter_set(self, parameters: Any, statement_config: "StatementConfig") -> Any:
        """Prepare a single parameter set for database driver consumption.

        Args:
            parameters: Single parameter set in any format
            statement_config: Statement configuration for parameter style detection

        Returns:
            Processed parameter set with TypedParameter objects unwrapped
        """
        if not parameters:
            return []

        if isinstance(parameters, dict):
            if not parameters:
                return []
            parameter_config = statement_config.get_parameter_config()
            if parameter_config.default_parameter_style in {
                ParameterStyle.NUMERIC,  # PostgreSQL $1, $2
                ParameterStyle.QMARK,  # SQLite ?, ?
                ParameterStyle.POSITIONAL_PYFORMAT,  # MySQL %s, %s
            }:
                # Convert dict to ordered list based on numeric or param_ keys
                ordered_params = []
                # Sort by numeric key or param_0, param_1 pattern to maintain parameter order
                sorted_items = sorted(
                    parameters.items(),
                    key=lambda item: int(item[0])
                    if item[0].isdigit()
                    else (int(item[0][6:]) if item[0].startswith("param_") and item[0][6:].isdigit() else float("inf")),
                )
                for _, value in sorted_items:
                    ordered_params.append(value.value if isinstance(value, TypedParameter) else value)
                return ordered_params

            # For named parameter styles, keep as dict
            return {k: (v.value if isinstance(v, TypedParameter) else v) for k, v in parameters.items()}

        if isinstance(parameters, (list, tuple)):
            return [p.value if isinstance(p, TypedParameter) else p for p in parameters]

        return [parameters.value if isinstance(parameters, TypedParameter) else parameters]

    def _prepare_script_sql(self, statement: "SQL") -> str:
        """Prepare SQL script for execution by embedding parameters as static values.

        Since most database drivers don't support parameters in executescript
        methods, this method compiles the SQL with ParameterStyle.STATIC to
        embed parameter values directly in the SQL string.

        Args:
            statement: SQL statement marked as a script

        Returns:
            SQL string with parameters embedded as static values
        """
        sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
        return sql

    def _apply_pipeline_transformations(
        self, expression: "exp.Expression", parameters: Any = None, config: "Optional[StatementConfig]" = None
    ) -> tuple["exp.Expression", Any]:
        """Apply pipeline transformations to SQL expression.

        This method creates and applies a transformation pipeline based on
        the SQL configuration, allowing drivers to leverage the pipeline
        architecture for consistent SQL processing.

        Args:
            expression: SQLGlot expression to transform
            parameters: Optional parameters for the SQL
            config: SQL configuration (uses driver's config if not provided)

        Returns:
            Tuple of (transformed expression, processed parameters)
        """
        config = config or self.statement_config
        pipeline = create_pipeline_from_config(config, driver_adapter=self)
        context = SQLTransformContext(
            current_expression=expression,
            original_expression=expression,
            parameters=parameters,
            dialect=str(self.dialect),
            metadata={},
            driver_adapter=self,
        )
        result_context = pipeline(context)
        return result_context.current_expression, result_context.merged_parameters

    # ================================================================================
    # SQL Compilation Methods
    # ================================================================================

    def _get_compiled_sql(self, statement: "SQL", statement_config: "StatementConfig") -> tuple[str, Any]:
        """Get compiled SQL with optimal parameter style (only converts when needed).

        Args:
            statement: SQL statement to compile
            statement_config: Complete statement configuration including parameter config, dialect, etc.

        Returns:
            Tuple of (compiled_sql, parameters)
        """
        parameter_config = statement_config.get_parameter_config()
        target_style = parameter_config.execution_target_style

        # Optimization: Only pass target_style if it's set and different from current statement's style
        if target_style and target_style != parameter_config.default_parameter_style:
            return statement.compile(placeholder_style=target_style)

        # No conversion needed - use default compilation
        return statement.compile()

    # ================================================================================
    # Unified Execution Methods
    # ================================================================================

    def _perform_execute(self, cursor: Any, statement: "SQL") -> Any:
        """Unified execution logic that delegates to driver-specific methods.

        This method implements the common execution pattern shared by all drivers:
        1. Compile SQL with driver's parameter style
        2. Route to appropriate execution method based on statement type
        3. Let driver implement the specific database execution logic

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement to execute

        Returns:
            Whatever the driver-specific execution method returns
        """
        # Compile with driver's parameter style
        sql, params = self._get_compiled_sql(statement, self.statement_config)

        if statement.is_script:
            # Check if driver needs static compilation (e.g., SQLite executescript)
            if self.statement_config.needs_static_script_compilation:
                # Use static compilation for databases that don't support parameters in scripts
                static_sql = self._prepare_script_sql(statement)
                return self._execute_script(cursor, static_sql, None, self.statement_config)
            # Prepare parameters for script execution
            prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)
            return self._execute_script(cursor, sql, prepared_params, self.statement_config)
        if statement.is_many:
            # Prepare parameters for executemany
            prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=True)
            return self._execute_many(cursor, sql, prepared_params)
        # Prepare parameters for single execution
        prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)
        return self._execute_statement(cursor, sql, prepared_params)

    def _execute_script(self, cursor: Any, sql: str, prepared_params: Any, statement_config: "StatementConfig") -> Any:
        """Execute a SQL script (multiple statements).

        Default implementation splits script and executes statements individually.
        Drivers can override for database-specific script execution methods.

        Args:
            cursor: Database cursor/connection object
            sql: Compiled SQL script
            prepared_params: Prepared parameters
            statement_config: Statement configuration for dialect information

        Returns:
            Driver-specific result
        """
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
        last_result = None
        for stmt in statements:
            # split_script_statements already removes empty strings, no need to check again
            last_result = self._execute_statement(cursor, stmt, prepared_params)
        return last_result

    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Execute SQL with multiple parameter sets (executemany).

        Must be implemented by each driver for database-specific executemany logic.

        Args:
            cursor: Database cursor/connection object
            sql: Compiled SQL statement
            prepared_params: List of prepared parameter sets

        Returns:
            Driver-specific result

        Raises:
            NotImplementedError: Must be implemented by driver subclasses
        """
        msg = f"{type(self).__name__} must implement _execute_many"
        raise NotImplementedError(msg)

    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Execute a single SQL statement.

        Must be implemented by each driver for database-specific execution logic.

        Args:
            cursor: Database cursor/connection object
            sql: Compiled SQL statement
            prepared_params: Prepared parameters

        Returns:
            Driver-specific result

        Raises:
            NotImplementedError: Must be implemented by driver subclasses
        """
        msg = f"{type(self).__name__} must implement _execute_single"
        raise NotImplementedError(msg)
