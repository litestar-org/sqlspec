"""Common driver attributes and utilities."""

import contextlib
from typing import TYPE_CHECKING, Any, Optional, Union

import sqlglot
from mypy_extensions import trait
from sqlglot import exp
from sqlglot.tokens import TokenType

from sqlspec.exceptions import NotFoundError
from sqlspec.parameters import DriverParameterConfig, ParameterStyle, ParameterValidator
from sqlspec.parameters.types import TypedParameter
from sqlspec.statement.cache import SQLCache, anonymous_returns_rows_cache
from sqlspec.statement.pipeline import SQLTransformContext, create_pipeline_from_config
from sqlspec.statement.splitter import split_sql_script
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.typing import T


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

    __slots__ = ("_compiled_cache", "_prepared_counter", "_prepared_statements", "config", "connection")

    # Core attributes
    connection: "Any"
    config: "SQLConfig"
    dialect: "DialectType"
    parameter_config: "DriverParameterConfig"
    _compiled_cache: "Optional[SQLCache]"
    _prepared_statements: "dict[str, str]"
    _prepared_counter: int

    # ================================================================================
    # Initialization
    # ================================================================================

    def __init__(self, connection: "Any", config: "Optional[SQLConfig]" = None) -> None:
        """Initialize driver adapter with connection and caching support.

        Args:
            connection: Database connection instance
            config: SQL configuration
        """
        self.connection = connection
        self.config = config or SQLConfig()
        self._compiled_cache = SQLCache() if self.config.enable_caching else None
        self._prepared_statements = {}
        self._prepared_counter = 0

        super().__init__()

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
            try:
                expression = sqlglot.parse_one(expression, read=self.dialect or None)
            except Exception:
                # If parsing fails, fall back to token analysis
                return self._has_parameters_by_tokens(expression)

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

        if not param_infos:
            return self.parameter_config.default_parameter_style

        detected_styles = {p.style for p in param_infos}
        if len(detected_styles) > 1:
            return self.parameter_config.default_parameter_style
        detected_style = next(iter(detected_styles))
        return detected_style or self.parameter_config.default_parameter_style

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

    def _split_script_statements(self, script: str, strip_trailing_semicolon: bool = False) -> list[str]:
        """Split a SQL script into individual statements.

        Uses a robust lexer-driven state machine to handle multi-statement scripts,
        including complex constructs like PL/SQL blocks, T-SQL batches, and nested blocks.
        Particularly useful for databases that don't natively support multi-statement
        execution (e.g., Oracle, some async drivers).

        Args:
            script: The SQL script to split
            strip_trailing_semicolon: If True, remove trailing semicolons from statements

        Returns:
            A list of individual SQL statements
        """
        return split_sql_script(script, dialect=str(self.dialect), strip_trailing_semicolon=strip_trailing_semicolon)

    def _prepare_driver_parameters(self, parameters: Any) -> Any:
        """Prepare parameters for database driver consumption.

        Normalizes parameter structure and unwraps TypedParameter objects
        to their underlying values, which database drivers expect.
        TypeCoercionMixin handles parameter normalization.

        Args:
            parameters: Parameters in any format (dict, list, tuple, scalar, TypedParameter)

        Returns:
            Parameters with TypedParameter objects unwrapped to primitive values
        """
        if not parameters:
            return []
        if isinstance(parameters, dict):
            return {k: (v.value if isinstance(v, TypedParameter) else v) for k, v in parameters.items()}
        if isinstance(parameters, (list, tuple)):
            return [p.value if isinstance(p, TypedParameter) else p for p in parameters]
        return [parameters.value if isinstance(parameters, TypedParameter) else parameters]

    def _prepare_driver_parameters_many(self, parameters: Any) -> "list[Any]":
        """Prepare parameter sequences for executemany operations.

        Handles sequences of parameter sets, unwrapping TypedParameter
        objects in each set for database driver consumption.

        Args:
            parameters: Sequence of parameter sets for executemany

        Returns:
            List of parameter sets with TypedParameter objects unwrapped
        """
        if not parameters:
            return []
        return [self._prepare_driver_parameters(param_set) for param_set in parameters]

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
        self, expression: "exp.Expression", parameters: Any = None, config: "Optional[SQLConfig]" = None
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
        config = config or self.config
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
    # Caching Methods
    # ================================================================================

    def _get_compiled_sql(self, statement: "SQL", target_style: ParameterStyle) -> tuple[str, Any]:
        """Get compiled SQL with caching.

        Args:
            statement: SQL statement to compile
            target_style: Target parameter style for compilation

        Returns:
            Tuple of (compiled_sql, parameters)
        """
        if self._compiled_cache is None:
            return statement.compile(placeholder_style=target_style)
        cache_key = self._adapter_cache_key(statement, target_style)
        cached = self._compiled_cache.get(cache_key)
        if cached is not None:
            return cached  # type: ignore[no-any-return]
        result = statement.compile(placeholder_style=target_style)
        self._compiled_cache.set(cache_key, result)
        return result

    def _adapter_cache_key(self, statement: "SQL", style: ParameterStyle) -> str:
        """Generate adapter-specific cache key.

        Args:
            statement: SQL statement
            style: Parameter style

        Returns:
            Cache key string
        """
        # Use statement's internal cache key which includes SQL hash, params, and dialect
        base_key = statement._cache_key()
        # Add adapter-specific context
        return f"{self.__class__.__name__}:{style.value}:{base_key}"

    def _get_or_create_prepared_statement_name(self, sql_hash: str) -> str:
        """Get or create a prepared statement name for the given SQL.

        Used by PostgreSQL and other databases that support prepared statements.

        Args:
            sql_hash: Hash of the SQL statement

        Returns:
            Prepared statement name
        """
        if sql_hash in self._prepared_statements:
            return self._prepared_statements[sql_hash]
        self._prepared_counter += 1
        stmt_name = f"sqlspec_ps_{self._prepared_counter}"
        self._prepared_statements[sql_hash] = stmt_name
        return stmt_name

    def _clear_adapter_cache(self) -> None:
        """Clear all adapter-level caches."""
        if self._compiled_cache is not None:
            self._compiled_cache.clear()
        self._prepared_statements.clear()
        self._prepared_counter = 0
