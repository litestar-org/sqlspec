"""Common driver attributes and utilities."""

import contextlib
from abc import ABC
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

import sqlglot
from mypy_extensions import mypyc_attr
from sqlglot import exp
from sqlglot.tokens import TokenType

from sqlspec.exceptions import NotFoundError
from sqlspec.parameters import DriverParameterConfig, ParameterStyle, ParameterValidator, TypedParameter
from sqlspec.statement.cache import anonymous_returns_rows_cache
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


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class CommonDriverAttributesMixin(ABC):
    """Common attributes and methods for driver adapters."""

    __slots__ = ("config", "connection")

    dialect: "DialectType"
    """The SQL dialect supported by the underlying database driver."""
    parameter_config: "ClassVar[DriverParameterConfig]"
    """The parameter configuration for this driver."""

    def __init__(self, connection: "Any", config: "Optional[SQLConfig]" = None) -> None:
        """Initialize async driver adapter.

        Args:
            connection: Database connection instance
            config: SQL configuration
        """
        self.connection = connection
        self.config = config or SQLConfig()
        super().__init__()

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

        # Extract raw SQL for analysis
        sql_str = statement.to_sql(placeholder_style=None) if isinstance(statement, SQL) else str(statement)

        validator = ParameterValidator()
        param_infos = validator.extract_parameters(sql_str)

        if not param_infos:
            return self.parameter_config.default_parameter_style

        detected_styles = {p.style for p in param_infos}

        # If mixed styles detected, use the driver's configured style
        if len(detected_styles) > 1:
            return self.parameter_config.default_parameter_style

        # Single style detected - return it if valid, otherwise use configured style
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

        if not isinstance(parameters, (list, tuple)):
            return [parameters.value if isinstance(parameters, TypedParameter) else parameters]

        return [p.value if isinstance(p, TypedParameter) else p for p in parameters]

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
