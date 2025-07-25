"""Common driver attributes and utilities."""

from abc import ABC
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

import sqlglot
from mypy_extensions import mypyc_attr
from sqlglot import exp
from sqlglot.tokens import TokenType

from sqlspec.exceptions import NotFoundError
from sqlspec.statement.cache import anonymous_returns_rows_cache
from sqlspec.statement.parameters import ParameterStyle, ParameterValidator, TypedParameter
from sqlspec.statement.pipeline import SQLTransformContext, create_pipeline_from_config
from sqlspec.statement.splitter import split_sql_script
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.typing import ConnectionT, T


__all__ = ("CommonDriverAttributesMixin",)


logger = get_logger("driver")


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class CommonDriverAttributesMixin(ABC):
    """Common attributes and methods for driver adapters."""

    __slots__ = ("config", "connection")

    connection: "ConnectionT"  # type: ignore[valid-type]
    """The connection object."""
    config: "Optional[SQLConfig]"
    """The SQL configuration."""

    connection_type: "ClassVar[type[ConnectionT]]"  # type: ignore[valid-type]
    """The connection type used by this driver adapter."""

    dialect: "DialectType"
    """The SQL dialect supported by the underlying database driver."""
    supported_parameter_styles: "tuple[ParameterStyle, ...]"
    """The parameter styles supported by this driver."""
    default_parameter_style: "ParameterStyle"
    """The default parameter style to convert to when unsupported style is detected."""
    supports_native_parquet_export: "ClassVar[bool]" = False
    """Indicates if the driver supports native Parquet export operations."""
    supports_native_parquet_import: "ClassVar[bool]" = False
    """Indicates if the driver supports native Parquet import operations."""
    supports_native_arrow_export: "ClassVar[bool]" = False
    """Indicates if the driver supports native Arrow export operations."""
    supports_native_arrow_import: "ClassVar[bool]" = False
    """Indicates if the driver supports native Arrow import operations."""

    def _connection(self, connection: "Optional[Any]" = None) -> "Any":
        raise NotImplementedError

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
            return self._check_anonymous_returns_rows(expression)
        return False

    def _check_anonymous_returns_rows(self, expression: "exp.Anonymous") -> bool:
        """Check if an Anonymous expression returns rows.

        Handles SQL that failed to parse by:
        1. Checking the cache for previously analyzed expressions
        2. Attempting to re-parse the SQL directly (without sanitization)
        3. Using tokenizer as fallback to detect row-returning statements

        Results are cached to avoid re-parsing the same SQL repeatedly.
        """
        sql_text = str(expression.this) if expression.this else ""
        if not sql_text.strip():
            return False

        # Check cache first
        cache_key = f"returns_rows:{hash(sql_text)}"
        cached_result = anonymous_returns_rows_cache.get(cache_key)
        if cached_result is not None:
            return bool(cached_result)

        # Perform the actual check
        result = self._check_anonymous_returns_rows_uncached(sql_text)

        # Cache the result
        anonymous_returns_rows_cache.set(cache_key, result)

        return result

    def _check_anonymous_returns_rows_uncached(self, sql_text: str) -> bool:
        """Uncached implementation of anonymous expression checking."""
        try:
            # Try parsing the SQL directly - SQLGlot might succeed this time
            parsed = sqlglot.parse_one(sql_text, read=None)
            if isinstance(parsed, (exp.Select, exp.Values, exp.Table, exp.Show, exp.Describe, exp.Pragma, exp.Command)):
                return True
            if isinstance(parsed, exp.With) and parsed.expressions:
                return self.returns_rows(parsed.expressions[-1])
            if isinstance(parsed, (exp.Insert, exp.Update, exp.Delete)):
                return bool(parsed.find(exp.Returning))
            # Don't return False for unrecognized types - fall through to tokenizer
        except Exception:  # noqa: S110
            pass  # Fall through to tokenizer

        # Use tokenizer as fallback
        try:
            tokens = list(sqlglot.tokenize(sql_text, read=None))
            row_returning_tokens = {
                TokenType.SELECT,
                TokenType.WITH,
                TokenType.VALUES,
                TokenType.TABLE,
                TokenType.SHOW,
                TokenType.DESCRIBE,
                TokenType.PRAGMA,
            }
            for token in tokens:
                if token.token_type in {TokenType.COMMENT, TokenType.SEMICOLON}:
                    continue
                # Found a significant token, check if it's row-returning
                return token.token_type in row_returning_tokens

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
            return self.default_parameter_style

        detected_styles = {p.style for p in param_infos}

        # Check for unsupported or mixed styles
        if detected_styles - set(self.supported_parameter_styles) or len(detected_styles) > 1:
            return self.default_parameter_style

        # Single supported style detected
        detected_style = next(iter(detected_styles))
        if detected_style not in self.supported_parameter_styles:
            return self.default_parameter_style

        return detected_style

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

    def _convert_parameters_to_driver_format(  # noqa: C901
        self, sql: str, parameters: Any, target_style: "Optional[ParameterStyle]" = None
    ) -> Any:
        """Convert parameters to the format expected by the driver.

        Analyzes SQL to understand parameter style and only converts when there's
        a mismatch between provided parameters and driver expectations.

        Handles various conversion scenarios:
        - Single scalar parameter to dict/list
        - Dict to list (positional) conversion
        - List to dict (named) conversion
        - Special handling for numeric-named parameters (e.g., :1, :2)
        - Automatic param_N key generation when needed
        """
        if parameters is None:
            return None

        validator = ParameterValidator()
        param_info_list = validator.extract_parameters(sql)

        if not param_info_list:
            return None

        if target_style is None:
            target_style = self.default_parameter_style

        actual_styles = {p.style for p in param_info_list if p.style}
        if len(actual_styles) == 1:
            detected_style = actual_styles.pop()
            if detected_style != target_style:
                target_style = detected_style

        driver_expects_dict = target_style in {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.POSITIONAL_COLON,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
            ParameterStyle.NAMED_PYFORMAT,
        }

        params_are_dict = isinstance(parameters, (dict, Mapping))
        params_are_sequence = isinstance(parameters, (list, tuple, Sequence)) and not isinstance(
            parameters, (str, bytes)
        )

        if len(param_info_list) == 1 and not params_are_dict and not params_are_sequence:
            if driver_expects_dict:
                param_info = param_info_list[0]
                if param_info.name:
                    return {param_info.name: parameters}
                return {f"param_{param_info.ordinal}": parameters}
            return [parameters]

        if driver_expects_dict and params_are_dict:
            if target_style == ParameterStyle.POSITIONAL_COLON and all(
                p.name and p.name.isdigit() for p in param_info_list
            ):
                numeric_keys_expected = {p.name for p in param_info_list if p.name}
                if not numeric_keys_expected.issubset(parameters.keys()):
                    numeric_result: dict[str, Any] = {}
                    param_values = list(parameters.values())
                    for param_info in param_info_list:
                        if param_info.name and param_info.ordinal < len(param_values):
                            numeric_result[param_info.name] = param_values[param_info.ordinal]
                    return numeric_result

            if all(key.startswith("param_") and key[6:].isdigit() for key in parameters):
                sql_param_names = {p.name for p in param_info_list if p.name}
                if sql_param_names and not any(name.startswith("param_") for name in sql_param_names):
                    pass

            return parameters

        if not driver_expects_dict and params_are_sequence:
            return parameters

        if driver_expects_dict and params_are_sequence:
            dict_result: dict[str, Any] = {}
            for i, (param_info, value) in enumerate(zip(param_info_list, parameters)):
                if param_info.name:
                    if param_info.style == ParameterStyle.POSITIONAL_COLON and param_info.name.isdigit():
                        dict_result[param_info.name] = value
                    else:
                        dict_result[param_info.name] = value
                else:
                    dict_result[f"param_{i}"] = value
            return dict_result

        if not driver_expects_dict and params_are_dict:
            if all(key.startswith("param_") and key[6:].isdigit() for key in parameters):
                positional_result: list[Any] = []
                for i in range(len(param_info_list)):
                    key = f"param_{i}"
                    if key in parameters:
                        positional_result.append(parameters[key])
                return positional_result

            positional_params: list[Any] = []
            for param_info in param_info_list:
                if param_info.name and param_info.name in parameters:
                    positional_params.append(parameters[param_info.name])
                elif f"param_{param_info.ordinal}" in parameters:
                    positional_params.append(parameters[f"param_{param_info.ordinal}"])
                else:
                    param_values = list(parameters.values())
                    if param_info.ordinal < len(param_values):
                        positional_params.append(param_values[param_info.ordinal])
            return positional_params or list(parameters.values())

        return parameters

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
        if config is None:
            config = SQLConfig()
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
