"""Common driver attributes and utilities."""

import re
from abc import ABC
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional

import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType

from sqlspec.exceptions import NotFoundError
from sqlspec.statement import SQLConfig
from sqlspec.statement.parameters import ParameterStyle, ParameterValidator
from sqlspec.statement.splitter import split_sql_script
from sqlspec.typing import ConnectionT, DictRow, RowT, T
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


__all__ = ("CommonDriverAttributesMixin",)


logger = get_logger("driver")


class CommonDriverAttributesMixin(ABC, Generic[ConnectionT, RowT]):
    """Common attributes and methods for driver adapters."""

    __slots__ = ("config", "connection", "default_row_type")

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

    def __init__(
        self,
        connection: "ConnectionT",
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = dict[str, Any],
    ) -> None:
        """Initialize with connection, config, and default_row_type.

        Args:
            connection: The database connection
            config: SQL statement configuration
            default_row_type: Default row type for results (DictRow, TupleRow, etc.)
        """
        self.connection = connection
        self.config = config or SQLConfig()
        self.default_row_type = default_row_type or dict[str, Any]

    def _connection(self, connection: "Optional[ConnectionT]" = None) -> "ConnectionT":
        return connection or self.connection

    def returns_rows(self, expression: "Optional[exp.Expression]") -> bool:
        """Check if the SQL expression is expected to return rows.

        Args:
            expression: The SQL expression.

        Returns:
            True if the expression is a SELECT, VALUES, or WITH statement
            that is not a CTE definition.
        """
        if expression is None:
            return False
        if isinstance(expression, (exp.Select, exp.Values, exp.Table, exp.Show, exp.Describe, exp.Pragma, exp.Command)):
            return True
        if isinstance(expression, exp.With) and expression.expressions:
            return self.returns_rows(expression.expressions[-1])
        if isinstance(expression, (exp.Insert, exp.Update, exp.Delete)):
            return bool(expression.find(exp.Returning))
        # Handle Anonymous expressions (failed to parse) using a robust approach
        if isinstance(expression, exp.Anonymous):
            return self._check_anonymous_returns_rows(expression)
        return False

    def _check_anonymous_returns_rows(self, expression: "exp.Anonymous") -> bool:
        """Check if an Anonymous expression returns rows using robust methods.

        This method handles SQL that failed to parse (often due to database-specific
        placeholders) by:
        1. Attempting to re-parse with placeholders sanitized
        2. Using the tokenizer as a fallback for keyword detection

        Args:
            expression: The Anonymous expression to check

        Returns:
            True if the expression likely returns rows
        """

        sql_text = str(expression.this) if expression.this else ""
        if not sql_text.strip():
            return False

        # Regex to find common SQL placeholders: ?, %s, $1, $2, :name, etc.
        placeholder_regex = re.compile(r"(\?|%s|\$\d+|:\w+|%\(\w+\)s)")

        # Approach 1: Try to re-parse with placeholders replaced
        try:
            # Replace placeholders with a dummy literal that sqlglot can parse
            sanitized_sql = placeholder_regex.sub("1", sql_text)

            # If we replaced any placeholders, try parsing again
            if sanitized_sql != sql_text:
                parsed = sqlglot.parse_one(sanitized_sql, read=None)
                # Check if it's a query type that returns rows
                if isinstance(
                    parsed, (exp.Select, exp.Values, exp.Table, exp.Show, exp.Describe, exp.Pragma, exp.Command)
                ):
                    return True
                if isinstance(parsed, exp.With) and parsed.expressions:
                    return self.returns_rows(parsed.expressions[-1])
                if isinstance(parsed, (exp.Insert, exp.Update, exp.Delete)):
                    return bool(parsed.find(exp.Returning))
                if not isinstance(parsed, exp.Anonymous):
                    return False
        except Exception:
            logger.debug("Could not parse using placeholders.  Using tokenizer. %s", sql_text)

        # Approach 2: Use tokenizer for robust keyword detection
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
                return token.token_type in row_returning_tokens

        except Exception:
            return False

        return False

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
        """Convert parameters to the format expected by the driver, but only when necessary.

        This method analyzes the SQL to understand what parameter style is used
        and only converts when there's a mismatch between provided parameters
        and what the driver expects.

        Args:
            sql: The SQL string with placeholders
            parameters: The parameters in any format (dict, list, tuple, scalar)
            target_style: Optional override for the target parameter style

        Returns:
            Parameters in the format expected by the database driver
        """
        if parameters is None:
            return None

        # Extract parameter info from the SQL
        validator = ParameterValidator()
        param_info_list = validator.extract_parameters(sql)

        if not param_info_list:
            # No parameters in SQL, return None
            return None

        # Determine the target style from the SQL if not provided
        if target_style is None:
            target_style = self.default_parameter_style

        actual_styles = {p.style for p in param_info_list if p.style}
        if len(actual_styles) == 1:
            detected_style = actual_styles.pop()
            if detected_style != target_style:
                target_style = detected_style

        # Analyze what format the driver expects based on the placeholder style
        driver_expects_dict = target_style in {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.POSITIONAL_COLON,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
            ParameterStyle.NAMED_PYFORMAT,
        }

        # Check if parameters are already in the correct format
        params_are_dict = isinstance(parameters, (dict, Mapping))
        params_are_sequence = isinstance(parameters, (list, tuple, Sequence)) and not isinstance(
            parameters, (str, bytes)
        )

        # Single scalar parameter
        if len(param_info_list) == 1 and not params_are_dict and not params_are_sequence:
            if driver_expects_dict:
                # Convert scalar to dict
                param_info = param_info_list[0]
                if param_info.name:
                    return {param_info.name: parameters}
                return {f"param_{param_info.ordinal}": parameters}
            return [parameters]

        if driver_expects_dict and params_are_dict:
            if target_style == ParameterStyle.POSITIONAL_COLON and all(
                p.name and p.name.isdigit() for p in param_info_list
            ):
                # If all parameters are numeric but named, convert to dict
                # SQL has numeric placeholders but params might have named keys
                # Only convert if keys don't match
                numeric_keys_expected = {p.name for p in param_info_list if p.name}
                if not numeric_keys_expected.issubset(parameters.keys()):
                    # Need to convert named keys to numeric positions
                    numeric_result: dict[str, Any] = {}
                    param_values = list(parameters.values())
                    for param_info in param_info_list:
                        if param_info.name and param_info.ordinal < len(param_values):
                            numeric_result[param_info.name] = param_values[param_info.ordinal]
                    return numeric_result

            # Special case: Auto-generated param_N style when SQL expects specific names
            if all(key.startswith("param_") and key[6:].isdigit() for key in parameters):
                # Check if SQL has different parameter names
                sql_param_names = {p.name for p in param_info_list if p.name}
                if sql_param_names and not any(name.startswith("param_") for name in sql_param_names):
                    # SQL has specific names, not param_N style - don't use these params as-is
                    # This likely indicates a mismatch in parameter generation
                    # For now, pass through and let validation catch it
                    pass

            # Otherwise, dict format matches - return as-is
            return parameters

        if not driver_expects_dict and params_are_sequence:
            # Formats match - return as-is
            return parameters

        # Formats don't match - need conversion
        if driver_expects_dict and params_are_sequence:
            # Convert positional to dict
            dict_result: dict[str, Any] = {}
            for i, (param_info, value) in enumerate(zip(param_info_list, parameters)):
                if param_info.name:
                    # Use the name from SQL
                    if param_info.style == ParameterStyle.POSITIONAL_COLON and param_info.name.isdigit():
                        # Oracle uses string keys even for numeric placeholders
                        dict_result[param_info.name] = value
                    else:
                        dict_result[param_info.name] = value
                else:
                    # Use param_N format for unnamed placeholders
                    dict_result[f"param_{i}"] = value
            return dict_result

        if not driver_expects_dict and params_are_dict:
            # Convert dict to positional
            # First check if it's already in param_N format
            if all(key.startswith("param_") and key[6:].isdigit() for key in parameters):
                # Extract values in order
                positional_result: list[Any] = []
                for i in range(len(param_info_list)):
                    key = f"param_{i}"
                    if key in parameters:
                        positional_result.append(parameters[key])
                return positional_result

            # Convert named dict to positional based on parameter order in SQL
            positional_params: list[Any] = []
            for param_info in param_info_list:
                if param_info.name and param_info.name in parameters:
                    positional_params.append(parameters[param_info.name])
                elif f"param_{param_info.ordinal}" in parameters:
                    positional_params.append(parameters[f"param_{param_info.ordinal}"])
                else:
                    # Try to match by position if we have a simple dict
                    param_values = list(parameters.values())
                    if param_info.ordinal < len(param_values):
                        positional_params.append(param_values[param_info.ordinal])
            return positional_params or list(parameters.values())

        # This shouldn't happen, but return as-is
        return parameters

    def _split_script_statements(self, script: str, strip_trailing_semicolon: bool = False) -> list[str]:
        """Split a SQL script into individual statements.

        This method uses a robust lexer-driven state machine to handle
        multi-statement scripts, including complex constructs like
        PL/SQL blocks, T-SQL batches, and nested blocks.

        Args:
            script: The SQL script to split
            strip_trailing_semicolon: If True, remove trailing semicolons from statements

        Returns:
            A list of individual SQL statements

        Note:
            This is particularly useful for databases that don't natively
            support multi-statement execution (e.g., Oracle, some async drivers).
        """
        # The split_sql_script function already handles dialect mapping and fallback
        return split_sql_script(script, dialect=str(self.dialect), strip_trailing_semicolon=strip_trailing_semicolon)
