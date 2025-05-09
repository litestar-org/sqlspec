# ruff: noqa: RUF100, PLR6301, PLR0912, PLR0915, C901, PLR0911, PLR0914, N806
import logging
import re
from dataclasses import dataclass
from typing import (
    Any,
    Optional,
    Union,
)

import sqlglot
from sqlglot import exp

from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.typing import StatementParameterType

__all__ = ("SQLStatement",)

logger = logging.getLogger("sqlspec")

# Regex to find :param style placeholders, skipping those inside quotes or SQL comments
# Adapted from previous version in psycopg adapter
PARAM_REGEX = re.compile(
    r"""(?<![:\w]) # Negative lookbehind to avoid matching things like ::type or \:escaped
    (?:
        (?P<dquote>"(?:[^"]|"")*") |     # Double-quoted strings (support SQL standard escaping "")
        (?P<squote>'(?:[^']|'')*') |     # Single-quoted strings (support SQL standard escaping '')
        (?P<comment>--.*?\n|\/\*.*?\*\/) | # SQL comments (single line or multi-line)
        : (?P<var_name>[a-zA-Z_][a-zA-Z0-9_]*)   # :var_name identifier
    )
    """,
    re.VERBOSE | re.DOTALL,
)

# Regex to find ? placeholders, skipping those inside quotes or SQL comments
QMARK_REGEX = re.compile(
    r"""(?P<dquote>\"[^\"]*\") | # Double-quoted strings
         (?P<squote>'[^']*') | # Single-quoted strings
         (?P<comment>--[^\n]*|/\*.*?\*/) | # SQL comments (single/multi-line)
         (?P<qmark>\?) # The question mark placeholder
      """,
    re.VERBOSE | re.DOTALL,
)


# Regex for $name style, avoiding comments/strings
# Lookbehind ensures it's not preceded by a word char or another $
DOLLAR_NAME_REGEX = re.compile(
    r"""(?<![\w\$])
    (?:
        (?P<dquote>"(?:[^"]|"")*") |     # Double-quoted strings
        (?P<squote>'(?:[^']|'')*') |     # Single-quoted strings
        (?P<comment>--.*?\n|\/\*.*?\*\/) | # SQL comments
        \$ (?P<var_name>[a-zA-Z_][a-zA-Z0-9_]*)   # $var_name identifier
    )
    """,
    re.VERBOSE | re.DOTALL,
)

# Regex for @name style, avoiding comments/strings
# Lookbehind ensures it's not preceded by a word char or another @
AT_NAME_REGEX = re.compile(
    r"""(?<![\w@])
    (?:
        (?P<dquote>"(?:[^"]|"")*") |     # Double-quoted strings
        (?P<squote>'(?:[^']|'')*') |     # Single-quoted strings
        (?P<comment>--.*?\n|\/\*.*?\*\/) | # SQL comments
        @ (?P<var_name>[a-zA-Z_][a-zA-Z0-9_]*)   # @var_name identifier
    )
    """,
    re.VERBOSE | re.DOTALL,
)


@dataclass()
class SQLStatement:
    """An immutable representation of a SQL statement with its parameters.

    This class encapsulates the SQL statement and its parameters, providing
    a clean interface for parameter binding and SQL statement formatting.
    """

    sql: str
    """The raw SQL statement."""
    parameters: Optional[StatementParameterType] = None
    """The parameters for the SQL statement."""
    kwargs: Optional[dict[str, Any]] = None
    """Keyword arguments passed for parameter binding."""

    _merged_parameters: Optional[Union[StatementParameterType, dict[str, Any]]] = None

    def __post_init__(self) -> None:
        """Merge parameters and kwargs after initialization."""
        merged_params = self.parameters

        if self.kwargs:
            if merged_params is None:
                merged_params = self.kwargs
            elif isinstance(merged_params, dict):
                # Merge kwargs into parameters dict, kwargs take precedence
                merged_params = {**merged_params, **self.kwargs}
            else:
                # If parameters is sequence or scalar, kwargs replace it
                # Consider adding a warning here if this behavior is surprising
                merged_params = self.kwargs

        self._merged_parameters = merged_params

    def process(self) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process the SQL statement and merged parameters for execution.

        This method validates the parameters against the SQL statement using sqlglot
        parsing but returns the *original* SQL string and the merged parameters.
        The actual formatting of SQL placeholders and parameter structures for the
        DBAPI driver is delegated to the specific adapter.

        Returns:
            A tuple containing the *original* SQL string and the merged/validated
            parameters (dict, tuple, list, or None).

        Raises:
            SQLParsingError: If the SQL statement contains parameter placeholders
                but no parameters were provided, or if parsing fails unexpectedly.
        """
        # Parse the SQL to find expected parameters
        try:
            expression = self._parse_sql()
            # Find all parameter expressions (:name, ?, etc.)
            sql_params = list(expression.find_all(exp.Parameter))
        except SQLParsingError as e:
            # If parsing fails, we cannot validate accurately.
            # Let adapters handle potentially valid but unparsable SQL.
            # Log the parsing error for debugging.
            logger.debug(
                "SQL parsing failed during validation: %s. Returning original SQL and parameters for adapter.", e
            )
            # Return original SQL and parameters for the adapter to attempt processing
            # (Adapters might use regex or other means if parsing fails)
            return self.sql, self._merged_parameters

        if self._merged_parameters is None:
            # If no parameters were provided, but the parsed SQL expects them, raise an error.
            if sql_params:
                placeholder_types = {"named" if p.name else "positional" for p in sql_params}
                msg = f"SQL statement contains {', '.join(placeholder_types)} parameter placeholders, but no parameters were provided. SQL: {self.sql}"
                raise SQLParsingError(msg)
            # No parameters provided and none found in SQL - OK
            return self.sql, None

        # Validate provided parameters against parsed SQL parameters
        if isinstance(self._merged_parameters, dict):
            self._validate_dict_params(sql_params, self._merged_parameters)
        elif isinstance(self._merged_parameters, (tuple, list)):
            self._validate_sequence_params(sql_params, self._merged_parameters)
        else:  # Scalar parameter
            self._validate_scalar_param(sql_params, self._merged_parameters)

        # Return the original SQL and the merged parameters for the adapter to process
        return self.sql, self._merged_parameters

    def _parse_sql(self) -> exp.Expression:
        """Parse the SQL using sqlglot.

        Raises:
            SQLParsingError: If the SQL statement cannot be parsed.

        Returns:
            The parsed SQL expression.
        """
        # Use a generic dialect or try autodetection if specific dialect knowledge is removed.
        # For validation purposes, 'postgres' is often a good lenient default.
        # Alternatively, let the caller (adapter) provide the dialect if needed for parsing hints.
        # For now, let's keep it simple and assume a generic parse works for validation.
        try:
            # Removed read=self.dialect as self.dialect is removed.
            # parse_one without 'read' uses the standard dialect by default.
            return sqlglot.parse_one(self.sql)
        except Exception as e:
            error_detail = str(e)
            # Removed dialect from error message
            msg = f"Failed to parse SQL for validation: {error_detail}\nSQL: {self.sql}"
            raise SQLParsingError(msg) from e

    def _validate_dict_params(self, sql_params: list[exp.Parameter], parameter_dict: dict[str, Any]) -> None:
        """Validates dictionary parameters against parsed SQL parameters."""
        named_sql_params = [p for p in sql_params if p.name]
        unnamed_sql_params = [p for p in sql_params if not p.name]

        if unnamed_sql_params:
            msg = f"Dictionary parameters provided, but found unnamed placeholders (e.g., '?') in SQL: {self.sql}"
            raise ParameterStyleMismatchError(msg)

        if not named_sql_params and parameter_dict:
            # Check with regex as a fallback confirmation if sqlglot finds no named params
            regex_found = any(
                match.group("var_name")
                for match in PARAM_REGEX.finditer(self.sql)
                if not (match.group("dquote") or match.group("squote") or match.group("comment"))
            )
            if not regex_found:
                msg = f"Dictionary parameters provided, but no named placeholders (e.g., ':name') found in SQL: {self.sql}"
                raise ParameterStyleMismatchError(msg)
            # SQLglot didn't find named params but regex did - log warning, proceed.
            logger.warning(
                "SQLglot found no named parameters, but regex did. Proceeding with validation. SQL: %s", self.sql
            )

        required_keys = {p.name for p in named_sql_params}
        provided_keys = set(parameter_dict.keys())

        missing_keys = required_keys - provided_keys
        if missing_keys:
            msg = f"Named parameters found in SQL but not provided: {missing_keys}. SQL: {self.sql}"
            raise SQLParsingError(msg)

    def _validate_sequence_params(
        self, sql_params: list[exp.Parameter], params: Union[tuple[Any, ...], list[Any]]
    ) -> None:
        """Validates sequence parameters against parsed SQL parameters."""
        named_sql_params = [p for p in sql_params if p.name]
        unnamed_sql_params = [p for p in sql_params if not p.name]

        if named_sql_params:
            # No need to store msg if we are raising immediately
            msg = f"Sequence parameters provided, but found named placeholders (e.g., ':name') in SQL: {self.sql}"
            raise ParameterStyleMismatchError(msg)

        expected_count = len(unnamed_sql_params)
        actual_count = len(params)

        if expected_count != actual_count:
            # Double-check with regex if counts mismatch, as parsing might miss some complex cases
            regex_count = 0
            for match in QMARK_REGEX.finditer(self.sql):
                if match.group("qmark"):
                    regex_count += 1

            if regex_count != actual_count:
                msg = (
                    f"Parameter count mismatch. SQL expects {expected_count} (sqlglot) / {regex_count} (regex) positional parameters, "
                    f"but {actual_count} were provided. SQL: {self.sql}"
                )
                raise SQLParsingError(msg)
            # Counts mismatch with sqlglot but match with simple regex - log warning, proceed.
            logger.warning(
                "Parameter count mismatch (sqlglot: %d, provided: %d), but regex count (%d) matches provided. Proceeding. SQL: %s",
                expected_count,
                actual_count,
                regex_count,
                self.sql,
            )

    def _validate_scalar_param(self, sql_params: list[exp.Parameter], param_value: Any) -> None:
        """Validates a single scalar parameter against parsed SQL parameters."""
        self._validate_sequence_params(sql_params, (param_value,))  # Treat scalar as a single-element sequence
