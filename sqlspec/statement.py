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

CONSOLIDATED_PARAM_REGEX = re.compile(
    r"""
    # Fields to skip (matched first, then ignored in processing logic)
    (?P<dquote>"(?:[^"]|"")*") |     # Double-quoted strings (SQL standard "" escaping)
    (?P<squote>'(?:[^']|'')*') |     # Single-quoted strings (SQL standard '' escaping)
    (?P<comment>--.*?\n|\/\*.*?\*\/) | # SQL comments (single-line or multi-line)

    # Actual placeholders - attempt to match one of these
    (?: # Non-capturing group for colon-numeric POSITIONAL parameter (e.g., :1, :23)
        (?<![:\\w]) # Negative lookbehind: not preceded by ':' or word character
        : # Literal colon
        (?P<var_colon_numeric>[1-9][0-9]*) # Captured number (1-9 followed by 0 or more digits)
    ) |
    (?: # Non-capturing group for colon-prefixed named parameter (e.g., :name)
        (?<![:\\w]) # Negative lookbehind: not preceded by ':' or word character
        : # Literal colon
        (?P<var_colon_named>[a-zA-Z_][a-zA-Z0-9_]*) # Captured variable name
    ) |
    (?: # Non-capturing group for question mark positional parameter (e.g., ?)
        # Needs careful lookaround if we want to avoid matching e.g. JSON operators '?|'
        # For now, keeping it simple as SQL '?' is usually well-spaced or at end of comparisons.
        # A full context-aware parse (like sqlglot) is primary, this is a fallback.
        (?P<var_qmark>\?) # Captured question mark
    ) |
    (?: # Non-capturing group for dollar-prefixed NAMED parameter (e.g., $name) - PRIORITIZE THIS OVER $n
        (?<![\w\$]) # Negative lookbehind: not preceded by word character or '$'
        \$ # Literal dollar
        (?P<var_dollar>[a-zA-Z_][a-zA-Z0-9_]*) # Captured variable name (must start with letter/underscore)
    ) |
    (?: # Non-capturing group for at-symbol-prefixed named parameter (e.g., @name)
        (?<![\\w@]) # Negative lookbehind: not preceded by word character or '@'
        @ # Literal at-symbol
        (?P<var_at>[a-zA-Z_][a-zA-Z0-9_]*) # Captured variable name
    ) |
    (?: # Non-capturing group for pyformat named parameter (e.g., %(name)s)
        (?<!%) # Negative lookbehind: not preceded by '%' (to avoid %%%(name)s being treated as a param)
        % # Literal percent
        \((?P<var_pyformat>[a-zA-Z_][a-zA-Z0-9_]*)\) # Captured variable name inside parentheses
        [a-zA-Z] # Type specifier (e.g., s, d, f) - simple match for any letter
    ) |
    (?: # Non-capturing group for format/printf POSITIONAL parameter (e.g., %s, %d) - AFTER pyformat
        (?<!%) # Negative lookbehind: not preceded by another % (to avoid %%%%s)
        %
        (?P<var_format_type>[a-zA-Z]) # Captured type specifier (s, d, f, etc.)
    ) |
    (?: # Non-capturing group for numeric dollar POSITIONAL parameter (e.g., $1, $2)
        (?<![\w\$]) # Negative lookbehind: not preceded by word char or '$'
        \$ # Literal dollar
        (?P<var_numeric>[1-9][0-9]*) # Captured number (1-9 followed by 0 or more digits)
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
            # Find all parameter expressions (:name, ?, @name, $1, etc.)
            # These are nodes that sqlglot considers as bind parameters.
            all_sqlglot_placeholders = list(expression.find_all(exp.Placeholder, exp.Parameter))
        except SQLParsingError as e:
            logger.debug(
                "SQL parsing failed during validation: %s. Returning original SQL and parameters for adapter.", e
            )
            return self.sql, self._merged_parameters

        if self._merged_parameters is None:
            # If no parameters were provided, but the parsed SQL expects them, raise an error.
            if all_sqlglot_placeholders:
                placeholder_types_desc = []
                for p_node in all_sqlglot_placeholders:
                    if isinstance(p_node, exp.Parameter) and p_node.name:
                        placeholder_types_desc.append(f"named (e.g., :{p_node.name}, @{p_node.name})")
                    elif isinstance(p_node, exp.Placeholder) and p_node.this and not p_node.this.isdigit():
                        placeholder_types_desc.append(f"named (e.g., :{p_node.this})")
                    elif isinstance(p_node, exp.Placeholder) and p_node.this is None:
                        placeholder_types_desc.append("positional (?)")
                    # Add more descriptions for other types like numeric $1 if necessary
                # Make unique descriptions
                desc_str = ", ".join(sorted(set(placeholder_types_desc))) or "unknown"
                msg = f"SQL statement contains {desc_str} parameter placeholders, but no parameters were provided. SQL: {self.sql}"
                raise SQLParsingError(msg)
            # No parameters provided and none found in SQL - OK
            return self.sql, None

        # Validate provided parameters against parsed SQL parameters
        if isinstance(self._merged_parameters, dict):
            self._validate_dict_params(all_sqlglot_placeholders, self._merged_parameters)
        elif isinstance(self._merged_parameters, (tuple, list)):
            self._validate_sequence_params(all_sqlglot_placeholders, self._merged_parameters)
        else:  # Scalar parameter
            self._validate_scalar_param(all_sqlglot_placeholders, self._merged_parameters)

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

    def _validate_dict_params(
        self, all_sqlglot_placeholders: list[Union[exp.Parameter, exp.Placeholder]], parameter_dict: dict[str, Any]
    ) -> None:
        sqlglot_named_params: dict[str, Union[exp.Parameter, exp.Placeholder]] = {}
        has_positional_qmark = False

        for p_node in all_sqlglot_placeholders:
            if (
                isinstance(p_node, exp.Parameter) and p_node.name and not p_node.name.isdigit()
            ):  # @name, $name (non-numeric)
                sqlglot_named_params[p_node.name] = p_node
            elif isinstance(p_node, exp.Placeholder) and p_node.this and not p_node.this.isdigit():  # :name
                sqlglot_named_params[p_node.this] = p_node
            elif isinstance(p_node, exp.Placeholder) and p_node.this is None:  # ?
                has_positional_qmark = True
            # Ignores numeric placeholders like $1, :1 for dict validation for now

        if has_positional_qmark:
            msg = f"Dictionary parameters provided, but found unnamed placeholders ('?') in SQL: {self.sql}"
            raise ParameterStyleMismatchError(msg)

        # Regex check as a fallback (can be simplified or removed if sqlglot is trusted)
        regex_named_placeholders_found = False
        for match in CONSOLIDATED_PARAM_REGEX.finditer(self.sql):
            if match.group("dquote") or match.group("squote") or match.group("comment"):
                continue
            if match.group("var_colon") or match.group("var_dollar") or match.group("var_at"):
                regex_named_placeholders_found = True

        if not sqlglot_named_params and parameter_dict:
            if not regex_named_placeholders_found:
                msg = f"Dictionary parameters provided, but no named placeholders (e.g., ':name', '$name', '@name') found by sqlglot or regex in SQL: {self.sql}"
                raise ParameterStyleMismatchError(msg)
            logger.warning(
                "SQLglot found no named parameters, but regex did. Proceeding with validation. SQL: %s", self.sql
            )

        required_keys = set(sqlglot_named_params.keys())
        provided_keys = set(parameter_dict.keys())

        missing_keys = required_keys - provided_keys
        if missing_keys:
            msg = f"Named parameters found in SQL by sqlglot but not provided: {missing_keys}. SQL: {self.sql}"
            raise SQLParsingError(msg)

    def _validate_sequence_params(
        self,
        all_sqlglot_placeholders: list[Union[exp.Parameter, exp.Placeholder]],
        params: Union[tuple[Any, ...], list[Any]],
    ) -> None:
        sqlglot_named_param_names = []  # For detecting named params
        sqlglot_positional_count = 0  # For counting ?, $1, :1 etc.

        for p_node in all_sqlglot_placeholders:
            if isinstance(p_node, exp.Parameter) and p_node.name and not p_node.name.isdigit():  # @name, $name
                sqlglot_named_param_names.append(p_node.name)
            elif isinstance(p_node, exp.Placeholder) and p_node.this and not p_node.this.isdigit():  # :name
                sqlglot_named_param_names.append(p_node.this)
            elif isinstance(p_node, exp.Placeholder) and p_node.this is None:  # ?
                sqlglot_positional_count += 1
            elif isinstance(p_node, exp.Parameter) and (  # noqa: PLR0916
                (p_node.name and p_node.name.isdigit())
                or (
                    not p_node.name
                    and p_node.this
                    and isinstance(p_node.this, (str, exp.Identifier, exp.Number))
                    and str(p_node.this).isdigit()
                )
            ):
                # $1, :1 style (parsed as Parameter with name="1" or this="1" or this=Identifier(this="1") or this=Number(this=1))
                sqlglot_positional_count += 1
            elif (
                isinstance(p_node, exp.Placeholder) and p_node.this and p_node.this.isdigit()
            ):  # :1 style (Placeholder with this="1")
                sqlglot_positional_count += 1

        # Regex check (can be simplified if sqlglot part is robust)
        regex_named_placeholders_found = False
        for match in CONSOLIDATED_PARAM_REGEX.finditer(self.sql):
            if match.group("dquote") or match.group("squote") or match.group("comment"):
                continue
            if match.group("var_colon") or match.group("var_dollar") or match.group("var_at"):
                regex_named_placeholders_found = True

        if sqlglot_named_param_names or regex_named_placeholders_found:
            found_by = []
            if sqlglot_named_param_names:
                found_by.append(f"sqlglot ({', '.join(sorted(set(sqlglot_named_param_names)))})")
            if regex_named_placeholders_found and not sqlglot_named_param_names:
                found_by.append("regex")
            msg = (
                f"Sequence parameters provided, but found named placeholders "
                f"by {', '.join(found_by)} in SQL: {self.sql}"
            )
            raise ParameterStyleMismatchError(msg)

        expected_count_sqlglot = sqlglot_positional_count
        actual_count_provided = len(params)

        if expected_count_sqlglot != actual_count_provided:
            if sqlglot_positional_count != actual_count_provided:
                msg = (
                    f"Parameter count mismatch. SQL expects {expected_count_sqlglot} (sqlglot) / {sqlglot_positional_count} (regex) positional '?' parameters, "
                    f"but {actual_count_provided} were provided. SQL: {self.sql}"
                )
                raise SQLParsingError(msg)
            logger.warning(
                "Parameter count mismatch (sqlglot: %d, provided: %d), but regex count for '?' (%d) matches provided. Proceeding. SQL: %s",
                expected_count_sqlglot,
                actual_count_provided,
                sqlglot_positional_count,
                self.sql,
            )

    def _validate_scalar_param(
        self, all_sqlglot_placeholders: list[Union[exp.Parameter, exp.Placeholder]], param_value: Any
    ) -> None:
        """Validates a single scalar parameter against parsed SQL parameters."""
        self._validate_sequence_params(
            all_sqlglot_placeholders, (param_value,)
        )  # Treat scalar as a single-element sequence
