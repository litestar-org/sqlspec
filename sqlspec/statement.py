# ruff: noqa: RUF100, PLR6301, PLR0912, PLR0915, C901, PLR0911, PLR0914, N806
import logging
import re
from dataclasses import dataclass
from functools import cached_property
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


@dataclass()
class SQLStatement:
    """An immutable representation of a SQL statement with its parameters.

    This class encapsulates the SQL statement and its parameters, providing
    a clean interface for parameter binding and SQL statement formatting.
    """

    dialect: str
    """The SQL dialect to use for parsing (e.g., 'postgres', 'mysql'). Defaults to 'postgres' if None."""
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

        Returns:
            A tuple containing the processed SQL string and the processed parameters
            ready for database driver execution.

        Raises:
            SQLParsingError: If the SQL statement contains parameter placeholders, but no parameters were provided.

        Returns:
            A tuple containing the processed SQL string and the processed parameters
            ready for database driver execution.
        """
        if self._merged_parameters is None:
            # Validate that the SQL doesn't expect parameters if none were provided
            # Parse ONLY if we need to validate
            try:  # Add try/except in case parsing fails even here
                expression = self._parse_sql()
            except SQLParsingError:
                # If parsing fails, we can't validate, but maybe that's okay if no params were passed?
                # Log a warning? For now, let the original error propagate if needed.
                # Or, maybe assume it's okay if _merged_parameters is None?
                # Let's re-raise for now, as unparsable SQL is usually bad.
                logger.warning("SQL statement is unparsable: %s", self.sql)
                return self.sql, None
            if list(expression.find_all(exp.Parameter)):
                msg = "SQL statement contains parameter placeholders, but no parameters were provided."
                raise SQLParsingError(msg)
            return self.sql, None

        if isinstance(self._merged_parameters, dict):
            # Pass only the dict, parsing happens inside
            return self._process_dict_params(self._merged_parameters)

        if isinstance(self._merged_parameters, (tuple, list)):
            # Pass only the sequence, parsing happens inside if needed for validation
            return self._process_sequence_params(self._merged_parameters)

        # Assume it's a single scalar value otherwise
        # Pass only the value, parsing happens inside for validation
        return self._process_scalar_param(self._merged_parameters)

    def _parse_sql(self) -> exp.Expression:
        """Parse the SQL using sqlglot.

        Raises:
            SQLParsingError: If the SQL statement cannot be parsed.

        Returns:
            The parsed SQL expression.
        """
        parse_dialect = self.dialect or "postgres"
        try:
            read_dialect = parse_dialect or None
            return sqlglot.parse_one(self.sql, read=read_dialect)
        except Exception as e:
            # Ensure the original sqlglot error message is included
            error_detail = str(e)
            msg = f"Failed to parse SQL with dialect '{parse_dialect or 'auto-detected'}': {error_detail}\nSQL: {self.sql}"
            raise SQLParsingError(msg) from e

    def _process_dict_params(
        self,
        parameter_dict: dict[str, Any],
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Processes dictionary parameters based on dialect capabilities.

        Raises:
            ParameterStyleMismatchError: If the SQL statement contains unnamed placeholders (e.g., '?') in the SQL query.
            SQLParsingError: If the SQL statement contains named parameters, but no parameters were provided.

        Returns:
            A tuple containing the processed SQL string and the processed parameters
            ready for database driver execution.
        """
        # Attempt to parse with sqlglot first (for other dialects like postgres, mysql)
        named_sql_params: Optional[list[exp.Parameter]] = None
        unnamed_sql_params: Optional[list[exp.Parameter]] = None
        sqlglot_parsed_ok = False
        # --- Dialect-Specific Bypasses for Native Handling ---
        if self.dialect == "sqlite":  # Handles :name natively
            return self.sql, parameter_dict

        # Add bypass for postgres handled by specific adapters (e.g., asyncpg)
        if self.dialect == "postgres":
            # The adapter (e.g., asyncpg) will handle :name -> $n conversion.
            # SQLStatement just validates parameters against the original SQL here.
            # Perform validation using regex if sqlglot parsing fails, otherwise use sqlglot.
            try:
                expression = self._parse_sql()
                sql_params = list(expression.find_all(exp.Parameter))
                named_sql_params = [p for p in sql_params if p.name]
                unnamed_sql_params = [p for p in sql_params if not p.name]

                if unnamed_sql_params:
                    msg = "Cannot use dictionary parameters with unnamed placeholders (e.g., '?') found by sqlglot for postgres."
                    raise ParameterStyleMismatchError(msg)

                # Validate keys using sqlglot results
                required_keys = {p.name for p in named_sql_params}
                provided_keys = set(parameter_dict.keys())
                missing_keys = required_keys - provided_keys
                if missing_keys:
                    msg = (
                        f"Named parameters found in SQL (via sqlglot) but not provided: {missing_keys}. SQL: {self.sql}"
                    )
                    raise SQLParsingError(msg)  # noqa: TRY301
                # Allow extra keys

            except SQLParsingError as e:
                logger.debug("SQLglot parsing failed for postgres dict params, attempting regex validation: %s", e)
                # Regex validation fallback (without conversion)
                postgres_found_params_regex: list[str] = []
                for match in PARAM_REGEX.finditer(self.sql):
                    if match.group("dquote") or match.group("squote") or match.group("comment"):
                        continue
                    if match.group("var_name"):
                        var_name = match.group("var_name")
                        postgres_found_params_regex.append(var_name)
                        if var_name not in parameter_dict:
                            msg = f"Named parameter ':{var_name}' found in SQL (via regex) but not provided. SQL: {self.sql}"
                            raise SQLParsingError(msg)  # noqa: B904

                if not postgres_found_params_regex and parameter_dict:
                    msg = f"Dictionary parameters provided, but no named placeholders (:name) found via regex. SQL: {self.sql}"
                    raise ParameterStyleMismatchError(msg)  # noqa: B904
                # Allow extra keys with regex check too

            # Return the *original* SQL and the processed dict for the adapter to handle
            return self.sql, parameter_dict

        if self.dialect == "duckdb":  # Handles $name natively (and :name via driver? Check driver docs)
            # Bypass sqlglot/regex checks. Trust user SQL ($name or ?) + dict for DuckDB driver.
            # We lose :name -> $name conversion *if* sqlglot parsing fails, but avoid errors on valid $name SQL.
            return self.sql, parameter_dict
        # --- End Bypasses ---

        try:
            expression = self._parse_sql()
            sql_params = list(expression.find_all(exp.Parameter))
            named_sql_params = [p for p in sql_params if p.name]
            unnamed_sql_params = [p for p in sql_params if not p.name]
            sqlglot_parsed_ok = True
            logger.debug("SQLglot parsed dict params successfully for: %s", self.sql)
        except SQLParsingError as e:
            logger.debug("SQLglot parsing failed for dict params, attempting regex fallback: %s", e)
            # Proceed using regex fallback below

        # Check for unnamed placeholders if parsing worked
        if sqlglot_parsed_ok and unnamed_sql_params:
            msg = "Cannot use dictionary parameters with unnamed placeholders (e.g., '?') found by sqlglot."
            raise ParameterStyleMismatchError(msg)

        # Determine if we need to use regex fallback
        # Use fallback if: parsing failed OR (parsing worked BUT found no named params when a dict was provided)
        use_regex_fallback = not sqlglot_parsed_ok or (not named_sql_params and parameter_dict)

        if use_regex_fallback:
            # Regex fallback logic for :name -> self.param_style conversion
            # ... (regex fallback code as implemented previously) ...
            logger.debug("Using regex fallback for dict param processing: %s", self.sql)
            # --- Regex Fallback Logic ---
            regex_processed_sql_parts: list[str] = []
            ordered_params = []
            last_end = 0
            regex_found_params: list[str] = []

            for match in PARAM_REGEX.finditer(self.sql):
                # Skip matches that are comments or quoted strings
                if match.group("dquote") or match.group("squote") or match.group("comment"):
                    continue

                if match.group("var_name"):
                    var_name = match.group("var_name")
                    regex_found_params.append(var_name)
                    # Get start and end from the match object for the :var_name part
                    # The var_name group itself doesn't include the leading :, so adjust start.
                    start = match.start("var_name") - 1
                    end = match.end("var_name")

                    if var_name not in parameter_dict:
                        msg = (
                            f"Named parameter ':{var_name}' found in SQL (via regex) but not provided. SQL: {self.sql}"
                        )
                        raise SQLParsingError(msg)

                    regex_processed_sql_parts.extend((self.sql[last_end:start], self.param_style))  # Use target style
                    ordered_params.append(parameter_dict[var_name])
                    last_end = end

            regex_processed_sql_parts.append(self.sql[last_end:])

            # Validation with regex results
            if not regex_found_params and parameter_dict:
                msg = f"Dictionary parameters provided, but no named placeholders (e.g., :name) found via regex in the SQL query for dialect '{self.dialect}'. SQL: {self.sql}"
                raise ParameterStyleMismatchError(msg)

            provided_keys = set(parameter_dict.keys())
            missing_keys = set(regex_found_params) - provided_keys  # Should be caught above, but double check
            if missing_keys:
                msg = f"Named parameters found in SQL (via regex) but not provided: {missing_keys}. SQL: {self.sql}"
                raise SQLParsingError(msg)

            extra_keys = provided_keys - set(regex_found_params)
            if extra_keys:
                # Allow extra keys
                pass

            return "".join(regex_processed_sql_parts), tuple(ordered_params)

        # Sqlglot Logic (if parsing worked and found params)
        # ... (sqlglot logic as implemented previously, including :name -> %s conversion) ...
        logger.debug("Using sqlglot results for dict param processing: %s", self.sql)

        # Ensure named_sql_params is iterable, default to empty list if None (shouldn't happen ideally)
        active_named_params = named_sql_params or []

        if not active_named_params and not parameter_dict:
            # No SQL params found by sqlglot, no provided params dict -> OK
            return self.sql, ()

        # Validation with sqlglot results
        required_keys = {p.name for p in active_named_params}  # Use active_named_params
        provided_keys = set(parameter_dict.keys())

        missing_keys = required_keys - provided_keys
        if missing_keys:
            msg = f"Named parameters found in SQL (via sqlglot) but not provided: {missing_keys}. SQL: {self.sql}"
            raise SQLParsingError(msg)

        extra_keys = provided_keys - required_keys
        if extra_keys:
            pass  # Allow extra keys

        # Note: DuckDB handled by bypass above if sqlglot fails.
        # This block handles successful sqlglot parse for other dialects.
        # We don't need the specific DuckDB $name conversion here anymore,
        # as the bypass handles the native $name case.
        # The general logic converts :name -> self.param_style for dialects like postgres.
        # if self.dialect == "duckdb": ... (Removed specific block here)

        # For other dialects requiring positional conversion (using sqlglot param info):
        sqlglot_processed_parts: list[str] = []
        ordered_params = []
        last_end = 0
        for param in active_named_params:  # Use active_named_params
            start = param.this.this.start
            end = param.this.this.end
            sqlglot_processed_parts.extend((self.sql[last_end:start], self.param_style))
            ordered_params.append(parameter_dict[param.name])
            last_end = end
        sqlglot_processed_parts.append(self.sql[last_end:])
        return "".join(sqlglot_processed_parts), tuple(ordered_params)

    def _process_sequence_params(
        self, params: Union[tuple[Any, ...], list[Any]]
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Processes a sequence of parameters.

        Returns:
            A tuple containing the processed SQL string and the processed parameters
            ready for database driver execution.
        """
        return self.sql, params

    def _process_scalar_param(
        self, param_value: Any
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Processes a single scalar parameter value.

        Returns:
            A tuple containing the processed SQL string and the processed parameters
            ready for database driver execution.
        """
        return self.sql, (param_value,)

    @cached_property
    def param_style(self) -> str:
        """Get the parameter style based on the dialect.

        Returns:
            The parameter style placeholder for the dialect.
        """
        dialect = self.dialect

        # Map dialects to parameter styles for placeholder replacement
        # Note: Used when converting named params (:name) for dialects needing positional.
        # Dialects supporting named params natively (SQLite, DuckDB) are handled via bypasses.
        dialect_to_param_style = {
            "postgres": "%s",
            "mysql": "%s",
            "oracle": ":1",
            "mssql": "?",
            "bigquery": "?",
            "snowflake": "?",
            "cockroach": "%s",
            "db2": "?",
        }
        # Default to '?' for unknown/unhandled dialects or when dialect=None is forced
        return dialect_to_param_style.get(dialect, "?")
