# ruff: noqa: RUF100, PLR6301, PLR0912, PLR0915, C901, PLR0911, PLR0914, N806
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Union,
)

import sqlglot
from sqlglot import exp

from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.typing import StatementParameterType

if TYPE_CHECKING:
    from sqlspec.filters import StatementFilter

__all__ = ("SQLStatement",)

logger = logging.getLogger("sqlspec")


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
    dialect: Optional[str] = None
    """SQL dialect to use for parsing. If not provided, sqlglot will try to auto-detect."""

    _merged_parameters: Optional[Union[StatementParameterType, dict[str, Any]]] = field(default=None, init=False)
    _parsed_expression: Optional[exp.Expression] = field(default=None, init=False)
    _param_counter: int = field(default=0, init=False)

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

    def process(
        self,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]], Optional[exp.Expression]]":
        """Process the SQL statement and merged parameters for execution.

        This method validates the parameters against the SQL statement using sqlglot
        parsing but returns the *original* SQL string, the merged parameters,
        and the parsed sqlglot expression if successful.
        The actual formatting of SQL placeholders and parameter structures for the
        DBAPI driver is delegated to the specific adapter.

        Returns:
            A tuple containing the *original* SQL string, the merged/validated
            parameters (dict, tuple, list, or None), and the parsed sqlglot expression
            (or None if parsing failed).

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
            self._parsed_expression = None
            return self.sql, self._merged_parameters, None

        if self._merged_parameters is None:
            # If no parameters were provided, but the parsed SQL expects them, raise an error.
            if all_sqlglot_placeholders:
                placeholder_types_desc = []
                for p_node in all_sqlglot_placeholders:
                    if isinstance(p_node, exp.Parameter) and p_node.name:
                        placeholder_types_desc.append(f"named (e.g., :{p_node.name}, @{p_node.name})")
                    elif (
                        isinstance(p_node, exp.Placeholder)
                        and p_node.this
                        and not isinstance(p_node.this, (exp.Identifier, exp.Literal))
                        and not str(p_node.this).isdigit()
                    ):
                        placeholder_types_desc.append(f"named (e.g., :{p_node.this})")
                    elif isinstance(p_node, exp.Parameter) and p_node.name and p_node.name.isdigit():
                        placeholder_types_desc.append("positional (e.g., $1, :1)")
                    elif isinstance(p_node, exp.Placeholder) and p_node.this is None:
                        placeholder_types_desc.append("positional (?)")
                desc_str = ", ".join(sorted(set(placeholder_types_desc))) or "unknown"
                msg = f"SQL statement contains {desc_str} parameter placeholders, but no parameters were provided. SQL: {self.sql}"
                raise SQLParsingError(msg)
            return self.sql, None, self._parsed_expression

        # Validate provided parameters against parsed SQL parameters
        if isinstance(self._merged_parameters, dict):
            self._validate_dict_params(all_sqlglot_placeholders, self._merged_parameters)
        elif isinstance(self._merged_parameters, (tuple, list)):
            self._validate_sequence_params(all_sqlglot_placeholders, self._merged_parameters)
        else:  # Scalar parameter
            self._validate_scalar_param(all_sqlglot_placeholders, self._merged_parameters)

        # Return the original SQL and the merged parameters for the adapter to process
        return self.sql, self._merged_parameters, self._parsed_expression

    def _parse_sql(self) -> exp.Expression:
        """Parse the SQL using sqlglot.

        Raises:
            SQLParsingError: If the SQL statement cannot be parsed.

        Returns:
            The parsed SQL expression.
        """
        try:
            if not self.sql.strip():
                self._parsed_expression = exp.Select()
                return self._parsed_expression
            # Use the provided dialect if available, otherwise sqlglot will try to auto-detect
            self._parsed_expression = sqlglot.parse_one(self.sql, dialect=self.dialect)
            if self._parsed_expression is None:
                self._parsed_expression = exp.Select()  # type: ignore[unreachable]
        except Exception as e:
            msg = f"Failed to parse SQL for validation: {e!s}\nSQL: {self.sql}"
            self._parsed_expression = None
            raise SQLParsingError(msg) from e
        else:
            return self._parsed_expression

    def _validate_dict_params(
        self, all_sqlglot_placeholders: Sequence[exp.Expression], parameter_dict: dict[str, Any]
    ) -> None:
        sqlglot_named_params: dict[str, Union[exp.Parameter, exp.Placeholder]] = {}
        has_positional_qmark = False

        for p_node in all_sqlglot_placeholders:
            if (
                isinstance(p_node, exp.Parameter) and p_node.name and not p_node.name.isdigit()
            ):  # @name, $name (non-numeric)
                sqlglot_named_params[p_node.name] = p_node
            elif (
                isinstance(p_node, exp.Placeholder)
                and p_node.this
                and not isinstance(p_node.this, (exp.Identifier, exp.Literal))
                and not str(p_node.this).isdigit()
            ):  # :name
                sqlglot_named_params[str(p_node.this)] = p_node
            elif isinstance(p_node, exp.Placeholder) and p_node.this is None:  # ?
                has_positional_qmark = True
            # Ignores numeric placeholders like $1, :1 for dict validation for now

        if has_positional_qmark:
            msg = f"Dictionary parameters provided, but found unnamed placeholders ('?') in SQL: {self.sql}"
            raise ParameterStyleMismatchError(msg)

        if not sqlglot_named_params and parameter_dict:
            msg = f"Dictionary parameters provided, but no named placeholders (e.g., ':name', '$name', '@name') found by sqlglot in SQL: {self.sql}"
            raise ParameterStyleMismatchError(msg)

        missing_keys = set(sqlglot_named_params.keys()) - set(parameter_dict.keys())
        if missing_keys:
            msg = f"Named parameters found in SQL by sqlglot but not provided: {missing_keys}. SQL: {self.sql}"
            raise SQLParsingError(msg)

    def _validate_sequence_params(
        self,
        all_sqlglot_placeholders: Sequence[exp.Expression],
        params: Union[tuple[Any, ...], list[Any]],
    ) -> None:
        sqlglot_named_param_names = []  # For detecting named params
        sqlglot_positional_count = 0  # For counting ?, $1, :1 etc.

        for p_node in all_sqlglot_placeholders:
            if isinstance(p_node, exp.Parameter) and p_node.name and not p_node.name.isdigit():  # @name, $name
                sqlglot_named_param_names.append(p_node.name)
            elif (
                isinstance(p_node, exp.Placeholder)
                and p_node.this
                and not isinstance(p_node.this, (exp.Identifier, exp.Literal))
                and not str(p_node.this).isdigit()
            ):  # :name
                sqlglot_named_param_names.append(str(p_node.this))
            elif isinstance(p_node, exp.Placeholder) and p_node.this is None:  # ?
                sqlglot_positional_count += 1
            elif isinstance(p_node, exp.Parameter) and (  # noqa: PLR0916
                (p_node.name and p_node.name.isdigit())
                or (
                    not p_node.name
                    and p_node.this
                    and isinstance(p_node.this, (str, exp.Identifier, exp.Literal))
                    and str(p_node.this).isdigit()
                )
            ):
                # $1, :1 style (parsed as Parameter with name="1" or this="1" or this=Identifier(this="1") or this=Literal(this=1))
                sqlglot_positional_count += 1
            elif (
                isinstance(p_node, exp.Placeholder) and p_node.this and str(p_node.this).isdigit()
            ):  # :1 style (Placeholder with this="1")
                sqlglot_positional_count += 1

        if sqlglot_named_param_names:
            msg = f"Sequence parameters provided, but found named placeholders ({', '.join(sorted(set(sqlglot_named_param_names)))}) in SQL: {self.sql}"
            raise ParameterStyleMismatchError(msg)

        actual_count_provided = len(params)

        if sqlglot_positional_count != actual_count_provided:
            msg = (
                f"Parameter count mismatch. SQL expects {sqlglot_positional_count} (sqlglot) positional "
                f"parameters, but {actual_count_provided} were provided. SQL: {self.sql}"
            )
            raise SQLParsingError(msg)

    def _validate_scalar_param(self, all_sqlglot_placeholders: Sequence[exp.Expression], param_value: Any) -> None:
        """Validates a single scalar parameter against parsed SQL parameters."""
        self._validate_sequence_params(
            all_sqlglot_placeholders, (param_value,)
        )  # Treat scalar as a single-element sequence

    def get_expression(self) -> exp.Expression:
        """Get the parsed SQLglot expression, parsing if necessary.

        Returns:
            The SQLglot expression.
        """
        if self._parsed_expression is None:
            self._parse_sql()
        if self._parsed_expression is None:  # Still None after parsing attempt
            return exp.Select()  # Return an empty SELECT as fallback
        return self._parsed_expression

    def generate_param_name(self, base_name: str) -> str:
        """Generates a unique parameter name.

        Args:
            base_name: The base name for the parameter.

        Returns:
            The generated parameter name.
        """
        self._param_counter += 1
        safe_base_name = "".join(c if c.isalnum() else "_" for c in base_name if c.isalnum() or c == "_")
        return f"param_{safe_base_name}_{self._param_counter}"

    def add_condition(self, condition: exp.Condition, params: Optional[dict[str, Any]] = None) -> None:
        """Adds a condition to the WHERE clause of the query.

        Args:
            condition: The condition to add to the WHERE clause.
            params: The parameters to add to the statement parameters.
        """
        expression = self.get_expression()
        if not isinstance(expression, (exp.Select, exp.Update, exp.Delete)):
            return  # Cannot add WHERE to some expressions

        # Update the expression
        expression.where(condition, copy=False)

        # Update the parameters
        if params:
            if self._merged_parameters is None:
                self._merged_parameters = params
            elif isinstance(self._merged_parameters, dict):
                self._merged_parameters.update(params)
            else:
                # Convert to dict if not already
                self._merged_parameters = params

        # Update the SQL string
        self.sql = expression.sql(dialect=self.dialect)

    def add_order_by(self, field_name: str, direction: str = "asc") -> None:
        """Adds an ORDER BY clause.

        Args:
            field_name: The name of the field to order by.
            direction: The direction to order by ("asc" or "desc").
        """
        expression = self.get_expression()
        if not isinstance(expression, exp.Select):
            return

        expression.order_by(exp.Ordered(this=exp.column(field_name), desc=direction.lower() == "desc"), copy=False)
        self.sql = expression.sql(dialect=self.dialect)

    def add_limit(self, limit_val: int, param_name: Optional[str] = None) -> None:
        """Adds a LIMIT clause.

        Args:
            limit_val: The value for the LIMIT clause.
            param_name: Optional name for the parameter.
        """
        expression = self.get_expression()
        if not isinstance(expression, exp.Select):
            return

        if param_name:
            expression.limit(exp.Placeholder(this=param_name), copy=False)
            if self._merged_parameters is None:
                self._merged_parameters = {param_name: limit_val}
            elif isinstance(self._merged_parameters, dict):
                self._merged_parameters[param_name] = limit_val
        else:
            expression.limit(exp.Literal.number(limit_val), copy=False)

        self.sql = expression.sql(dialect=self.dialect)

    def add_offset(self, offset_val: int, param_name: Optional[str] = None) -> None:
        """Adds an OFFSET clause.

        Args:
            offset_val: The value for the OFFSET clause.
            param_name: Optional name for the parameter.
        """
        expression = self.get_expression()
        if not isinstance(expression, exp.Select):
            return

        if param_name:
            expression.offset(exp.Placeholder(this=param_name), copy=False)
            if self._merged_parameters is None:
                self._merged_parameters = {param_name: offset_val}
            elif isinstance(self._merged_parameters, dict):
                self._merged_parameters[param_name] = offset_val
        else:
            expression.offset(exp.Literal.number(offset_val), copy=False)

        self.sql = expression.sql(dialect=self.dialect)

    def apply_filter(self, filter_obj: "StatementFilter") -> "SQLStatement":
        """Apply a statement filter to this statement.

        Args:
            filter_obj: The filter to apply.

        Returns:
            The modified statement.
        """
        from sqlspec.filters import apply_filter

        return apply_filter(self, filter_obj)

    def to_sql(self, dialect: Optional[str] = None) -> str:
        """Generate SQL string using the specified dialect.

        Args:
            dialect: SQL dialect to use for SQL generation. If None, uses the statement's dialect.

        Returns:
            SQL string in the specified dialect.
        """
        expression = self.get_expression()
        return expression.sql(dialect=dialect or self.dialect)
