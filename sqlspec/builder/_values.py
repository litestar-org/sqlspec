"""VALUES expression builder.

Provides a builder interface for constructing SQL VALUES clauses with
parameter binding and optional table aliasing.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Self

from sqlglot import exp

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder._parsing_utils import resolve_value_expression
from sqlspec.exceptions import SQLBuilderError

__all__ = ("Values",)


class Values(QueryBuilder):
    """Builder for SQL VALUES clauses.

    Constructs parameterized VALUES expressions that can be executed directly,
    used as Common Table Expressions (CTEs), or embedded in FROM clauses.
    """

    __slots__ = ("_alias", "_columns", "_rows")

    def __init__(
        self,
        rows: Sequence[Sequence[Any] | Mapping[str, Any]] | None = None,
        *,
        alias: str | None = None,
        columns: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize a VALUES builder.

        Args:
            rows: Sequence of row tuples/lists or mappings.
            alias: Optional table alias for the VALUES clause.
            columns: Optional column names for the table alias.
            **kwargs: Additional QueryBuilder options.
        """
        self._init_query_builder(kwargs)
        if alias is not None:
            _validate_values_alias(alias)
        self._alias: str | None = alias
        self._columns: list[str] | None = _validate_columns(columns) if columns is not None else None
        self._rows: list[list[Any]] = []
        self._initialize_expression()

        if rows is not None:
            self.add_rows(rows)

    @property
    def alias_name(self) -> str | None:
        """Get the table alias name if defined.

        Returns:
            The alias name, or None if unaliased.
        """
        return self._alias

    @property
    def columns(self) -> list[str] | None:
        """Get the column names if defined.

        Returns:
            List of column names, or None if not defined.
        """
        return list(self._columns) if self._columns is not None else None

    def as_(self, alias: str) -> Self:
        """Set the table alias for this VALUES clause.

        Args:
            alias: Table alias name.

        Returns:
            Self for method chaining.
        """
        _validate_values_alias(alias)
        self._alias = alias
        if self._rows:
            self._rebuild_expression()
        return self

    def set_columns(self, *columns: str) -> Self:
        """Set the column names for this VALUES clause.

        Args:
            *columns: Column names to assign.

        Returns:
            Self for method chaining.

        Raises:
            SQLBuilderError: If column count does not match row width.
        """
        validated_columns = _validate_columns(columns)
        if self._rows and len(validated_columns) != len(self._rows[0]):
            msg = f"Column count ({len(validated_columns)}) does not match row width ({len(self._rows[0])})."
            raise SQLBuilderError(msg)
        self._columns = validated_columns
        if self._rows:
            self._rebuild_expression()
        return self

    def add_rows(self, rows: Sequence[Sequence[Any] | Mapping[str, Any]]) -> Self:
        """Add rows to the VALUES clause.

        Args:
            rows: Sequence of row tuples, lists, or mappings.

        Returns:
            Self for method chaining.

        Raises:
            SQLBuilderError: If rows is empty, non-uniform, or column count mismatches.
        """
        if not rows:
            msg = "VALUES clause requires at least one row."
            raise SQLBuilderError(msg)

        columns = self._columns
        first_row = rows[0]
        if isinstance(first_row, Mapping):
            if columns is None:
                if not first_row:
                    msg = "VALUES clause rows must contain at least one column."
                    raise SQLBuilderError(msg)
                columns = _validate_columns(list(first_row.keys()))
            if not columns:
                msg = "VALUES clause rows must contain at least one column."
                raise SQLBuilderError(msg)
            expected_keys = set(columns)
            normalized_rows: list[list[Any]] = []
            for idx, r in enumerate(rows):
                if not isinstance(r, Mapping):
                    msg = f"Row {idx} is not a mapping like the initial row."
                    raise SQLBuilderError(msg)
                for key in r:
                    if not isinstance(key, str) or not key.strip() or "\x00" in key:
                        msg = f"Invalid column key {key!r} in VALUES clause."
                        raise SQLBuilderError(msg)
                if set(r.keys()) != expected_keys:
                    msg = "All rows in VALUES clause must have the same keys as the initial row."
                    raise SQLBuilderError(msg)
                normalized_rows.append([r[k] for k in columns])
        else:
            if not isinstance(first_row, Sequence) or isinstance(first_row, (str, bytes, bytearray)):
                msg = "VALUES rows must be sequences or mappings."
                raise SQLBuilderError(msg)
            expected_len = len(first_row)
            if expected_len == 0:
                msg = "VALUES clause rows must contain at least one column."
                raise SQLBuilderError(msg)
            normalized_rows = []
            for idx, r in enumerate(rows):
                if not isinstance(r, Sequence) or isinstance(r, (str, bytes, bytearray)):
                    msg = f"Row {idx} must be a sequence."
                    raise SQLBuilderError(msg)
                if len(r) != expected_len:
                    msg = "All rows in VALUES clause must have the same number of columns."
                    raise SQLBuilderError(msg)
                normalized_rows.append(list(r))

            if columns is not None and len(columns) != expected_len:
                msg = f"Column count ({len(columns)}) does not match row width ({expected_len})."
                raise SQLBuilderError(msg)

        if self._rows and len(normalized_rows[0]) != len(self._rows[0]):
            msg = "All rows in VALUES clause must have the same number of columns."
            raise SQLBuilderError(msg)
        self._columns = columns
        self._rows.extend(normalized_rows)
        self._rebuild_expression()
        return self

    def _create_base_expression(self) -> exp.Values:
        """Create initial empty VALUES expression."""
        return exp.Values()

    def _rebuild_expression(self) -> None:
        """Rebuild the underlying sqlglot expression and parameter bindings."""
        cte_parameters = {
            str(placeholder.this): self._parameters[str(placeholder.this)]
            for cte in self._with_ctes.values()
            for placeholder in cte.find_all(exp.Placeholder)
            if str(placeholder.this) in self._parameters
        }
        self._parameters.clear()
        self._parameters.update(cte_parameters)
        self._parameter_name_counters.clear()
        self._parameter_counter = 0

        tuple_expressions: list[exp.Tuple] = []
        for row in self._rows:
            row_expressions: list[exp.Expr] = []
            for col_idx, val in enumerate(row):
                if self._columns and col_idx < len(self._columns):
                    col_name = self._columns[col_idx]
                else:
                    col_name = f"col_{col_idx + 1}"

                resolved = resolve_value_expression(val, self, col_name)
                if resolved is not None:
                    row_expressions.append(resolved)
                else:
                    placeholder, _ = self.create_placeholder(val, col_name)
                    row_expressions.append(placeholder)
            tuple_expressions.append(exp.Tuple(expressions=row_expressions))

        values_expr = exp.Values(expressions=tuple_expressions)
        if self._alias:
            if self._columns:
                self._expression = exp.alias_(
                    values_expr, alias=self._alias, table=[exp.to_identifier(c) for c in self._columns], copy=False
                )
            else:
                self._expression = exp.alias_(values_expr, alias=self._alias, copy=False)
        else:
            self._expression = values_expr

    def _build_final_expression(self, *, copy: bool = False) -> exp.Expr:
        """Construct the final expression for the VALUES clause.

        Args:
            copy: Whether to copy the expression.

        Returns:
            SQLGlot expression representing the VALUES clause.

        Raises:
            SQLBuilderError: If no rows have been provided.
        """
        if not self._rows:
            msg = "VALUES clause requires at least one row."
            raise SQLBuilderError(msg)
        expression = super()._build_final_expression(copy=copy)
        with_clause = expression.args.pop("with_", None)
        if with_clause is not None:
            if not expression.args.get("alias"):
                expression = exp.alias_(expression, "_values", table=self._columns or True, copy=False)
            expression = exp.select("*", copy=False).from_(expression, copy=False)
            expression.set("with_", with_clause)
        return expression


def _validate_values_alias(alias: Any) -> str:
    """Validate that a VALUES table alias is a non-empty string without NUL bytes."""
    if not isinstance(alias, str) or not alias.strip() or "\x00" in alias:
        msg = f"Invalid alias {alias!r}: expected a non-empty identifier."
        raise SQLBuilderError(msg)
    return alias


def _validate_columns(columns: Sequence[Any]) -> list[str]:
    """Validate VALUES column names are unique, non-empty strings without NUL bytes."""
    validated: list[str] = []
    for col in columns:
        if not isinstance(col, str) or not col.strip() or "\x00" in col:
            msg = f"Invalid column name {col!r} in VALUES clause."
            raise SQLBuilderError(msg)
        validated.append(col)
    if len(set(validated)) != len(validated):
        msg = "VALUES clause column names must be unique."
        raise SQLBuilderError(msg)
    return validated
