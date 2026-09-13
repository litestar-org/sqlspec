"""VALUES expression builder.

Provides a builder interface for constructing SQL VALUES clauses with
parameter binding and optional table aliasing.
"""

from collections.abc import Mapping, Sequence
from typing import Any

from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder._parsing_utils import extract_sql_object_expression
from sqlspec.core import SQLResult
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_expression_and_sql

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
        self._alias: str | None = alias
        self._columns: list[str] | None = list(columns) if columns is not None else None
        self._rows: list[list[Any]] = []
        self._initialize_expression()

        if rows is not None:
            self.add_rows(rows)

    def _create_base_expression(self) -> exp.Values:
        """Create initial empty VALUES expression."""
        return exp.Values()

    @property
    def _expected_result_type(self) -> type[SQLResult]:
        """Return expected result type for VALUES queries."""
        return SQLResult

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
        if self._rows and len(columns) != len(self._rows[0]):
            msg = f"Column count ({len(columns)}) does not match row width ({len(self._rows[0])})."
            raise SQLBuilderError(msg)
        self._columns = list(columns)
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

        first_row = rows[0]
        if isinstance(first_row, Mapping):
            if self._columns is None:
                self._columns = list(first_row.keys())
            if not self._columns:
                msg = "VALUES clause rows must contain at least one column."
                raise SQLBuilderError(msg)
            expected_keys = set(self._columns)
            normalized_rows: list[list[Any]] = []
            for idx, r in enumerate(rows):
                if not isinstance(r, Mapping):
                    msg = f"Row {idx} is not a mapping like the initial row."
                    raise SQLBuilderError(msg)
                if set(r.keys()) != expected_keys:
                    msg = "All rows in VALUES clause must have the same keys as the initial row."
                    raise SQLBuilderError(msg)
                normalized_rows.append([r[k] for k in self._columns])
        else:
            if not isinstance(first_row, (list, tuple)):
                msg = "VALUES rows must be sequences or mappings."
                raise SQLBuilderError(msg)
            expected_len = len(first_row)
            if expected_len == 0:
                msg = "VALUES clause rows must contain at least one column."
                raise SQLBuilderError(msg)
            normalized_rows = []
            for idx, r in enumerate(rows):
                if not isinstance(r, (list, tuple)):
                    msg = f"Row {idx} must be a sequence."
                    raise SQLBuilderError(msg)
                if len(r) != expected_len:
                    msg = "All rows in VALUES clause must have the same number of columns."
                    raise SQLBuilderError(msg)
                normalized_rows.append(list(r))

            if self._columns is not None and len(self._columns) != expected_len:
                msg = f"Column count ({len(self._columns)}) does not match row width ({expected_len})."
                raise SQLBuilderError(msg)

        if self._rows and len(normalized_rows[0]) != len(self._rows[0]):
            msg = "All rows in VALUES clause must have the same number of columns."
            raise SQLBuilderError(msg)
        self._rows.extend(normalized_rows)
        self._rebuild_expression()
        return self

    def _rebuild_expression(self) -> None:
        """Rebuild the underlying sqlglot expression and parameter bindings."""
        self._parameters.clear()
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

                if isinstance(val, exp.Expr):
                    row_expressions.append(val)
                elif has_expression_and_sql(val):
                    row_expressions.append(extract_sql_object_expression(val, builder=self))
                else:
                    placeholder, _ = self.create_placeholder(val, col_name)
                    row_expressions.append(placeholder)
            tuple_expressions.append(exp.Tuple(expressions=row_expressions))

        values_expr = exp.Values(expressions=tuple_expressions)
        if self._alias:
            if self._columns:
                self._expression = exp.alias_(values_expr, alias=self._alias, table=self._columns)
            else:
                self._expression = exp.alias_(values_expr, alias=self._alias)
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
        return super()._build_final_expression(copy=copy)
