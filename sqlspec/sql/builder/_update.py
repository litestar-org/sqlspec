# ruff: noqa: PLR6301
"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Union

from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.sql.builder._base import QueryBuilder

__all__ = ("UpdateBuilder",)

logger = logging.getLogger("sqlspec")


@dataclass
class UpdateBuilder(QueryBuilder):
    """Builder for UPDATE statements."""

    _table: Optional[str] = field(default=None, init=False)

    def _create_base_expression(self) -> exp.Expression:
        """Create a base UPDATE expression.

        Returns:
            exp.Expression: A new sqlglot Update expression.
        """
        return exp.Update()

    def table(self, table: str) -> "UpdateBuilder":
        """Set target table for UPDATE.

        Args:
            table: The table name to update.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.

        Returns:
            UpdateBuilder: The current builder instance for method chaining.
        """
        self._table = table
        if self._expression is None:
            self._expression = exp.Update()
        if not isinstance(self._expression, exp.Update):
            msg = "Cannot set table for a non-UPDATE expression."
            raise SQLBuilderError(msg)
        self._expression = self._expression.table(exp.table_(table), copy=False)
        return self

    def set(self, column: Union[str, exp.Expression], value: Any) -> "UpdateBuilder":
        """Set a column to a value with parameter binding.

        Args:
            column: The column name or expression to set.
            value: The value to set the column to. Will be parameterized for safety.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.

        Returns:
            UpdateBuilder: The current builder instance for method chaining.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            msg = "Cannot add SET clause to non-UPDATE expression."
            raise SQLBuilderError(msg)

        col_expr = exp.column(column) if isinstance(column, str) else column
        param_name = self._add_parameter(value, name=f"set_{column}" if isinstance(column, str) else None)
        value_expr = exp.Placeholder(this=param_name)

        set_expr = exp.Set(this=col_expr, expression=value_expr)
        self._expression = self._expression.set(set_expr, copy=False)  # type: ignore[call-arg,arg-type,func-returns-value]
        return self

    def where(self, condition: Union[str, exp.Expression]) -> "UpdateBuilder":
        """Add WHERE clause to filter rows to update.

        Args:
            condition: The WHERE condition as a string or sqlglot expression.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.

        Returns:
            UpdateBuilder: The current builder instance for method chaining.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            msg = "Cannot add WHERE clause to non-UPDATE expression."
            raise SQLBuilderError(msg)

        condition_expr = exp.condition(condition) if isinstance(condition, str) else condition
        self._expression = self._expression.where(condition_expr, copy=False)
        return self

    def join(
        self,
        table: Union[str, exp.Expression, "QueryBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
        join_type: str = "INNER",
    ) -> "UpdateBuilder":
        """Add JOIN clause for multi-table updates.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.
            join_type: The type of JOIN (INNER, LEFT, RIGHT, FULL).

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.

        Returns:
            UpdateBuilder: The current builder instance for method chaining.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            msg = "Cannot add JOIN clause to non-UPDATE expression."
            raise SQLBuilderError(msg)

        # Handle different table types
        if isinstance(table, str):
            table_expr = exp.table_(table, alias=alias)
        elif hasattr(table, "build"):
            subquery = exp.paren(exp.maybe_parse(table.build().sql, dialect=self.dialect))  # pyright: ignore
            table_expr = exp.alias_(subquery, alias) if alias else subquery  # type: ignore[assignment]
        else:
            table_expr = exp.alias_(table, alias) if alias else table  # type: ignore[assignment]

        on_expr = exp.condition(on) if isinstance(on, str) else on
        join_expr = exp.Join(this=table_expr, on=on_expr, kind=join_type)

        self._expression = self._expression.join(join_expr, copy=False)  # type: ignore[attr-defined]
        return self

    def inner_join(
        self,
        table: Union[str, exp.Expression, "QueryBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
    ) -> "UpdateBuilder":
        """Add INNER JOIN clause.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.

        Returns:
            UpdateBuilder: The current builder instance for method chaining.
        """
        return self.join(table, on, alias, "INNER")

    def left_join(
        self,
        table: Union[str, exp.Expression, "QueryBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
    ) -> "UpdateBuilder":
        """Add LEFT JOIN clause.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.

        Returns:
            UpdateBuilder: The current builder instance for method chaining.
        """
        return self.join(table, on, alias, "LEFT")

    def right_join(
        self,
        table: Union[str, exp.Expression, "QueryBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
    ) -> "UpdateBuilder":
        """Add RIGHT JOIN clause.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.

        Returns:
            UpdateBuilder: The current builder instance for method chaining.
        """
        return self.join(table, on, alias, "RIGHT")

    def from_(self, table: Union[str, exp.Expression]) -> "UpdateBuilder":
        """Add FROM clause for multi-table updates (MySQL/PostgreSQL style).

        Args:
            table: The table name or expression for the FROM clause.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.

        Returns:
            UpdateBuilder: The current builder instance for method chaining.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            msg = "Cannot add FROM clause to non-UPDATE expression."
            raise SQLBuilderError(msg)

        table_expr = exp.table_(table) if isinstance(table, str) else table
        self._expression = self._expression.from_(table_expr, copy=False)
        return self
