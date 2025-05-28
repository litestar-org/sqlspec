# ruff: noqa: PLR6301
"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Union, cast

from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.sql.builder._base import QueryBuilder

__all__ = ("DeleteBuilder",)

logger = logging.getLogger("sqlspec")


@dataclass
class DeleteBuilder(QueryBuilder):
    """Builder for DELETE statements."""

    _table: Optional[str] = field(default=None, init=False)

    def _create_base_expression(self) -> exp.Expression:
        """Create a base DELETE expression.

        Returns:
            exp.Expression: A new sqlglot Delete expression.
        """
        return exp.Delete()

    def from_(self, table: str) -> "DeleteBuilder":
        """Set target table for DELETE.

        Args:
            table: The table name to delete from.

        Returns:
            DeleteBuilder: The current builder instance for method chaining.
        """
        self._table = table
        if not isinstance(self._expression, exp.Delete):
            self._expression = exp.Delete()
        self._expression = cast("exp.Delete", self._expression).from_(exp.to_table(table), copy=False)  # type: ignore[attr-defined,redundant-cast]
        return self

    def where(self, condition: Union[str, exp.Expression]) -> "DeleteBuilder":
        """Add WHERE clause to filter rows to delete.

        Args:
            condition: The WHERE condition as a string or sqlglot expression.

        Raises:
            SQLBuilderError: If the current expression is not a DELETE statement.

        Returns:
            DeleteBuilder: The current builder instance for method chaining.
        """
        if self._expression is None or not isinstance(self._expression, exp.Delete):
            msg = "Cannot add WHERE clause to non-DELETE expression."
            raise SQLBuilderError(msg)

        condition_expr = exp.condition(condition) if isinstance(condition, str) else condition
        self._expression = self._expression.where(condition_expr, copy=False)
        return self

    def where_eq(self, column: Union[str, exp.Expression], value: Any) -> "DeleteBuilder":
        """Add WHERE column = value clause with parameter binding.

        Args:
            column: The column name or expression to compare.
            value: The value to compare against. Will be parameterized for safety.

        Returns:
            DeleteBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        param_name = self._add_parameter(value, name=f"where_{column}" if isinstance(column, str) else None)
        value_expr = exp.Placeholder(this=param_name)
        condition = exp.EQ(this=col_expr, expression=value_expr)
        return self.where(condition)

    def where_in(
        self, column: Union[str, exp.Expression], values: Union[list[Any], tuple[Any, ...]]
    ) -> "DeleteBuilder":
        """Add WHERE column IN (values) clause with parameter binding.

        Args:
            column: The column name or expression to check.
            values: The list or tuple of values to check against. Will be parameterized for safety.

        Returns:
            DeleteBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        param_names = [self._add_parameter(val) for val in values]
        value_exprs = [exp.Placeholder(this=name) for name in param_names]
        in_expr = exp.In(this=col_expr, expressions=value_exprs)
        return self.where(in_expr)

    def join(
        self,
        table: Union[str, exp.Expression, "QueryBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
        join_type: str = "INNER",
    ) -> "DeleteBuilder":
        """Add JOIN clause for multi-table deletes.

        Note: This is primarily for MySQL-style DELETE with JOINs.
        Not all databases support JOINs in DELETE statements.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.
            join_type: The type of JOIN (INNER, LEFT, RIGHT, FULL).

        Raises:
            SQLBuilderError: If the current expression is not a DELETE statement.

        Returns:
            DeleteBuilder: The current builder instance for method chaining.
        """
        if self._expression is None or not isinstance(self._expression, exp.Delete):
            msg = "Cannot add JOIN clause to non-DELETE expression."
            raise SQLBuilderError(msg)

        # Handle different table types
        if isinstance(table, str):
            table_expr = exp.table_(table, alias=alias)
        elif hasattr(table, "build"):  # QueryBuilder instance
            # For subqueries, we need to build the SQL and parse it back
            subquery = exp.paren(exp.maybe_parse(table.build().sql, dialect=self.dialect))  # type: ignore[attr-defined]
            table_expr = exp.alias_(subquery, alias) if alias else subquery  # type: ignore[assignment]
        else:
            table_expr = exp.alias_(table, alias) if alias else table  # type: ignore[assignment]

        on_expr = exp.condition(on) if isinstance(on, str) else on
        join_expr = exp.Join(this=table_expr, on=on_expr, kind=join_type)

        # For DELETE with JOIN, we need to add it to the FROM clause rather than a join method
        # This follows MySQL syntax: DELETE t1 FROM table1 t1 JOIN table2 t2 ON ...
        if not self._expression.find(exp.From):
            # If no FROM clause exists yet, this is unusual for DELETE but we'll handle it
            msg = "Cannot add JOIN to DELETE without a FROM clause. Use from_() first."
            raise SQLBuilderError(msg)

        # Add the join to the existing FROM clause
        from_clause = self._expression.find(exp.From)
        if from_clause:
            current_joins = from_clause.args.get("joins", [])
            from_clause.set("joins", [*current_joins, join_expr])

        return self

    def inner_join(
        self,
        table: Union[str, exp.Expression, "QueryBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
    ) -> "DeleteBuilder":
        """Add INNER JOIN clause.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.

        Returns:
            DeleteBuilder: The current builder instance for method chaining.
        """
        return self.join(table, on, alias, "INNER")

    def left_join(
        self,
        table: Union[str, exp.Expression, "QueryBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
    ) -> "DeleteBuilder":
        """Add LEFT JOIN clause.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.

        Returns:
            DeleteBuilder: The current builder instance for method chaining.
        """
        return self.join(table, on, alias, "LEFT")

    def right_join(
        self,
        table: Union[str, exp.Expression, "QueryBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
    ) -> "DeleteBuilder":
        """Add RIGHT JOIN clause.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.

        Returns:
            DeleteBuilder: The current builder instance for method chaining.
        """
        return self.join(table, on, alias, "RIGHT")
