# ruff: noqa: PLR6301, SLF001
"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder._base import QueryBuilder, WhereClauseMixin
from sqlspec.statement.result import ExecuteResult

if TYPE_CHECKING:
    from sqlspec.statement.builder._select import SelectBuilder

__all__ = ("UpdateBuilder",)

logger = logging.getLogger("sqlspec")


@dataclass(unsafe_hash=True)
class UpdateBuilder(QueryBuilder[ExecuteResult], WhereClauseMixin):
    """Builder for UPDATE statements.

    This builder provides a fluent interface for constructing SQL UPDATE statements
    with automatic parameter binding and validation.

    Example:
        ```python
        # Basic UPDATE
        update_query = (
            UpdateBuilder()
            .table("users")
            .set("name", "John Doe")
            .set("email", "john@example.com")
            .where("id = 1")
        )

        # UPDATE with parameterized conditions
        update_query = (
            UpdateBuilder()
            .table("users")
            .set("status", "active")
            .where_eq("id", 123)
        )

        # UPDATE with FROM clause (PostgreSQL style)
        update_query = (
            UpdateBuilder()
            .table("users", "u")
            .set("name", "Updated Name")
            .from_("profiles", "p")
            .where("u.id = p.user_id AND p.is_verified = true")
        )
        ```
    """

    _table: "Optional[str]" = field(default=None, init=False)

    @property
    def _expected_result_type(self) -> "type[ExecuteResult]":
        """Return the expected result type for this builder."""
        return ExecuteResult

    def _create_base_expression(self) -> exp.Update:
        """Create a base UPDATE expression.

        Returns:
            A new sqlglot Update expression with empty clauses.
        """
        return exp.Update(this=None, expressions=[], joins=[])

    def table(self, table_name: str, alias: "Optional[str]" = None) -> Self:
        """Set the table to update.

        Args:
            table_name: The name of the table.
            alias: Optional alias for the table.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            self._expression = self._create_base_expression()

        table_expr: exp.Expression = exp.to_table(table_name, alias=alias)
        self._expression.set("this", table_expr)
        self._table = table_name
        return self

    def set(self, column: "Union[str, exp.Expression]", value: Any) -> Self:
        """Set a column to a value with parameter binding.

        Args:
            column: The column name or expression to set.
            value: The value to set the column to. Will be parameterized for safety.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            msg = "Cannot add SET clause to non-UPDATE expression."
            raise SQLBuilderError(msg)

        col_expr: exp.Expression = exp.column(column) if isinstance(column, str) else column
        param_name = self._add_parameter(value)
        value_expr: exp.Expression = exp.var(param_name)

        # Create a SET expression
        set_expr = exp.Set(this=col_expr, expression=value_expr)

        # Add to the expressions list
        if not self._expression.args.get("expressions"):
            self._expression.set("expressions", [])
        self._expression.args["expressions"].append(set_expr)

        return self

    def set_multiple(self, **values: Any) -> Self:
        """Set multiple columns with values.

        This is a convenience method for setting multiple columns at once.

        Args:
            **values: Keyword arguments where keys are column names and values are the values to set.

        Returns:
            The current builder instance for method chaining.
        """
        for column, value in values.items():
            self.set(column, value)
        return self

    def where(self, condition: "Union[str, exp.Expression]") -> Self:
        """Add WHERE clause to filter rows to update.

        Args:
            condition: The WHERE condition as a string or sqlglot expression.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            msg = "Cannot add WHERE clause to non-UPDATE expression."
            raise SQLBuilderError(msg)

        condition_expr: exp.Expression = exp.condition(condition) if isinstance(condition, str) else condition
        self._expression = self._expression.where(condition_expr)
        return self

    def from_(self, table: "Union[str, exp.Expression, SelectBuilder]", alias: "Optional[str]" = None) -> Self:
        """Add a FROM clause to the UPDATE statement (e.g., for PostgreSQL).

        Args:
            table: The table name, expression, or subquery to add to the FROM clause.
            alias: Optional alias for the table in the FROM clause.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            msg = "Cannot add FROM clause to non-UPDATE expression. Set the main table first."
            raise SQLBuilderError(msg)

        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = exp.to_table(table, alias=alias)
        elif isinstance(table, QueryBuilder):
            subquery_builder_params: dict[str, Any] = table._parameters
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)

            subquery_exp: exp.Expression = exp.paren(table._expression or exp.select())
            table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
        elif isinstance(table, exp.Expression):
            table_expr = exp.alias_(table, alias) if alias else table
        else:
            msg = f"Unsupported table type for FROM clause: {type(table)}"
            raise SQLBuilderError(msg)

        if self._expression.args.get("from") is None:
            self._expression.set("from", exp.From(expressions=[]))

        from_clause = self._expression.args["from"]
        if hasattr(from_clause, "append"):
            from_clause.append("expressions", table_expr)
        else:
            # Handle different sqlglot versions
            if not from_clause.expressions:
                from_clause.expressions = []
            from_clause.expressions.append(table_expr)

        return self

    def join(
        self,
        table: "Union[str, exp.Expression, SelectBuilder]",
        on: "Union[str, exp.Expression]",
        alias: "Optional[str]" = None,
        join_type: str = "INNER",
    ) -> Self:
        """Add JOIN clause to the UPDATE statement.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.
            join_type: Type of join (INNER, LEFT, RIGHT, FULL).

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            msg = "Cannot add JOIN clause to non-UPDATE expression."
            raise SQLBuilderError(msg)

        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = exp.table_(table, alias=alias)
        elif isinstance(table, QueryBuilder):
            subquery = table.build()
            subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=self.dialect))
            table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp

            # Merge parameters
            subquery_params = table._parameters
            if subquery_params:
                for p_name, p_value in subquery_params.items():
                    self.add_parameter(p_value, name=p_name)
        else:
            table_expr = table

        on_expr: exp.Expression = exp.condition(on) if isinstance(on, str) else on

        join_type_upper = join_type.upper()
        if join_type_upper == "INNER":
            join_expr = exp.Join(this=table_expr, on=on_expr)
        elif join_type_upper == "LEFT":
            join_expr = exp.Join(this=table_expr, on=on_expr, side="LEFT")
        elif join_type_upper == "RIGHT":
            join_expr = exp.Join(this=table_expr, on=on_expr, side="RIGHT")
        elif join_type_upper == "FULL":
            join_expr = exp.Join(this=table_expr, on=on_expr, side="FULL", kind="OUTER")
        else:
            msg = f"Unsupported join type: {join_type}"
            raise SQLBuilderError(msg)

        # Add join to the UPDATE expression
        if not self._expression.args.get("joins"):
            self._expression.set("joins", [])
        self._expression.args["joins"].append(join_expr)

        return self
