# ruff: noqa: PLR6301, SLF001
"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, Union

from sqlglot import exp
from typing_extensions import LiteralString, Self

from sqlspec.statement.builder._base import QueryBuilder, WhereClauseMixin
from sqlspec.statement.result import ExecuteResult

if TYPE_CHECKING:
    from sqlspec.statement.builder._select import SelectBuilder

__all__ = ("DeleteBuilder",)


@dataclass(unsafe_hash=True)
class DeleteBuilder(QueryBuilder[ExecuteResult[dict[str, Any]]], WhereClauseMixin):
    """Builder for DELETE statements.

    This builder provides a fluent interface for constructing SQL DELETE statements
    with automatic parameter binding and validation. It does not support JOIN
    operations to maintain cross-dialect compatibility and safety.

    Example:
        ```python
        # Basic DELETE
        delete_query = (
            DeleteBuilder().from_("users").where("age < 18")
        )

        # DELETE with parameterized conditions
        delete_query = (
            DeleteBuilder()
            .from_("users")
            .where_eq("status", "inactive")
            .where_in("category", ["test", "demo"])
        )
        ```
    """

    _table: "Optional[str]" = field(default=None, init=False)

    @property
    def _expected_result_type(self) -> "type[ExecuteResult[dict[str, Any]]]":
        """Get the expected result type for DELETE operations.

        Returns:
            The ExecuteResult type for DELETE statements.
        """
        return ExecuteResult[dict[str, Any]]

    def _create_base_expression(self) -> "exp.Delete":
        """Create a new sqlglot Delete expression.

        Returns:
            A new sqlglot Delete expression.
        """
        return exp.Delete()

    def from_(self, table: str) -> "Self":
        """Set the target table for the DELETE statement.

        Args:
            table: The table name to delete from.

        Returns:
            The current builder instance for method chaining.
        """
        self._table = table
        if not isinstance(self._expression, exp.Delete):
            current_expr_type = type(self._expression).__name__
            self._raise_sql_builder_error(f"Base expression for DeleteBuilder is {current_expr_type}, expected Delete.")

        self._expression.set("this", exp.to_table(table))
        return self

    def where(self, condition: "Union[LiteralString, exp.Expression]") -> "Self":
        """Add a WHERE clause to filter rows to delete.

        Args:
            condition: The WHERE condition as a literal string or sqlglot expression.

        Returns:
            The current builder instance for method chaining.
        """
        if not self._table:
            self._raise_sql_builder_error("Cannot apply WHERE clause: target table not set. Use from_() first.")
        if not isinstance(self._expression, exp.Delete):
            self._raise_sql_builder_error(
                f"Cannot apply WHERE clause to non-DELETE expression: {type(self._expression).__name__}."
            )

        self._expression = self._expression.where(condition, dialect=self.dialect_name)
        return self

    def where_eq(self, column: "Union[str, exp.Column]", value: Any) -> "Self":
        """Add an equality condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.
            value: The value to compare against. Will be automatically parameterized.

        Returns:
            The current builder instance for method chaining.
        """
        _, param_name = self.add_parameter(value)
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.eq(exp.var(param_name))
        return self.where(condition)

    def where_in(
        self,
        column: "Union[str, exp.Column]",
        values: "Union[tuple[Any, ...], list[Any], SelectBuilder]",
    ) -> "Self":
        """Add an IN condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.
            values: A tuple/list of values or a SelectBuilder for a subquery.
                   List/tuple values will be automatically parameterized.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column

        if isinstance(values, (tuple, list)):
            param_names: list[str] = []
            for val in values:
                _, param_name = self.add_parameter(val)
                param_names.append(param_name)
            condition_values: list[exp.Expression] = [exp.var(name) for name in param_names]
            condition: exp.Expression = col_expr.isin(*condition_values)
        elif isinstance(values, QueryBuilder):
            # Merge parameters from subquery
            subquery_builder_params: dict[str, Any] = values._parameters
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)

            # Use the subquery expression directly
            subquery_expression = values.build().sql
            condition = col_expr.isin(subquery_expression)
        else:
            self._raise_sql_builder_error(
                f"Unsupported 'values' type for WHERE IN: {type(values)}. Expected tuple, list, or SelectBuilder."
            )
        return self.where(condition)

    def where_not_in(
        self,
        column: "Union[str, exp.Column]",
        values: "Union[tuple[Any, ...], list[Any], SelectBuilder]",
    ) -> "Self":
        """Add a NOT IN condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.
            values: A tuple/list of values or a SelectBuilder for a subquery.
                   List/tuple values will be automatically parameterized.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column

        if isinstance(values, (tuple, list)):
            param_names: list[str] = []
            for val in values:
                _, param_name = self.add_parameter(val)
                param_names.append(param_name)
            condition_values: list[exp.Expression] = [exp.var(name) for name in param_names]
            condition: exp.Expression = col_expr.isin(*condition_values).not_()
        elif isinstance(values, QueryBuilder):
            # Merge parameters from subquery
            subquery_builder_params: dict[str, Any] = values._parameters
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)

            # Use the subquery expression directly
            subquery_expression = values.build().sql
            condition = col_expr.isin(subquery_expression).not_()
        else:
            self._raise_sql_builder_error(
                f"Unsupported 'values' type for WHERE NOT IN: {type(values)}. Expected tuple, list, or SelectBuilder."
            )
        return self.where(condition)

    def where_between(
        self,
        column: "Union[str, exp.Column]",
        low: Any,
        high: Any,
    ) -> "Self":
        """Add a BETWEEN condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.
            low: The lower bound value. Will be automatically parameterized.
            high: The upper bound value. Will be automatically parameterized.

        Returns:
            The current builder instance for method chaining.
        """
        _, low_param = self.add_parameter(low)
        _, high_param = self.add_parameter(high)

        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.between(exp.var(low_param), exp.var(high_param))
        return self.where(condition)

    def where_like(self, column: "Union[str, exp.Column]", pattern: str) -> "Self":
        """Add a LIKE condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.
            pattern: The LIKE pattern. Will be automatically parameterized.

        Returns:
            The current builder instance for method chaining.
        """
        _, param_name = self.add_parameter(pattern)
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.like(exp.var(param_name))
        return self.where(condition)

    def where_not_like(self, column: "Union[str, exp.Column]", pattern: str) -> "Self":
        """Add a NOT LIKE condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.
            pattern: The LIKE pattern. Will be automatically parameterized.

        Returns:
            The current builder instance for method chaining.
        """
        _, param_name = self.add_parameter(pattern)
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.like(exp.var(param_name)).not_()
        return self.where(condition)

    def where_is_null(self, column: "Union[str, exp.Column]") -> "Self":
        """Add an IS NULL condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null())
        return self.where(condition)

    def where_is_not_null(self, column: "Union[str, exp.Column]") -> "Self":
        """Add an IS NOT NULL condition to the WHERE clause.

        Args:
            column: The column name or sqlglot Column expression.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null()).not_()
        return self.where(condition)

    def where_exists(self, subquery: "Union[SelectBuilder, str]") -> "Self":
        """Add a WHERE EXISTS clause.

        Args:
            subquery: The subquery for the EXISTS clause. Can be a SelectBuilder instance or raw SQL string.

        Returns:
            The current builder instance for method chaining.
        """
        if isinstance(subquery, QueryBuilder):
            # Merge parameters from subquery
            subquery_builder_params: dict[str, Any] = subquery._parameters
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)

            # Get the subquery SQL
            sub_sql_obj = subquery.build()
            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=self.dialect_name)
        else:
            sub_expr = exp.maybe_parse(subquery, dialect=self.dialect_name)

        if not sub_expr:
            msg = f"Could not parse subquery for EXISTS: {subquery}"
            self._raise_sql_builder_error(msg)

        exists_expr = exp.Exists(this=sub_expr)
        return self.where(exists_expr)

    def where_not_exists(self, subquery: "Union[SelectBuilder, str]") -> "Self":
        """Add a WHERE NOT EXISTS clause.

        Args:
            subquery: The subquery for the NOT EXISTS clause. Can be a SelectBuilder instance or raw SQL string.

        Returns:
            The current builder instance for method chaining.
        """
        if isinstance(subquery, QueryBuilder):
            # Merge parameters from subquery
            subquery_builder_params: dict[str, Any] = subquery._parameters
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)

            # Get the subquery SQL
            sub_sql_obj = subquery.build()
            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=self.dialect_name)
        else:
            sub_expr = exp.maybe_parse(subquery, dialect=self.dialect_name)

        if not sub_expr:
            msg = f"Could not parse subquery for NOT EXISTS: {subquery}"
            self._raise_sql_builder_error(msg)

        not_exists_expr = exp.Not(this=exp.Exists(this=sub_expr))
        return self.where(not_exists_expr)
