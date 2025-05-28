# ruff: noqa: PLR6301, PLR0904
"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import logging
from dataclasses import dataclass
from typing import Any, Optional, Union

from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.sql.builder._base import QueryBuilder

__all__ = ("SelectBuilder",)

logger = logging.getLogger("sqlspec")


@dataclass
class SelectBuilder(QueryBuilder):
    """Builder for SELECT statements."""

    def _create_base_expression(self) -> exp.Expression:
        """Create a base SELECT expression

        Returns:
            exp.Expression: A new sqlglot Select expression.
        """
        return exp.Select()

    def select(self, *columns: Union[str, exp.Expression]) -> "SelectBuilder":
        """Add columns to SELECT clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add select columns to a non-SELECT expression."
            raise SQLBuilderError(msg)

        for column in columns:
            self._expression = self._expression.select(
                column if isinstance(column, exp.Expression) else exp.column(column), copy=False
            )
        return self

    def from_(self, table: Union[str, exp.Expression, "SelectBuilder"], alias: Optional[str] = None) -> "SelectBuilder":
        """Add FROM clause.

        Args:
            table: The table name, expression, or subquery to select from.
            alias: Optional alias for the table.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or if the table type is unsupported.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add from to a non-SELECT expression."
            raise SQLBuilderError(msg)

        from_expr: exp.Expression
        if isinstance(table, str):
            from_expr = exp.table_(table, alias=alias)
        elif isinstance(table, SelectBuilder):
            # Handle subquery
            subquery = table.build()
            subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=self.dialect))
            from_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
            # Merge parameters from subquery
            self._parameters.update(subquery.parameters)
        else:
            from_expr = table

        self._expression = self._expression.from_(from_expr, copy=False)
        return self

    def join(
        self,
        table: Union[str, exp.Expression, "SelectBuilder"],
        on: Optional[Union[str, exp.Expression]] = None,
        alias: Optional[str] = None,
        join_type: str = "INNER",
    ) -> "SelectBuilder":
        """Add JOIN clause.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.
            join_type: Type of join (INNER, LEFT, RIGHT, FULL).

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add join to a non-SELECT expression."
            raise SQLBuilderError(msg)

        # Prepare the table expression
        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = exp.table_(table, alias=alias)
        elif isinstance(table, SelectBuilder):
            subquery = table.build()
            subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=self.dialect))
            table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
            # Merge parameters from subquery
            self._parameters.update(subquery.parameters)
        else:
            table_expr = table

        # Prepare the ON condition
        on_expr: Optional[exp.Expression] = None
        if on is not None:
            on_expr = exp.condition(on) if isinstance(on, str) else on

        # Create the appropriate join expression
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

        self._expression = self._expression.join(join_expr, copy=False)
        return self

    def inner_join(
        self,
        table: Union[str, exp.Expression, "SelectBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
    ) -> "SelectBuilder":
        """Add INNER JOIN clause.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        return self.join(table, on, alias, "INNER")

    def left_join(
        self,
        table: Union[str, exp.Expression, "SelectBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
    ) -> "SelectBuilder":
        """Add LEFT JOIN clause.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        return self.join(table, on, alias, "LEFT")

    def right_join(
        self,
        table: Union[str, exp.Expression, "SelectBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
    ) -> "SelectBuilder":
        """Add RIGHT JOIN clause.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        return self.join(table, on, alias, "RIGHT")

    def full_join(
        self,
        table: Union[str, exp.Expression, "SelectBuilder"],
        on: Union[str, exp.Expression],
        alias: Optional[str] = None,
    ) -> "SelectBuilder":
        """Add FULL OUTER JOIN clause.

        Args:
            table: The table name, expression, or subquery to join.
            on: The JOIN condition.
            alias: Optional alias for the joined table.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        return self.join(table, on, alias, "FULL")

    def with_(
        self,
        name: str,
        query: Union["SelectBuilder", str],
        recursive: bool = False,
        columns: Optional[list[str]] = None,
    ) -> "SelectBuilder":
        """Add WITH clause (Common Table Expression).

        Args:
            name: The name of the CTE.
            query: The query for the CTE (SelectBuilder instance or SQL string).
            recursive: Whether this is a recursive CTE.
            columns: Optional column names for the CTE.

        Raises:
            SQLBuilderError: If the query type is unsupported.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add WITH clause to a non-SELECT expression."
            raise SQLBuilderError(msg)

        # Prepare the CTE query
        if isinstance(query, SelectBuilder):
            cte_sql = query.build().sql
            cte_expr = exp.maybe_parse(cte_sql, dialect=self.dialect)  # type: ignore[var-annotated]
            # Merge parameters from the CTE query
            self._parameters.update(query.build().parameters)
        else:
            cte_expr = exp.maybe_parse(query, dialect=self.dialect)

        if not cte_expr:
            msg = f"Could not parse CTE query: {query}"
            raise SQLBuilderError(msg)

        # Create CTE expression
        cte_alias_expr = exp.alias_(cte_expr, name)
        if columns:
            # Add column specifications if provided
            cte_alias_expr = exp.alias_(cte_expr, name, table=columns)

        # Create or update WITH clause
        existing_with = self._expression.args.get("with")
        if existing_with:
            # Add to existing WITH clause
            existing_with.expressions.append(cte_alias_expr)
            if recursive:
                existing_with.set("recursive", recursive)
        else:
            # Create new WITH clause and attach to expression
            self._expression = self._expression.with_(cte_alias_expr, as_=cte_alias_expr.alias, copy=False)
            if recursive:
                # Find the WITH clause and set recursive flag
                with_clause = self._expression.find(exp.With)
                if with_clause:
                    with_clause.set("recursive", recursive)

        return self

    def where(self, condition: Union[str, exp.Expression, exp.Condition, tuple[str, Any]]) -> "SelectBuilder":
        """Add WHERE clause.

        Args:
            condition: The condition to add to the WHERE clause. Can be a string or an sqlglot Expression.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or if the condition type is unsupported.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add where to a non-SELECT expression."
            raise SQLBuilderError(msg)
        if isinstance(condition, exp.Condition):
            condition_expr = condition
        if isinstance(condition, str):
            condition_expr = exp.condition(condition)
        elif isinstance(condition, tuple):
            condition_expr = exp.EQ(
                this=exp.column(condition[0]),
                expression=exp.Placeholder(this=self._add_parameter(condition[1], name=f"{condition[0]}_eq_val")),
            )
        else:
            condition_expr = condition  # type: ignore[assignment]
        self._expression = self._expression.where(condition_expr, copy=False)
        return self

    def group_by(self, *columns: Union[str, exp.Expression]) -> "SelectBuilder":
        """Add GROUP BY clause.

        Args:
            *columns: Columns to group by. Can be strings (column names) or sqlglot expressions.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add GROUP BY to a non-SELECT expression."
            raise SQLBuilderError(msg)

        for column in columns:
            group_expr = exp.column(column) if isinstance(column, str) else column
            self._expression = self._expression.group_by(group_expr, copy=False)
        return self

    def having(self, condition: Union[str, exp.Expression]) -> "SelectBuilder":
        """Add HAVING clause.

        Args:
            condition: The condition for the HAVING clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add HAVING to a non-SELECT expression."
            raise SQLBuilderError(msg)

        having_expr = exp.condition(condition) if isinstance(condition, str) else condition

        self._expression = self._expression.having(having_expr, copy=False)
        return self

    def limit(self, count: int) -> "SelectBuilder":
        """Add LIMIT clause

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Select):
            # This should ideally not happen if _create_base_expression did its job
            msg = "Limit can only be applied to a SELECT expression."
            raise SQLBuilderError(msg)
        self._expression = self._expression.limit(exp.Literal.number(count), copy=False)
        return self

    def offset(self, value: int) -> "SelectBuilder":
        """Add OFFSET clause

        Args:
            value: The number of rows to skip before starting to return rows.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Select):
            msg = "Offset can only be applied to a SELECT expression."
            raise SQLBuilderError(msg)
        self._expression = self._expression.offset(exp.Literal.number(value), copy=False)
        return self

    def order_by(self, *items: Union[str, exp.Ordered]) -> "SelectBuilder":
        """Add ORDER BY clause.

        Args:
            *items: Columns to order by. Can be strings (column names) or
                    sqlglot.exp.Ordered instances for specific directions (e.g., exp.column("name").desc()).

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or if the item type is unsupported.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Select):
            msg = "Order by can only be applied to a SELECT expression."
            raise SQLBuilderError(msg)

        current_expr = self._expression
        for item in items:
            order_item = exp.column(item).asc() if isinstance(item, str) else item
            current_expr = current_expr.order_by(order_item, copy=False)
        self._expression = current_expr
        return self

    def distinct(self, *columns: Union[str, exp.Expression]) -> "SelectBuilder":
        """Add DISTINCT clause to SELECT.

        Args:
            *columns: Optional columns to make distinct. If none provided, applies DISTINCT to all selected columns.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add DISTINCT to a non-SELECT expression."
            raise SQLBuilderError(msg)

        # If no columns specified, make the entire SELECT distinct
        if not columns:
            self._expression.set("distinct", exp.Distinct())
        else:
            # Apply DISTINCT to specific columns
            distinct_columns = [
                column if isinstance(column, exp.Expression) else exp.column(column) for column in columns
            ]
            self._expression.set("distinct", exp.Distinct(expressions=distinct_columns))

        return self

    def union(self, other: "SelectBuilder", all_: bool = False) -> "SelectBuilder":
        """Combine this query with another using UNION.

        Args:
            other: Another SelectBuilder to union with.
            all_: If True, use UNION ALL instead of UNION.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: A new SelectBuilder instance with the UNION expression.
        """
        # Build both queries
        left_query = self.build()
        right_query = other.build()

        # Parse the SQL strings back to expressions
        left_expr = exp.maybe_parse(left_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]
        right_expr = exp.maybe_parse(right_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]

        if not left_expr or not right_expr:
            msg = "Could not parse queries for UNION operation"
            raise SQLBuilderError(msg)

        # Create UNION expression
        union_expr = exp.union(left_expr, right_expr, distinct=not all_)

        # Create new builder with the union expression
        new_builder = SelectBuilder(dialect=self.dialect)
        new_builder._expression = union_expr

        # Merge parameters from both queries
        new_builder._parameters.update(left_query.parameters)
        new_builder._parameters.update(right_query.parameters)

        return new_builder

    def intersect(self, other: "SelectBuilder") -> "SelectBuilder":
        """Combine this query with another using INTERSECT.

        Args:
            other: Another SelectBuilder to intersect with.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: A new SelectBuilder instance with the INTERSECT expression.
        """
        # Build both queries
        left_query = self.build()
        right_query = other.build()

        # Parse the SQL strings back to expressions
        left_expr = exp.maybe_parse(left_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]
        right_expr = exp.maybe_parse(right_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]

        if not left_expr or not right_expr:
            msg = "Could not parse queries for INTERSECT operation"
            raise SQLBuilderError(msg)

        # Create INTERSECT expression
        intersect_expr = exp.intersect(left_expr, right_expr)

        # Create new builder with the intersect expression
        new_builder = SelectBuilder(dialect=self.dialect)
        new_builder._expression = intersect_expr

        # Merge parameters from both queries
        new_builder._parameters.update(left_query.parameters)
        new_builder._parameters.update(right_query.parameters)

        return new_builder

    def except_(self, other: "SelectBuilder") -> "SelectBuilder":
        """Combine this query with another using EXCEPT.

        Args:
            other: Another SelectBuilder to except with.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: A new SelectBuilder instance with the EXCEPT expression.
        """
        # Build both queries
        left_query = self.build()
        right_query = other.build()

        # Parse the SQL strings back to expressions
        left_expr = exp.maybe_parse(left_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]
        right_expr = exp.maybe_parse(right_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]

        if not left_expr or not right_expr:
            msg = "Could not parse queries for EXCEPT operation"
            raise SQLBuilderError(msg)

        # Create EXCEPT expression
        except_expr = exp.except_(left_expr, right_expr)

        # Create new builder with the except expression
        new_builder = SelectBuilder(dialect=self.dialect)
        new_builder._expression = except_expr

        # Merge parameters from both queries
        new_builder._parameters.update(left_query.parameters)
        new_builder._parameters.update(right_query.parameters)

        return new_builder

    def where_exists(self, subquery: Union["SelectBuilder", str]) -> "SelectBuilder":
        """Add WHERE EXISTS clause.

        Args:
            subquery: The subquery for the EXISTS clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if isinstance(subquery, SelectBuilder):
            sub_sql = subquery.build()
            sub_expr = exp.maybe_parse(sub_sql.sql, dialect=self.dialect)  # type: ignore[var-annotated]
            self._parameters.update(sub_sql.parameters)
        else:
            sub_expr = exp.maybe_parse(subquery, dialect=self.dialect)

        if not sub_expr:
            msg = f"Could not parse subquery for EXISTS: {subquery}"
            raise SQLBuilderError(msg)

        exists_expr = exp.Exists(this=sub_expr)
        return self.where(exists_expr)

    def where_not_exists(self, subquery: Union["SelectBuilder", str]) -> "SelectBuilder":
        """Add WHERE NOT EXISTS clause.

        Args:
            subquery: The subquery for the NOT EXISTS clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if isinstance(subquery, SelectBuilder):
            sub_sql = subquery.build()
            sub_expr = exp.maybe_parse(sub_sql.sql, dialect=self.dialect)  # type: ignore[var-annotated]
            self._parameters.update(sub_sql.parameters)
        else:
            sub_expr = exp.maybe_parse(subquery, dialect=self.dialect)

        if not sub_expr:
            msg = f"Could not parse subquery for NOT EXISTS: {subquery}"
            raise SQLBuilderError(msg)

        not_exists_expr = exp.Not(this=exp.Exists(this=sub_expr))
        return self.where(not_exists_expr)

    def where_in(
        self, column: Union[str, exp.Expression], values: Union[list[Any], tuple[Any, ...], "SelectBuilder", str]
    ) -> "SelectBuilder":
        """Add WHERE column IN (...) clause.

        Args:
            column: The column to check.
            values: List of values, subquery, or SQL string.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        if isinstance(values, (list, tuple)):
            # Handle list of values
            value_exprs = []
            for value in values:
                param_name = self._add_parameter(value, name=f"{column}_in_val")
                value_exprs.append(exp.Placeholder(this=param_name))
            in_expr = exp.In(this=col_expr, expressions=value_exprs)
        elif isinstance(values, SelectBuilder):
            # Handle subquery
            sub_sql = values.build()
            sub_expr = exp.maybe_parse(sub_sql.sql, dialect=self.dialect)  # type: ignore[var-annotated]
            if not sub_expr:
                msg = f"Could not parse subquery for IN clause: {values}"
                raise SQLBuilderError(msg)
            self._parameters.update(sub_sql.parameters)
            in_expr = exp.In(this=col_expr, expressions=[sub_expr])
        else:
            # Handle SQL string
            sub_expr = exp.maybe_parse(values, dialect=self.dialect)
            if not sub_expr:
                msg = f"Could not parse SQL for IN clause: {values}"
                raise SQLBuilderError(msg)
            in_expr = exp.In(this=col_expr, expressions=[sub_expr])
        return self.where(in_expr)

    def where_not_in(
        self, column: Union[str, exp.Expression], values: Union[list[Any], tuple[Any, ...], "SelectBuilder", str]
    ) -> "SelectBuilder":
        """Add WHERE column NOT IN (...) clause.

        Args:
            column: The column to check.
            values: List of values, subquery, or SQL string.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        if isinstance(values, (list, tuple)):
            # Handle list of values
            value_exprs = []
            for value in values:
                param_name = self._add_parameter(value, name=f"{column}_not_in_val")
                value_exprs.append(exp.Placeholder(this=param_name))
            not_in_expr = exp.Not(this=exp.In(this=col_expr, expressions=value_exprs))
        elif isinstance(values, SelectBuilder):
            # Handle subquery
            sub_sql = values.build()
            sub_expr = exp.maybe_parse(sub_sql.sql, dialect=self.dialect)  # type: ignore[var-annotated]
            if not sub_expr:
                msg = f"Could not parse subquery for NOT IN clause: {values}"
                raise SQLBuilderError(msg)
            self._parameters.update(sub_sql.parameters)
            not_in_expr = exp.Not(this=exp.In(this=col_expr, expressions=[sub_expr]))
        else:
            # Handle SQL string
            sub_expr = exp.maybe_parse(values, dialect=self.dialect)
            if not sub_expr:
                msg = f"Could not parse SQL for NOT IN clause: {values}"
                raise SQLBuilderError(msg)
            not_in_expr = exp.Not(this=exp.In(this=col_expr, expressions=[sub_expr]))

        return self.where(not_in_expr)

    def where_like(
        self, column: Union[str, exp.Expression], pattern: str, escape: Optional[str] = None
    ) -> "SelectBuilder":
        """Add WHERE column LIKE pattern clause.

        Args:
            column: The column to check.
            pattern: The LIKE pattern.
            escape: Optional escape character.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        param_name = self._add_parameter(pattern, name=f"{column}_like_pattern")
        pattern_expr = exp.Placeholder(this=param_name)

        if escape:
            escape_param = self._add_parameter(escape, name=f"{column}_like_escape")
            like_expr = exp.Like(this=col_expr, expression=pattern_expr, escape=exp.Placeholder(this=escape_param))
        else:
            like_expr = exp.Like(this=col_expr, expression=pattern_expr)

        return self.where(like_expr)

    def where_between(self, column: Union[str, exp.Expression], start: Any, end: Any) -> "SelectBuilder":
        """Add WHERE column BETWEEN start AND end clause.

        Args:
            column: The column to check.
            start: The start value.
            end: The end value.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        start_param = self._add_parameter(start, name=f"{column}_between_start")
        end_param = self._add_parameter(end, name=f"{column}_between_end")

        between_expr = exp.Between(
            this=col_expr, low=exp.Placeholder(this=start_param), high=exp.Placeholder(this=end_param)
        )

        return self.where(between_expr)

    def where_null(self, column: Union[str, exp.Expression]) -> "SelectBuilder":
        """Add WHERE column IS NULL clause.

        Args:
            column: The column to check for NULL.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        null_expr = exp.Is(this=col_expr, expression=exp.Null())
        return self.where(null_expr)

    def where_not_null(self, column: Union[str, exp.Expression]) -> "SelectBuilder":
        """Add WHERE column IS NOT NULL clause.

        Args:
            column: The column to check for NOT NULL.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        not_null_expr = exp.Is(this=col_expr, expression=exp.Null())
        not_null_expr = exp.Not(this=not_null_expr)  # type: ignore[assignment]
        return self.where(not_null_expr)

    def count_(self, column: Union[str, exp.Expression] = "*", alias: Optional[str] = None) -> "SelectBuilder":
        """Add COUNT function to SELECT clause.

        Args:
            column: The column to count (default is "*").
            alias: Optional alias for the count.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if column == "*":
            count_expr = exp.Count(this=exp.Star())
        else:
            col_expr = exp.column(column) if isinstance(column, str) else column
            count_expr = exp.Count(this=col_expr)

        select_expr = exp.alias_(count_expr, alias) if alias else count_expr
        return self.select(select_expr)

    def sum_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> "SelectBuilder":
        """Add SUM function to SELECT clause.

        Args:
            column: The column to sum.
            alias: Optional alias for the sum.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        sum_expr = exp.Sum(this=col_expr)

        select_expr = exp.alias_(sum_expr, alias) if alias else sum_expr
        return self.select(select_expr)

    def avg_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> "SelectBuilder":
        """Add AVG function to SELECT clause.

        Args:
            column: The column to average.
            alias: Optional alias for the average.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        avg_expr = exp.Avg(this=col_expr)

        select_expr = exp.alias_(avg_expr, alias) if alias else avg_expr
        return self.select(select_expr)

    def max_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> "SelectBuilder":
        """Add MAX function to SELECT clause.

        Args:
            column: The column to find maximum.
            alias: Optional alias for the maximum.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        max_expr = exp.Max(this=col_expr)

        select_expr = exp.alias_(max_expr, alias) if alias else max_expr
        return self.select(select_expr)

    def min_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> "SelectBuilder":
        """Add MIN function to SELECT clause.

        Args:
            column: The column to find minimum.
            alias: Optional alias for the minimum.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        min_expr = exp.Min(this=col_expr)

        select_expr = exp.alias_(min_expr, alias) if alias else min_expr
        return self.select(select_expr)

    def window(
        self,
        function_expr: Union[str, exp.Expression],
        partition_by: Optional[Union[str, list[str], exp.Expression, list[exp.Expression]]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression, list[exp.Expression]]] = None,
        frame: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> "SelectBuilder":
        """Add a window function to the SELECT clause.

        Args:
            function_expr: The window function expression (e.g., "COUNT(*)", "ROW_NUMBER()").
            partition_by: Column(s) to partition by.
            order_by: Column(s) to order by within the window.
            frame: Window frame specification (e.g., "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW").
            alias: Optional alias for the window function.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or function parsing fails.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add window function to a non-SELECT expression."
            raise SQLBuilderError(msg)

        # Parse function expression
        if isinstance(function_expr, str):
            func_expr = exp.maybe_parse(function_expr, dialect=self.dialect)  # type: ignore[var-annotated]
            if not func_expr:
                msg = f"Could not parse function expression: {function_expr}"
                raise SQLBuilderError(msg)
        else:
            func_expr = function_expr

        # Build OVER clause
        over_args = {}

        # Handle partition by
        if partition_by:
            if isinstance(partition_by, str):
                over_args["partition_by"] = [exp.column(partition_by)]
            elif isinstance(partition_by, list):
                over_args["partition_by"] = [exp.column(col) if isinstance(col, str) else col for col in partition_by]  # type: ignore[misc]
            elif isinstance(partition_by, exp.Expression):
                over_args["partition_by"] = [partition_by]  # type: ignore[list-item]

        # Handle order by
        if order_by:
            if isinstance(order_by, str):
                over_args["order"] = [exp.column(order_by).asc()]  # type: ignore[list-item]
            elif isinstance(order_by, list):
                over_args["order"] = [exp.column(col).asc() if isinstance(col, str) else col for col in order_by]  # type: ignore[misc]
            elif isinstance(order_by, exp.Expression):
                over_args["order"] = [order_by]  # type: ignore[list-item]

        # Handle frame specification
        if frame:
            frame_expr = exp.maybe_parse(frame, dialect=self.dialect)  # type: ignore[var-annotated]
            if frame_expr:
                over_args["frame"] = frame_expr

        # Create window expression
        window_expr = exp.Window(this=func_expr, **over_args)
        self._expression = self._expression.select(exp.alias_(window_expr, alias) if alias else window_expr, copy=False)
        return self

    def case_(self, alias: Optional[str] = None) -> "CaseBuilder":
        """Create a CASE expression for the SELECT clause.

        Args:
            alias: Optional alias for the CASE expression.

        Returns:
            CaseBuilder: A CaseBuilder instance for building the CASE expression.
        """
        return CaseBuilder(self, alias)


@dataclass
class CaseBuilder:
    """Builder for CASE expressions."""

    _parent: SelectBuilder
    _alias: Optional[str]
    _case_expr: exp.Case

    def __init__(self, parent: SelectBuilder, alias: Optional[str] = None) -> None:
        """Initialize CaseBuilder.

        Args:
            parent: The parent SelectBuilder.
            alias: Optional alias for the CASE expression.
        """
        self._parent = parent
        self._alias = alias
        self._case_expr = exp.Case()

    def when(self, condition: Union[str, exp.Expression], value: Any) -> "CaseBuilder":
        """Add WHEN clause to CASE expression.

        Args:
            condition: The condition to test.
            value: The value to return if condition is true.

        Returns:
            CaseBuilder: The current builder instance for method chaining.
        """
        cond_expr = exp.condition(condition) if isinstance(condition, str) else condition
        # Use a direct placeholder for the case value
        case_param_name = f"case_when_{len(self._case_expr.args.get('ifs', []))}"
        param_name = self._parent.add_parameter(value, case_param_name)
        value_expr = exp.Placeholder(this=param_name)

        when_expr = exp.When(this=cond_expr, expression=value_expr)

        if not self._case_expr.args.get("ifs"):
            self._case_expr.set("ifs", [])
        self._case_expr.args["ifs"].append(when_expr)

        return self

    def else_(self, value: Any) -> "CaseBuilder":
        """Add ELSE clause to CASE expression.

        Args:
            value: The default value to return.

        Returns:
            CaseBuilder: The current builder instance for method chaining.
        """
        # Use a direct placeholder for the case value
        case_param_name = "case_else_value"
        param_name = self._parent.add_parameter(value, case_param_name)
        value_expr = exp.Placeholder(this=param_name)
        self._case_expr.set("default", value_expr)

        return self

    def end(self) -> SelectBuilder:
        """Complete the CASE expression and add it to the SELECT clause.

        Returns:
            SelectBuilder: The parent SelectBuilder for continued chaining.
        """
        select_expr = exp.alias_(self._case_expr, self._alias) if self._alias else self._case_expr

        return self._parent.select(select_expr)
