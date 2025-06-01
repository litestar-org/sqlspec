# ruff: noqa: PLR0904, SLF001
"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Union

from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder._base import QueryBuilder
from sqlspec.statement.parameters import ParameterConverter
from sqlspec.statement.result import SelectResult
from sqlspec.typing import DictRow

__all__ = ("SelectBuilder",)

logger = logging.getLogger("sqlspec")


@dataclass
class SelectBuilder(QueryBuilder[SelectResult[DictRow]]):
    """Builds SELECT queries."""

    _expression: "Optional[exp.Select]" = field(default=None, init=False)
    _with_parts: "dict[str, Union[exp.CTE, SelectBuilder]]" = field(default_factory=dict, init=False)

    def __post_init__(self) -> "None":
        super().__post_init__()
        if self._expression is None:
            self._create_base_expression()

    @property
    def _expected_result_type(self) -> "type[SelectResult[DictRow]]":
        """Get the expected result type for SELECT operations.

        Returns:
            type: The SelectResult type.
        """
        from sqlspec.statement.result import SelectResult

        return SelectResult

    def _create_base_expression(self) -> "exp.Select":
        if self._expression is None:
            self._expression = exp.Select()
        return self._expression

    def select(self, *columns: "Union[str, exp.Expression]") -> "SelectBuilder":
        """Add columns to SELECT clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add select columns to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        for column in columns:
            self._expression = self._expression.select(
                column if isinstance(column, exp.Expression) else exp.column(column), copy=False
            )
        return self

    def from_(
        self, table: "Union[str, exp.Expression, SelectBuilder]", alias: "Optional[str]" = None
    ) -> "SelectBuilder":
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
            msg = "Cannot add from to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        from_expr: exp.Expression
        if isinstance(table, str):
            from_expr = exp.table_(table, alias=alias)
        elif isinstance(table, SelectBuilder):
            subquery = table.build()
            subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=self.dialect))
            from_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp

            current_params = self._parameters
            merged_params = ParameterConverter.merge_parameters(
                parameters=subquery.parameters,
                args=current_params if isinstance(current_params, list) else None,  # type: ignore[unreachable]
                kwargs=current_params if isinstance(current_params, dict) else {},
            )
            self._parameters = merged_params  # type: ignore[assignment]
        else:
            from_expr = table

        self._expression = self._expression.from_(from_expr, copy=False)
        return self

    def join(
        self,
        table: "Union[str, exp.Expression, SelectBuilder]",
        on: "Optional[Union[str, exp.Expression]]" = None,
        alias: "Optional[str]" = None,
        join_type: "str" = "INNER",
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
            msg = "Cannot add join to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = exp.table_(table, alias=alias)
        elif isinstance(table, SelectBuilder):
            subquery = table.build()
            subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=self.dialect))
            table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp

            current_params = self._parameters
            merged_params = ParameterConverter.merge_parameters(
                parameters=subquery.parameters,
                args=current_params if isinstance(current_params, list) else None,  # type: ignore[unreachable]
                kwargs=current_params if isinstance(current_params, dict) else {},
            )
            self._parameters = merged_params  # type: ignore[assignment]
        else:
            table_expr = table

        on_expr: Optional[exp.Expression] = None
        if on is not None:
            on_expr = exp.condition(on) if isinstance(on, str) else on

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
        table: "Union[str, exp.Expression, SelectBuilder]",
        on: "Union[str, exp.Expression]",
        alias: "Optional[str]" = None,
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
        table: "Union[str, exp.Expression, SelectBuilder]",
        on: "Union[str, exp.Expression]",
        alias: "Optional[str]" = None,
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
        table: "Union[str, exp.Expression, SelectBuilder]",
        on: "Union[str, exp.Expression]",
        alias: "Optional[str]" = None,
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
        table: "Union[str, exp.Expression, SelectBuilder]",
        on: "Union[str, exp.Expression]",
        alias: "Optional[str]" = None,
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

    def cross_join(
        self,
        table: "Union[str, exp.Expression, SelectBuilder]",
        alias: "Optional[str]" = None,
    ) -> "SelectBuilder":
        """Add CROSS JOIN clause.

        Args:
            table: The table name, expression, or subquery to cross join.
            alias: Optional alias for the joined table.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add cross join to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = exp.table_(table, alias=alias)
        elif isinstance(table, SelectBuilder):
            subquery = table.build()
            subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=self.dialect))
            table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp

            current_params = self._parameters
            merged_params = ParameterConverter.merge_parameters(
                parameters=subquery.parameters,
                args=current_params if isinstance(current_params, list) else None,  # type: ignore[unreachable]
                kwargs=current_params if isinstance(current_params, dict) else {},
            )
            self._parameters = merged_params  # type: ignore[assignment]
        else:
            table_expr = table

        # CROSS JOIN doesn't have an ON clause
        join_expr = exp.Join(this=table_expr, kind="CROSS")
        self._expression = self._expression.join(join_expr, copy=False)
        return self

    def pivot(
        self,
        aggregate_expr: "Union[str, exp.Expression]",
        for_column: "Union[str, exp.Expression]",
        in_values: "Union[list[str], list[exp.Expression]]",
        alias: "Optional[str]" = None,
    ) -> "SelectBuilder":
        """Add PIVOT operation to transform rows into columns.

        Args:
            aggregate_expr: The aggregation function (e.g., "SUM(sales)", "COUNT(*)").
            for_column: The column whose values become new column headers.
            in_values: List of values to pivot on (e.g., ["Q1", "Q2", "Q3", "Q4"]).
            alias: Optional alias for the pivot table.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.

        Example:
            ```python
            builder = (
                sql.select("product", "Q1", "Q2", "Q3", "Q4")
                .from_("sales_data")
                .pivot(
                    "SUM(sales)", "quarter", ["Q1", "Q2", "Q3", "Q4"]
                )
            )
            ```
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add pivot to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        # Parse the aggregate expression
        agg_expr: exp.Expression
        if isinstance(aggregate_expr, str):
            parsed_agg = exp.maybe_parse(aggregate_expr, dialect=self.dialect)  # type: ignore[var-annotated]
            if not parsed_agg:
                msg = f"Could not parse aggregate expression: {aggregate_expr}"
                raise SQLBuilderError(msg)
            agg_expr = parsed_agg
        else:
            agg_expr = aggregate_expr

        # Parse the FOR column
        for_expr: exp.Expression
        for_expr = exp.column(for_column) if isinstance(for_column, str) else for_column

        # Parse the IN values
        in_expressions: list[exp.Expression] = []
        for value in in_values:
            if isinstance(value, str):
                in_expressions.append(exp.Literal.string(value))
            else:
                in_expressions.append(value)

        # Find the FROM clause and apply the PIVOT
        from_clause = self._expression.find(exp.From)
        if from_clause and from_clause.this:
            # Create a proper PIVOT table expression
            # Format: table PIVOT (agg_func FOR column IN (values))
            pivot_args = {
                "this": from_clause.this,
                "expressions": [agg_expr],
                "field": for_expr,
                "unpivot": False,
            }

            # Add IN clause if values provided
            if in_expressions:
                pivot_args["in"] = in_expressions

            pivoted_table = exp.Pivot(**pivot_args)

            if alias:
                pivoted_table = exp.alias_(pivoted_table, alias)

            from_clause.set("this", pivoted_table)
        else:
            msg = "Cannot apply PIVOT without a FROM clause."
            raise SQLBuilderError(msg)

        return self

    def unpivot(
        self,
        value_column: "str",
        name_column: "str",
        for_columns: "list[str]",
        alias: "Optional[str]" = None,
    ) -> "SelectBuilder":
        """Add UNPIVOT operation to transform columns into rows.

        Args:
            value_column: Name for the column that will contain the unpivoted values.
            name_column: Name for the column that will contain the original column names.
            for_columns: List of column names to unpivot.
            alias: Optional alias for the unpivot table.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.

        Example:
            ```python
            builder = (
                sql.select("product", "quarter", "sales")
                .from_("pivot_data")
                .unpivot("sales", "quarter", ["Q1", "Q2", "Q3", "Q4"])
            )
            ```
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add unpivot to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        # Create column expressions for the UNPIVOT
        column_expressions = [exp.Literal.string(col) for col in for_columns]

        # Find the FROM clause and apply the UNPIVOT
        from_clause = self._expression.find(exp.From)
        if from_clause and from_clause.this:
            # Create the UNPIVOT expression
            # Format: table UNPIVOT (value_col FOR name_col IN (columns))
            unpivot_args = {
                "this": from_clause.this,
                "expressions": [exp.column(value_column)],
                "field": exp.column(name_column),
                "unpivot": True,
            }

            # Add IN clause for columns to unpivot
            if column_expressions:
                unpivot_args["in"] = column_expressions

            unpivoted_table = exp.Pivot(**unpivot_args)

            if alias:
                unpivoted_table = exp.alias_(unpivoted_table, alias)

            from_clause.set("this", unpivoted_table)
        else:
            msg = "Cannot apply UNPIVOT without a FROM clause."
            raise SQLBuilderError(msg)

        return self

    def with_(
        self,
        name: "str",
        query: "Union[SelectBuilder, str]",
        recursive: "bool" = False,
        columns: "Optional[list[str]]" = None,
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
            msg = "Cannot add WITH clause to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        cte_expr: Optional[exp.Expression]
        if isinstance(query, SelectBuilder):
            built_query = query.build()
            cte_sql = built_query.sql
            cte_expr = exp.maybe_parse(cte_sql, dialect=self.dialect)  # type: ignore[var-annotated]

            current_params = self._parameters
            merged_params = ParameterConverter.merge_parameters(
                parameters=built_query.parameters,
                args=current_params if isinstance(current_params, list) else None,  # type: ignore[unreachable]
                kwargs=current_params if isinstance(current_params, dict) else {},
            )
            self._parameters = merged_params  # type: ignore[assignment]
        else:
            cte_expr = exp.maybe_parse(query, dialect=self.dialect)

        if not cte_expr:
            msg = f"Could not parse CTE query: {query}"  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        cte_alias_expr = exp.alias_(cte_expr, name)  # type: ignore[var-annotated]
        if columns:
            cte_alias_expr = exp.alias_(cte_expr, name, table=columns)

        existing_with = self._expression.args.get("with")
        if existing_with:
            existing_with.expressions.append(cte_alias_expr)
            if recursive:
                existing_with.set("recursive", recursive)
        else:
            self._expression = self._expression.with_(cte_alias_expr, as_=cte_alias_expr.alias, copy=False)
            if recursive:
                with_clause = self._expression.find(exp.With)
                if with_clause:
                    with_clause.set("recursive", recursive)
        return self

    def where(self, condition: "Union[str, exp.Expression, exp.Condition, tuple[str, Any]]") -> "SelectBuilder":
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
            msg = "Cannot add where to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        condition_expr: exp.Expression
        if isinstance(condition, exp.Condition):
            condition_expr = condition
        elif isinstance(condition, str):
            condition_expr = exp.condition(condition)
        elif isinstance(condition, tuple):
            param_name = self._add_parameter(condition[1])
            condition_expr = exp.EQ(
                this=exp.column(condition[0]),
                expression=exp.Placeholder(this=param_name),
            )
        else:
            condition_expr = condition  # type: ignore[assignment]
        self._expression = self._expression.where(condition_expr, copy=False)
        return self

    def group_by(self, *columns: "Union[str, exp.Expression]", rollup: bool = False) -> "SelectBuilder":
        """Add GROUP BY clause with optional ROLLUP support.

        Args:
            *columns: Columns to group by. Can be strings (column names) or sqlglot expressions.
            rollup: If True, use ROLLUP to generate subtotals and grand totals.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.

        Example:
            ```python
            # Regular GROUP BY
            builder.group_by("product", "region")

            # GROUP BY with ROLLUP for subtotals
            builder.group_by("product", "region", rollup=True)
            ```
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add GROUP BY to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        if rollup and columns:
            # Create ROLLUP expression with all columns
            column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
            rollup_expr = exp.Rollup(expressions=column_exprs)
            self._expression = self._expression.group_by(rollup_expr, copy=False)
        else:
            # Regular GROUP BY
            for column in columns:
                group_expr = exp.column(column) if isinstance(column, str) else column
                self._expression = self._expression.group_by(group_expr, copy=False)
        return self

    def having(self, condition: "Union[str, exp.Expression]") -> "SelectBuilder":
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
            msg = "Cannot add HAVING to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        having_expr = exp.condition(condition) if isinstance(condition, str) else condition
        self._expression = self._expression.having(having_expr, copy=False)
        return self

    def limit(self, count: "int") -> "SelectBuilder":
        """Add LIMIT clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Select):
            msg = "Limit can only be applied to a SELECT expression."  # type: ignore[unreachable]
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
            msg = "Offset can only be applied to a SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)
        self._expression = self._expression.offset(exp.Literal.number(value), copy=False)
        return self

    def order_by(self, *items: "Union[str, exp.Ordered]") -> "SelectBuilder":
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
            msg = "Order by can only be applied to a SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        current_expr = self._expression
        for item in items:
            order_item = exp.column(item).asc() if isinstance(item, str) else item
            current_expr = current_expr.order_by(order_item, copy=False)
        self._expression = current_expr
        return self

    def distinct(self, *columns: "Union[str, exp.Expression]") -> "SelectBuilder":
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
            msg = "Cannot add DISTINCT to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        if not columns:
            self._expression.set("distinct", exp.Distinct())
        else:
            distinct_columns = [
                column if isinstance(column, exp.Expression) else exp.column(column) for column in columns
            ]
            self._expression.set("distinct", exp.Distinct(expressions=distinct_columns))
        return self

    def union(self, other: "SelectBuilder", all_: "bool" = False) -> "SelectBuilder":
        """Combine this query with another using UNION.

        Args:
            other: Another SelectBuilder to union with.
            all_: If True, use UNION ALL instead of UNION.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The new builder instance for the union query.
        """
        left_query = self.build()
        right_query = other.build()

        left_expr = exp.maybe_parse(left_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]
        right_expr = exp.maybe_parse(right_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]

        if not left_expr or not right_expr:
            msg = "Could not parse queries for UNION operation"  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        union_expr = exp.union(left_expr, right_expr, distinct=not all_)
        new_builder = SelectBuilder(dialect=self.dialect)
        new_builder._expression = union_expr  # type: ignore[assignment]

        # Merge parameters with conflict resolution
        merged_params = dict(left_query.parameters)  # Start with left parameters

        # Add right parameters, renaming if there are conflicts
        for param_name, param_value in right_query.parameters.items():
            if param_name in merged_params:
                # Find a new name for the conflicting parameter
                counter = 1
                new_param_name = f"{param_name}_right_{counter}"
                while new_param_name in merged_params:
                    counter += 1
                    new_param_name = f"{param_name}_right_{counter}"

                # Update the SQL to use the new parameter name
                right_sql_updated = right_expr.sql(dialect=self.dialect)
                right_sql_updated = right_sql_updated.replace(f":{param_name}", f":{new_param_name}")
                right_expr = exp.maybe_parse(right_sql_updated, dialect=self.dialect)

                # Recreate the union with updated right expression
                union_expr = exp.union(left_expr, right_expr, distinct=not all_)
                new_builder._expression = union_expr  # type: ignore[assignment]

                merged_params[new_param_name] = param_value
            else:
                merged_params[param_name] = param_value

        new_builder._parameters = merged_params
        return new_builder

    def intersect(self, other: "SelectBuilder") -> "SelectBuilder":
        """Add INTERSECT clause.

        Args:
            other: Another SelectBuilder to intersect with.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The new builder instance for the intersect query.
        """
        left_query = self.build()
        right_query = other.build()

        left_expr = exp.maybe_parse(left_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]
        right_expr = exp.maybe_parse(right_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]

        if not left_expr or not right_expr:
            msg = "Could not parse queries for INTERSECT operation"  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        intersect_expr = exp.intersect(left_expr, right_expr, distinct=True)
        new_builder = SelectBuilder(dialect=self.dialect)
        new_builder._expression = intersect_expr  # type: ignore[assignment]

        merged_params_after_left = ParameterConverter.merge_parameters(
            parameters=left_query.parameters,
            args=None,
            kwargs=new_builder._parameters,  # Initially {}
        )
        final_merged_params = ParameterConverter.merge_parameters(
            parameters=right_query.parameters,
            args=merged_params_after_left if isinstance(merged_params_after_left, list) else None,
            kwargs=merged_params_after_left if isinstance(merged_params_after_left, dict) else {},
        )
        new_builder._parameters = final_merged_params  # type: ignore[assignment]
        return new_builder

    def except_(self, other: "SelectBuilder") -> "SelectBuilder":
        """Combine this query with another using EXCEPT.

        Args:
            other: Another SelectBuilder to except with.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The new builder instance for the except query.
        """
        left_query = self.build()
        right_query = other.build()

        left_expr = exp.maybe_parse(left_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]
        right_expr = exp.maybe_parse(right_query.sql, dialect=self.dialect)  # type: ignore[var-annotated]

        if not left_expr or not right_expr:
            msg = "Could not parse queries for EXCEPT operation"
            raise SQLBuilderError(msg)

        new_builder = SelectBuilder(dialect=self.dialect)
        new_builder._expression = exp.except_(left_expr, right_expr)  # type: ignore[assignment]

        merged_params_after_left = ParameterConverter.merge_parameters(
            parameters=left_query.parameters,
            args=None,
            kwargs=new_builder._parameters,
        )
        final_merged_params = ParameterConverter.merge_parameters(
            parameters=right_query.parameters,
            args=merged_params_after_left if isinstance(merged_params_after_left, list) else None,
            kwargs=merged_params_after_left if isinstance(merged_params_after_left, dict) else {},
        )
        new_builder._parameters = final_merged_params  # type: ignore[assignment]
        return new_builder

    def where_exists(self, subquery: "Union[SelectBuilder, str]") -> "SelectBuilder":
        """Add WHERE EXISTS clause.

        Args:
            subquery: The subquery for the EXISTS clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if isinstance(subquery, SelectBuilder):
            sub_sql_obj = subquery.build()
            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=self.dialect)  # type: ignore[var-annotated]

            current_params = self._parameters
            merged_params = ParameterConverter.merge_parameters(
                parameters=sub_sql_obj.parameters,
                args=current_params if isinstance(current_params, list) else None,  # type: ignore[unreachable]
                kwargs=current_params if isinstance(current_params, dict) else {},
            )
            self._parameters = merged_params  # type: ignore[assignment]
        else:
            sub_expr = exp.maybe_parse(subquery, dialect=self.dialect)

        if not sub_expr:
            msg = f"Could not parse subquery for EXISTS: {subquery}"
            raise SQLBuilderError(msg)

        exists_expr = exp.Exists(this=sub_expr)
        return self.where(exists_expr)

    def where_not_exists(self, subquery: "Union[SelectBuilder, str]") -> "SelectBuilder":
        """Add WHERE NOT EXISTS clause.

        Args:
            subquery: The subquery for the NOT EXISTS clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if isinstance(subquery, SelectBuilder):
            sub_sql_obj = subquery.build()
            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=self.dialect)  # type: ignore[var-annotated]

            current_params = self._parameters
            merged_params = ParameterConverter.merge_parameters(
                parameters=sub_sql_obj.parameters,
                args=current_params if isinstance(current_params, list) else None,  # type: ignore[unreachable]
                kwargs=current_params if isinstance(current_params, dict) else {},
            )
            self._parameters = merged_params  # type: ignore[assignment]
        else:
            sub_expr = exp.maybe_parse(subquery, dialect=self.dialect)

        if not sub_expr:
            msg = f"Could not parse subquery for NOT EXISTS: {subquery}"
            raise SQLBuilderError(msg)

        not_exists_expr = exp.Not(this=exp.Exists(this=sub_expr))
        return self.where(not_exists_expr)

    def where_in(
        self, column: "Union[str, exp.Expression]", values: "Union[list[Any], tuple[Any, ...], SelectBuilder, str]"
    ) -> "SelectBuilder":
        """Add a WHERE column IN (values) condition.

        Args:
            column: The column to check.
            values: List of values, subquery, or SQL string.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        final_condition: exp.Condition

        if isinstance(values, (list, tuple)):
            if not values:
                return self.where(exp.false())

            placeholders: list[exp.Expression] = []
            for value_item in values:
                param_name = self._add_parameter(value_item)
                placeholders.append(exp.Placeholder(this=param_name))
            final_condition = exp.In(this=col_expr, expressions=placeholders)

        elif isinstance(values, SelectBuilder):
            sub_sql_obj = values.build()
            if not sub_sql_obj.sql:
                msg = "Subquery for IN produced an empty SQL string."
                raise SQLBuilderError(msg)

            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=self.dialect)  # type: ignore[var-annotated]
            if not sub_expr:
                msg = f"Could not parse subquery for IN: {values}"
                raise SQLBuilderError(msg)

            final_condition = exp.In(this=col_expr, query=sub_expr)

            current_params = self._parameters
            merged_params = ParameterConverter.merge_parameters(
                parameters=sub_sql_obj.parameters,
                args=current_params if isinstance(current_params, list) else None,  # type: ignore[unreachable]
                kwargs=current_params if isinstance(current_params, dict) else {},
            )
            if merged_params is not None:
                self._parameters = merged_params  # type: ignore[assignment]

        elif isinstance(values, str):
            if not values.strip():
                msg = "Raw SQL subquery for IN cannot be empty."
                raise SQLBuilderError(msg)
            sub_expr = exp.maybe_parse(values, dialect=self.dialect)
            if not sub_expr:
                msg = f"Could not parse raw SQL subquery for IN: {values}"  # type: ignore[unreachable]
                raise SQLBuilderError(msg)
            final_condition = exp.In(this=col_expr, query=sub_expr)  # type: ignore[unreachable]
        else:
            msg = f"Unsupported type for 'values' in WHERE IN: {type(values)}"  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        return self.where(final_condition)

    def where_not_in(
        self, column: "Union[str, exp.Expression]", values: "Union[list[Any], tuple[Any, ...], SelectBuilder, str]"
    ) -> "SelectBuilder":
        """Add a WHERE column NOT IN (values) condition.

        Args:
            column: The column to check.
            values: List of values, subquery, or SQL string.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            SelectBuilder: The current builder instance.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        final_condition: exp.Condition

        if isinstance(values, (list, tuple)):
            if not values:
                return self.where(exp.true())

            placeholders: list[exp.Expression] = []
            for value_item in values:
                param_name = self._add_parameter(value_item)
                placeholders.append(exp.Placeholder(this=param_name))
            final_condition = exp.Not(this=exp.In(this=col_expr, expressions=placeholders))

        elif isinstance(values, SelectBuilder):
            sub_sql_obj = values.build()
            if not sub_sql_obj.sql:
                msg = "Subquery for NOT IN produced an empty SQL string."
                raise SQLBuilderError(msg)

            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=self.dialect)  # type: ignore[var-annotated]
            if not sub_expr:
                msg = f"Could not parse subquery for NOT IN: {values}"
                raise SQLBuilderError(msg)

            final_condition = exp.Not(this=exp.In(this=col_expr, query=sub_expr))

            current_params = self._parameters
            merged_params = ParameterConverter.merge_parameters(
                parameters=sub_sql_obj.parameters,
                args=current_params if isinstance(current_params, list) else None,  # type: ignore[unreachable]
                kwargs=current_params if isinstance(current_params, dict) else {},
            )
            if merged_params is not None:
                self._parameters = merged_params  # type: ignore[assignment]

        elif isinstance(values, str):  # Raw SQL subquery
            if not values.strip():
                msg = "Raw SQL subquery for NOT IN cannot be empty."
                raise SQLBuilderError(msg)
            sub_expr = exp.maybe_parse(values, dialect=self.dialect)
            if not sub_expr:
                msg = f"Could not parse raw SQL subquery for NOT IN: {values}"  # type: ignore[unreachable]
                raise SQLBuilderError(msg)
            final_condition = exp.Not(this=exp.In(this=col_expr, query=sub_expr))  # type: ignore[unreachable]
        else:
            msg = f"Unsupported type for 'values' in WHERE NOT IN: {type(values)}"  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        return self.where(final_condition)

    def where_like(
        self, column: "Union[str, exp.Expression]", pattern: "str", escape: "Optional[str]" = None
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

        param_name = self._add_parameter(pattern)
        pattern_expr = exp.Placeholder(this=param_name)

        like_expr: exp.Like
        if escape:
            escape_param = self._add_parameter(escape)
            like_expr = exp.Like(this=col_expr, expression=pattern_expr, escape=exp.Placeholder(this=escape_param))
        else:
            like_expr = exp.Like(this=col_expr, expression=pattern_expr)
        return self.where(like_expr)

    def where_between(self, column: "Union[str, exp.Expression]", start: "Any", end: "Any") -> "SelectBuilder":
        """Add WHERE column BETWEEN start AND end clause.

        Args:
            column: The column to check.
            start: The start value.
            end: The end value.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        start_param = self._add_parameter(start)
        end_param = self._add_parameter(end)

        between_expr = exp.Between(
            this=col_expr, low=exp.Placeholder(this=start_param), high=exp.Placeholder(this=end_param)
        )
        return self.where(between_expr)

    def where_null(self, column: "Union[str, exp.Expression]") -> "SelectBuilder":
        """Add WHERE column IS NULL clause.

        Args:
            column: The column to check for NULL.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        null_expr = exp.Is(this=col_expr, expression=exp.Null())
        return self.where(null_expr)

    def where_not_null(self, column: "Union[str, exp.Expression]") -> "SelectBuilder":
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

    def where_any(
        self, column: "Union[str, exp.Expression]", values: "Union[list[Any], tuple[Any, ...], SelectBuilder, str]"
    ) -> "SelectBuilder":
        """Add a WHERE column = ANY(values) condition.

        Args:
            column: The column to check.
            values: List of values, tuple, subquery, or SQL string.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or values type is unsupported.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        if isinstance(values, (list, tuple)):
            # For list/tuple, add as a single parameter and use ANY with array
            param_name = self._add_parameter(values)
            array_expr = exp.Placeholder(this=param_name)
            any_condition = exp.EQ(this=col_expr, expression=exp.Any(this=array_expr))
            return self.where(any_condition)

        if isinstance(values, SelectBuilder):
            # For subquery
            sub_sql_obj = values.build()
            if not sub_sql_obj.sql:
                msg = "Subquery for ANY produced an empty SQL string."
                raise SQLBuilderError(msg)

            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=self.dialect)  # type: ignore[var-annotated]
            if not sub_expr:
                msg = f"Could not parse subquery for ANY: {values}"
                raise SQLBuilderError(msg)

            any_condition = exp.EQ(this=col_expr, expression=exp.Any(this=sub_expr))

            # Merge parameters
            current_params = self._parameters
            merged_params = ParameterConverter.merge_parameters(
                parameters=sub_sql_obj.parameters,
                args=current_params if isinstance(current_params, list) else None,  # type: ignore[unreachable]
                kwargs=current_params if isinstance(current_params, dict) else {},
            )
            if merged_params is not None:
                self._parameters = merged_params  # type: ignore[assignment]

            return self.where(any_condition)

        if isinstance(values, str):
            # For raw SQL string
            if not values.strip():
                msg = "Raw SQL for ANY cannot be empty."
                raise SQLBuilderError(msg)
            sub_expr = exp.maybe_parse(values, dialect=self.dialect)
            if not sub_expr:
                msg = f"Could not parse raw SQL for ANY: {values}"  # type: ignore[unreachable]
                raise SQLBuilderError(msg)
            any_condition = exp.EQ(this=col_expr, expression=exp.Any(this=sub_expr))  # type: ignore[unreachable]
            return self.where(any_condition)

        msg = f"Unsupported values type for ANY clause: {type(values)}"  # type: ignore[unreachable]
        raise SQLBuilderError(msg)

    def where_not_any(
        self, column: "Union[str, exp.Expression]", values: "Union[list[Any], tuple[Any, ...], SelectBuilder, str]"
    ) -> "SelectBuilder":
        """Add a WHERE NOT (column = ANY(values)) condition.

        Args:
            column: The column to check.
            values: List of values, tuple, subquery, or SQL string.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or values type is unsupported.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        if isinstance(values, (list, tuple)):
            # For list/tuple, add as a single parameter and use NOT ANY with array
            param_name = self._add_parameter(values)
            array_expr = exp.Placeholder(this=param_name)
            not_any_condition = exp.Not(this=exp.EQ(this=col_expr, expression=exp.Any(this=array_expr)))
            return self.where(not_any_condition)

        if isinstance(values, SelectBuilder):
            # For subquery
            sub_sql_obj = values.build()
            if not sub_sql_obj.sql:
                msg = "Subquery for NOT ANY produced an empty SQL string."
                raise SQLBuilderError(msg)

            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=self.dialect)  # type: ignore[var-annotated]
            if not sub_expr:
                msg = f"Could not parse subquery for NOT ANY: {values}"
                raise SQLBuilderError(msg)

            not_any_condition = exp.Not(this=exp.EQ(this=col_expr, expression=exp.Any(this=sub_expr)))

            # Merge parameters
            current_params = self._parameters
            merged_params = ParameterConverter.merge_parameters(
                parameters=sub_sql_obj.parameters,
                args=current_params if isinstance(current_params, list) else None,
                kwargs=current_params if isinstance(current_params, dict) else {},
            )
            if merged_params is not None:
                self._parameters = merged_params  # type: ignore[assignment]

            return self.where(not_any_condition)

        if isinstance(values, str):
            # For raw SQL string
            if not values.strip():
                msg = "Raw SQL for NOT ANY cannot be empty."
                raise SQLBuilderError(msg)
            sub_expr = exp.maybe_parse(values, dialect=self.dialect)
            if not sub_expr:
                msg = f"Could not parse raw SQL for NOT ANY: {values}"  # type: ignore[unreachable]
                raise SQLBuilderError(msg)
            not_any_condition = exp.Not(this=exp.EQ(this=col_expr, expression=exp.Any(this=sub_expr)))  # type: ignore[unreachable]
            return self.where(not_any_condition)

        msg = f"Unsupported values type for ANY clause: {type(values)}"  # type: ignore[unreachable]
        raise SQLBuilderError(msg)

    def count_(self, column: "Union[str, exp.Expression]" = "*", alias: "Optional[str]" = None) -> "SelectBuilder":
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

    def sum_(self, column: "Union[str, exp.Expression]", alias: "Optional[str]" = None) -> "SelectBuilder":
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

    def avg_(self, column: "Union[str, exp.Expression]", alias: "Optional[str]" = None) -> "SelectBuilder":
        col_expr = exp.column(column) if isinstance(column, str) else column
        avg_expr = exp.Avg(this=col_expr)
        select_expr = exp.alias_(avg_expr, alias) if alias else avg_expr
        return self.select(select_expr)

    def max_(self, column: "Union[str, exp.Expression]", alias: "Optional[str]" = None) -> "SelectBuilder":
        col_expr = exp.column(column) if isinstance(column, str) else column
        max_expr = exp.Max(this=col_expr)
        select_expr = exp.alias_(max_expr, alias) if alias else max_expr
        return self.select(select_expr)

    def min_(self, column: "Union[str, exp.Expression]", alias: "Optional[str]" = None) -> "SelectBuilder":
        col_expr = exp.column(column) if isinstance(column, str) else column
        min_expr = exp.Min(this=col_expr)
        select_expr = exp.alias_(min_expr, alias) if alias else min_expr
        return self.select(select_expr)

    def window(
        self,
        function_expr: "Union[str, exp.Expression]",
        partition_by: "Optional[Union[str, list[str], exp.Expression, list[exp.Expression]]]" = None,
        order_by: "Optional[Union[str, list[str], exp.Expression, list[exp.Expression]]]" = None,
        frame: "Optional[str]" = None,
        alias: "Optional[str]" = None,
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
            msg = "Cannot add window function to a non-SELECT expression."  # type: ignore[unreachable]
            raise SQLBuilderError(msg)

        func_expr_parsed: exp.Expression
        if isinstance(function_expr, str):
            parsed = exp.maybe_parse(function_expr, dialect=self.dialect)  # type: ignore[var-annotated]
            if not parsed:
                msg = f"Could not parse function expression: {function_expr}"
                raise SQLBuilderError(msg)
            func_expr_parsed = parsed
        else:
            func_expr_parsed = function_expr

        over_args: dict[str, Any] = {}  # Stringified dict
        if partition_by:
            if isinstance(partition_by, str):
                over_args["partition_by"] = [exp.column(partition_by)]
            elif isinstance(partition_by, list):  # Check for list
                over_args["partition_by"] = [exp.column(col) if isinstance(col, str) else col for col in partition_by]
            elif isinstance(partition_by, exp.Expression):  # Check for exp.Expression
                over_args["partition_by"] = [partition_by]

        if order_by:
            if isinstance(order_by, str):
                over_args["order"] = exp.column(order_by).asc()
            elif isinstance(order_by, list):
                # For multiple order columns, create multiple ordered expressions
                if len(order_by) == 1:
                    col = order_by[0]
                    over_args["order"] = exp.column(col).asc() if isinstance(col, str) else col
                else:
                    # For multiple columns, we need to handle this differently
                    # SQLGlot expects a single order expression, so we'll use the first one
                    col = order_by[0]
                    over_args["order"] = exp.column(col).asc() if isinstance(col, str) else col
            elif isinstance(order_by, exp.Expression):
                over_args["order"] = order_by

        if frame:
            frame_expr = exp.maybe_parse(frame, dialect=self.dialect)  # type: ignore[var-annotated]
            if frame_expr:
                over_args["frame"] = frame_expr

        window_expr = exp.Window(this=func_expr_parsed, **over_args)
        self._expression = self._expression.select(exp.alias_(window_expr, alias) if alias else window_expr, copy=False)  # pyright: ignore
        return self

    def case_(self, alias: "Optional[str]" = None) -> "CaseBuilder":
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

    _parent: "SelectBuilder"
    _alias: "Optional[str]"
    _case_expr: "exp.Case"

    def __init__(self, parent: "SelectBuilder", alias: "Optional[str]" = None) -> None:
        """Initialize CaseBuilder.

        Args:
            parent: The parent SelectBuilder.
            alias: Optional alias for the CASE expression.
        """
        self._parent = parent
        self._alias = alias
        self._case_expr = exp.Case()

    def when(self, condition: "Union[str, exp.Expression]", value: "Any") -> "CaseBuilder":
        """Add WHEN clause to CASE expression.

        Args:
            condition: The condition to test.
            value: The value to return if condition is true.

        Returns:
            CaseBuilder: The current builder instance for method chaining.
        """
        cond_expr = exp.condition(condition) if isinstance(condition, str) else condition
        param_name = self._parent._add_parameter(value)
        value_expr = exp.Placeholder(this=param_name)

        when_clause = exp.When(this=cond_expr, then=value_expr)

        if not self._case_expr.args.get("ifs"):
            self._case_expr.set("ifs", [])
        self._case_expr.args["ifs"].append(when_clause)
        return self

    def else_(self, value: "Any") -> "CaseBuilder":
        param_name = self._parent._add_parameter(value)
        value_expr = exp.Placeholder(this=param_name)
        self._case_expr.set("default", value_expr)
        return self

    def end(self) -> "SelectBuilder":
        """Finalize the CASE expression and add it to the SELECT clause.

        Returns:
            SelectBuilder: The parent SelectBuilder instance.
        """
        select_expr = exp.alias_(self._case_expr, self._alias) if self._alias else self._case_expr
        return self._parent.select(select_expr)
