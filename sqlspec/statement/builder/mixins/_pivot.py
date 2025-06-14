from typing import TYPE_CHECKING, Optional, Union, cast

from sqlglot import exp

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.builder.select import SelectBuilder

__all__ = ("PivotClauseMixin",)


class PivotClauseMixin:
    """Mixin class to add PIVOT functionality to a SelectBuilder."""

    _expression: "Optional[exp.Expression]" = None
    dialect: "DialectType" = None

    def pivot(
        self: "PivotClauseMixin",
        aggregate_function: Union[str, exp.Expression],
        aggregate_column: Union[str, exp.Expression],
        pivot_column: Union[str, exp.Expression],
        pivot_values: list[Union[str, int, float, exp.Expression]],
        alias: Optional[str] = None,
    ) -> "SelectBuilder":
        """Adds a PIVOT clause to the SELECT statement.

        Example:
            `query.pivot(aggregate_function="SUM", aggregate_column="Sales", pivot_column="Quarter", pivot_values=["Q1", "Q2", "Q3", "Q4"], alias="PivotTable")`

        Args:
            aggregate_function: The aggregate function to use (e.g., "SUM", "AVG").
            aggregate_column: The column to be aggregated.
            pivot_column: The column whose unique values will become new column headers.
            pivot_values: A list of specific values from the pivot_column to be turned into columns.
            alias: Optional alias for the pivoted table/subquery.

        Returns:
            The SelectBuilder instance for chaining.
        """
        current_expr = self._expression
        if not isinstance(current_expr, exp.Select):
            msg = "Pivot can only be applied to a Select expression managed by SelectBuilder."
            raise TypeError(msg)

        agg_func_name = aggregate_function if isinstance(aggregate_function, str) else aggregate_function.name
        agg_col_expr = exp.column(aggregate_column) if isinstance(aggregate_column, str) else aggregate_column
        pivot_col_expr = exp.column(pivot_column) if isinstance(pivot_column, str) else pivot_column

        pivot_agg_expr = exp.func(agg_func_name, agg_col_expr)

        pivot_value_exprs: list[exp.Expression] = []
        for val in pivot_values:
            if isinstance(val, exp.Expression):
                pivot_value_exprs.append(val)
            elif isinstance(val, str):
                pivot_value_exprs.append(exp.Literal.string(val))
            elif isinstance(val, (int, float)):
                pivot_value_exprs.append(exp.Literal.number(val))
            else:
                pivot_value_exprs.append(exp.Literal.string(str(val)))

        # Create the pivot expression with proper fields structure
        in_expr = exp.In(this=pivot_col_expr, expressions=pivot_value_exprs)

        pivot_node = exp.Pivot(expressions=[pivot_agg_expr], fields=[in_expr], unpivot=False)

        if alias:
            pivot_node.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))

        # Add pivot to the table in the FROM clause
        from_clause = current_expr.args.get("from")
        if from_clause and isinstance(from_clause, exp.From):
            table = from_clause.this
            if isinstance(table, exp.Table):
                # Add to pivots array
                existing_pivots = table.args.get("pivots", [])
                existing_pivots.append(pivot_node)
                table.set("pivots", existing_pivots)

        return cast("SelectBuilder", self)
