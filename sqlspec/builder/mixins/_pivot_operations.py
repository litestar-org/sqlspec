"""Pivot and Unpivot operations mixins for SQL builders."""

from typing import TYPE_CHECKING, Optional, Union, cast

from mypy_extensions import trait
from sqlglot import exp

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.builder._select import Select

__all__ = ("PivotClauseMixin", "UnpivotClauseMixin")


@trait
class PivotClauseMixin:
    """Mixin class to add PIVOT functionality to a Select."""

    __slots__ = ()
    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    dialect: "DialectType" = None

    def pivot(
        self: "PivotClauseMixin",
        aggregate_function: Union[str, exp.Expression],
        aggregate_column: Union[str, exp.Expression],
        pivot_column: Union[str, exp.Expression],
        pivot_values: list[Union[str, int, float, exp.Expression]],
        alias: Optional[str] = None,
    ) -> "Select":
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
            elif isinstance(val, (str, int, float)):
                pivot_value_exprs.append(exp.convert(val))
            else:
                pivot_value_exprs.append(exp.convert(str(val)))

        in_expr = exp.In(this=pivot_col_expr, expressions=pivot_value_exprs)

        pivot_node = exp.Pivot(expressions=[pivot_agg_expr], fields=[in_expr], unpivot=False)

        if alias:
            pivot_node.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))

        from_clause = current_expr.args.get("from")
        if from_clause and isinstance(from_clause, exp.From):
            table = from_clause.this
            if isinstance(table, exp.Table):
                existing_pivots = table.args.get("pivots", [])
                existing_pivots.append(pivot_node)
                table.set("pivots", existing_pivots)

        return cast("Select", self)


@trait
class UnpivotClauseMixin:
    """Mixin class to add UNPIVOT functionality to a Select."""

    __slots__ = ()
    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    dialect: "DialectType" = None

    def unpivot(
        self: "UnpivotClauseMixin",
        value_column_name: str,
        name_column_name: str,
        columns_to_unpivot: list[Union[str, exp.Expression]],
        alias: Optional[str] = None,
    ) -> "Select":
        """Adds an UNPIVOT clause to the SELECT statement.

        Example:
            `query.unpivot(value_column_name="Sales", name_column_name="Quarter", columns_to_unpivot=["Q1Sales", "Q2Sales"], alias="UnpivotTable")`

        Args:
            value_column_name: The name for the new column that will hold the values from the unpivoted columns.
            name_column_name: The name for the new column that will hold the names of the original unpivoted columns.
            columns_to_unpivot: A list of columns to be unpivoted into rows.
            alias: Optional alias for the unpivoted table/subquery.

        Raises:
            TypeError: If the current expression is not a Select expression.

        Returns:
            The Select instance for chaining.
        """
        current_expr = self._expression
        if not isinstance(current_expr, exp.Select):
            msg = "Unpivot can only be applied to a Select expression managed by Select."
            raise TypeError(msg)

        value_col_ident = exp.to_identifier(value_column_name)
        name_col_ident = exp.to_identifier(name_column_name)

        unpivot_cols_exprs: list[exp.Expression] = []
        for col_name_or_expr in columns_to_unpivot:
            if isinstance(col_name_or_expr, exp.Expression):
                unpivot_cols_exprs.append(col_name_or_expr)
            elif isinstance(col_name_or_expr, str):
                unpivot_cols_exprs.append(exp.column(col_name_or_expr))
            else:
                unpivot_cols_exprs.append(exp.column(str(col_name_or_expr)))

        in_expr = exp.In(this=name_col_ident, expressions=unpivot_cols_exprs)

        unpivot_node = exp.Pivot(expressions=[value_col_ident], fields=[in_expr], unpivot=True)

        if alias:
            unpivot_node.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))

        from_clause = current_expr.args.get("from")
        if from_clause and isinstance(from_clause, exp.From):
            table = from_clause.this
            if isinstance(table, exp.Table):
                existing_pivots = table.args.get("pivots", [])
                existing_pivots.append(unpivot_node)
                table.set("pivots", existing_pivots)

        return cast("Select", self)
