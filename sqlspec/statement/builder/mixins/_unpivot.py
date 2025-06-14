from typing import TYPE_CHECKING, Optional, Union, cast

from sqlglot import exp

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.builder.select import SelectBuilder

__all__ = ("UnpivotClauseMixin",)


class UnpivotClauseMixin:
    """Mixin class to add UNPIVOT functionality to a SelectBuilder."""

    _expression: "Optional[exp.Expression]" = None
    dialect: "DialectType" = None

    def unpivot(
        self: "UnpivotClauseMixin",
        value_column_name: str,
        name_column_name: str,
        columns_to_unpivot: list[Union[str, exp.Expression]],
        alias: Optional[str] = None,
    ) -> "SelectBuilder":
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
            The SelectBuilder instance for chaining.
        """
        current_expr = self._expression
        if not isinstance(current_expr, exp.Select):
            # SelectBuilder's __init__ ensures _expression is exp.Select.
            msg = "Unpivot can only be applied to a Select expression managed by SelectBuilder."
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
                # Fallback for other types, should ideally be an error or more specific handling
                unpivot_cols_exprs.append(exp.column(str(col_name_or_expr)))

        # Create the unpivot expression (stored as Pivot with unpivot=True)
        in_expr = exp.In(this=name_col_ident, expressions=unpivot_cols_exprs)

        unpivot_node = exp.Pivot(expressions=[value_col_ident], fields=[in_expr], unpivot=True)

        if alias:
            unpivot_node.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))

        # Add unpivot to the table in the FROM clause
        from_clause = current_expr.args.get("from")
        if from_clause and isinstance(from_clause, exp.From):
            table = from_clause.this
            if isinstance(table, exp.Table):
                # Add to pivots array
                existing_pivots = table.args.get("pivots", [])
                existing_pivots.append(unpivot_node)
                table.set("pivots", existing_pivots)

        return cast("SelectBuilder", self)
