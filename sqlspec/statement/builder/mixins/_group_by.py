from typing import Optional, Union

from sqlglot import exp
from typing_extensions import Self

__all__ = ("GroupByClauseMixin",)


class GroupByClauseMixin:
    """Mixin providing GROUP BY clause functionality for SQL builders."""

    _expression: Optional[exp.Expression] = None

    def group_by(self, *columns: Union[str, exp.Expression]) -> Self:
        """Add GROUP BY clause.

        Args:
            *columns: Columns to group by. Can be column names, expressions,
                     or special grouping expressions like ROLLUP, CUBE, etc.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None or not isinstance(self._expression, exp.Select):
            return self

        for column in columns:
            self._expression = self._expression.group_by(
                exp.column(column) if isinstance(column, str) else column, copy=False
            )
        return self

    def group_by_rollup(self, *columns: Union[str, exp.Expression]) -> Self:
        """Add GROUP BY ROLLUP clause.

        ROLLUP generates subtotals and grand totals for a hierarchical set of columns.

        Args:
            *columns: Columns to include in the rollup hierarchy.

        Returns:
            The current builder instance for method chaining.

        Example:
            ```python
            # GROUP BY ROLLUP(product, region)
            query = (
                sql.select("product", "region", sql.sum("sales"))
                .from_("sales_data")
                .group_by_rollup("product", "region")
            )
            ```
        """
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        rollup_expr = exp.Rollup(expressions=column_exprs)
        return self.group_by(rollup_expr)

    def group_by_cube(self, *columns: Union[str, exp.Expression]) -> Self:
        """Add GROUP BY CUBE clause.

        CUBE generates subtotals for all possible combinations of the specified columns.

        Args:
            *columns: Columns to include in the cube.

        Returns:
            The current builder instance for method chaining.

        Example:
            ```python
            # GROUP BY CUBE(product, region)
            query = (
                sql.select("product", "region", sql.sum("sales"))
                .from_("sales_data")
                .group_by_cube("product", "region")
            )
            ```
        """
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        cube_expr = exp.Cube(expressions=column_exprs)
        return self.group_by(cube_expr)

    def group_by_grouping_sets(self, *column_sets: Union[tuple[str, ...], list[str]]) -> Self:
        """Add GROUP BY GROUPING SETS clause.

        GROUPING SETS allows you to specify multiple grouping sets in a single query.

        Args:
            *column_sets: Sets of columns to group by. Each set can be a tuple or list.
                         Empty tuple/list creates a grand total grouping.

        Returns:
            The current builder instance for method chaining.

        Example:
            ```python
            # GROUP BY GROUPING SETS ((product), (region), ())
            query = (
                sql.select("product", "region", sql.sum("sales"))
                .from_("sales_data")
                .group_by_grouping_sets(("product",), ("region",), ())
            )
            ```
        """
        set_expressions = []
        for column_set in column_sets:
            if isinstance(column_set, (tuple, list)):
                if len(column_set) == 0:
                    # Empty set for grand total
                    set_expressions.append(exp.Tuple(expressions=[]))
                else:
                    columns = [exp.column(col) for col in column_set]
                    set_expressions.append(exp.Tuple(expressions=columns))
            else:
                # Single column
                set_expressions.append(exp.column(column_set))

        grouping_sets_expr = exp.GroupingSets(expressions=set_expressions)
        return self.group_by(grouping_sets_expr)
