from typing import Optional, Union

from sqlglot import exp
from typing_extensions import Self

__all__ = ("AggregateFunctionsMixin", )


class AggregateFunctionsMixin:
    """Mixin providing aggregate function methods for SQL builders."""

    def count_(self, column: Union[str, exp.Expression] = "*", alias: Optional[str] = None) -> Self:
        """Add COUNT function to SELECT clause.

        Args:
            column: The column to count (default is "*").
            alias: Optional alias for the count.

        Returns:
            The current builder instance for method chaining.
        """
        if column == "*":
            count_expr = exp.Count(this=exp.Star())
        else:
            col_expr = exp.column(column) if isinstance(column, str) else column
            count_expr = exp.Count(this=col_expr)

        select_expr = exp.alias_(count_expr, alias) if alias else count_expr
        return self.select(select_expr)  # type: ignore[attr-defined]

    def sum_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add SUM function to SELECT clause.

        Args:
            column: The column to sum.
            alias: Optional alias for the sum.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        sum_expr = exp.Sum(this=col_expr)
        select_expr = exp.alias_(sum_expr, alias) if alias else sum_expr
        return self.select(select_expr)  # type: ignore[attr-defined]

    def avg_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add AVG function to SELECT clause.

        Args:
            column: The column to average.
            alias: Optional alias for the average.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        avg_expr = exp.Avg(this=col_expr)
        select_expr = exp.alias_(avg_expr, alias) if alias else avg_expr
        return self.select(select_expr)  # type: ignore[attr-defined]

    def max_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add MAX function to SELECT clause.

        Args:
            column: The column to find the maximum of.
            alias: Optional alias for the maximum.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        max_expr = exp.Max(this=col_expr)
        select_expr = exp.alias_(max_expr, alias) if alias else max_expr
        return self.select(select_expr)  # type: ignore[attr-defined]

    def min_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add MIN function to SELECT clause.

        Args:
            column: The column to find the minimum of.
            alias: Optional alias for the minimum.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        min_expr = exp.Min(this=col_expr)
        select_expr = exp.alias_(min_expr, alias) if alias else min_expr
        return self.select(select_expr)  # type: ignore[attr-defined]
