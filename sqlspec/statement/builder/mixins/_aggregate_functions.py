from typing import Any, Optional, Union

from sqlglot import exp

__all__ = ("AggregateFunctionsMixin",)


class AggregateFunctionsMixin:
    """Mixin providing aggregate function methods for SQL builders."""

    def count_(self, column: Union[str, exp.Expression] = "*", alias: Optional[str] = None) -> Any:
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

    def sum_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Any:
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

    def avg_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Any:
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

    def max_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Any:
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

    def min_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Any:
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

    def array_agg(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Any:
        """Add ARRAY_AGG aggregate function to SELECT clause.

        Args:
            column: The column to aggregate into an array.
            alias: Optional alias for the result.

        Returns:
            The current builder instance for method chaining.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        array_agg_expr: Union[exp.ArrayAgg, exp.Anonymous]
        try:
            array_agg_expr = exp.ArrayAgg(this=col_expr)
        except AttributeError:
            array_agg_expr = exp.Anonymous(this="ARRAY_AGG", expressions=[col_expr])  # pyright: ignore  #TODO: fallback for dialects lacking ArrayAgg
        select_expr = exp.alias_(array_agg_expr, alias) if alias else array_agg_expr
        return self.select(select_expr)  # type: ignore[attr-defined]

    def bool_and(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Any:
        """Add BOOL_AND aggregate function to SELECT clause (PostgreSQL, DuckDB, etc).

        Args:
            column: The boolean column to aggregate.
            alias: Optional alias for the result.

        Returns:
            The current builder instance for method chaining.

        Note:
            Uses exp.Anonymous for BOOL_AND. Not all dialects support this function.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        bool_and_expr = exp.Anonymous(this="BOOL_AND", expressions=[col_expr])
        select_expr = exp.alias_(bool_and_expr, alias) if alias else bool_and_expr
        return self.select(select_expr)  # type: ignore[attr-defined]

    def bool_or(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Any:
        """Add BOOL_OR aggregate function to SELECT clause (PostgreSQL, DuckDB, etc).

        Args:
            column: The boolean column to aggregate.
            alias: Optional alias for the result.

        Returns:
            The current builder instance for method chaining.

        Note:
            Uses exp.Anonymous for BOOL_OR. Not all dialects support this function.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        bool_or_expr = exp.Anonymous(this="BOOL_OR", expressions=[col_expr])
        select_expr = exp.alias_(bool_or_expr, alias) if alias else bool_or_expr
        return self.select(select_expr)  # type: ignore[attr-defined]
