from typing import TYPE_CHECKING, Optional, Union, cast

from sqlglot import exp
from typing_extensions import Self

if TYPE_CHECKING:
    from sqlspec.protocols import SelectBuilderProtocol

__all__ = ("AggregateFunctionsMixin",)


class AggregateFunctionsMixin:
    """Mixin providing aggregate function methods for SQL builders."""

    def count_(self, column: "Union[str, exp.Expression]" = "*", alias: Optional[str] = None) -> Self:
        """Add COUNT function to SELECT clause.

        Args:
            column: The column to count (default is "*").
            alias: Optional alias for the count.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        if column == "*":
            count_expr = exp.Count(this=exp.Star())
        else:
            col_expr = exp.column(column) if isinstance(column, str) else column
            count_expr = exp.Count(this=col_expr)

        select_expr = exp.alias_(count_expr, alias) if alias else count_expr
        return cast("Self", builder.select(select_expr))

    def sum_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add SUM function to SELECT clause.

        Args:
            column: The column to sum.
            alias: Optional alias for the sum.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        sum_expr = exp.Sum(this=col_expr)
        select_expr = exp.alias_(sum_expr, alias) if alias else sum_expr
        return cast("Self", builder.select(select_expr))

    def avg_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add AVG function to SELECT clause.

        Args:
            column: The column to average.
            alias: Optional alias for the average.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        avg_expr = exp.Avg(this=col_expr)
        select_expr = exp.alias_(avg_expr, alias) if alias else avg_expr
        return cast("Self", builder.select(select_expr))

    def max_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add MAX function to SELECT clause.

        Args:
            column: The column to find the maximum of.
            alias: Optional alias for the maximum.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        max_expr = exp.Max(this=col_expr)
        select_expr = exp.alias_(max_expr, alias) if alias else max_expr
        return cast("Self", builder.select(select_expr))

    def min_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add MIN function to SELECT clause.

        Args:
            column: The column to find the minimum of.
            alias: Optional alias for the minimum.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        min_expr = exp.Min(this=col_expr)
        select_expr = exp.alias_(min_expr, alias) if alias else min_expr
        return cast("Self", builder.select(select_expr))

    def array_agg(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add ARRAY_AGG aggregate function to SELECT clause.

        Args:
            column: The column to aggregate into an array.
            alias: Optional alias for the result.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        array_agg_expr = exp.ArrayAgg(this=col_expr)
        select_expr = exp.alias_(array_agg_expr, alias) if alias else array_agg_expr
        return cast("Self", builder.select(select_expr))

    def count_distinct(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add COUNT(DISTINCT column) to SELECT clause.

        Args:
            column: The column to count distinct values of.
            alias: Optional alias for the count.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        count_expr = exp.Count(this=exp.Distinct(expressions=[col_expr]))
        select_expr = exp.alias_(count_expr, alias) if alias else count_expr
        return cast("Self", builder.select(select_expr))

    def stddev(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add STDDEV aggregate function to SELECT clause.

        Args:
            column: The column to calculate standard deviation of.
            alias: Optional alias for the result.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        stddev_expr = exp.Stddev(this=col_expr)
        select_expr = exp.alias_(stddev_expr, alias) if alias else stddev_expr
        return cast("Self", builder.select(select_expr))

    def stddev_pop(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add STDDEV_POP aggregate function to SELECT clause.

        Args:
            column: The column to calculate population standard deviation of.
            alias: Optional alias for the result.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        stddev_pop_expr = exp.StddevPop(this=col_expr)
        select_expr = exp.alias_(stddev_pop_expr, alias) if alias else stddev_pop_expr
        return cast("Self", builder.select(select_expr))

    def stddev_samp(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add STDDEV_SAMP aggregate function to SELECT clause.

        Args:
            column: The column to calculate sample standard deviation of.
            alias: Optional alias for the result.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        stddev_samp_expr = exp.StddevSamp(this=col_expr)
        select_expr = exp.alias_(stddev_samp_expr, alias) if alias else stddev_samp_expr
        return cast("Self", builder.select(select_expr))

    def variance(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add VARIANCE aggregate function to SELECT clause.

        Args:
            column: The column to calculate variance of.
            alias: Optional alias for the result.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        variance_expr = exp.Variance(this=col_expr)
        select_expr = exp.alias_(variance_expr, alias) if alias else variance_expr
        return cast("Self", builder.select(select_expr))

    def var_pop(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add VAR_POP aggregate function to SELECT clause.

        Args:
            column: The column to calculate population variance of.
            alias: Optional alias for the result.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        var_pop_expr = exp.VariancePop(this=col_expr)
        select_expr = exp.alias_(var_pop_expr, alias) if alias else var_pop_expr
        return cast("Self", builder.select(select_expr))

    def string_agg(self, column: Union[str, exp.Expression], separator: str = ",", alias: Optional[str] = None) -> Self:
        """Add STRING_AGG aggregate function to SELECT clause.

        Args:
            column: The column to aggregate into a string.
            separator: The separator between values (default is comma).
            alias: Optional alias for the result.

        Returns:
            The current builder instance for method chaining.

        Note:
            Different databases have different names for this function:
            - PostgreSQL: STRING_AGG
            - MySQL: GROUP_CONCAT
            - SQLite: GROUP_CONCAT
            SQLGlot will handle the translation.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        # Use GroupConcat which SQLGlot can translate to STRING_AGG for Postgres
        string_agg_expr = exp.GroupConcat(this=col_expr, separator=exp.Literal.string(separator))
        select_expr = exp.alias_(string_agg_expr, alias) if alias else string_agg_expr
        return cast("Self", builder.select(select_expr))

    def json_agg(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add JSON_AGG aggregate function to SELECT clause.

        Args:
            column: The column to aggregate into a JSON array.
            alias: Optional alias for the result.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        json_agg_expr = exp.JSONArrayAgg(this=col_expr)
        select_expr = exp.alias_(json_agg_expr, alias) if alias else json_agg_expr
        return cast("Self", builder.select(select_expr))
