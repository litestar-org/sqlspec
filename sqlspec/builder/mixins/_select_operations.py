"""SELECT clause mixins consolidated into a single module."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._parsing_utils import parse_column_expression, parse_table_expression
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_query_builder_parameters, is_expression

if TYPE_CHECKING:
    from sqlspec.builder._column import Column, FunctionColumn
    from sqlspec.protocols import SelectBuilderProtocol, SQLBuilderProtocol

__all__ = ("CaseBuilder", "SelectClauseMixin")


@trait
class SelectClauseMixin:
    """Consolidated mixin providing all SELECT-related clauses and functionality."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    def select(self, *columns: Union[str, exp.Expression, "Column", "FunctionColumn"]) -> Self:
        """Add columns to SELECT clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "Cannot add select columns to a non-SELECT expression."
            raise SQLBuilderError(msg)
        for column in columns:
            builder._expression = builder._expression.select(parse_column_expression(column), copy=False)
        return cast("Self", builder)

    def distinct(self, *columns: Union[str, exp.Expression, "Column", "FunctionColumn"]) -> Self:
        """Add DISTINCT clause to SELECT.

        Args:
            *columns: Optional columns to make distinct. If none provided, applies DISTINCT to all selected columns.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "Cannot add DISTINCT to a non-SELECT expression."
            raise SQLBuilderError(msg)
        if not columns:
            builder._expression.set("distinct", exp.Distinct())
        else:
            distinct_columns = [parse_column_expression(column) for column in columns]
            builder._expression.set("distinct", exp.Distinct(expressions=distinct_columns))
        return cast("Self", builder)

    def from_(self, table: Union[str, exp.Expression, Any], alias: Optional[str] = None) -> Self:
        """Add FROM clause.

        Args:
            table: The table name, expression, or subquery to select from.
            alias: Optional alias for the table.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or if the table type is unsupported.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "FROM clause is only supported for SELECT statements."
            raise SQLBuilderError(msg)
        from_expr: exp.Expression
        if isinstance(table, str):
            from_expr = parse_table_expression(table, alias)
        elif is_expression(table):
            from_expr = exp.alias_(table, alias) if alias else table
        elif has_query_builder_parameters(table):
            subquery = table.build()
            sql_str = subquery.sql if hasattr(subquery, "sql") and not callable(subquery.sql) else str(subquery)
            subquery_exp = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(builder, "dialect", None)))
            from_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
            current_parameters = getattr(builder, "_parameters", None)
            merged_parameters = getattr(type(builder), "ParameterConverter", None)
            if merged_parameters and hasattr(subquery, "parameters"):
                subquery_parameters = getattr(subquery, "parameters", {})
                merged_parameters = merged_parameters.merge_parameters(
                    parameters=subquery_parameters,
                    args=current_parameters if isinstance(current_parameters, list) else None,
                    kwargs=current_parameters if isinstance(current_parameters, dict) else {},
                )
                setattr(builder, "_parameters", merged_parameters)
        else:
            from_expr = table
        builder._expression = builder._expression.from_(from_expr, copy=False)
        return cast("Self", builder)

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
                    set_expressions.append(exp.Tuple(expressions=[]))
                else:
                    columns = [exp.column(col) for col in column_set]
                    set_expressions.append(exp.Tuple(expressions=columns))
            else:
                set_expressions.append(exp.column(column_set))

        grouping_sets_expr = exp.GroupingSets(expressions=set_expressions)
        return self.group_by(grouping_sets_expr)

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
        string_agg_expr = exp.GroupConcat(this=col_expr, separator=exp.convert(separator))
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

    def window(
        self,
        function_expr: Union[str, exp.Expression],
        partition_by: Optional[Union[str, list[str], exp.Expression, list[exp.Expression]]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression, list[exp.Expression]]] = None,
        frame: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> Self:
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
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add window function to a non-SELECT expression."
            raise SQLBuilderError(msg)

        func_expr_parsed: exp.Expression
        if isinstance(function_expr, str):
            parsed: Optional[exp.Expression] = exp.maybe_parse(function_expr, dialect=getattr(self, "dialect", None))
            if not parsed:
                msg = f"Could not parse function expression: {function_expr}"
                raise SQLBuilderError(msg)
            func_expr_parsed = parsed
        else:
            func_expr_parsed = function_expr

        over_args: dict[str, Any] = {}
        if partition_by:
            if isinstance(partition_by, str):
                over_args["partition_by"] = [exp.column(partition_by)]
            elif isinstance(partition_by, list):
                over_args["partition_by"] = [exp.column(col) if isinstance(col, str) else col for col in partition_by]
            elif isinstance(partition_by, exp.Expression):
                over_args["partition_by"] = [partition_by]

        if order_by:
            if isinstance(order_by, str):
                over_args["order"] = exp.column(order_by).asc()
            elif isinstance(order_by, list):
                order_expressions: list[Union[exp.Expression, exp.Column]] = []
                for col in order_by:
                    if isinstance(col, str):
                        order_expressions.append(exp.column(col).asc())
                    else:
                        order_expressions.append(col)
                over_args["order"] = exp.Order(expressions=order_expressions)
            elif isinstance(order_by, exp.Expression):
                over_args["order"] = order_by

        if frame:
            frame_expr: Optional[exp.Expression] = exp.maybe_parse(frame, dialect=getattr(self, "dialect", None))
            if frame_expr:
                over_args["frame"] = frame_expr

        window_expr = exp.Window(this=func_expr_parsed, **over_args)
        self._expression.select(exp.alias_(window_expr, alias) if alias else window_expr, copy=False)
        return self

    def case_(self, alias: "Optional[str]" = None) -> "CaseBuilder":
        """Create a CASE expression for the SELECT clause.

        Args:
            alias: Optional alias for the CASE expression.

        Returns:
            CaseBuilder: A CaseBuilder instance for building the CASE expression.
        """
        builder = cast("SelectBuilderProtocol", self)
        return CaseBuilder(builder, alias)


@dataclass
class CaseBuilder:
    """Builder for CASE expressions."""

    _parent: "SelectBuilderProtocol"
    _alias: Optional[str]
    _case_expr: exp.Case

    def __init__(self, parent: "SelectBuilderProtocol", alias: "Optional[str]" = None) -> None:
        """Initialize CaseBuilder.

        Args:
            parent: The parent builder with select capabilities.
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
        param_name = self._parent._generate_unique_parameter_name("case_when_value")
        param_name = self._parent.add_parameter(value, name=param_name)[1]
        value_expr = exp.Placeholder(this=param_name)

        when_clause = exp.When(this=cond_expr, then=value_expr)

        if not self._case_expr.args.get("ifs"):
            self._case_expr.set("ifs", [])
        self._case_expr.args["ifs"].append(when_clause)
        return self

    def else_(self, value: "Any") -> "CaseBuilder":
        """Add ELSE clause to CASE expression.

        Args:
            value: The value to return if no conditions match.

        Returns:
            CaseBuilder: The current builder instance for method chaining.
        """
        param_name = self._parent._generate_unique_parameter_name("case_else_value")
        param_name = self._parent.add_parameter(value, name=param_name)[1]
        value_expr = exp.Placeholder(this=param_name)
        self._case_expr.set("default", value_expr)
        return self

    def end(self) -> "SelectBuilderProtocol":
        """Finalize the CASE expression and add it to the SELECT clause.

        Returns:
            The parent builder instance.
        """
        select_expr = exp.alias_(self._case_expr, self._alias) if self._alias else self._case_expr
        return self._parent.select(select_expr)
