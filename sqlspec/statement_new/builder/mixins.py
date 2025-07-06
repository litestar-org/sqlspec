"""Consolidated query builder mixins."""

from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlglot import exp, parse_one
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement_new.parsing_utils import (
    parse_column_expression,
    parse_condition_expression,
    parse_order_expression,
    parse_table_expression,
)
from sqlspec.utils.type_guards import (
    has_expression_attr,
    has_parameter_builder,
    has_query_builder_parameters,
    has_sql_method,
    has_sqlglot_expression,
    has_to_statement,
    has_with_method,
    is_expression,
    is_iterable_parameters,
)

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.protocols import SelectBuilderProtocol, SQLBuilderProtocol
    from sqlspec.statement_new.builder import case
    from sqlspec.statement_new.builder.column import Column, ColumnExpression, FunctionColumn


# Constants
TUPLE_CONDITION_TWO_PARTS = 2
TUPLE_CONDITION_THREE_PARTS = 3


class CoreQueryMixin:
    """Core query operations: WHERE, ORDER BY, LIMIT, OFFSET, FROM, SELECT, DISTINCT."""

    def _build_operator_expression(
        self, operator: str, col_expr: exp.Expression, placeholder_expr: exp.Expression
    ) -> exp.Expression:
        """Build expression for operator in WHERE condition."""
        operator_map = {
            "=": exp.EQ,
            "==": exp.EQ,
            "!=": exp.NEQ,
            "<>": exp.NEQ,
            "<": exp.LT,
            "<=": exp.LTE,
            ">": exp.GT,
            ">=": exp.GTE,
            "like": exp.Like,
            "in": exp.In,
            "any": exp.Any,
        }
        operator = operator.lower()
        if operator == "not like":
            return exp.Not(this=exp.Like(this=col_expr, expression=placeholder_expr))
        if operator == "not in":
            return exp.Not(this=exp.In(this=col_expr, expression=placeholder_expr))
        if operator == "not any":
            return exp.Not(this=exp.Any(this=col_expr, expression=placeholder_expr))

        expr_class = operator_map.get(operator)
        if expr_class is None:
            msg = f"Unsupported operator in WHERE condition: {operator}"
            raise SQLBuilderError(msg)
        return expr_class(this=col_expr, expression=placeholder_expr)

    def where(
        self,
        condition: Union[str, exp.Expression, exp.Condition, tuple[str, Any], tuple[str, str, Any], "ColumnExpression"],
    ) -> Self:
        """Add a WHERE clause to the statement."""
        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            msg = "Cannot add WHERE clause: expression is not initialized."
            raise SQLBuilderError(msg)
        valid_types = (exp.Select, exp.Update, exp.Delete)
        if not isinstance(builder._expression, valid_types):
            msg = f"Cannot add WHERE clause to unsupported expression type: {type(builder._expression).__name__}."
            raise SQLBuilderError(msg)

        if isinstance(builder._expression, exp.Delete) and not builder._expression.args.get("this"):
            msg = "WHERE clause requires a table to be set. Use from() to set the table first."
            raise SQLBuilderError(msg)

        condition_expr: exp.Expression
        if isinstance(condition, tuple):
            if len(condition) == TUPLE_CONDITION_TWO_PARTS:
                param_name = builder.add_parameter(condition[1])[1]
                condition_expr = exp.EQ(
                    this=parse_column_expression(condition[0]), expression=exp.Placeholder(this=param_name)
                )
            elif len(condition) == TUPLE_CONDITION_THREE_PARTS:
                column, operator, value = cast("tuple[str, str, Any]", condition)
                param_name = builder.add_parameter(value)[1]
                col_expr = parse_column_expression(column)
                placeholder_expr = exp.Placeholder(this=param_name)
                condition_expr = self._build_operator_expression(operator, col_expr, placeholder_expr)
            else:
                msg = f"WHERE tuple must have 2 or 3 elements, got {len(condition)}"
                raise SQLBuilderError(msg)
        elif has_sqlglot_expression(condition):
            raw_expr = condition.sqlglot_expression
            if raw_expr is not None:
                condition_expr = builder._parameterize_expression(raw_expr)
            else:
                condition_expr = parse_condition_expression(str(condition))
        else:
            if not isinstance(condition, (str, exp.Expression, tuple)):
                condition = str(condition)
            condition_expr = parse_condition_expression(condition)

        if isinstance(builder._expression, exp.Delete):
            builder._expression = builder._expression.where(
                condition_expr, dialect=getattr(builder, "dialect_name", None)
            )
        else:
            builder._expression = builder._expression.where(condition_expr, copy=False)
        return cast("Self", builder)

    def where_eq(self, column: "Union[str, exp.Column]", value: Any) -> "Self":
        _, param_name = self.add_parameter(value)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.eq(exp.var(param_name))
        return self.where(condition)

    def where_neq(self, column: "Union[str, exp.Column]", value: Any) -> "Self":
        _, param_name = self.add_parameter(value)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.neq(exp.var(param_name))
        return self.where(condition)

    def where_lt(self, column: "Union[str, exp.Column]", value: Any) -> "Self":
        _, param_name = self.add_parameter(value)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = exp.LT(this=col_expr, expression=exp.var(param_name))
        return self.where(condition)

    def where_lte(self, column: "Union[str, exp.Column]", value: Any) -> "Self":
        _, param_name = self.add_parameter(value)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = exp.LTE(this=col_expr, expression=exp.var(param_name))
        return self.where(condition)

    def where_gt(self, column: "Union[str, exp.Column]", value: Any) -> "Self":
        _, param_name = self.add_parameter(value)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = exp.GT(this=col_expr, expression=exp.var(param_name))
        return self.where(condition)

    def where_gte(self, column: "Union[str, exp.Column]", value: Any) -> "Self":
        _, param_name = self.add_parameter(value)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = exp.GTE(this=col_expr, expression=exp.var(param_name))
        return self.where(condition)

    def where_between(self, column: "Union[str, exp.Column]", low: Any, high: Any) -> "Self":
        _, low_param = self.add_parameter(low)  # type: ignore[attr-defined]
        _, high_param = self.add_parameter(high)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.between(exp.var(low_param), exp.var(high_param))
        return self.where(condition)

    def where_like(self, column: "Union[str, exp.Column]", pattern: str, escape: Optional[str] = None) -> "Self":
        _, param_name = self.add_parameter(pattern)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if escape is not None:
            cond = exp.Like(this=col_expr, expression=exp.var(param_name), escape=exp.Literal.string(str(escape)))
        else:
            cond = col_expr.like(exp.var(param_name))
        condition: exp.Expression = cond
        return self.where(condition)

    def where_not_like(self, column: "Union[str, exp.Column]", pattern: str) -> "Self":
        _, param_name = self.add_parameter(pattern)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.like(exp.var(param_name)).not_()
        return self.where(condition)

    def where_ilike(self, column: "Union[str, exp.Column]", pattern: str) -> "Self":
        _, param_name = self.add_parameter(pattern)  # type: ignore[attr-defined]
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.ilike(exp.var(param_name))
        return self.where(condition)

    def where_is_null(self, column: "Union[str, exp.Column]") -> "Self":
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null())
        return self.where(condition)

    def where_is_not_null(self, column: "Union[str, exp.Column]") -> "Self":
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null()).not_()
        return self.where(condition)

    def where_exists(self, subquery: "Union[str, Any]") -> "Self":
        sub_expr: exp.Expression
        if has_query_builder_parameters(subquery):
            subquery_builder_params: dict[str, Any] = subquery.parameters
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]
            sub_sql_obj = subquery.build()  # pyright: ignore
            sql_str = (
                sub_sql_obj.sql if has_sql_method(sub_sql_obj) and not callable(sub_sql_obj.sql) else str(sub_sql_obj)
            )
            sub_expr = exp.maybe_parse(sql_str, dialect=getattr(self, "dialect_name", None))
        else:
            sub_expr = exp.maybe_parse(str(subquery), dialect=getattr(self, "dialect_name", None))

        if sub_expr is None:
            msg = "Could not parse subquery for EXISTS"
            raise SQLBuilderError(msg)

        exists_expr = exp.Exists(this=sub_expr)
        return self.where(exists_expr)

    def where_not_exists(self, subquery: "Union[str, Any]") -> "Self":
        sub_expr: exp.Expression
        if has_query_builder_parameters(subquery):
            subquery_builder_params: dict[str, Any] = subquery.parameters
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]
            sub_sql_obj = subquery.build()  # pyright: ignore
            sql_str = (
                sub_sql_obj.sql if has_sql_method(sub_sql_obj) and not callable(sub_sql_obj.sql) else str(sub_sql_obj)
            )
            sub_expr = exp.maybe_parse(sql_str, dialect=getattr(self, "dialect_name", None))
        else:
            sub_expr = exp.maybe_parse(str(subquery), dialect=getattr(self, "dialect_name", None))

        if sub_expr is None:
            msg = "Could not parse subquery for NOT EXISTS"
            raise SQLBuilderError(msg)

        not_exists_expr = exp.Not(this=exp.Exists(this=sub_expr))
        return self.where(not_exists_expr)

    def where_not_null(self, column: "Union[str, exp.Column]") -> "Self":
        return self.where_is_not_null(column)

    def where_in(self, column: "Union[str, exp.Column]", values: Any) -> "Self":
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery = values.build()  # pyright: ignore
                sql_str = subquery.sql if has_sql_method(subquery) and not callable(subquery.sql) else str(subquery)
                subquery_exp = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(self, "dialect_name", None)))
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = col_expr.isin(subquery_exp)
            return self.where(condition)
        if not is_iterable_parameters(values) or isinstance(values, (str, bytes)):
            msg = "Unsupported type for 'values' in WHERE IN"
            raise SQLBuilderError(msg)
        params = []
        for v in values:
            _, param_name = self.add_parameter(v)  # type: ignore[attr-defined]
            params.append(exp.var(param_name))
        condition = col_expr.isin(*params)
        return self.where(condition)

    def where_not_in(self, column: "Union[str, exp.Column]", values: Any) -> "Self":
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery = values.build()  # pyright: ignore
                sql_str = subquery.sql if has_sql_method(subquery) and not callable(subquery.sql) else str(subquery)
                subquery_exp = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(self, "dialect_name", None)))
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = exp.Not(this=col_expr.isin(subquery_exp))
            return self.where(condition)
        if not is_iterable_parameters(values) or isinstance(values, (str, bytes)):
            msg = "Values for where_not_in must be a non-string iterable or subquery."
            raise SQLBuilderError(msg)
        params = []
        for v in values:
            _, param_name = self.add_parameter(v)  # type: ignore[attr-defined]
            params.append(exp.var(param_name))
        condition = exp.Not(this=col_expr.isin(*params))
        return self.where(condition)

    def where_null(self, column: "Union[str, exp.Column]") -> "Self":
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null())
        return self.where(condition)

    def where_any(self, column: "Union[str, exp.Column]", values: Any) -> "Self":
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery = values.build()  # pyright: ignore
                sql_str = subquery.sql if has_sql_method(subquery) and not callable(subquery.sql) else str(subquery)
                subquery_exp = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(self, "dialect_name", None)))
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = exp.EQ(this=col_expr, expression=exp.Any(this=subquery_exp))
            return self.where(condition)
        if isinstance(values, str):
            try:
                parsed_expr = parse_one(values)
                if isinstance(parsed_expr, (exp.Select, exp.Union, exp.Subquery)):
                    subquery_exp = exp.paren(parsed_expr)
                    condition = exp.EQ(this=col_expr, expression=exp.Any(this=subquery_exp))
                    return self.where(condition)
            except Exception:  # noqa: S110
                pass
            msg = "Unsupported type for 'values' in WHERE ANY"
            raise SQLBuilderError(msg)
        if not is_iterable_parameters(values) or isinstance(values, bytes):
            msg = "Unsupported type for 'values' in WHERE ANY"
            raise SQLBuilderError(msg)
        params = []
        for v in values:
            _, param_name = self.add_parameter(v)  # type: ignore[attr-defined]
            params.append(exp.var(param_name))
        tuple_expr = exp.Tuple(expressions=params)
        condition = exp.EQ(this=col_expr, expression=exp.Any(this=tuple_expr))
        return self.where(condition)

    def where_not_any(self, column: "Union[str, exp.Column]", values: Any) -> "Self":
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery = values.build()  # pyright: ignore
                sql_str = subquery.sql if has_sql_method(subquery) and not callable(subquery.sql) else str(subquery)
                subquery_exp = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(self, "dialect_name", None)))
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = exp.NEQ(this=col_expr, expression=exp.Any(this=subquery_exp))
            return self.where(condition)
        if isinstance(values, str):
            try:
                parsed_expr = parse_one(values)
                if isinstance(parsed_expr, (exp.Select, exp.Union, exp.Subquery)):
                    subquery_exp = exp.paren(parsed_expr)
                    condition = exp.NEQ(this=col_expr, expression=exp.Any(this=subquery_exp))
                    return self.where(condition)
            except Exception:  # noqa: S110
                pass
            msg = "Unsupported type for 'values' in WHERE NOT ANY"
            raise SQLBuilderError(msg)
        if not is_iterable_parameters(values) or isinstance(values, bytes):
            msg = "Unsupported type for 'values' in WHERE NOT ANY"
            raise SQLBuilderError(msg)
        params = []
        for v in values:
            _, param_name = self.add_parameter(v)  # type: ignore[attr-defined]
            params.append(exp.var(param_name))
        tuple_expr = exp.Tuple(expressions=params)
        condition = exp.NEQ(this=col_expr, expression=exp.Any(this=tuple_expr))
        return self.where(condition)

    def order_by(self, *items: Union[str, exp.Ordered], desc: bool = False) -> Self:
        """Add ORDER BY clause."""
        builder = cast("SQLBuilderProtocol", self)
        if not isinstance(builder._expression, exp.Select):
            msg = "ORDER BY is only supported for SELECT statements."
            raise SQLBuilderError(msg)

        current_expr = builder._expression
        for item in items:
            if isinstance(item, str):
                order_item = parse_order_expression(item)
                if desc:
                    order_item = order_item.desc()
            else:
                order_item = item
            current_expr = current_expr.order_by(order_item, copy=False)
        builder._expression = current_expr
        return cast("Self", builder)

    def limit(self, value: int) -> Self:
        """Add LIMIT clause."""
        builder = cast("SQLBuilderProtocol", self)
        if not isinstance(builder._expression, exp.Select):
            msg = "LIMIT is only supported for SELECT statements."
            raise SQLBuilderError(msg)
        builder._expression = builder._expression.limit(exp.Literal.number(value), copy=False)
        return cast("Self", builder)

    def offset(self, value: int) -> Self:
        """Add OFFSET clause."""
        builder = cast("SQLBuilderProtocol", self)
        if not isinstance(builder._expression, exp.Select):
            msg = "OFFSET is only supported for SELECT statements."
            raise SQLBuilderError(msg)
        builder._expression = builder._expression.offset(exp.Literal.number(value), copy=False)
        return cast("Self", builder)

    def from_(self, table: Union[str, exp.Expression, Any], alias: Optional[str] = None) -> Self:
        """Add FROM clause."""
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
            subquery = table.build()  # pyright: ignore
            sql_str = subquery.sql if has_sql_method(subquery) and not callable(subquery.sql) else str(subquery)
            subquery_exp = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(builder, "dialect", None)))
            from_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
        else:
            from_expr = table
        builder._expression = builder._expression.from_(from_expr, copy=False)
        return cast("Self", builder)

    def select(self, *columns: Union[str, exp.Expression, "Column", "FunctionColumn"]) -> Self:
        """Add columns to SELECT clause."""
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
        """Add DISTINCT clause to SELECT."""
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


class JoinOperationsMixin:
    """All JOIN operations."""

    def join(
        self,
        table: Union[str, exp.Expression, Any],
        on: Optional[Union[str, exp.Expression]] = None,
        alias: Optional[str] = None,
        join_type: str = "INNER",
    ) -> Self:
        """Add JOIN clause."""
        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "JOIN clause is only supported for SELECT statements."
            raise SQLBuilderError(msg)
        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = parse_table_expression(table, alias)
        elif has_query_builder_parameters(table):
            if has_expression_attr(table) and getattr(table, "_expression", None) is not None:
                table_expr_value = getattr(table, "_expression", None)
                if table_expr_value is not None:
                    subquery_exp = exp.paren(table_expr_value.copy())  # pyright: ignore
                else:
                    subquery_exp = exp.paren(exp.Anonymous(this=""))
                table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
            else:
                subquery = table.build()  # pyright: ignore
                sql_str = subquery.sql if has_sql_method(subquery) and not callable(subquery.sql) else str(subquery)
                subquery_exp = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(builder, "dialect", None)))
                table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
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
        builder._expression = builder._expression.join(join_expr, copy=False)
        return cast("Self", builder)

    def inner_join(
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression], alias: Optional[str] = None
    ) -> Self:
        return self.join(table, on, alias, "INNER")

    def left_join(
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression], alias: Optional[str] = None
    ) -> Self:
        return self.join(table, on, alias, "LEFT")

    def right_join(
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression], alias: Optional[str] = None
    ) -> Self:
        return self.join(table, on, alias, "RIGHT")

    def full_join(
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression], alias: Optional[str] = None
    ) -> Self:
        return self.join(table, on, alias, "FULL")

    def cross_join(self, table: Union[str, exp.Expression, Any], alias: Optional[str] = None) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "Cannot add cross join to a non-SELECT expression."
            raise SQLBuilderError(msg)
        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = parse_table_expression(table, alias)
        elif has_query_builder_parameters(table):
            if has_expression_attr(table) and getattr(table, "_expression", None) is not None:
                table_expr_value = getattr(table, "_expression", None)
                if table_expr_value is not None:
                    subquery_exp = exp.paren(table_expr_value.copy())  # pyright: ignore
                else:
                    subquery_exp = exp.paren(exp.Anonymous(this=""))
                table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
            else:
                subquery = table.build()  # pyright: ignore
                sql_str = subquery.sql if has_sql_method(subquery) and not callable(subquery.sql) else str(subquery)
                subquery_exp = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(builder, "dialect", None)))
                table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
        else:
            table_expr = table
        join_expr = exp.Join(this=table_expr, kind="CROSS")
        builder._expression = builder._expression.join(join_expr, copy=False)
        return cast("Self", builder)


class AggregationMixin:
    """GROUP BY, HAVING, and aggregate functions."""

    _expression: Optional[exp.Expression] = None

    def group_by(self, *columns: Union[str, exp.Expression]) -> Self:
        """Add GROUP BY clause."""
        if self._expression is None or not isinstance(self._expression, exp.Select):
            return self
        for column in columns:
            self._expression = self._expression.group_by(
                exp.column(column) if isinstance(column, str) else column, copy=False
            )
        return self

    def group_by_rollup(self, *columns: Union[str, exp.Expression]) -> Self:
        """Add GROUP BY ROLLUP clause."""
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        rollup_expr = exp.Rollup(expressions=column_exprs)
        return self.group_by(rollup_expr)

    def group_by_cube(self, *columns: Union[str, exp.Expression]) -> Self:
        """Add GROUP BY CUBE clause."""
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        cube_expr = exp.Cube(expressions=column_exprs)
        return self.group_by(cube_expr)

    def group_by_grouping_sets(self, *column_sets: Union[tuple[str, ...], list[str]]) -> Self:
        """Add GROUP BY GROUPING SETS clause."""
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

    def having(self, condition: Union[str, exp.Expression]) -> Self:
        """Add HAVING clause."""
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add HAVING to a non-SELECT expression."
            raise SQLBuilderError(msg)
        having_expr = exp.condition(condition) if isinstance(condition, str) else condition
        self._expression = self._expression.having(having_expr, copy=False)
        return self

    def count_(self, column: "Union[str, exp.Expression]" = "*", alias: Optional[str] = None) -> Self:
        """Add COUNT function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        if column == "*":
            count_expr = exp.Count(this=exp.Star())
        else:
            col_expr = exp.column(column) if isinstance(column, str) else column
            count_expr = exp.Count(this=col_expr)
        select_expr = exp.alias_(count_expr, alias) if alias else count_expr
        return cast("Self", builder.select(select_expr))

    def sum_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add SUM function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        sum_expr = exp.Sum(this=col_expr)
        select_expr = exp.alias_(sum_expr, alias) if alias else sum_expr
        return cast("Self", builder.select(select_expr))

    def avg_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add AVG function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        avg_expr = exp.Avg(this=col_expr)
        select_expr = exp.alias_(avg_expr, alias) if alias else avg_expr
        return cast("Self", builder.select(select_expr))

    def max_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add MAX function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        max_expr = exp.Max(this=col_expr)
        select_expr = exp.alias_(max_expr, alias) if alias else max_expr
        return cast("Self", builder.select(select_expr))

    def min_(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add MIN function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        min_expr = exp.Min(this=col_expr)
        select_expr = exp.alias_(min_expr, alias) if alias else min_expr
        return cast("Self", builder.select(select_expr))

    def array_agg(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add ARRAY_AGG aggregate function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        array_agg_expr = exp.ArrayAgg(this=col_expr)
        select_expr = exp.alias_(array_agg_expr, alias) if alias else array_agg_expr
        return cast("Self", builder.select(select_expr))

    def count_distinct(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add COUNT(DISTINCT column) to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        count_expr = exp.Count(this=exp.Distinct(expressions=[col_expr]))
        select_expr = exp.alias_(count_expr, alias) if alias else count_expr
        return cast("Self", builder.select(select_expr))

    def stddev(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add STDDEV aggregate function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        stddev_expr = exp.Stddev(this=col_expr)
        select_expr = exp.alias_(stddev_expr, alias) if alias else stddev_expr
        return cast("Self", builder.select(select_expr))

    def stddev_pop(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add STDDEV_POP aggregate function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        stddev_pop_expr = exp.StddevPop(this=col_expr)
        select_expr = exp.alias_(stddev_pop_expr, alias) if alias else stddev_pop_expr
        return cast("Self", builder.select(select_expr))

    def stddev_samp(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add STDDEV_SAMP aggregate function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        stddev_samp_expr = exp.StddevSamp(this=col_expr)
        select_expr = exp.alias_(stddev_samp_expr, alias) if alias else stddev_samp_expr
        return cast("Self", builder.select(select_expr))

    def variance(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add VARIANCE aggregate function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        variance_expr = exp.Variance(this=col_expr)
        select_expr = exp.alias_(variance_expr, alias) if alias else variance_expr
        return cast("Self", builder.select(select_expr))

    def var_pop(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add VAR_POP aggregate function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        var_pop_expr = exp.VariancePop(this=col_expr)
        select_expr = exp.alias_(var_pop_expr, alias) if alias else var_pop_expr
        return cast("Self", builder.select(select_expr))

    def string_agg(self, column: Union[str, exp.Expression], separator: str = ",", alias: Optional[str] = None) -> Self:
        """Add STRING_AGG aggregate function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        string_agg_expr = exp.GroupConcat(this=col_expr, separator=exp.Literal.string(separator))
        select_expr = exp.alias_(string_agg_expr, alias) if alias else string_agg_expr
        return cast("Self", builder.select(select_expr))

    def json_agg(self, column: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add JSON_AGG aggregate function to SELECT clause."""
        builder = cast("SelectBuilderProtocol", self)
        col_expr = exp.column(column) if isinstance(column, str) else column
        json_agg_expr = exp.JSONArrayAgg(this=col_expr)
        select_expr = exp.alias_(json_agg_expr, alias) if alias else json_agg_expr
        return cast("Self", builder.select(select_expr))


class DatabaseSpecificMixin:
    """Database-specific features not universally supported by SQLGlot."""

    _expression: Optional[exp.Expression] = None
    dialect: Optional["DialectType"] = None

    def with_(
        self, name: str, query: Union[Any, str], recursive: bool = False, columns: Optional[list[str]] = None
    ) -> Self:
        """Add WITH clause (Common Table Expression)."""
        if self._expression is None:
            msg = "Cannot add WITH clause: expression not initialized."
            raise SQLBuilderError(msg)

        if not has_with_method(self._expression) and not isinstance(
            self._expression, (exp.Select, exp.Insert, exp.Update, exp.Delete)
        ):
            msg = f"Cannot add WITH clause to {type(self._expression).__name__} expression."
            raise SQLBuilderError(msg)

        cte_expr: Optional[exp.Expression] = None
        if has_to_statement(query):
            built_query = query.to_statement()  # pyright: ignore
            cte_sql = built_query.to_sql()
            cte_expr = exp.maybe_parse(cte_sql, dialect=getattr(self, "dialect", None))
            if has_parameter_builder(self):
                parameters = getattr(built_query, "parameters", None) or {}
                for param_name, param_value in parameters.items():
                    self.add_parameter(param_value, name=param_name)  # pyright: ignore
        elif isinstance(query, str):
            cte_expr = exp.maybe_parse(query, dialect=getattr(self, "dialect", None))
        elif isinstance(query, exp.Expression):
            cte_expr = query

        if not cte_expr:
            msg = f"Could not parse CTE query: {query}"
            raise SQLBuilderError(msg)

        if columns:
            cte_alias_expr = exp.alias_(cte_expr, name, table=[exp.to_identifier(col) for col in columns])
        else:
            cte_alias_expr = exp.alias_(cte_expr, name)

        if has_with_method(self._expression):
            existing_with = self._expression.args.get("with")  # pyright: ignore
            if existing_with:
                existing_with.expressions.append(cte_alias_expr)
                if recursive:
                    existing_with.set("recursive", recursive)
            else:
                self._expression = self._expression.with_(cte_alias_expr, as_=name, copy=False)  # pyright: ignore
                if recursive:
                    with_clause = self._expression.find(exp.With)
                    if with_clause:
                        with_clause.set("recursive", recursive)
        else:
            if not hasattr(self, "_with_ctes"):  # This is checking for an internal attribute, not a protocol
                setattr(self, "_with_ctes", {})
            self._with_ctes[name] = exp.CTE(this=cte_expr, alias=exp.to_table(name))  # type: ignore[attr-defined]

        return self

    def window(
        self,
        function_expr: Union[str, exp.Expression],
        partition_by: Optional[Union[str, list[str], exp.Expression, list[exp.Expression]]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression, list[exp.Expression]]] = None,
        frame: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> Self:
        """Add a window function to the SELECT clause."""
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

    def returning(self, *columns: Union[str, exp.Expression]) -> Self:
        """Add RETURNING clause to the statement."""
        if self._expression is None:
            msg = "Cannot add RETURNING: expression is not initialized."
            raise SQLBuilderError(msg)
        valid_types = (exp.Insert, exp.Update, exp.Delete)
        if not isinstance(self._expression, valid_types):
            msg = "RETURNING is only supported for INSERT, UPDATE, and DELETE statements."
            raise SQLBuilderError(msg)
        returning_exprs = [exp.column(c) if isinstance(c, str) else c for c in columns]
        self._expression.set("returning", exp.Returning(expressions=returning_exprs))
        return self

    def pivot(
        self,
        aggregate_function: Union[str, exp.Expression],
        aggregate_column: Union[str, exp.Expression],
        pivot_column: Union[str, exp.Expression],
        pivot_values: list[Union[str, int, float, exp.Expression]],
        alias: Optional[str] = None,
    ) -> Self:
        """Add PIVOT clause to the SELECT statement."""
        current_expr = self._expression
        if not isinstance(current_expr, exp.Select):
            msg = "Pivot can only be applied to a Select expression."
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

        return self

    def unpivot(
        self,
        value_column_name: str,
        name_column_name: str,
        columns_to_unpivot: list[Union[str, exp.Expression]],
        alias: Optional[str] = None,
    ) -> Self:
        """Add UNPIVOT clause to the SELECT statement."""
        current_expr = self._expression
        if not isinstance(current_expr, exp.Select):
            msg = "Unpivot can only be applied to a Select expression."
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

        return self

    def union(self, other: Any, all_: bool = False) -> Self:
        """Combine this query with another using UNION."""
        left_query = self.build()  # type: ignore[attr-defined]
        right_query = other.build()
        left_expr: Optional[exp.Expression] = exp.maybe_parse(left_query.sql, dialect=getattr(self, "dialect", None))
        right_expr: Optional[exp.Expression] = exp.maybe_parse(right_query.sql, dialect=getattr(self, "dialect", None))
        if not left_expr or not right_expr:
            msg = "Could not parse queries for UNION operation"
            raise SQLBuilderError(msg)
        union_expr = exp.union(left_expr, right_expr, distinct=not all_)
        new_builder = type(self)()
        new_builder.dialect = getattr(self, "dialect", None)
        new_builder._expression = union_expr
        merged_params = dict(left_query.parameters)
        for param_name, param_value in right_query.parameters.items():
            if param_name in merged_params:
                counter = 1
                new_param_name = f"{param_name}_right_{counter}"
                while new_param_name in merged_params:
                    counter += 1
                    new_param_name = f"{param_name}_right_{counter}"

                def rename_parameter(node: exp.Expression) -> exp.Expression:
                    if isinstance(node, exp.Placeholder) and node.name == param_name:  # noqa: B023
                        return exp.Placeholder(this=new_param_name)  # noqa: B023
                    return node

                right_expr = right_expr.transform(rename_parameter)
                union_expr = exp.union(left_expr, right_expr, distinct=not all_)
                new_builder._expression = union_expr
                merged_params[new_param_name] = param_value
            else:
                merged_params[param_name] = param_value
        new_builder._parameters = merged_params  # pyright: ignore
        return new_builder

    def intersect(self, other: Any) -> Self:
        """Add INTERSECT clause."""
        left_query = self.build()  # type: ignore[attr-defined]
        right_query = other.build()
        left_expr: Optional[exp.Expression] = exp.maybe_parse(left_query.sql, dialect=getattr(self, "dialect", None))
        right_expr: Optional[exp.Expression] = exp.maybe_parse(right_query.sql, dialect=getattr(self, "dialect", None))
        if not left_expr or not right_expr:
            msg = "Could not parse queries for INTERSECT operation"
            raise SQLBuilderError(msg)
        intersect_expr = exp.intersect(left_expr, right_expr, distinct=True)
        new_builder = type(self)()
        new_builder.dialect = getattr(self, "dialect", None)
        new_builder._expression = intersect_expr
        merged_params = dict(left_query.parameters)
        merged_params.update(right_query.parameters)
        new_builder._parameters = merged_params  # pyright: ignore
        return new_builder

    def except_(self, other: Any) -> Self:
        """Combine this query with another using EXCEPT."""
        left_query = self.build()  # type: ignore[attr-defined]
        right_query = other.build()
        left_expr: Optional[exp.Expression] = exp.maybe_parse(left_query.sql, dialect=getattr(self, "dialect", None))
        right_expr: Optional[exp.Expression] = exp.maybe_parse(right_query.sql, dialect=getattr(self, "dialect", None))
        if not left_expr or not right_expr:
            msg = "Could not parse queries for EXCEPT operation"
            raise SQLBuilderError(msg)
        except_expr = exp.except_(left_expr, right_expr)
        new_builder = type(self)()
        new_builder.dialect = getattr(self, "dialect", None)
        new_builder._expression = except_expr
        merged_params = dict(left_query.parameters)
        merged_params.update(right_query.parameters)
        new_builder._parameters = merged_params  # pyright: ignore
        return new_builder

    def case(self, expression: Optional[Union[str, exp.Expression]] = None) -> "case.CaseBuilder":
        """Start a CASE expression.

        Args:
            expression: Optional expression for a searched CASE statement.
                        If None, creates a simple CASE statement.

        Returns:
            A CaseBuilder instance for chaining WHEN clauses.
        """
        from sqlspec.statement_new.builder.case import CaseBuilder

        return CaseBuilder(self, expression)  # type: ignore[arg-type]


class MergeOperationsMixin:
    """MERGE statement operations."""

    _expression: Optional[exp.Expression] = None

    def into(self, table: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Set the target table for the MERGE operation (INTO clause)."""
        if self._expression is None:
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))  # pyright: ignore
        if not isinstance(self._expression, exp.Merge):  # pyright: ignore
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))  # pyright: ignore
        self._expression.set("this", exp.to_table(table, alias=alias) if isinstance(table, str) else table)
        return self

    def using(self, source: Union[str, exp.Expression, Any], alias: Optional[str] = None) -> Self:
        """Set the source data for the MERGE operation (USING clause)."""
        if self._expression is None:
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))
        if not isinstance(self._expression, exp.Merge):
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))

        source_expr: exp.Expression
        if isinstance(source, str):
            source_expr = exp.to_table(source, alias=alias)
        elif has_query_builder_parameters(source) and has_expression_attr(source):
            subquery_builder_params = source.parameters
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]
            subquery_exp = exp.paren(getattr(source, "_expression", exp.select()))
            source_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
        elif isinstance(source, exp.Expression):
            source_expr = source
            if alias:
                source_expr = exp.alias_(source_expr, alias)
        else:
            msg = f"Unsupported source type for USING clause: {type(source)}"
            raise SQLBuilderError(msg)

        self._expression.set("using", source_expr)
        return self

    def on(self, condition: Union[str, exp.Expression]) -> Self:
        """Set the join condition for the MERGE operation (ON clause)."""
        if self._expression is None:
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))
        if not isinstance(self._expression, exp.Merge):
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))

        condition_expr: exp.Expression
        if isinstance(condition, str):
            parsed_condition: Optional[exp.Expression] = exp.maybe_parse(
                condition, dialect=getattr(self, "dialect", None)
            )
            if not parsed_condition:
                msg = f"Could not parse ON condition: {condition}"
                raise SQLBuilderError(msg)
            condition_expr = parsed_condition
        elif isinstance(condition, exp.Expression):
            condition_expr = condition
        else:
            msg = f"Unsupported condition type for ON clause: {type(condition)}"
            raise SQLBuilderError(msg)

        self._expression.set("on", condition_expr)
        return self

    def _add_when_clause(self, when_clause: exp.When) -> None:
        """Helper to add a WHEN clause to the MERGE statement."""
        if self._expression is None:
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))
        if not isinstance(self._expression, exp.Merge):
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))

        whens = self._expression.args.get("whens")
        if not whens:
            whens = exp.Whens(expressions=[])
            self._expression.set("whens", whens)

        whens.append("expressions", when_clause)

    def when_matched_then_update(
        self, set_values: dict[str, Any], condition: Optional[Union[str, exp.Expression]] = None
    ) -> Self:
        """Define the UPDATE action for matched rows."""
        update_expressions: list[exp.EQ] = []
        for col, val in set_values.items():
            param_name = self.add_parameter(val)[1]  # type: ignore[attr-defined]
            update_expressions.append(exp.EQ(this=exp.column(col), expression=exp.var(param_name)))

        when_args: dict[str, Any] = {"matched": True, "then": exp.Update(expressions=update_expressions)}

        if condition:
            condition_expr: exp.Expression
            if isinstance(condition, str):
                parsed_cond: Optional[exp.Expression] = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
                if not parsed_cond:
                    msg = f"Could not parse WHEN clause condition: {condition}"
                    raise SQLBuilderError(msg)
                condition_expr = parsed_cond
            elif isinstance(condition, exp.Expression):
                condition_expr = condition
            else:
                msg = f"Unsupported condition type for WHEN clause: {type(condition)}"
                raise SQLBuilderError(msg)
            when_args["this"] = condition_expr

        when_clause = exp.When(**when_args)
        self._add_when_clause(when_clause)
        return self

    def when_matched_then_delete(self, condition: Optional[Union[str, exp.Expression]] = None) -> Self:
        """Define the DELETE action for matched rows."""
        when_args: dict[str, Any] = {"matched": True, "then": exp.Delete()}

        if condition:
            condition_expr: exp.Expression
            if isinstance(condition, str):
                parsed_cond: Optional[exp.Expression] = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
                if not parsed_cond:
                    msg = f"Could not parse WHEN clause condition: {condition}"
                    raise SQLBuilderError(msg)
                condition_expr = parsed_cond
            elif isinstance(condition, exp.Expression):
                condition_expr = condition
            else:
                msg = f"Unsupported condition type for WHEN clause: {type(condition)}"
                raise SQLBuilderError(msg)
            when_args["this"] = condition_expr

        when_clause = exp.When(**when_args)
        self._add_when_clause(when_clause)
        return self

    def when_not_matched_then_insert(
        self,
        columns: Optional[list[str]] = None,
        values: Optional[list[Any]] = None,
        condition: Optional[Union[str, exp.Expression]] = None,
        by_target: bool = True,
    ) -> Self:
        """Define the INSERT action for rows not matched."""
        insert_args: dict[str, Any] = {}
        if columns and values:
            if len(columns) != len(values):
                msg = "Number of columns must match number of values for INSERT."
                raise SQLBuilderError(msg)

            parameterized_values: list[exp.Expression] = []
            for val in values:
                param_name = self.add_parameter(val)[1]  # type: ignore[attr-defined]
                parameterized_values.append(exp.var(param_name))

            insert_args["this"] = exp.Tuple(expressions=[exp.column(c) for c in columns])
            insert_args["expression"] = exp.Tuple(expressions=parameterized_values)
        elif columns and not values:
            msg = "Specifying columns without values for INSERT action is complex and not fully supported yet."
            raise SQLBuilderError(msg)
        elif not columns and not values:
            pass
        else:
            msg = "Cannot specify values without columns for INSERT action."
            raise SQLBuilderError(msg)

        when_args: dict[str, Any] = {"matched": False, "then": exp.Insert(**insert_args)}

        if not by_target:
            when_args["source"] = True

        if condition:
            condition_expr: exp.Expression
            if isinstance(condition, str):
                parsed_cond: Optional[exp.Expression] = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
                if not parsed_cond:
                    msg = f"Could not parse WHEN clause condition: {condition}"
                    raise SQLBuilderError(msg)
                condition_expr = parsed_cond
            elif isinstance(condition, exp.Expression):
                condition_expr = condition
            else:
                msg = f"Unsupported condition type for WHEN clause: {type(condition)}"
                raise SQLBuilderError(msg)
            when_args["this"] = condition_expr

        when_clause = exp.When(**when_args)
        self._add_when_clause(when_clause)  # type: ignore[attr-defined]
        return self

    def when_not_matched_by_source_then_update(
        self, set_values: dict[str, Any], condition: Optional[Union[str, exp.Expression]] = None
    ) -> Self:
        """Define the UPDATE action for rows not matched by source."""
        update_expressions: list[exp.EQ] = []
        for col, val in set_values.items():
            param_name = self.add_parameter(val)[1]  # type: ignore[attr-defined]
            update_expressions.append(exp.EQ(this=exp.column(col), expression=exp.var(param_name)))

        when_args: dict[str, Any] = {
            "matched": False,
            "source": True,
            "then": exp.Update(expressions=update_expressions),
        }

        if condition:
            condition_expr: exp.Expression
            if isinstance(condition, str):
                parsed_cond: Optional[exp.Expression] = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
                if not parsed_cond:
                    msg = f"Could not parse WHEN clause condition: {condition}"
                    raise SQLBuilderError(msg)
                condition_expr = parsed_cond
            elif isinstance(condition, exp.Expression):
                condition_expr = condition
            else:
                msg = f"Unsupported condition type for WHEN clause: {type(condition)}"
                raise SQLBuilderError(msg)
            when_args["this"] = condition_expr

        when_clause = exp.When(**when_args)
        self._add_when_clause(when_clause)  # type: ignore[attr-defined]
        return self

    def when_not_matched_by_source_then_delete(self, condition: Optional[Union[str, exp.Expression]] = None) -> Self:
        """Define the DELETE action for rows not matched by source."""
        when_args: dict[str, Any] = {"matched": False, "source": True, "then": exp.Delete()}

        if condition:
            condition_expr: exp.Expression
            if isinstance(condition, str):
                parsed_cond: Optional[exp.Expression] = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
                if not parsed_cond:
                    msg = f"Could not parse WHEN clause condition: {condition}"
                    raise SQLBuilderError(msg)
                condition_expr = parsed_cond
            elif isinstance(condition, exp.Expression):
                condition_expr = condition
            else:
                msg = f"Unsupported condition type for WHEN clause: {type(condition)}"
                raise SQLBuilderError(msg)
            when_args["this"] = condition_expr

        when_clause = exp.When(**when_args)
        self._add_when_clause(when_clause)  # type: ignore[attr-defined]
        return self


class InsertOperationsMixin:
    """INSERT statement operations."""

    _expression: Optional[exp.Expression] = None

    def into(self, table: Union[str, exp.Expression], columns: Optional[list[str]] = None) -> Self:
        """Set the target table for INSERT operation."""
        if self._expression is None:
            self._expression = exp.Insert()
        if not isinstance(self._expression, exp.Insert):
            msg = "INTO clause is only supported for INSERT statements."
            raise SQLBuilderError(msg)
        table_expr = exp.to_table(table) if isinstance(table, str) else table
        self._expression.set("this", table_expr)
        if columns:
            schema_expr = exp.Schema(expressions=[exp.to_identifier(col) for col in columns])
            self._expression.set("schema", schema_expr)
        return self

    def values(self, *rows: Union[tuple[Any, ...], list[Any], dict[str, Any]]) -> Self:
        """Add VALUES to INSERT statement."""
        if self._expression is None:
            self._expression = exp.Insert()
        if not isinstance(self._expression, exp.Insert):
            msg = "VALUES clause is only supported for INSERT statements."
            raise SQLBuilderError(msg)

        values_list: list[exp.Tuple] = []
        for row in rows:
            if isinstance(row, dict):
                if not self._expression.args.get("schema"):
                    columns = list(row.keys())
                    schema_expr = exp.Schema(expressions=[exp.to_identifier(col) for col in columns])
                    self._expression.set("schema", schema_expr)
                row_values = list(row.values())
            elif isinstance(row, (tuple, list)):
                row_values = list(row)
            else:
                msg = f"Unsupported row type: {type(row)}"
                raise SQLBuilderError(msg)

            value_exprs: list[exp.Expression] = []
            for value in row_values:
                param_name = self.add_parameter(value)[1]  # type: ignore[attr-defined]
                value_exprs.append(exp.var(param_name))
            values_list.append(exp.Tuple(expressions=value_exprs))

        values_expr = exp.Values(expressions=values_list)
        self._expression.set("expression", values_expr)
        return self

    def from_(self, query: Union[str, exp.Expression, Any]) -> Self:
        """Add INSERT FROM SELECT."""
        if self._expression is None:
            self._expression = exp.Insert()
        if not isinstance(self._expression, exp.Insert):
            msg = "FROM clause in INSERT is only supported for INSERT statements."
            raise SQLBuilderError(msg)

        query_expr: exp.Expression
        if isinstance(query, str):
            parsed = exp.maybe_parse(query, dialect=getattr(self, "dialect", None))
            if not parsed:
                msg = f"Could not parse query for INSERT FROM: {query}"
                raise SQLBuilderError(msg)
            query_expr = parsed
        elif has_query_builder_parameters(query):
            subquery = query.build()  # pyright: ignore
            sql_str = subquery.sql if has_sql_method(subquery) and not callable(subquery.sql) else str(subquery)
            query_expr = exp.maybe_parse(sql_str, dialect=getattr(self, "dialect", None))
            if has_parameter_builder(self) and has_query_builder_parameters(subquery):
                for param_name, param_value in subquery.parameters.items():  # pyright: ignore
                    self.add_parameter(param_value, name=param_name)  # pyright: ignore
        else:
            query_expr = query

        self._expression.set("expression", query_expr)
        return self


class UpdateOperationsMixin:
    """UPDATE statement operations."""

    _expression: Optional[exp.Expression] = None

    def table(self, table: Union[str, exp.Expression]) -> Self:
        """Set the table to update."""
        if self._expression is None:
            self._expression = exp.Update()
        if not isinstance(self._expression, exp.Update):
            msg = "TABLE clause is only supported for UPDATE statements."
            raise SQLBuilderError(msg)
        table_expr = exp.to_table(table) if isinstance(table, str) else table
        self._expression.set("this", table_expr)
        return self

    def set(self, column: Optional[str] = None, value: Any = None, **columns: Any) -> Self:
        """Set column values for UPDATE."""
        if self._expression is None:
            self._expression = exp.Update()
        if not isinstance(self._expression, exp.Update):
            msg = "SET clause is only supported for UPDATE statements."
            raise SQLBuilderError(msg)

        updates: dict[str, Any] = {}
        if column is not None and value is not None:
            updates[column] = value
        updates.update(columns)

        expressions: list[exp.EQ] = []
        for col, val in updates.items():
            param_name = self.add_parameter(val)[1]  # type: ignore[attr-defined]
            expressions.append(exp.EQ(this=exp.column(col), expression=exp.var(param_name)))

        existing_expressions = self._expression.args.get("expressions", [])
        all_expressions = existing_expressions + expressions
        self._expression.set("expressions", all_expressions)
        return self

    def from_(self, table: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Add FROM clause to UPDATE statement (PostgreSQL style)."""
        if self._expression is None:
            self._expression = exp.Update()
        if not isinstance(self._expression, exp.Update):
            msg = "FROM clause in UPDATE is only supported for UPDATE statements."
            raise SQLBuilderError(msg)
        table_expr = exp.to_table(table, alias=alias) if isinstance(table, str) else table
        self._expression.set("from", exp.From(this=table_expr))
        return self


class DeleteOperationsMixin:
    """DELETE statement operations."""

    _expression: Optional[exp.Expression] = None

    def from_(self, table: Union[str, exp.Expression]) -> Self:
        """Set the table to delete from."""
        if self._expression is None:
            self._expression = exp.Delete()
        if not isinstance(self._expression, exp.Delete):
            msg = "FROM clause is only supported for DELETE statements."
            raise SQLBuilderError(msg)
        table_expr = exp.to_table(table) if isinstance(table, str) else table
        self._expression.set("this", table_expr)
        return self
