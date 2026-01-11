"""SELECT statement builder.

Provides a fluent interface for building SQL SELECT queries with
parameter binding and validation.
"""

# pyright: reportPrivateUsage=false, reportPrivateImportUsage=false

import re
from typing import TYPE_CHECKING, Any, Final, Union, cast

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._base import BuiltQuery, QueryBuilder
from sqlspec.builder._explain import ExplainMixin
from sqlspec.builder._join import JoinClauseMixin
from sqlspec.builder._parsing_utils import (
    extract_column_name,
    extract_expression,
    parse_column_expression,
    parse_condition_expression,
    parse_order_expression,
    parse_table_expression,
    to_expression,
)
from sqlspec.core import SQL, ParameterStyle, ParameterValidator, SQLResult
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import (
    has_expression_and_sql,
    has_parameter_builder,
    has_sqlglot_expression,
    is_expression,
    is_iterable_parameters,
)

BETWEEN_BOUND_COUNT = 2
PAIR_LENGTH = 2
TRIPLE_LENGTH = 3


if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.builder._column import Column, ColumnExpression, FunctionColumn
    from sqlspec.builder._expression_wrappers import ExpressionWrapper
    from sqlspec.protocols import SQLBuilderProtocol

__all__ = (
    "Case",
    "CaseBuilder",
    "CommonTableExpressionMixin",
    "HavingClauseMixin",
    "LimitOffsetClauseMixin",
    "OrderByClauseMixin",
    "PivotClauseMixin",
    "ReturningClauseMixin",
    "Select",
    "SelectClauseMixin",
    "SetOperationMixin",
    "SubqueryBuilder",
    "UnpivotClauseMixin",
    "WhereClauseMixin",
    "WindowFunctionBuilder",
)


def is_explicitly_quoted(identifier: Any) -> bool:
    """Detect if identifier was provided with explicit quotes."""
    if not isinstance(identifier, str):
        return False
    stripped = identifier.strip()
    return (stripped.startswith('"') and stripped.endswith('"')) or (
        stripped.startswith("`") and stripped.endswith("`")
    )


def _expr_eq(col: "exp.Expression", placeholder: "exp.Placeholder") -> "exp.Expression":
    return exp.EQ(this=col, expression=placeholder)


def _expr_neq(col: "exp.Expression", placeholder: "exp.Placeholder") -> "exp.Expression":
    return exp.NEQ(this=col, expression=placeholder)


def _expr_gt(col: "exp.Expression", placeholder: "exp.Placeholder") -> "exp.Expression":
    return exp.GT(this=col, expression=placeholder)


def _expr_gte(col: "exp.Expression", placeholder: "exp.Placeholder") -> "exp.Expression":
    return exp.GTE(this=col, expression=placeholder)


def _expr_lt(col: "exp.Expression", placeholder: "exp.Placeholder") -> "exp.Expression":
    return exp.LT(this=col, expression=placeholder)


def _expr_lte(col: "exp.Expression", placeholder: "exp.Placeholder") -> "exp.Expression":
    return exp.LTE(this=col, expression=placeholder)


def _expr_like_exp(col: "exp.Expression", placeholder: "exp.Placeholder") -> "exp.Expression":
    return exp.Like(this=col, expression=placeholder)


def _expr_like_method(col: "exp.Expression", placeholder: "exp.Placeholder") -> "exp.Expression":
    return cast("exp.Expression", col.like(placeholder))


def _expr_not_like(col: "exp.Expression", placeholder: "exp.Placeholder") -> "exp.Expression":
    return exp.Not(this=exp.Like(this=col, expression=placeholder))


def _expr_like_not(col: "exp.Expression", placeholder: "exp.Placeholder") -> "exp.Expression":
    return exp.Not(this=cast("exp.Expression", col.like(placeholder)))


def _expr_ilike(col: "exp.Expression", placeholder: "exp.Placeholder") -> "exp.Expression":
    return cast("exp.Expression", col.ilike(placeholder))


_SIMPLE_OPERATOR_MAP: dict[str, Any] = {
    "=": _expr_eq,
    "==": _expr_eq,
    "!=": _expr_neq,
    "<>": _expr_neq,
    ">": _expr_gt,
    ">=": _expr_gte,
    "<": _expr_lt,
    "<=": _expr_lte,
    "LIKE": _expr_like_exp,
    "NOT LIKE": _expr_not_like,
}


class Case:
    """Represent a SQL CASE expression with structured components."""

    __slots__ = ("conditions", "default")

    def __init__(self, *ifs: exp.Expression, default: exp.Expression | None = None) -> None:
        self.conditions = list(ifs)
        self.default = default

    def when(self, condition: str | exp.Expression, result: Any) -> "Case":
        condition_expr = parse_condition_expression(condition)
        result_expr = to_expression(result)
        self.conditions.append(exp.If(this=condition_expr, true=result_expr))
        return self

    def else_(self, value: Any) -> "Case":
        self.default = to_expression(value)
        return self

    def end(self) -> "Case":
        return self

    def as_(self, alias: str) -> exp.Alias:
        return cast("exp.Alias", exp.alias_(self.expression, alias))

    @property
    def expression(self) -> exp.Case:
        return exp.Case(ifs=self.conditions, default=self.default)


class CaseBuilder:
    """Fluent builder for CASE expressions used within SELECT clauses."""

    __slots__ = ()

    def __call__(self, *args: Any, default: Any | None = None) -> Case:
        conditions = [to_expression(arg) for arg in args]
        default_expr = to_expression(default) if default is not None else None
        return Case(*conditions, default=default_expr)


class SubqueryBuilder:
    """Helper to build subquery expressions for EXISTS/IN/ANY/ALL operations."""

    __slots__ = ("_operation",)

    def __init__(self, operation: str) -> None:
        self._operation = operation

    def __call__(self, subquery: Any) -> exp.Expression:
        if isinstance(subquery, exp.Expression):
            subquery_expr = subquery
        elif has_parameter_builder(subquery):
            built_query = subquery.build()
            sql_text = built_query.sql if isinstance(built_query, BuiltQuery) else str(built_query)
            dialect = subquery.dialect if isinstance(subquery, QueryBuilder) else None
            parsed_expr: exp.Expression | None = exp.maybe_parse(sql_text, dialect=dialect)
            if parsed_expr is None:
                msg = f"Could not parse subquery SQL: {sql_text}"
                raise SQLBuilderError(msg)
            subquery_expr = parsed_expr
        else:
            dialect = subquery.dialect if isinstance(subquery, (QueryBuilder, BuiltQuery)) else None
            parsed_expr = exp.maybe_parse(str(subquery), dialect=dialect)
            if parsed_expr is None:
                msg = f"Could not convert subquery to expression: {subquery}"
                raise SQLBuilderError(msg)
            subquery_expr = parsed_expr

        if self._operation == "exists":
            return exp.Exists(this=subquery_expr)
        if self._operation == "in":
            return exp.In(expressions=[subquery_expr])
        if self._operation == "any":
            return exp.Any(this=subquery_expr)
        if self._operation == "all":
            return exp.All(this=subquery_expr)
        msg = f"Unknown subquery operation: {self._operation}"
        raise SQLBuilderError(msg)


class WindowFunctionBuilder:
    """Helper to fluently construct window function expressions."""

    __slots__ = ("_function_args", "_function_name", "_order_by", "_partition_by")

    def __init__(self, function_name: str, *function_args: Any) -> None:
        self._function_name = function_name
        self._function_args: list[exp.Expression] = [to_expression(arg) for arg in function_args]
        self._partition_by: list[exp.Expression] = []
        self._order_by: list[exp.Ordered] = []

    def __call__(self, *function_args: Any) -> "WindowFunctionBuilder":
        self._function_args = [to_expression(arg) for arg in function_args]
        return self

    def partition_by(self, *columns: str | exp.Expression) -> "WindowFunctionBuilder":
        self._partition_by = [exp.column(column) if isinstance(column, str) else column for column in columns]
        return self

    def order_by(self, *columns: str | exp.Expression) -> "WindowFunctionBuilder":
        ordered_columns: list[exp.Ordered] = []
        for column in columns:
            if isinstance(column, str):
                ordered_columns.append(exp.column(column).asc())
            elif isinstance(column, exp.Ordered):
                ordered_columns.append(column)
            else:
                ordered_columns.append(exp.Ordered(this=column, desc=False))
        self._order_by = ordered_columns
        return self

    def _build_function_expression(self) -> exp.Expression:
        expressions = self._function_args or []
        return exp.Anonymous(this=self._function_name, expressions=expressions)

    def build(self) -> exp.Window:
        over_args: dict[str, Any] = {}
        if self._partition_by:
            over_args["partition_by"] = self._partition_by
        if self._order_by:
            over_args["order"] = exp.Order(expressions=self._order_by)
        return exp.Window(this=self._build_function_expression(), **over_args)

    def as_(self, alias: str) -> exp.Alias:
        return cast("exp.Alias", exp.alias_(self.build(), alias))


def _ensure_select_expression(
    mixin: "SQLBuilderProtocol", *, error_message: str, initialize: bool = True
) -> exp.Select:
    expression = mixin.get_expression()
    if expression is None and initialize:
        mixin.set_expression(exp.Select())
        expression = mixin.get_expression()

    if not isinstance(expression, exp.Select):
        raise SQLBuilderError(error_message)

    return expression


@trait
class SelectClauseMixin:
    """Mixin providing SELECT clause methods."""

    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def select(self, *columns: Union[str, exp.Expression, "Column", "FunctionColumn", SQL, Case]) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        select_expr = _ensure_select_expression(builder, error_message="Cannot add columns to non-SELECT expression.")
        for column in columns:
            column_expr = column.expression if isinstance(column, Case) else parse_column_expression(column, builder)
            select_expr = select_expr.select(column_expr, copy=False)
        self.set_expression(select_expr)
        return cast("Self", builder)

    def distinct(self, *columns: Union[str, exp.Expression, "Column", "FunctionColumn", SQL]) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        select_expr = _ensure_select_expression(builder, error_message="Cannot add DISTINCT to non-SELECT expression.")
        if not columns:
            select_expr.set("distinct", exp.Distinct())
        else:
            distinct_columns = [parse_column_expression(column, builder) for column in columns]
            select_expr.set("distinct", exp.Distinct(expressions=distinct_columns))
        builder.set_expression(select_expr)
        return cast("Self", builder)

    def from_(
        self,
        table: str | exp.Expression | Any,
        alias: str | None = None,
        as_of: Any | None = None,
        as_of_type: str | None = None,
    ) -> Self:
        """Set the FROM clause and optionally attach temporal versioning.

        ``as_of`` copies the resolved table expression, normalizes aliases, and adds an ``exp.Version`` so sqlglot's generator emits dialect-specific time-travel SQL.
        """
        builder = cast("SQLBuilderProtocol", self)
        select_expr = _ensure_select_expression(builder, error_message="FROM clause only valid for SELECT.")
        from_expr: exp.Expression

        if isinstance(table, str):
            from_expr = parse_table_expression(table, alias)
        elif is_expression(table):
            from_expr = exp.alias_(table, alias) if alias else table
        elif has_parameter_builder(table):
            subquery_expression = table.get_expression()
            if subquery_expression is None:
                msg = "Subquery builder has no expression to include in FROM clause."
                raise SQLBuilderError(msg)

            subquery_copy = subquery_expression.copy()
            base_builder = cast("QueryBuilder", builder)
            param_mapping = base_builder._merge_cte_parameters(alias or "subquery", table.parameters)
            if param_mapping:
                subquery_copy = base_builder._update_placeholders_in_expression(subquery_copy, param_mapping)

            wrapped_subquery = exp.paren(subquery_copy)
            from_expr = exp.alias_(wrapped_subquery, alias) if alias else wrapped_subquery
        else:
            from_expr = table

        if as_of is not None:
            inner_expr = from_expr.copy()
            target_alias = alias

            if isinstance(inner_expr, exp.Alias):
                target_alias = inner_expr.alias
                inner_expr = inner_expr.this

            if target_alias is None and isinstance(inner_expr, exp.Table):
                alias_expr = inner_expr.args.get("alias")
                if alias_expr is not None:
                    target_alias = alias_expr.this
                    inner_expr.set("alias", None)

            version = exp.Version(this=as_of_type or "TIMESTAMP", kind="AS OF", expression=exp.convert(as_of))
            inner_expr.set("version", version)
            from_expr = exp.alias_(inner_expr, target_alias) if target_alias else inner_expr

        builder.set_expression(select_expr.from_(from_expr, copy=False))
        return cast("Self", builder)

    def group_by(self, *columns: str | exp.Expression) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        select_expr = builder.get_expression()
        if select_expr is None or not isinstance(select_expr, exp.Select):
            return cast("Self", builder)

        for column in columns:
            column_expr = exp.column(column) if isinstance(column, str) else column
            select_expr = select_expr.group_by(column_expr, copy=False)
        builder.set_expression(select_expr)
        return cast("Self", builder)

    def group_by_rollup(self, *columns: str | exp.Expression) -> Self:
        column_exprs = [exp.column(column) if isinstance(column, str) else column for column in columns]
        rollup_expr = exp.Rollup(expressions=column_exprs)
        return self.group_by(rollup_expr)

    def group_by_cube(self, *columns: str | exp.Expression) -> Self:
        column_exprs = [exp.column(column) if isinstance(column, str) else column for column in columns]
        cube_expr = exp.Cube(expressions=column_exprs)
        return self.group_by(cube_expr)

    def group_by_grouping_sets(self, *column_sets: tuple[str, ...] | list[str]) -> Self:
        grouping_sets = [
            exp.Tuple(expressions=[exp.column(col) if isinstance(col, str) else col for col in column_set])
            for column_set in column_sets
        ]
        grouping_expr = exp.GroupingSets(expressions=grouping_sets)
        return self.group_by(grouping_expr)


@trait
class OrderByClauseMixin:
    __slots__ = ()

    _expression: exp.Expression | None

    def order_by(self, *items: Union[str, exp.Ordered, "Column"], desc: bool = False) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        select_expr = _ensure_select_expression(builder, error_message="ORDER BY only valid for SELECT.")

        current_expr = select_expr
        for item in items:
            if isinstance(item, str):
                order_item = parse_order_expression(item)
                if desc:
                    order_item = order_item.desc()
            else:
                extracted_item = extract_expression(item)
                order_item = extracted_item.desc() if desc and not isinstance(item, exp.Ordered) else extracted_item
            current_expr = current_expr.order_by(order_item, copy=False)
        builder.set_expression(current_expr)
        return cast("Self", builder)


@trait
class LimitOffsetClauseMixin:
    __slots__ = ()

    _expression: exp.Expression | None

    def limit(self, value: int) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        select_expr = _ensure_select_expression(builder, error_message="LIMIT only valid for SELECT.")
        builder.set_expression(select_expr.limit(exp.convert(value), copy=False))
        return cast("Self", builder)

    def offset(self, value: int) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        select_expr = _ensure_select_expression(builder, error_message="OFFSET only valid for SELECT.")
        builder.set_expression(select_expr.offset(exp.convert(value), copy=False))
        return cast("Self", builder)


@trait
class ReturningClauseMixin:
    __slots__ = ()

    _expression: exp.Expression | None

    def returning(self, *columns: Union[str, exp.Expression, "Column", "ExpressionWrapper", Case]) -> Self:
        if self._expression is None:
            msg = "Cannot add RETURNING: expression not initialized."
            raise SQLBuilderError(msg)
        if not isinstance(self._expression, (exp.Insert, exp.Update, exp.Delete)):
            msg = "RETURNING only supported for INSERT, UPDATE, DELETE statements."
            raise SQLBuilderError(msg)
        returning_exprs = [extract_expression(col) for col in columns]
        self._expression.set("returning", exp.Returning(expressions=returning_exprs))
        return self


@trait
class WhereClauseMixin:
    __slots__ = ()

    def _merge_sql_object_parameters(self, sql_obj: Any) -> None:
        builder = cast("SQLBuilderProtocol", self)
        builder._merge_sql_object_parameters(sql_obj)

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def _create_parameterized_condition(
        self,
        column: str | exp.Column,
        value: Any,
        condition_factory: "Callable[[exp.Expression, exp.Placeholder], exp.Expression]",
    ) -> exp.Expression:
        builder = cast("SQLBuilderProtocol", self)
        column_name = extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        placeholder = exp.Placeholder(this=param_name)
        return condition_factory(col_expr, placeholder)

    def _get_existing_where_clause(self) -> exp.Where | None:
        builder = cast("SQLBuilderProtocol", self)
        expression = builder.get_expression()
        if isinstance(expression, (exp.Select, exp.Update, exp.Delete)):
            where_clause = expression.args.get("where")
            if isinstance(where_clause, exp.Where):
                return where_clause
        return None

    def _combine_with_or(self, new_condition: exp.Expression) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        expression = builder.get_expression()
        if expression is None or not isinstance(expression, (exp.Select, exp.Update, exp.Delete)):
            msg = "OR WHERE clause not supported for current expression. Use where() first."
            raise SQLBuilderError(msg)

        where_clause = self._get_existing_where_clause()
        if where_clause is None or where_clause.this is None:
            msg = "Cannot add OR WHERE clause: no existing WHERE clause found. Use where() before or_where()."
            raise SQLBuilderError(msg)

        combined_condition = exp.Or(this=where_clause.this, expression=new_condition)
        where_clause.set("this", combined_condition)
        builder.set_expression(expression)
        return cast("Self", builder)

    def _handle_in_operator(
        self, column_exp: exp.Expression, value: Any, column_name: str = "column"
    ) -> exp.Expression:
        builder = cast("SQLBuilderProtocol", self)
        if has_parameter_builder(value) or isinstance(value, exp.Expression):
            subquery_expr = self._normalize_subquery_expression(value, builder)
            return exp.In(this=column_exp, expressions=[subquery_expr])
        if is_iterable_parameters(value):
            placeholders = []
            for index, element in enumerate(value):
                name_seed = column_name if len(value) == 1 else f"{column_name}_{index + 1}"
                param_name = builder._generate_unique_parameter_name(name_seed)
                _, param_name = builder.add_parameter(element, name=param_name)
                placeholders.append(exp.Placeholder(this=param_name))
            return exp.In(this=column_exp, expressions=placeholders)

        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        return exp.In(this=column_exp, expressions=[exp.Placeholder(this=param_name)])

    def _handle_not_in_operator(
        self, column_exp: exp.Expression, value: Any, column_name: str = "column"
    ) -> exp.Expression:
        builder = cast("SQLBuilderProtocol", self)
        if has_parameter_builder(value) or isinstance(value, exp.Expression):
            subquery_expr = self._normalize_subquery_expression(value, builder)
            return exp.Not(this=exp.In(this=column_exp, expressions=[subquery_expr]))
        if is_iterable_parameters(value):
            placeholders = []
            for index, element in enumerate(value):
                name_seed = column_name if len(value) == 1 else f"{column_name}_{index + 1}"
                param_name = builder._generate_unique_parameter_name(name_seed)
                _, param_name = builder.add_parameter(element, name=param_name)
                placeholders.append(exp.Placeholder(this=param_name))
            return exp.Not(this=exp.In(this=column_exp, expressions=placeholders))

        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        return exp.Not(this=exp.In(this=column_exp, expressions=[exp.Placeholder(this=param_name)]))

    def _handle_is_operator(self, column_exp: exp.Expression, value: Any) -> exp.Expression:
        value_expr = exp.Null() if value is None else exp.convert(value)
        return exp.Is(this=column_exp, expression=value_expr)

    def _handle_is_not_operator(self, column_exp: exp.Expression, value: Any) -> exp.Expression:
        value_expr = exp.Null() if value is None else exp.convert(value)
        return exp.Not(this=exp.Is(this=column_exp, expression=value_expr))

    def _handle_between_operator(
        self, column_exp: exp.Expression, value: Any, column_name: str = "column"
    ) -> exp.Expression:
        if is_iterable_parameters(value) and len(value) == BETWEEN_BOUND_COUNT:
            builder = cast("SQLBuilderProtocol", self)
            low, high = value
            low_param = builder._generate_unique_parameter_name(f"{column_name}_low")
            high_param = builder._generate_unique_parameter_name(f"{column_name}_high")
            _, low_param = builder.add_parameter(low, name=low_param)
            _, high_param = builder.add_parameter(high, name=high_param)
            return exp.Between(
                this=column_exp, low=exp.Placeholder(this=low_param), high=exp.Placeholder(this=high_param)
            )
        msg = f"BETWEEN operator requires a tuple of two values, got {type(value).__name__}"
        raise SQLBuilderError(msg)

    def _handle_not_between_operator(
        self, column_exp: exp.Expression, value: Any, column_name: str = "column"
    ) -> exp.Expression:
        if is_iterable_parameters(value) and len(value) == BETWEEN_BOUND_COUNT:
            builder = cast("SQLBuilderProtocol", self)
            low, high = value
            low_param = builder._generate_unique_parameter_name(f"{column_name}_low")
            high_param = builder._generate_unique_parameter_name(f"{column_name}_high")
            _, low_param = builder.add_parameter(low, name=low_param)
            _, high_param = builder.add_parameter(high, name=high_param)
            return exp.Not(
                this=exp.Between(
                    this=column_exp, low=exp.Placeholder(this=low_param), high=exp.Placeholder(this=high_param)
                )
            )
        msg = f"NOT BETWEEN operator requires a tuple of two values, got {type(value).__name__}"
        raise SQLBuilderError(msg)

    def _create_any_condition(self, column_expr: exp.Expression, values: Any, column_name: str) -> exp.Expression:
        builder = cast("SQLBuilderProtocol", self)
        if has_parameter_builder(values):
            subquery_expr = self._normalize_subquery_expression(values, builder)
            return exp.EQ(this=column_expr, expression=exp.Any(this=subquery_expr))
        if isinstance(values, exp.Expression):
            return exp.EQ(this=column_expr, expression=exp.Any(this=values))
        if has_sqlglot_expression(values):
            raw_expr = values.sqlglot_expression
            if isinstance(raw_expr, exp.Expression):
                return exp.EQ(this=column_expr, expression=exp.Any(this=raw_expr))
            parsed_expr: exp.Expression | None = exp.maybe_parse(str(values), dialect=builder.dialect)
            if parsed_expr is not None:
                return exp.EQ(this=column_expr, expression=exp.Any(this=parsed_expr))
        if has_expression_and_sql(values):
            self._merge_sql_object_parameters(values)
            expression_attr = values.expression
            if isinstance(expression_attr, exp.Expression):
                return exp.EQ(this=column_expr, expression=exp.Any(this=expression_attr))
            sql_text = values.sql
            parsed_expr = exp.maybe_parse(sql_text, dialect=builder.dialect)
            if parsed_expr is not None:
                return exp.EQ(this=column_expr, expression=exp.Any(this=parsed_expr))
        if isinstance(values, str):
            parsed_expr = exp.maybe_parse(values, dialect=builder.dialect)
            if isinstance(parsed_expr, (exp.Select, exp.Union, exp.Subquery)):
                return exp.EQ(this=column_expr, expression=exp.Any(this=exp.paren(parsed_expr)))
            msg = "Unsupported type for 'values' in WHERE ANY"
            raise SQLBuilderError(msg)
        if not is_iterable_parameters(values) or isinstance(values, (bytes, bytearray)):
            msg = "Unsupported type for 'values' in WHERE ANY"
            raise SQLBuilderError(msg)
        placeholders: list[exp.Expression] = []
        values_list = list(values)
        for index, element in enumerate(values_list):
            if len(values_list) == 1:
                param_name = builder._generate_unique_parameter_name(column_name)
            else:
                param_name = builder._generate_unique_parameter_name(f"{column_name}_any_{index + 1}")
            _, param_name = builder.add_parameter(element, name=param_name)
            placeholders.append(exp.Placeholder(this=param_name))
        tuple_expr = exp.Tuple(expressions=placeholders)
        return exp.EQ(this=column_expr, expression=exp.Any(this=tuple_expr))

    def _create_not_any_condition(self, column_expr: exp.Expression, values: Any, column_name: str) -> exp.Expression:
        builder = cast("SQLBuilderProtocol", self)
        if has_parameter_builder(values):
            subquery_expr = self._normalize_subquery_expression(values, builder)
            return exp.NEQ(this=column_expr, expression=exp.Any(this=subquery_expr))
        if isinstance(values, exp.Expression):
            return exp.NEQ(this=column_expr, expression=exp.Any(this=values))
        if has_sqlglot_expression(values):
            raw_expr = values.sqlglot_expression
            if isinstance(raw_expr, exp.Expression):
                return exp.NEQ(this=column_expr, expression=exp.Any(this=raw_expr))
            parsed_expr: exp.Expression | None = exp.maybe_parse(str(values), dialect=builder.dialect)
            if parsed_expr is not None:
                return exp.NEQ(this=column_expr, expression=exp.Any(this=parsed_expr))
        if has_expression_and_sql(values):
            self._merge_sql_object_parameters(values)
            expression_attr = values.expression
            if isinstance(expression_attr, exp.Expression):
                return exp.NEQ(this=column_expr, expression=exp.Any(this=expression_attr))
            sql_text = values.sql
            parsed_expr = exp.maybe_parse(sql_text, dialect=builder.dialect)
            if parsed_expr is not None:
                return exp.NEQ(this=column_expr, expression=exp.Any(this=parsed_expr))
        if isinstance(values, str):
            parsed_expr = exp.maybe_parse(values, dialect=builder.dialect)
            if isinstance(parsed_expr, (exp.Select, exp.Union, exp.Subquery)):
                return exp.NEQ(this=column_expr, expression=exp.Any(this=exp.paren(parsed_expr)))
            msg = "Unsupported type for 'values' in WHERE NOT ANY"
            raise SQLBuilderError(msg)
        if not is_iterable_parameters(values) or isinstance(values, (bytes, bytearray)):
            msg = "Unsupported type for 'values' in WHERE NOT ANY"
            raise SQLBuilderError(msg)
        placeholders: list[exp.Expression] = []
        values_list = list(values)
        for index, element in enumerate(values_list):
            if len(values_list) == 1:
                param_name = builder._generate_unique_parameter_name(column_name)
            else:
                param_name = builder._generate_unique_parameter_name(f"{column_name}_not_any_{index + 1}")
            _, param_name = builder.add_parameter(element, name=param_name)
            placeholders.append(exp.Placeholder(this=param_name))
        tuple_expr = exp.Tuple(expressions=placeholders)
        return exp.NEQ(this=column_expr, expression=exp.Any(this=tuple_expr))

    def _normalize_subquery_expression(self, subquery: Any, builder: "SQLBuilderProtocol") -> exp.Expression:
        if has_parameter_builder(subquery):
            subquery_builder = cast("QueryBuilder", subquery)
            safe_query: BuiltQuery = subquery_builder.build()
            parsed_subquery: exp.Expression | None = exp.maybe_parse(safe_query.sql, dialect=builder.dialect)
            if parsed_subquery is None:
                msg = f"Could not parse subquery SQL: {safe_query.sql}"
                raise SQLBuilderError(msg)
            subquery_expr = exp.paren(parsed_subquery)
            parameters: Any = safe_query.parameters
            if isinstance(parameters, dict):
                param_mapping: dict[str, str] = {}
                query_builder = cast("QueryBuilder", builder)
                for param_name, param_value in parameters.items():
                    unique_name = query_builder._generate_unique_parameter_name(param_name)
                    param_mapping[param_name] = unique_name
                    query_builder.add_parameter(param_value, name=unique_name)
                if param_mapping:
                    updated = query_builder._update_placeholders_in_expression(parsed_subquery, param_mapping)
                    subquery_expr = exp.paren(updated)
            elif isinstance(parameters, (list, tuple)):
                for param_value in parameters:
                    builder.add_parameter(param_value)
            elif parameters is not None:
                builder.add_parameter(parameters)
            return subquery_expr

        if has_expression_and_sql(subquery):
            self._merge_sql_object_parameters(subquery)
            expression_attr = subquery.expression
            if isinstance(expression_attr, exp.Expression):
                return expression_attr
            sql_text = subquery.sql
            parsed_from_sql: exp.Expression | None = exp.maybe_parse(sql_text, dialect=builder.dialect)
            if parsed_from_sql is None:
                msg = f"Could not parse subquery SQL: {sql_text}"
                raise SQLBuilderError(msg)
            return parsed_from_sql

        if isinstance(subquery, exp.Expression):
            return subquery

        if isinstance(subquery, str):
            parsed_expression_from_str: exp.Expression | None = exp.maybe_parse(subquery, dialect=builder.dialect)
            if parsed_expression_from_str is None:
                msg = f"Could not parse subquery SQL: {subquery}"
                raise SQLBuilderError(msg)
            return parsed_expression_from_str

        converted_expr: exp.Expression = exp.convert(subquery)
        return converted_expr

    def _create_or_expression(self, conditions: "list[exp.Expression]") -> exp.Expression:
        if not conditions:
            msg = "OR expression requires at least one condition"
            raise SQLBuilderError(msg)

        return exp.or_(*conditions)

    def _process_tuple_condition(self, condition: "tuple[Any, ...]") -> exp.Expression:
        if len(condition) == PAIR_LENGTH:
            column, value = condition
            return self._create_parameterized_condition(column, value, _expr_eq)

        if len(condition) != TRIPLE_LENGTH:
            msg = f"Condition tuple must have 2 or 3 elements, got {len(condition)}"
            raise SQLBuilderError(msg)

        column_raw, operator, value = condition
        operator_upper = str(operator).upper()
        column_expr = parse_column_expression(column_raw)
        column_name = extract_column_name(column_raw)

        if operator_upper in _SIMPLE_OPERATOR_MAP:
            return self._create_parameterized_condition(column_raw, value, _SIMPLE_OPERATOR_MAP[operator_upper])

        if operator_upper == "IN":
            return self._handle_in_operator(column_expr, value, column_name)
        if operator_upper == "NOT IN":
            return self._handle_not_in_operator(column_expr, value, column_name)
        if operator_upper == "IS":
            return self._handle_is_operator(column_expr, value)
        if operator_upper == "IS NOT":
            return self._handle_is_not_operator(column_expr, value)
        if operator_upper == "BETWEEN":
            return self._handle_between_operator(column_expr, value, column_name)
        if operator_upper == "NOT BETWEEN":
            return self._handle_not_between_operator(column_expr, value, column_name)

        msg = f"Unsupported operator: {operator}"
        raise SQLBuilderError(msg)

    def _process_where_condition(
        self,
        condition: Union[
            str, exp.Expression, exp.Condition, tuple[str, Any], tuple[str, str, Any], "ColumnExpression", SQL
        ],
        values: tuple[Any, ...],
        operator: str | None,
        kwargs: dict[str, Any],
    ) -> exp.Expression:
        if values or kwargs:
            if not isinstance(condition, str):
                msg = "When values are provided, condition must be a string"
                raise SQLBuilderError(msg)

            validator = ParameterValidator()
            param_info = validator.extract_parameters(condition)

            if param_info:
                param_dict = dict(kwargs)
                positional_params = [
                    info
                    for info in param_info
                    if info.style in {ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_COLON, ParameterStyle.QMARK}
                ]

                if len(values) != len(positional_params):
                    msg = (
                        "Parameter count mismatch: condition has "
                        f"{len(positional_params)} positional placeholders, got {len(values)} values"
                    )
                    raise SQLBuilderError(msg)

                for index, value in enumerate(values):
                    param_dict[f"param_{index}"] = value

                condition = SQL(condition, param_dict)
            elif len(values) == 1 and not kwargs:
                if operator is not None:
                    return self._process_tuple_condition((condition, operator, values[0]))
                return self._process_tuple_condition((condition, values[0]))
            else:
                msg = f"Cannot bind parameters to condition without placeholders: {condition}"
                raise SQLBuilderError(msg)

        builder = cast("SQLBuilderProtocol", self)

        if isinstance(condition, str):
            return parse_condition_expression(condition)
        if isinstance(condition, (exp.Expression, exp.Condition)):
            return condition
        if isinstance(condition, tuple):
            return self._process_tuple_condition(condition)
        if has_parameter_builder(condition):
            column_expr_obj = cast("ColumnExpression", condition)
            expression_attr = cast("exp.Expression | None", column_expr_obj._expression)
            if expression_attr is None:
                msg = "Column expression is missing underlying sqlglot expression."
                raise SQLBuilderError(msg)
            return expression_attr
        if has_sqlglot_expression(condition):
            raw_expr = condition.sqlglot_expression
            if isinstance(raw_expr, exp.Expression):
                return builder._parameterize_expression(raw_expr)
            return parse_condition_expression(str(condition))
        if has_expression_and_sql(condition):
            expression_attr = condition.expression
            if isinstance(expression_attr, exp.Expression):
                self._merge_sql_object_parameters(condition)
                return expression_attr
            sql_text = condition.sql
            self._merge_sql_object_parameters(condition)
            return parse_condition_expression(sql_text)

        msg = f"Unsupported condition type: {type(condition).__name__}"
        raise SQLBuilderError(msg)

    def where(
        self,
        condition: Union[
            str, exp.Expression, exp.Condition, tuple[str, Any], tuple[str, str, Any], "ColumnExpression", SQL
        ],
        *values: Any,
        operator: str | None = None,
        **kwargs: Any,
    ) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        current_expr = builder.get_expression()
        if current_expr is None:
            msg = "Cannot add WHERE clause: expression is not initialized."
            raise SQLBuilderError(msg)

        if isinstance(current_expr, exp.Delete) and not current_expr.args.get("this"):
            msg = "WHERE clause requires a table to be set. Use from() to set the table first."
            raise SQLBuilderError(msg)

        where_expr = self._process_where_condition(condition, values, operator, kwargs)

        if isinstance(current_expr, (exp.Select, exp.Update, exp.Delete)):
            updated_expr = current_expr.where(where_expr, copy=False)
            builder.set_expression(updated_expr)
            return cast("Self", builder)
        msg = f"WHERE clause not supported for {type(current_expr).__name__}"
        raise SQLBuilderError(msg)

    def where_eq(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_eq)
        return self.where(condition)

    def where_neq(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_neq)
        return self.where(condition)

    def where_lt(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_lt)
        return self.where(condition)

    def where_lte(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_lte)
        return self.where(condition)

    def where_gt(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_gt)
        return self.where(condition)

    def where_gte(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_gte)
        return self.where(condition)

    def where_between(self, column: str | exp.Column, low: Any, high: Any) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        column_name = extract_column_name(column)
        low_param = builder._generate_unique_parameter_name(f"{column_name}_low")
        high_param = builder._generate_unique_parameter_name(f"{column_name}_high")
        _, low_param = builder.add_parameter(low, name=low_param)
        _, high_param = builder.add_parameter(high, name=high_param)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.between(exp.Placeholder(this=low_param), exp.Placeholder(this=high_param))
        return self.where(condition)

    def where_like(self, column: str | exp.Column, pattern: str, escape: str | None = None) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        column_name = extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(pattern, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if escape is not None:
            condition = exp.Like(
                this=col_expr, expression=exp.Placeholder(this=param_name), escape=exp.convert(str(escape))
            )
        else:
            condition = col_expr.like(exp.Placeholder(this=param_name))
        return self.where(condition)

    def where_not_like(self, column: str | exp.Column, pattern: str) -> Self:
        condition = self._create_parameterized_condition(column, pattern, _expr_not_like)
        return self.where(condition)

    def where_ilike(self, column: str | exp.Column, pattern: str) -> Self:
        condition = self._create_parameterized_condition(column, pattern, _expr_ilike)
        return self.where(condition)

    def where_is_null(self, column: str | exp.Column) -> Self:
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null())
        return self.where(condition)

    def where_is_not_null(self, column: str | exp.Column) -> Self:
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null()).not_()
        return self.where(condition)

    def where_in(self, column: str | exp.Column, values: Any) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_parameter_builder(values) or isinstance(values, (exp.Expression, str)):
            subquery_exp = self._normalize_subquery_expression(values, builder)
            return self.where(exp.In(this=col_expr, expressions=[subquery_exp]))

        condition = self._handle_in_operator(col_expr, values, extract_column_name(column))
        return self.where(condition)

    def where_not_in(self, column: str | exp.Column, values: Any) -> Self:
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition = self._handle_not_in_operator(col_expr, values, extract_column_name(column))
        return self.where(condition)

    def where_any(self, column: str | exp.Column, subquery: Any) -> Self:
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        column_name = extract_column_name(column)
        condition = self._create_any_condition(col_expr, subquery, column_name)
        return self.where(condition)

    def where_not_any(self, column: str | exp.Column, subquery: Any) -> Self:
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        column_name = extract_column_name(column)
        condition = self._create_not_any_condition(col_expr, subquery, column_name)
        return self.where(condition)

    def where_exists(self, subquery: Any) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        subquery_expr = self._normalize_subquery_expression(subquery, builder)
        return self.where(exp.Exists(this=subquery_expr))

    def where_not_exists(self, subquery: Any) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        subquery_expr = self._normalize_subquery_expression(subquery, builder)
        return self.where(exp.Not(this=exp.Exists(this=subquery_expr)))

    def where_like_any(self, column: str | exp.Column, patterns: list[str]) -> Self:
        conditions = [self._create_parameterized_condition(column, pattern, _expr_like_method) for pattern in patterns]
        or_condition = self._create_or_expression(conditions)
        return self.where(or_condition)

    def or_where_eq(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_eq)
        return self._combine_with_or(condition)

    def or_where_neq(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_neq)
        return self._combine_with_or(condition)

    def or_where_lt(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_lt)
        return self._combine_with_or(condition)

    def or_where_lte(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_lte)
        return self._combine_with_or(condition)

    def or_where_gt(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_gt)
        return self._combine_with_or(condition)

    def or_where_gte(self, column: str | exp.Column, value: Any) -> Self:
        condition = self._create_parameterized_condition(column, value, _expr_gte)
        return self._combine_with_or(condition)

    def or_where_between(self, column: str | exp.Column, low: Any, high: Any) -> Self:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition = self._handle_between_operator(column_expr, (low, high), extract_column_name(column))
        return self._combine_with_or(condition)

    def or_where_like(self, column: str | exp.Column, pattern: str, escape: str | None = None) -> Self:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        builder = cast("SQLBuilderProtocol", self)
        column_name = extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(pattern, name=param_name)
        placeholder = exp.Placeholder(this=param_name)
        if escape is not None:
            condition = exp.Like(this=column_expr, expression=placeholder, escape=exp.convert(str(escape)))
        else:
            condition = column_expr.like(placeholder)
        return self._combine_with_or(cast("exp.Expression", condition))

    def or_where_not_like(self, column: str | exp.Column, pattern: str) -> Self:
        condition = self._create_parameterized_condition(column, pattern, _expr_like_not)
        return self._combine_with_or(condition)

    def or_where_ilike(self, column: str | exp.Column, pattern: str) -> Self:
        condition = self._create_parameterized_condition(column, pattern, _expr_ilike)
        return self._combine_with_or(condition)

    def or_where_is_null(self, column: str | exp.Column) -> Self:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = column_expr.is_(exp.null())
        return self._combine_with_or(condition)

    def or_where_is_not_null(self, column: str | exp.Column) -> Self:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = column_expr.is_(exp.null()).not_()
        return self._combine_with_or(condition)

    def or_where_null(self, column: str | exp.Column) -> Self:
        return self.or_where_is_null(column)

    def or_where_not_null(self, column: str | exp.Column) -> Self:
        return self.or_where_is_not_null(column)

    def or_where_in(self, column: str | exp.Column, values: Any) -> Self:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition = self._handle_in_operator(column_expr, values, extract_column_name(column))
        return self._combine_with_or(condition)

    def or_where_not_in(self, column: str | exp.Column, values: Any) -> Self:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition = self._handle_not_in_operator(column_expr, values, extract_column_name(column))
        return self._combine_with_or(condition)

    def or_where_any(self, column: str | exp.Column, subquery: Any) -> Self:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition = self._create_any_condition(column_expr, subquery, extract_column_name(column))
        return self._combine_with_or(condition)

    def or_where_not_any(self, column: str | exp.Column, subquery: Any) -> Self:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition = self._create_not_any_condition(column_expr, subquery, extract_column_name(column))
        return self._combine_with_or(condition)

    def or_where_exists(self, subquery: Any) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        subquery_expr = self._normalize_subquery_expression(subquery, builder)
        condition = exp.Exists(this=subquery_expr)
        return self._combine_with_or(condition)

    def or_where_not_exists(self, subquery: Any) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        subquery_expr = self._normalize_subquery_expression(subquery, builder)
        condition = exp.Not(this=exp.Exists(this=subquery_expr))
        return self._combine_with_or(condition)

    def where_or(self, *conditions: str | tuple[str, Any] | tuple[str, str, Any] | exp.Expression) -> Self:
        if not conditions:
            msg = "where_or() requires at least one condition"
            raise SQLBuilderError(msg)

        builder = cast("SQLBuilderProtocol", self)
        if builder.get_expression() is None:
            msg = "Cannot add WHERE OR clause: expression is not initialized."
            raise SQLBuilderError(msg)

        processed_conditions = [self._process_where_condition(condition, (), None, {}) for condition in conditions]
        or_condition = self._create_or_expression(processed_conditions)
        return self.where(or_condition)

    def or_where(
        self,
        condition: Union[
            str, exp.Expression, exp.Condition, tuple[str, Any], tuple[str, str, Any], "ColumnExpression", SQL
        ],
        *values: Any,
        operator: str | None = None,
        **kwargs: Any,
    ) -> Self:
        or_condition = self._process_where_condition(condition, values, operator, kwargs)
        return self._combine_with_or(or_condition)


@trait
class HavingClauseMixin:
    __slots__ = ()

    def having(self, condition: str | exp.Expression | exp.Condition | tuple[str, Any] | tuple[str, str, Any]) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        current_expr = builder.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Select):
            return cast("Self", builder)

        if isinstance(condition, tuple):
            where_mixin = cast("WhereClauseMixin", self)
            having_expr = where_mixin._process_tuple_condition(condition)
        else:
            having_expr = parse_condition_expression(condition)

        builder.set_expression(current_expr.having(having_expr, copy=False))
        return cast("Self", builder)


@trait
class PivotClauseMixin:
    __slots__ = ()

    def pivot(
        self,
        aggregate_function: str | exp.Expression,
        aggregate_column: str | exp.Expression,
        pivot_column: str | exp.Expression,
        pivot_values: list[str | int | float | exp.Expression],
        alias: str | None = None,
    ) -> "Select":
        builder = cast("SQLBuilderProtocol", self)
        current_expr = builder.get_expression()
        if not isinstance(current_expr, exp.Select):
            msg = "Pivot can only be applied to a Select expression managed by SelectBuilder."
            raise TypeError(msg)

        agg_name = aggregate_function if isinstance(aggregate_function, str) else aggregate_function.name
        agg_column = exp.column(aggregate_column) if isinstance(aggregate_column, str) else aggregate_column
        pivot_col_expr = exp.column(pivot_column) if isinstance(pivot_column, str) else pivot_column

        pivot_agg_expr = exp.func(agg_name, agg_column)

        pivot_value_exprs: list[exp.Expression] = []
        for raw_value in pivot_values:
            if isinstance(raw_value, exp.Expression):
                pivot_value_exprs.append(raw_value)
            elif isinstance(raw_value, (str, int, float)):
                pivot_value_exprs.append(exp.convert(raw_value))
            else:
                pivot_value_exprs.append(exp.convert(str(raw_value)))

        in_expr = exp.In(this=pivot_col_expr, expressions=pivot_value_exprs)
        pivot_node = exp.Pivot(expressions=[pivot_agg_expr], fields=[in_expr], unpivot=False)

        if alias:
            pivot_node.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))

        from_clause = current_expr.args.get("from")
        if from_clause and isinstance(from_clause, exp.From):
            table = from_clause.this
            if isinstance(table, exp.Table):
                existing = table.args.get("pivots", [])
                existing.append(pivot_node)
                table.set("pivots", existing)

        return cast("Select", self)


@trait
class UnpivotClauseMixin:
    __slots__ = ()

    def unpivot(
        self,
        value_column_name: str,
        name_column_name: str,
        columns_to_unpivot: list[str | exp.Expression],
        alias: str | None = None,
    ) -> "Select":
        builder = cast("SQLBuilderProtocol", self)
        current_expr = builder.get_expression()
        if not isinstance(current_expr, exp.Select):
            msg = "Unpivot can only be applied to a Select expression managed by Select."
            raise TypeError(msg)

        value_identifier = exp.to_identifier(value_column_name)
        name_identifier = exp.to_identifier(name_column_name)

        unpivot_columns: list[exp.Expression] = []
        for column in columns_to_unpivot:
            if isinstance(column, exp.Expression):
                unpivot_columns.append(column)
            elif isinstance(column, str):
                unpivot_columns.append(exp.column(column))
            else:
                unpivot_columns.append(exp.column(str(column)))

        in_expr = exp.In(this=name_identifier, expressions=unpivot_columns)
        unpivot_node = exp.Pivot(expressions=[value_identifier], fields=[in_expr], unpivot=True)

        if alias:
            unpivot_node.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))

        from_clause = current_expr.args.get("from")
        if from_clause and isinstance(from_clause, exp.From):
            table = from_clause.this
            if isinstance(table, exp.Table):
                existing = table.args.get("pivots", [])
                existing.append(unpivot_node)
                table.set("pivots", existing)

        return cast("Select", self)


@trait
class CommonTableExpressionMixin:
    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    _with_ctes: Any
    dialect: Any

    def with_(self, name: str, query: Any | str, recursive: bool = False, columns: list[str] | None = None) -> Self:
        """Add a CTE via the WITH clause.

        When ``query`` is another builder we reuse its expression, merge parameters with unique names, and let sqlglot handle the actual CTE wrapping to avoid duplicating ``_with_ctes`` state.
        """
        builder = cast("QueryBuilder", self)
        expression = builder.get_expression()
        if expression is None:
            msg = "Cannot add WITH clause: expression not initialized."
            raise SQLBuilderError(msg)

        if not isinstance(expression, (exp.Select, exp.Insert, exp.Update, exp.Delete)):
            msg = f"Cannot add WITH clause to {type(expression).__name__} expression."
            raise SQLBuilderError(msg)

        cte_select: exp.Expression | None
        if isinstance(query, str):
            cte_select = exp.maybe_parse(query, dialect=self.dialect)
        elif isinstance(query, exp.Expression):
            cte_select = query
        else:
            cte_select = query.get_expression()
            if cte_select is None:
                msg = f"Could not get expression from builder: {query}"
                raise SQLBuilderError(msg)

            built_query = query.to_statement()
            parameters = built_query.parameters
            if isinstance(parameters, dict):
                param_mapping: dict[str, str] = {}
                for param_name, param_value in parameters.items():
                    unique_name = builder._generate_unique_parameter_name(f"{name}_{param_name}")
                    param_mapping[param_name] = unique_name
                    builder.add_parameter(param_value, name=unique_name)
                cte_select = builder._update_placeholders_in_expression(cte_select, param_mapping)
            elif isinstance(parameters, (list, tuple)):
                for param_value in parameters:
                    builder.add_parameter(param_value)
            elif parameters is not None:
                builder.add_parameter(parameters)

        if cte_select is None:
            msg = f"Could not parse CTE query: {query}"
            raise SQLBuilderError(msg)

        if isinstance(expression, (exp.Select, exp.Insert, exp.Update)):
            updated = expression.with_(name, as_=cte_select.copy(), recursive=recursive, copy=True)
            builder.set_expression(updated)

        return cast("Self", builder)


@trait
class SetOperationMixin:
    __slots__ = ()

    def get_expression(self) -> exp.Expression | None: ...
    def set_expression(self, expression: exp.Expression) -> None: ...
    def set_parameters(self, parameters: dict[str, Any]) -> None: ...

    dialect: Any = None

    def union(self, other: Any, all_: bool = False) -> Self:
        return self._combine_with_other(other, operator="union", distinct=not all_)

    def intersect(self, other: Any) -> Self:
        return self._combine_with_other(other, operator="intersect", distinct=True)

    def except_(self, other: Any) -> Self:
        return self._combine_with_other(other, operator="except", distinct=True)

    def _combine_with_other(self, other: Any, *, operator: str, distinct: bool) -> Self:
        builder = cast("QueryBuilder", self)

        if not isinstance(other, QueryBuilder):
            msg = "Set operations require another SQLSpec query builder."
            raise SQLBuilderError(msg)

        other_builder = other
        left_expr = builder._build_final_expression(copy=True)
        right_expr = other_builder._build_final_expression(copy=True)

        merged_parameters: dict[str, Any] = dict(builder.parameters)
        rename_map: dict[str, str] = {}
        for param_name, param_value in other_builder.parameters.items():
            target_name = param_name
            if target_name in merged_parameters:
                counter = 1
                while True:
                    candidate = f"{param_name}_right_{counter}"
                    if candidate not in merged_parameters:
                        target_name = candidate
                        break
                    counter += 1
                rename_map[param_name] = target_name
            merged_parameters[target_name] = param_value

        if rename_map:
            right_expr = builder._update_placeholders_in_expression(right_expr, rename_map)

        combined: exp.Expression
        if operator == "union":
            combined = exp.union(left_expr, right_expr, distinct=distinct)
        elif operator == "intersect":
            combined = exp.intersect(left_expr, right_expr, distinct=distinct)
        elif operator == "except":
            combined = exp.except_(left_expr, right_expr)
        else:  # pragma: no cover - defensive
            msg = f"Unsupported set operation: {operator}"
            raise SQLBuilderError(msg)

        new_builder = builder._spawn_like_self()
        new_builder.set_expression(combined)
        new_builder.set_parameters(merged_parameters)
        return cast("Self", new_builder)


TABLE_HINT_PATTERN: Final[str] = r"\b{}\b(\s+AS\s+\w+)?"


def _parse_hint_expression(hint: Any, dialect: "DialectType | str | None") -> exp.Expression:
    try:
        hint_str = str(hint)
        hint_expr: exp.Expression | None = exp.maybe_parse(hint_str, dialect=dialect)
        return hint_expr or exp.Anonymous(this=hint_str)
    except Exception:
        return exp.Anonymous(this=str(hint))


class _TableHintReplacer:
    __slots__ = ("_hint", "_table")

    def __init__(self, hint: str, table: str) -> None:
        self._hint = hint
        self._table = table

    def __call__(self, match: "re.Match[str]") -> str:
        alias_part = match.group(1) or ""
        return f"/*+ {self._hint} */ {self._table}{alias_part}"


class Select(
    QueryBuilder,
    WhereClauseMixin,
    OrderByClauseMixin,
    LimitOffsetClauseMixin,
    SelectClauseMixin,
    JoinClauseMixin,
    HavingClauseMixin,
    SetOperationMixin,
    CommonTableExpressionMixin,
    PivotClauseMixin,
    UnpivotClauseMixin,
    ExplainMixin,
):
    """Builder for SELECT queries.

    Provides a fluent interface for constructing SQL SELECT statements
    with parameter binding and validation.

    Example:
        >>> class User(BaseModel):
        ...     id: int
        ...     name: str
        >>> builder = Select("id", "name").from_("users")
        >>> result = driver.execute(builder)
    """

    __slots__ = ("_hints",)
    _expression: exp.Expression | None

    def __init__(self, *columns: str, **kwargs: Any) -> None:
        """Initialize SELECT with optional columns.

        Args:
            *columns: Column names to select (e.g., "id", "name", "u.email")
            **kwargs: Additional QueryBuilder arguments (dialect, schema, etc.)

        Examples:
            Select("id", "name")  # Shorthand for Select().select("id", "name")
            Select()              # Same as Select() - start empty
        """
        (dialect, schema, enable_optimization, optimize_joins, optimize_predicates, simplify_expressions) = (
            self._parse_query_builder_kwargs(kwargs)
        )
        super().__init__(
            dialect=dialect,
            schema=schema,
            enable_optimization=enable_optimization,
            optimize_joins=optimize_joins,
            optimize_predicates=optimize_predicates,
            simplify_expressions=simplify_expressions,
        )

        self._hints: list[dict[str, object]] = []

        self._initialize_expression()

        if columns:
            self.select(*columns)

    @property
    def _expected_result_type(self) -> "type[SQLResult]":
        """Get the expected result type for SELECT operations.

        Returns:
            type: The SelectResult type.
        """
        return SQLResult

    def _create_base_expression(self) -> exp.Select:
        """Create base SELECT expression."""
        if self._expression is None or not isinstance(self._expression, exp.Select):
            self._expression = exp.Select()
        return self._expression

    def with_hint(
        self, hint: "str", *, location: "str" = "statement", table: "str | None" = None, dialect: "str | None" = None
    ) -> "Self":
        """Attach an optimizer or dialect-specific hint to the query.

        Args:
            hint: The raw hint string (e.g., 'INDEX(users idx_users_name)').
            location: Where to apply the hint ('statement', 'table').
            table: Table name if the hint is for a specific table.
            dialect: Restrict the hint to a specific dialect (optional).

        Returns:
            The current builder instance for method chaining.
        """
        self._hints.append({"hint": hint, "location": location, "table": table, "dialect": dialect})
        return self

    def build(self, dialect: "DialectType" = None) -> "BuiltQuery":
        """Builds the SQL query string and parameters with hint injection.

        Args:
            dialect: Optional dialect override for SQL generation.

        Returns:
            BuiltQuery: A dataclass containing the SQL string and parameters.
        """
        safe_query = super().build(dialect=dialect)

        if not self._hints:
            return safe_query

        target_dialect = str(dialect) if dialect else self.dialect_name

        modified_expr = self._expression or self._create_base_expression()

        if isinstance(modified_expr, exp.Select):
            statement_hints = [h["hint"] for h in self._hints if h.get("location") == "statement"]
            if statement_hints:
                hint_expressions: list[exp.Expression] = [
                    _parse_hint_expression(hint, target_dialect) for hint in statement_hints
                ]

                if hint_expressions:
                    modified_expr.set("hint", exp.Hint(expressions=hint_expressions))

        modified_sql = modified_expr.sql(dialect=target_dialect, pretty=True)

        for hint_dict in self._hints:
            if hint_dict.get("location") == "table" and hint_dict.get("table"):
                table = str(hint_dict["table"])
                hint = str(hint_dict["hint"])
                pattern = TABLE_HINT_PATTERN.format(re.escape(table))

                modified_sql = re.sub(
                    pattern, _TableHintReplacer(hint, table), modified_sql, count=1, flags=re.IGNORECASE
                )

        return BuiltQuery(sql=modified_sql, parameters=safe_query.parameters, dialect=safe_query.dialect)

    def _validate_select_expression(self) -> None:
        """Validate that current expression is a valid SELECT statement.

        Raises:
            SQLBuilderError: If expression is None or not a SELECT statement
        """
        if self._expression is None or not isinstance(self._expression, exp.Select):
            msg = "Locking clauses can only be applied to SELECT statements"
            raise SQLBuilderError(msg)

    def _validate_lock_parameters(self, skip_locked: bool, nowait: bool) -> None:
        """Validate locking parameters for conflicting options.

        Args:
            skip_locked: Whether SKIP LOCKED option is enabled
            nowait: Whether NOWAIT option is enabled

        Raises:
            SQLBuilderError: If both skip_locked and nowait are True
        """
        if skip_locked and nowait:
            msg = "Cannot use both skip_locked and nowait"
            raise SQLBuilderError(msg)

    def for_update(
        self, *, skip_locked: bool = False, nowait: bool = False, of: "str | list[str] | None" = None
    ) -> "Self":
        """Add FOR UPDATE clause to SELECT statement for row-level locking.

        Args:
            skip_locked: Skip rows that are already locked (SKIP LOCKED)
            nowait: Return immediately if row is locked (NOWAIT)
            of: Table names/aliases to lock (FOR UPDATE OF table)

        Returns:
            Self for method chaining
        """
        self._validate_select_expression()
        self._validate_lock_parameters(skip_locked, nowait)

        assert self._expression is not None
        select_expr = cast("exp.Select", self._expression)

        lock_args: dict[str, Any] = {"update": True}

        if skip_locked:
            lock_args["wait"] = False
        elif nowait:
            lock_args["wait"] = True

        if of:
            tables = [of] if isinstance(of, str) else of
            lock_args["expressions"] = [exp.to_identifier(str(t), quoted=is_explicitly_quoted(t)) for t in tables]
            self._lock_targets_quoted = any(is_explicitly_quoted(t) for t in tables)
        else:
            self._lock_targets_quoted = False

        lock = exp.Lock(**lock_args)

        current_locks = select_expr.args.get("locks", [])
        current_locks.append(lock)
        select_expr.set("locks", current_locks)

        return self

    def for_share(
        self, *, skip_locked: bool = False, nowait: bool = False, of: "str | list[str] | None" = None
    ) -> "Self":
        """Add FOR SHARE clause for shared row-level locking.

        Args:
            skip_locked: Skip rows that are already locked (SKIP LOCKED)
            nowait: Return immediately if row is locked (NOWAIT)
            of: Table names/aliases to lock (FOR SHARE OF table)

        Returns:
            Self for method chaining
        """
        self._validate_select_expression()
        self._validate_lock_parameters(skip_locked, nowait)

        assert self._expression is not None
        select_expr = cast("exp.Select", self._expression)

        lock_args: dict[str, Any] = {"update": False}

        if skip_locked:
            lock_args["wait"] = False
        elif nowait:
            lock_args["wait"] = True

        if of:
            tables = [of] if isinstance(of, str) else of
            lock_args["expressions"] = [exp.to_identifier(str(t), quoted=is_explicitly_quoted(t)) for t in tables]
            self._lock_targets_quoted = any(is_explicitly_quoted(t) for t in tables)
        else:
            self._lock_targets_quoted = False

        lock = exp.Lock(**lock_args)

        current_locks = select_expr.args.get("locks", [])
        current_locks.append(lock)
        select_expr.set("locks", current_locks)

        return self

    def for_key_share(self) -> "Self":
        """Add FOR KEY SHARE clause (PostgreSQL-specific).

        FOR KEY SHARE is like FOR SHARE, but the lock is weaker:
        SELECT FOR UPDATE is blocked, but not SELECT FOR NO KEY UPDATE.

        Returns:
            Self for method chaining
        """
        self._validate_select_expression()

        assert self._expression is not None
        select_expr = cast("exp.Select", self._expression)

        lock = exp.Lock(update=False, key=True)

        current_locks = select_expr.args.get("locks", [])
        current_locks.append(lock)
        select_expr.set("locks", current_locks)

        return self

    def for_no_key_update(self) -> "Self":
        """Add FOR NO KEY UPDATE clause (PostgreSQL-specific).

        FOR NO KEY UPDATE is like FOR UPDATE, but the lock is weaker:
        it does not block SELECT FOR KEY SHARE commands that attempt to
        acquire a share lock on the same rows.

        Returns:
            Self for method chaining
        """
        self._validate_select_expression()

        assert self._expression is not None
        select_expr = cast("exp.Select", self._expression)

        lock = exp.Lock(update=True, key=False)

        current_locks = select_expr.args.get("locks", [])
        current_locks.append(lock)
        select_expr.set("locks", current_locks)

        return self
