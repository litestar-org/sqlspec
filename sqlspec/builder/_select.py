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
from sqlspec.builder._join import JoinClauseMixin, _attach_as_of_version
from sqlspec.builder._parsing_utils import (
    _PARAMETER_VALIDATOR,
    _coerce_column,
    extract_column_name,
    extract_expression,
    extract_sql_object_expression,
    parse_column_expression,
    parse_condition_expression,
    parse_order_expression,
    parse_table_expression,
    to_expression,
)
from sqlspec.core import SQL, ParameterStyle, SQLResult
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import (
    has_expression_and_sql,
    has_parameter_builder,
    has_sqlglot_expression,
    is_expression,
    is_iterable_parameters,
)

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

BETWEEN_BOUND_COUNT = 2
PAIR_LENGTH = 2
TRIPLE_LENGTH = 3


def is_explicitly_quoted(identifier: Any) -> bool:
    """Detect if identifier was provided with explicit quotes."""
    if not isinstance(identifier, str):
        return False
    stripped = identifier.strip()
    return (stripped.startswith('"') and stripped.endswith('"')) or (
        stripped.startswith("`") and stripped.endswith("`")
    )


def _expr_eq(col: "exp.Expr", placeholder: "exp.Placeholder") -> "exp.Expr":
    return exp.EQ(this=col, expression=placeholder)


def _expr_neq(col: "exp.Expr", placeholder: "exp.Placeholder") -> "exp.Expr":
    return exp.NEQ(this=col, expression=placeholder)


def _expr_gt(col: "exp.Expr", placeholder: "exp.Placeholder") -> "exp.Expr":
    return exp.GT(this=col, expression=placeholder)


def _expr_gte(col: "exp.Expr", placeholder: "exp.Placeholder") -> "exp.Expr":
    return exp.GTE(this=col, expression=placeholder)


def _expr_lt(col: "exp.Expr", placeholder: "exp.Placeholder") -> "exp.Expr":
    return exp.LT(this=col, expression=placeholder)


def _expr_lte(col: "exp.Expr", placeholder: "exp.Placeholder") -> "exp.Expr":
    return exp.LTE(this=col, expression=placeholder)


def _expr_like_exp(col: "exp.Expr", placeholder: "exp.Placeholder") -> "exp.Expr":
    return exp.Like(this=col, expression=placeholder)


def _expr_like_method(col: "exp.Expr", placeholder: "exp.Placeholder") -> "exp.Expr":
    return cast("exp.Expr", col.like(placeholder))


def _expr_not_like(col: "exp.Expr", placeholder: "exp.Placeholder") -> "exp.Expr":
    return exp.Not(this=exp.Like(this=col, expression=placeholder))


def _expr_ilike(col: "exp.Expr", placeholder: "exp.Placeholder") -> "exp.Expr":
    return cast("exp.Expr", col.ilike(placeholder))


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

    def __init__(self, *ifs: exp.Expr, default: exp.Expr | None = None) -> None:
        self.conditions = list(ifs)
        self.default = default

    def when(self, condition: str | exp.Expr, result: Any) -> "Case":
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

    def __call__(self, subquery: Any) -> exp.Expr:
        if isinstance(subquery, exp.Expr):
            subquery_expr = subquery
        elif has_parameter_builder(subquery):
            subquery_expr = cast("QueryBuilder", subquery)._build_final_expression(copy=True)
        else:
            dialect = subquery.dialect if isinstance(subquery, (QueryBuilder, BuiltQuery)) else None
            parsed_expr: exp.Expression | None = exp.maybe_parse(str(subquery), dialect=dialect)
            if parsed_expr is None:
                msg = f"Could not convert subquery to expression: {subquery}"
                raise SQLBuilderError(msg)
            subquery_expr = parsed_expr

        if self._operation == "exists":
            return exp.Exists(this=subquery_expr)
        if self._operation == "in":
            return exp.In(this=exp.Var(this=""), expressions=[subquery_expr])
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
        self._function_args: list[exp.Expr] = [to_expression(arg) for arg in function_args]
        self._partition_by: list[exp.Expr] = []
        self._order_by: list[exp.Ordered] = []

    def __call__(self, *function_args: Any) -> "WindowFunctionBuilder":
        self._function_args = [to_expression(arg) for arg in function_args]
        return self

    def partition_by(self, *columns: str | exp.Expr) -> "WindowFunctionBuilder":
        self._partition_by = [_coerce_column(column) for column in columns]
        return self

    def order_by(self, *columns: str | exp.Expr) -> "WindowFunctionBuilder":
        ordered_columns: list[exp.Ordered] = []
        for column in columns:
            if isinstance(column, str):
                ordered_columns.append(exp.column(column).asc())
            elif isinstance(column, exp.Ordered):
                ordered_columns.append(column)
            else:
                ordered_columns.append(exp.Ordered(this=column, desc=False, nulls_first=False))
        self._order_by = ordered_columns
        return self

    def _build_function_expression(self) -> exp.Expr:
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

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def select(self, *columns: Union[str, exp.Expr, "Column", "FunctionColumn", SQL, Case]) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        select_expr = _ensure_select_expression(builder, error_message="Cannot add columns to non-SELECT expression.")
        for column in columns:
            column_expr = column.expression if isinstance(column, Case) else parse_column_expression(column, builder)
            select_expr = select_expr.select(column_expr, copy=False)
        self.set_expression(select_expr)
        return cast("Self", builder)

    def select_only(self, *columns: Union[str, exp.Expr, "Column", "FunctionColumn", SQL, Case]) -> Self:
        """Replace currently selected columns with new ones."""
        builder = cast("SQLBuilderProtocol", self)
        select_expr = _ensure_select_expression(builder, error_message="Cannot add columns to non-SELECT expression.")
        # Clear existing expressions
        select_expr.set("expressions", [])
        for column in columns:
            column_expr = column.expression if isinstance(column, Case) else parse_column_expression(column, builder)
            select_expr = select_expr.select(column_expr, copy=False)
        self.set_expression(select_expr)
        return cast("Self", builder)

    def distinct(self, *columns: Union[str, exp.Expr, "Column", "FunctionColumn", SQL]) -> Self:
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
        table: str | exp.Expr | Any,
        alias: str | None = None,
        as_of: Any | None = None,
        as_of_type: str | None = None,
    ) -> Self:
        """Set the FROM clause and optionally attach temporal versioning.

        ``as_of`` copies the resolved table expression, normalizes aliases, and adds an ``exp.Version`` so sqlglot's generator emits dialect-specific time-travel SQL.
        """
        builder = cast("SQLBuilderProtocol", self)
        select_expr = _ensure_select_expression(builder, error_message="FROM clause only valid for SELECT.")
        from_expr: exp.Expr

        if isinstance(table, str):
            from_expr = parse_table_expression(table, alias, dialect=builder.dialect)
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
                subquery_copy = base_builder._update_placeholders(subquery_copy, param_mapping)

            wrapped_subquery = exp.paren(subquery_copy)
            from_expr = exp.alias_(wrapped_subquery, alias) if alias else wrapped_subquery
        else:
            from_expr = table

        if as_of is not None:
            from_expr = _attach_as_of_version(from_expr, alias, as_of, as_of_type)

        builder.set_expression(select_expr.from_(from_expr, copy=False))
        return cast("Self", builder)

    def group_by(self, *columns: str | exp.Expr) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        select_expr = builder.get_expression()
        if select_expr is None or not isinstance(select_expr, exp.Select):
            return cast("Self", builder)

        for column in columns:
            column_expr = _coerce_column(column)
            select_expr = select_expr.group_by(column_expr, copy=False)
        builder.set_expression(select_expr)
        return cast("Self", builder)

    def group_by_rollup(self, *columns: str | exp.Expr) -> Self:
        column_exprs = [_coerce_column(column) for column in columns]
        rollup_expr = exp.Rollup(expressions=column_exprs)
        return self.group_by(rollup_expr)

    def group_by_cube(self, *columns: str | exp.Expr) -> Self:
        column_exprs = [_coerce_column(column) for column in columns]
        cube_expr = exp.Cube(expressions=column_exprs)
        return self.group_by(cube_expr)

    def group_by_grouping_sets(self, *column_sets: tuple[str, ...] | list[str]) -> Self:
        grouping_sets = [
            exp.Tuple(expressions=[_coerce_column(col) for col in column_set]) for column_set in column_sets
        ]
        grouping_expr = exp.GroupingSets(expressions=grouping_sets)
        return self.group_by(grouping_expr)


@trait
class OrderByClauseMixin:
    __slots__ = ()

    _expression: exp.Expr | None

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
                if isinstance(extracted_item, exp.Alias):
                    alias_name = (extracted_item.alias or "").lower()
                    if alias_name in {"asc", "desc"}:
                        extracted_item = exp.Ordered(
                            this=extracted_item.this, desc=alias_name == "desc", nulls_first=False
                        )
                order_item = (
                    extracted_item.desc() if desc and not isinstance(extracted_item, exp.Ordered) else extracted_item
                )
            current_expr = current_expr.order_by(order_item, copy=False)
        builder.set_expression(current_expr)
        return cast("Self", builder)


@trait
class LimitOffsetClauseMixin:
    __slots__ = ()

    _expression: exp.Expr | None

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

    _expression: exp.Expr | None

    def returning(self, *columns: Union[str, exp.Expr, "Column", "ExpressionWrapper", Case]) -> Self:
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

    def _merge_parameters(self, sql_obj: Any) -> None:
        builder = cast("SQLBuilderProtocol", self)
        builder._merge_parameters(sql_obj)

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

    def _create_parameterized_condition(
        self, column: str | exp.Column, value: Any, condition_factory: "Callable[[exp.Expr, exp.Placeholder], exp.Expr]"
    ) -> exp.Expr:
        builder = cast("SQLBuilderProtocol", self)
        column_name = extract_column_name(column)
        param_name = builder._next_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        placeholder = exp.Placeholder(this=param_name)
        return condition_factory(col_expr, placeholder)

    def _current_where_clause(self) -> exp.Where | None:
        builder = cast("SQLBuilderProtocol", self)
        expression = builder.get_expression()
        if isinstance(expression, (exp.Select, exp.Update, exp.Delete)):
            where_clause = expression.args.get("where")
            if isinstance(where_clause, exp.Where):
                return where_clause
        return None

    def _combine_with_or(self, new_condition: exp.Expr) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        expression = builder.get_expression()
        if expression is None or not isinstance(expression, (exp.Select, exp.Update, exp.Delete)):
            msg = "OR WHERE clause not supported for current expression. Use where() first."
            raise SQLBuilderError(msg)

        where_clause = self._current_where_clause()
        if where_clause is None or where_clause.this is None:
            msg = "Cannot add OR WHERE clause: no existing WHERE clause found. Use where() before or_where()."
            raise SQLBuilderError(msg)

        combined_condition = exp.Or(this=where_clause.this, expression=new_condition)
        where_clause.set("this", combined_condition)
        builder.set_expression(expression)
        return cast("Self", builder)

    def _handle_in_operator(self, column_exp: exp.Expr, value: Any, column_name: str = "column") -> exp.Expr:
        builder = cast("SQLBuilderProtocol", self)
        if has_parameter_builder(value) or isinstance(value, exp.Expr):
            subquery_expr = self._normalize_subquery_expression(value, builder)
            return exp.In(this=column_exp, expressions=[subquery_expr])
        if is_iterable_parameters(value):
            placeholders = []
            for index, element in enumerate(value):
                name_seed = column_name if len(value) == 1 else f"{column_name}_{index + 1}"
                param_name = builder._next_parameter_name(name_seed)
                _, param_name = builder.add_parameter(element, name=param_name)
                placeholders.append(exp.Placeholder(this=param_name))
            return exp.In(this=column_exp, expressions=placeholders)

        param_name = builder._next_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        return exp.In(this=column_exp, expressions=[exp.Placeholder(this=param_name)])

    def _handle_not_in_operator(self, column_exp: exp.Expr, value: Any, column_name: str = "column") -> exp.Expr:
        builder = cast("SQLBuilderProtocol", self)
        if has_parameter_builder(value) or isinstance(value, exp.Expr):
            subquery_expr = self._normalize_subquery_expression(value, builder)
            return exp.Not(this=exp.In(this=column_exp, expressions=[subquery_expr]))
        if is_iterable_parameters(value):
            placeholders = []
            for index, element in enumerate(value):
                name_seed = column_name if len(value) == 1 else f"{column_name}_{index + 1}"
                param_name = builder._next_parameter_name(name_seed)
                _, param_name = builder.add_parameter(element, name=param_name)
                placeholders.append(exp.Placeholder(this=param_name))
            return exp.Not(this=exp.In(this=column_exp, expressions=placeholders))

        param_name = builder._next_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        return exp.Not(this=exp.In(this=column_exp, expressions=[exp.Placeholder(this=param_name)]))

    def _handle_is_operator(self, column_exp: exp.Expr, value: Any) -> exp.Expr:
        value_expr = exp.Null() if value is None else exp.convert(value)
        return exp.Is(this=column_exp, expression=value_expr)

    def _handle_is_not_operator(self, column_exp: exp.Expr, value: Any) -> exp.Expr:
        value_expr = exp.Null() if value is None else exp.convert(value)
        return exp.Not(this=exp.Is(this=column_exp, expression=value_expr))

    def _handle_between_operator(self, column_exp: exp.Expr, value: Any, column_name: str = "column") -> exp.Expr:
        if is_iterable_parameters(value) and len(value) == BETWEEN_BOUND_COUNT:
            builder = cast("SQLBuilderProtocol", self)
            low, high = value
            low_param = builder._next_parameter_name(f"{column_name}_low")
            high_param = builder._next_parameter_name(f"{column_name}_high")
            _, low_param = builder.add_parameter(low, name=low_param)
            _, high_param = builder.add_parameter(high, name=high_param)
            return exp.Between(
                this=column_exp, low=exp.Placeholder(this=low_param), high=exp.Placeholder(this=high_param)
            )
        msg = f"BETWEEN operator requires a tuple of two values, got {type(value).__name__}"
        raise SQLBuilderError(msg)

    def _handle_not_between_operator(self, column_exp: exp.Expr, value: Any, column_name: str = "column") -> exp.Expr:
        if is_iterable_parameters(value) and len(value) == BETWEEN_BOUND_COUNT:
            builder = cast("SQLBuilderProtocol", self)
            low, high = value
            low_param = builder._next_parameter_name(f"{column_name}_low")
            high_param = builder._next_parameter_name(f"{column_name}_high")
            _, low_param = builder.add_parameter(low, name=low_param)
            _, high_param = builder.add_parameter(high, name=high_param)
            return exp.Not(
                this=exp.Between(
                    this=column_exp, low=exp.Placeholder(this=low_param), high=exp.Placeholder(this=high_param)
                )
            )
        msg = f"NOT BETWEEN operator requires a tuple of two values, got {type(value).__name__}"
        raise SQLBuilderError(msg)

    def _any_comparison(self, column_expr: exp.Expr, values: Any, column_name: str, *, negate: bool) -> exp.Expr:
        builder = cast("SQLBuilderProtocol", self)
        comparison = exp.NEQ if negate else exp.EQ
        error_context = "WHERE NOT ANY" if negate else "WHERE ANY"
        parameter_suffix = "not_any" if negate else "any"
        if has_parameter_builder(values):
            subquery_expr = self._normalize_subquery_expression(values, builder)
            return comparison(this=column_expr, expression=exp.Any(this=subquery_expr))
        if isinstance(values, exp.Expr):
            return comparison(this=column_expr, expression=exp.Any(this=values))
        if has_sqlglot_expression(values):
            raw_expr = values.sqlglot_expression
            if isinstance(raw_expr, exp.Expr):
                return comparison(this=column_expr, expression=exp.Any(this=raw_expr))
            parsed_expr: exp.Expr | None = exp.maybe_parse(str(values), dialect=builder.dialect)
            if parsed_expr is not None:
                return comparison(this=column_expr, expression=exp.Any(this=parsed_expr))
        if has_expression_and_sql(values):
            self._merge_parameters(values)
            expression_attr = values.expression
            if isinstance(expression_attr, exp.Expr):
                return comparison(this=column_expr, expression=exp.Any(this=expression_attr))
            sql_text = values.sql
            parsed_expr = exp.maybe_parse(sql_text, dialect=builder.dialect)
            if parsed_expr is not None:
                return comparison(this=column_expr, expression=exp.Any(this=parsed_expr))
        if isinstance(values, str):
            parsed_expr = exp.maybe_parse(values, dialect=builder.dialect)
            if isinstance(parsed_expr, (exp.Select, exp.Union, exp.Subquery)):
                return comparison(this=column_expr, expression=exp.Any(this=exp.paren(parsed_expr)))
            msg = f"Unsupported type for 'values' in {error_context}"
            raise SQLBuilderError(msg)
        if not is_iterable_parameters(values) or isinstance(values, (bytes, bytearray)):
            msg = f"Unsupported type for 'values' in {error_context}"
            raise SQLBuilderError(msg)
        placeholders: list[exp.Expr] = []
        values_list = list(values)
        for index, element in enumerate(values_list):
            if len(values_list) == 1:
                param_name = builder._next_parameter_name(column_name)
            else:
                param_name = builder._next_parameter_name(f"{column_name}_{parameter_suffix}_{index + 1}")
            _, param_name = builder.add_parameter(element, name=param_name)
            placeholders.append(exp.Placeholder(this=param_name))
        tuple_expr = exp.Tuple(expressions=placeholders)
        return comparison(this=column_expr, expression=exp.Any(this=tuple_expr))

    def _create_any_condition(self, column_expr: exp.Expr, values: Any, column_name: str) -> exp.Expr:
        return self._any_comparison(column_expr, values, column_name, negate=False)

    def _create_not_any_condition(self, column_expr: exp.Expr, values: Any, column_name: str) -> exp.Expr:
        return self._any_comparison(column_expr, values, column_name, negate=True)

    def _normalize_subquery_expression(self, subquery: Any, builder: "SQLBuilderProtocol") -> exp.Expr:
        if has_parameter_builder(subquery):
            subquery_builder = cast("QueryBuilder", subquery)
            parsed_subquery = subquery_builder._build_final_expression(copy=True)
            subquery_expr = exp.paren(parsed_subquery)
            parameters: Any = subquery_builder.parameters
            if isinstance(parameters, dict):
                param_mapping: dict[str, str] = {}
                query_builder = cast("QueryBuilder", builder)
                for param_name, param_value in parameters.items():
                    unique_name = query_builder._next_parameter_name(param_name)
                    param_mapping[param_name] = unique_name
                    query_builder.add_parameter(param_value, name=unique_name)
                if param_mapping:
                    updated = query_builder._update_placeholders(parsed_subquery, param_mapping)
                    subquery_expr = exp.paren(updated)
            elif isinstance(parameters, (list, tuple)):
                for param_value in parameters:
                    builder.add_parameter(param_value)
            elif parameters is not None:
                builder.add_parameter(parameters)
            return subquery_expr

        if has_expression_and_sql(subquery):

            def parse_subquery(sql_text: str) -> exp.Expr:
                parsed_from_sql: exp.Expr | None = exp.maybe_parse(sql_text, dialect=builder.dialect)
                if parsed_from_sql is None:
                    msg = f"Could not parse subquery SQL: {sql_text}"
                    raise SQLBuilderError(msg)
                return parsed_from_sql

            return extract_sql_object_expression(subquery, builder=self, parse_sql=parse_subquery)

        if isinstance(subquery, exp.Expr):
            return subquery

        if isinstance(subquery, str):
            parsed_expression_from_str: exp.Expr | None = exp.maybe_parse(subquery, dialect=builder.dialect)
            if parsed_expression_from_str is None:
                msg = f"Could not parse subquery SQL: {subquery}"
                raise SQLBuilderError(msg)
            return parsed_expression_from_str

        converted_expr: exp.Expr = exp.convert(subquery)
        return converted_expr

    def _create_or_expression(self, conditions: "list[exp.Expr]") -> exp.Expr:
        if not conditions:
            msg = "OR expression requires at least one condition"
            raise SQLBuilderError(msg)

        return exp.or_(*conditions)

    def _process_tuple_condition(self, condition: "tuple[Any, ...]") -> exp.Expr:
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
        condition: Union[str, exp.Expr, tuple[str, Any], tuple[str, str, Any], "ColumnExpression", SQL],
        values: tuple[Any, ...],
        operator: str | None,
        kwargs: dict[str, Any],
    ) -> exp.Expr:
        def _normalize_condition_expression(expression: exp.Expr) -> exp.Expr:
            if isinstance(expression, exp.Alias) and isinstance(expression.this, exp.Expr):
                return expression.this
            return expression

        if values or kwargs:
            if not isinstance(condition, str):
                msg = "When values are provided, condition must be a string"
                raise SQLBuilderError(msg)

            param_info = _PARAMETER_VALIDATOR.extract_parameters(condition)

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
        if isinstance(condition, exp.Expr):
            return _normalize_condition_expression(condition)
        if isinstance(condition, tuple):
            return self._process_tuple_condition(condition)
        if has_parameter_builder(condition):
            column_expr_obj = cast("ColumnExpression", condition)
            expression_attr = cast("exp.Expr | None", column_expr_obj._expression)
            if expression_attr is None:
                msg = "Column expression is missing underlying sqlglot expression."
                raise SQLBuilderError(msg)
            return _normalize_condition_expression(expression_attr)
        if has_sqlglot_expression(condition):
            raw_expr = condition.sqlglot_expression
            if isinstance(raw_expr, exp.Expr):
                return builder._parameterize_expression(_normalize_condition_expression(raw_expr))
            return parse_condition_expression(str(condition))
        if has_expression_and_sql(condition):
            return _normalize_condition_expression(
                extract_sql_object_expression(condition, builder=self, parse_sql=parse_condition_expression)
            )

        msg = f"Unsupported condition type: {type(condition).__name__}"
        raise SQLBuilderError(msg)

    def where(
        self,
        condition: Union[str, exp.Expr, tuple[str, Any], tuple[str, str, Any], "ColumnExpression", SQL],
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

    def _build_comparison_condition(
        self, column: str | exp.Column, value: Any, condition_factory: "Callable[[exp.Expr, exp.Placeholder], exp.Expr]"
    ) -> exp.Expr:
        return self._create_parameterized_condition(column, value, condition_factory)

    def _build_between_condition(self, column: str | exp.Column, low: Any, high: Any) -> exp.Expr:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        return self._handle_between_operator(column_expr, (low, high), extract_column_name(column))

    def _build_like_condition(self, column: str | exp.Column, pattern: str, escape: str | None) -> exp.Expr:
        builder = cast("SQLBuilderProtocol", self)
        column_name = extract_column_name(column)
        param_name = builder._next_parameter_name(column_name)
        _, param_name = builder.add_parameter(pattern, name=param_name)
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        placeholder = exp.Placeholder(this=param_name)
        if escape is not None:
            return exp.Escape(
                this=exp.Like(this=column_expr, expression=placeholder), expression=exp.convert(str(escape))
            )
        return column_expr.like(placeholder)

    def _build_is_null_condition(self, column: str | exp.Column) -> exp.Expr:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        return column_expr.is_(exp.null())

    def _build_is_not_null_condition(self, column: str | exp.Column) -> exp.Expr:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        return column_expr.is_(exp.null()).not_()

    def _build_in_condition(self, column: str | exp.Column, values: Any) -> exp.Expr:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        return self._handle_in_operator(column_expr, values, extract_column_name(column))

    def _build_not_in_condition(self, column: str | exp.Column, values: Any) -> exp.Expr:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        return self._handle_not_in_operator(column_expr, values, extract_column_name(column))

    def _build_any_condition(self, column: str | exp.Column, subquery: Any) -> exp.Expr:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        return self._create_any_condition(column_expr, subquery, extract_column_name(column))

    def _build_not_any_condition(self, column: str | exp.Column, subquery: Any) -> exp.Expr:
        column_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        return self._create_not_any_condition(column_expr, subquery, extract_column_name(column))

    def _build_exists_condition(self, subquery: Any) -> exp.Expr:
        builder = cast("SQLBuilderProtocol", self)
        return exp.Exists(this=self._normalize_subquery_expression(subquery, builder))

    def _build_not_exists_condition(self, subquery: Any) -> exp.Expr:
        return exp.Not(this=self._build_exists_condition(subquery))

    def where_eq(self, column: str | exp.Column, value: Any) -> Self:
        return self.where(self._build_comparison_condition(column, value, _expr_eq))

    def where_neq(self, column: str | exp.Column, value: Any) -> Self:
        return self.where(self._build_comparison_condition(column, value, _expr_neq))

    def where_lt(self, column: str | exp.Column, value: Any) -> Self:
        return self.where(self._build_comparison_condition(column, value, _expr_lt))

    def where_lte(self, column: str | exp.Column, value: Any) -> Self:
        return self.where(self._build_comparison_condition(column, value, _expr_lte))

    def where_gt(self, column: str | exp.Column, value: Any) -> Self:
        return self.where(self._build_comparison_condition(column, value, _expr_gt))

    def where_gte(self, column: str | exp.Column, value: Any) -> Self:
        return self.where(self._build_comparison_condition(column, value, _expr_gte))

    def where_between(self, column: str | exp.Column, low: Any, high: Any) -> Self:
        return self.where(self._build_between_condition(column, low, high))

    def where_like(self, column: str | exp.Column, pattern: str, escape: str | None = None) -> Self:
        return self.where(self._build_like_condition(column, pattern, escape))

    def where_not_like(self, column: str | exp.Column, pattern: str) -> Self:
        return self.where(self._build_comparison_condition(column, pattern, _expr_not_like))

    def where_ilike(self, column: str | exp.Column, pattern: str) -> Self:
        return self.where(self._build_comparison_condition(column, pattern, _expr_ilike))

    def where_is_null(self, column: str | exp.Column) -> Self:
        return self.where(self._build_is_null_condition(column))

    def where_is_not_null(self, column: str | exp.Column) -> Self:
        return self.where(self._build_is_not_null_condition(column))

    def where_in(self, column: str | exp.Column, values: Any) -> Self:
        return self.where(self._build_in_condition(column, values))

    def where_not_in(self, column: str | exp.Column, values: Any) -> Self:
        return self.where(self._build_not_in_condition(column, values))

    def where_any(self, column: str | exp.Column, subquery: Any) -> Self:
        return self.where(self._build_any_condition(column, subquery))

    def where_not_any(self, column: str | exp.Column, subquery: Any) -> Self:
        return self.where(self._build_not_any_condition(column, subquery))

    def where_exists(self, subquery: Any) -> Self:
        return self.where(self._build_exists_condition(subquery))

    def where_not_exists(self, subquery: Any) -> Self:
        return self.where(self._build_not_exists_condition(subquery))

    def where_like_any(self, column: str | exp.Column, patterns: list[str]) -> Self:
        conditions = [self._create_parameterized_condition(column, pattern, _expr_like_method) for pattern in patterns]
        or_condition = self._create_or_expression(conditions)
        return self.where(or_condition)

    def or_where_eq(self, column: str | exp.Column, value: Any) -> Self:
        return self._combine_with_or(self._build_comparison_condition(column, value, _expr_eq))

    def or_where_neq(self, column: str | exp.Column, value: Any) -> Self:
        return self._combine_with_or(self._build_comparison_condition(column, value, _expr_neq))

    def or_where_lt(self, column: str | exp.Column, value: Any) -> Self:
        return self._combine_with_or(self._build_comparison_condition(column, value, _expr_lt))

    def or_where_lte(self, column: str | exp.Column, value: Any) -> Self:
        return self._combine_with_or(self._build_comparison_condition(column, value, _expr_lte))

    def or_where_gt(self, column: str | exp.Column, value: Any) -> Self:
        return self._combine_with_or(self._build_comparison_condition(column, value, _expr_gt))

    def or_where_gte(self, column: str | exp.Column, value: Any) -> Self:
        return self._combine_with_or(self._build_comparison_condition(column, value, _expr_gte))

    def or_where_between(self, column: str | exp.Column, low: Any, high: Any) -> Self:
        return self._combine_with_or(self._build_between_condition(column, low, high))

    def or_where_like(self, column: str | exp.Column, pattern: str, escape: str | None = None) -> Self:
        return self._combine_with_or(self._build_like_condition(column, pattern, escape))

    def or_where_not_like(self, column: str | exp.Column, pattern: str) -> Self:
        return self._combine_with_or(self._build_comparison_condition(column, pattern, _expr_not_like))

    def or_where_ilike(self, column: str | exp.Column, pattern: str) -> Self:
        return self._combine_with_or(self._build_comparison_condition(column, pattern, _expr_ilike))

    def or_where_is_null(self, column: str | exp.Column) -> Self:
        return self._combine_with_or(self._build_is_null_condition(column))

    def or_where_is_not_null(self, column: str | exp.Column) -> Self:
        return self._combine_with_or(self._build_is_not_null_condition(column))

    def or_where_null(self, column: str | exp.Column) -> Self:
        return self.or_where_is_null(column)

    def or_where_not_null(self, column: str | exp.Column) -> Self:
        return self.or_where_is_not_null(column)

    def or_where_in(self, column: str | exp.Column, values: Any) -> Self:
        return self._combine_with_or(self._build_in_condition(column, values))

    def or_where_not_in(self, column: str | exp.Column, values: Any) -> Self:
        return self._combine_with_or(self._build_not_in_condition(column, values))

    def or_where_any(self, column: str | exp.Column, subquery: Any) -> Self:
        return self._combine_with_or(self._build_any_condition(column, subquery))

    def or_where_not_any(self, column: str | exp.Column, subquery: Any) -> Self:
        return self._combine_with_or(self._build_not_any_condition(column, subquery))

    def or_where_exists(self, subquery: Any) -> Self:
        return self._combine_with_or(self._build_exists_condition(subquery))

    def or_where_not_exists(self, subquery: Any) -> Self:
        return self._combine_with_or(self._build_not_exists_condition(subquery))

    def where_or(self, *conditions: str | tuple[str, Any] | tuple[str, str, Any] | exp.Expr) -> Self:
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
        condition: Union[str, exp.Expr, exp.Condition, tuple[str, Any], tuple[str, str, Any], "ColumnExpression", SQL],
        *values: Any,
        operator: str | None = None,
        **kwargs: Any,
    ) -> Self:
        or_condition = self._process_where_condition(condition, values, operator, kwargs)
        return self._combine_with_or(or_condition)


@trait
class HavingClauseMixin:
    __slots__ = ()

    def having(self, condition: str | exp.Expr | exp.Condition | tuple[str, Any] | tuple[str, str, Any]) -> Self:
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
        aggregate_function: str | exp.Expr,
        aggregate_column: str | exp.Expr,
        pivot_column: str | exp.Expr,
        pivot_values: list[str | int | float | exp.Expr],
        alias: str | None = None,
    ) -> "Select":
        builder = cast("SQLBuilderProtocol", self)
        current_expr = builder.get_expression()
        if not isinstance(current_expr, exp.Select):
            msg = "Pivot can only be applied to a Select expression managed by SelectBuilder."
            raise TypeError(msg)

        agg_name = aggregate_function if isinstance(aggregate_function, str) else aggregate_function.name
        agg_column = _coerce_column(aggregate_column)
        pivot_col_expr = _coerce_column(pivot_column)

        pivot_agg_expr = exp.func(agg_name, agg_column)

        pivot_value_exprs: list[exp.Expr] = []
        for raw_value in pivot_values:
            if isinstance(raw_value, exp.Expr):
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
        columns_to_unpivot: list[str | exp.Expr],
        alias: str | None = None,
    ) -> "Select":
        builder = cast("SQLBuilderProtocol", self)
        current_expr = builder.get_expression()
        if not isinstance(current_expr, exp.Select):
            msg = "Unpivot can only be applied to a Select expression managed by Select."
            raise TypeError(msg)

        value_identifier = exp.to_identifier(value_column_name)
        name_identifier = exp.to_identifier(name_column_name)

        unpivot_columns: list[exp.Expr] = []
        for column in columns_to_unpivot:
            if isinstance(column, exp.Expr):
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

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...

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

        cte_select: exp.Expr | None
        if isinstance(query, str):
            cte_select = exp.maybe_parse(query, dialect=self.dialect)
        elif isinstance(query, exp.Expr):
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
                    unique_name = builder._next_parameter_name(f"{name}_{param_name}")
                    param_mapping[param_name] = unique_name
                    builder.add_parameter(param_value, name=unique_name)
                cte_select = builder._update_placeholders(cte_select, param_mapping)
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

    def get_expression(self) -> exp.Expr | None: ...
    def set_expression(self, expression: exp.Expr) -> None: ...
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
            right_expr = builder._update_placeholders(right_expr, rename_map)

        combined: exp.Expr
        if operator == "union":
            combined = exp.union(left_expr, right_expr, distinct=distinct)
        elif operator == "intersect":
            combined = exp.intersect(left_expr, right_expr, distinct=distinct)
        elif operator == "except":
            combined = exp.except_(left_expr, right_expr)
        else:  # pragma: no cover
            msg = f"Unsupported set operation: {operator}"
            raise SQLBuilderError(msg)

        new_builder = builder._spawn_like_self()
        new_builder.set_expression(combined)
        new_builder.set_parameters(merged_parameters)
        return cast("Self", new_builder)


TABLE_HINT_PATTERN: Final[str] = r"\b{}\b(\s+AS\s+\w+)?"


def _parse_hint_expression(hint: Any, dialect: "DialectType | str | None") -> exp.Expr:
    try:
        hint_str = str(hint)
        hint_expr: exp.Expr | None = exp.maybe_parse(hint_str, dialect=dialect)
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
    """

    __slots__ = ("_hints",)
    _expression: exp.Expr | None

    def __init__(self, *columns: str, **kwargs: Any) -> None:
        """Initialize SELECT with optional columns.

        Args:
            *columns: Column names to select
            **kwargs: Additional QueryBuilder arguments (dialect, schema, etc.)
        """
        self._init_query_builder(kwargs)

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
            hint: The raw hint string.
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
        target_dialect = str(dialect) if dialect else self.dialect_name

        modified_expr = self._expression or self._create_base_expression()
        original_hint = modified_expr.args.get("hint") if isinstance(modified_expr, exp.Select) else None
        had_original_hint = isinstance(modified_expr, exp.Select) and "hint" in modified_expr.args

        if isinstance(modified_expr, exp.Select):
            statement_hints = [h["hint"] for h in self._hints if h.get("location") == "statement"]
            if statement_hints:
                hint_expressions: list[exp.Expr] = [
                    _parse_hint_expression(hint, target_dialect) for hint in statement_hints
                ]

                if hint_expressions:
                    modified_expr.set("hint", exp.Hint(expressions=hint_expressions))

        try:
            safe_query = super().build(dialect=dialect)
        finally:
            if isinstance(modified_expr, exp.Select):
                modified_expr.set("hint", original_hint if had_original_hint else None)

        if not self._hints:
            return safe_query

        modified_sql = safe_query.sql

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

    def _add_lock(
        self, *, update: bool, skip_locked: bool = False, nowait: bool = False, of: "str | list[str] | None" = None
    ) -> "Self":
        self._validate_select_expression()
        self._validate_lock_parameters(skip_locked, nowait)

        assert self._expression is not None
        select_expr = cast("exp.Select", self._expression)

        lock_args: dict[str, Any] = {"update": update}

        if skip_locked:
            lock_args["wait"] = False
        elif nowait:
            lock_args["wait"] = True

        if of:
            tables = [of] if isinstance(of, str) else of
            lock_args["expressions"] = [exp.to_identifier(str(t), quoted=is_explicitly_quoted(t)) for t in tables]

        lock = exp.Lock(**lock_args)

        current_locks = select_expr.args.get("locks", [])
        current_locks.append(lock)
        select_expr.set("locks", current_locks)

        return self

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
        return self._add_lock(update=True, skip_locked=skip_locked, nowait=nowait, of=of)

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
        return self._add_lock(update=False, skip_locked=skip_locked, nowait=nowait, of=of)

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
