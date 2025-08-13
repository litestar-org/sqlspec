from typing import TYPE_CHECKING, Any, Optional, Union, cast

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._parsing_utils import parse_table_expression
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_query_builder_parameters

if TYPE_CHECKING:
    from sqlspec.protocols import SQLBuilderProtocol

__all__ = ("JoinClauseMixin",)


@trait
class JoinClauseMixin:
    """Mixin providing JOIN clause methods for SELECT builders."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    def join(
        self,
        table: Union[str, exp.Expression, Any],
        on: Optional[Union[str, exp.Expression]] = None,
        alias: Optional[str] = None,
        join_type: str = "INNER",
    ) -> Self:
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
            if hasattr(table, "_expression") and getattr(table, "_expression", None) is not None:
                table_expr_value = getattr(table, "_expression", None)
                if table_expr_value is not None:
                    subquery_exp = exp.paren(table_expr_value)
                else:
                    subquery_exp = exp.paren(exp.Anonymous(this=""))
                table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
            else:
                subquery = table.build()
                sql_str = subquery.sql if hasattr(subquery, "sql") and not callable(subquery.sql) else str(subquery)
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
            if hasattr(table, "_expression") and getattr(table, "_expression", None) is not None:
                table_expr_value = getattr(table, "_expression", None)
                if table_expr_value is not None:
                    subquery_exp = exp.paren(table_expr_value)
                else:
                    subquery_exp = exp.paren(exp.Anonymous(this=""))
                table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
            else:
                subquery = table.build()
                sql_str = subquery.sql if hasattr(subquery, "sql") and not callable(subquery.sql) else str(subquery)
                subquery_exp = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(builder, "dialect", None)))
                table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
        else:
            table_expr = table
        join_expr = exp.Join(this=table_expr, kind="CROSS")
        builder._expression = builder._expression.join(join_expr, copy=False)
        return cast("Self", builder)
