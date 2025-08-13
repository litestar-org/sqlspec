from typing import TYPE_CHECKING, Any, Optional, Union, cast

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._parsing_utils import parse_table_expression
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_query_builder_parameters

if TYPE_CHECKING:
    from sqlspec.core.statement import SQL
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
        on: Optional[Union[str, exp.Expression, "SQL"]] = None,
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
            if isinstance(on, str):
                on_expr = exp.condition(on)
            elif hasattr(on, "expression") and hasattr(on, "sql"):
                # Handle SQL objects (from sql.raw with parameters)
                expression = getattr(on, "expression", None)
                if expression is not None and isinstance(expression, exp.Expression):
                    # Merge parameters from SQL object into builder
                    if hasattr(on, "parameters") and hasattr(builder, "add_parameter"):
                        sql_parameters = getattr(on, "parameters", {})
                        for param_name, param_value in sql_parameters.items():
                            builder.add_parameter(param_value, name=param_name)
                    on_expr = expression
                else:
                    # If expression is None, fall back to parsing the raw SQL
                    sql_text = getattr(on, "sql", "")
                    # Merge parameters even when parsing raw SQL
                    if hasattr(on, "parameters") and hasattr(builder, "add_parameter"):
                        sql_parameters = getattr(on, "parameters", {})
                        for param_name, param_value in sql_parameters.items():
                            builder.add_parameter(param_value, name=param_name)
                    on_expr = exp.maybe_parse(sql_text) or exp.condition(str(sql_text))
            # For other types (should be exp.Expression)
            elif isinstance(on, exp.Expression):
                on_expr = on
            else:
                # Last resort - convert to string and parse
                on_expr = exp.condition(str(on))
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
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression, "SQL"], alias: Optional[str] = None
    ) -> Self:
        return self.join(table, on, alias, "INNER")

    def left_join(
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression, "SQL"], alias: Optional[str] = None
    ) -> Self:
        return self.join(table, on, alias, "LEFT")

    def right_join(
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression, "SQL"], alias: Optional[str] = None
    ) -> Self:
        return self.join(table, on, alias, "RIGHT")

    def full_join(
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression, "SQL"], alias: Optional[str] = None
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
