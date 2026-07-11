# pyright: reportPrivateUsage=false, reportPrivateImportUsage=false
"""JOIN operation mixins.

Provides mixins for JOIN operations in SELECT statements.
"""

from typing import TYPE_CHECKING, Any, Union, cast, final

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._base import BuiltQuery, QueryBuilder
from sqlspec.builder._parsing_utils import extract_sql_object_expression, parse_table_expression
from sqlspec.builder._temporal import register_version_generators
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_expression_and_parameters, has_expression_and_sql, has_parameter_builder

if TYPE_CHECKING:
    from sqlspec.core import SQL
    from sqlspec.protocols import HasParameterBuilderProtocol, SQLBuilderProtocol

__all__ = ("JoinBuilder", "JoinClauseMixin", "create_join_builder")


def _parse_join_condition(builder: "SQLBuilderProtocol", on: Union[str, exp.Expr, "SQL"] | None) -> exp.Expr | None:
    if on is None:
        return None
    if isinstance(on, str):
        return exp.condition(on)
    if has_expression_and_sql(on) or has_expression_and_parameters(on):

        def parse_join_condition(sql_text: str) -> exp.Expr:
            return exp.maybe_parse(sql_text, dialect=builder.dialect) or exp.condition(sql_text)

        return extract_sql_object_expression(on, builder=builder, parse_sql=parse_join_condition)
    if isinstance(on, exp.Expr):
        return on
    return exp.condition(str(on))


def _table_from_builder(table: Any, alias: str | None, builder: "SQLBuilderProtocol") -> exp.Expr:
    subquery_expression: exp.Expr
    builder_table = cast("HasParameterBuilderProtocol", table)
    parameters = builder_table.parameters

    if isinstance(table, QueryBuilder):
        subquery_expression = table._build_final_expression(copy=True)
    else:
        subquery_result = builder_table.build()
        sql_text = subquery_result.sql if isinstance(subquery_result, BuiltQuery) else str(subquery_result)
        subquery_expression = exp.maybe_parse(sql_text, dialect=builder.dialect) or exp.convert(sql_text)

    if parameters:
        for param_name, param_value in parameters.items():
            builder.add_parameter(param_value, name=param_name)

    subquery_exp = exp.paren(subquery_expression)
    return exp.alias_(subquery_exp, alias) if alias else subquery_exp


def _parse_join_table(builder: "SQLBuilderProtocol", table: str | exp.Expr | Any, alias: str | None) -> exp.Expr:
    if isinstance(table, str):
        return parse_table_expression(table, alias, dialect=builder.dialect)
    if has_parameter_builder(table):
        return _table_from_builder(table, alias, builder)
    if isinstance(table, exp.Expr):
        return table
    return cast("exp.Expr", table)


def _join_for_type(table_expr: exp.Expr, on_expr: exp.Expr | None, join_type: str) -> exp.Join:
    join_type_upper = join_type.upper()
    if join_type_upper == "INNER":
        return exp.Join(this=table_expr, on=on_expr)
    if join_type_upper == "LEFT":
        return exp.Join(this=table_expr, on=on_expr, side="LEFT")
    if join_type_upper == "RIGHT":
        return exp.Join(this=table_expr, on=on_expr, side="RIGHT")
    if join_type_upper == "FULL":
        return exp.Join(this=table_expr, on=on_expr, side="FULL", kind="OUTER")
    if join_type_upper == "CROSS":
        return exp.Join(this=table_expr, kind="CROSS")
    msg = f"Unsupported join type: {join_type}"
    raise SQLBuilderError(msg)


def _apply_lateral_modifier(join_expr: exp.Join) -> None:
    current_kind = join_expr.args.get("kind")
    current_side = join_expr.args.get("side")

    if current_kind == "CROSS":
        join_expr.set("kind", "CROSS LATERAL")
    elif current_kind == "OUTER" and current_side == "FULL":
        join_expr.set("side", "FULL")
        join_expr.set("kind", "OUTER LATERAL")
    elif current_side:
        join_expr.set("kind", f"{current_side} LATERAL")
        join_expr.set("side", None)
    else:
        join_expr.set("kind", "LATERAL")


def _attach_as_of_version(
    table_expr: exp.Expr, alias: str | None, as_of: Any, as_of_type: str | None = None
) -> exp.Expr:
    register_version_generators()

    inner_table = table_expr.copy()
    target_alias = alias

    if isinstance(inner_table, exp.Alias):
        target_alias = inner_table.alias
        inner_table = inner_table.this
    elif isinstance(inner_table, exp.Table):
        alias_expr = inner_table.args.get("alias")
        if alias_expr is not None:
            target_alias = alias_expr.this
            inner_table.set("alias", None)

    version = exp.Version(this=as_of_type or "TIMESTAMP", kind="AS OF", expression=exp.convert(as_of))
    inner_table.set("version", version)
    return exp.alias_(inner_table, target_alias) if target_alias else inner_table


def build_join_clause(
    builder: "SQLBuilderProtocol",
    table: str | exp.Expr | Any,
    on: Union[str, exp.Expr, "SQL"] | None,
    alias: str | None,
    join_type: str,
    *,
    lateral: bool = False,
) -> exp.Join:
    table_expr = _parse_join_table(builder, table, alias)
    on_expr = _parse_join_condition(builder, on)
    join_expr = _join_for_type(table_expr, on_expr, join_type)
    if lateral:
        _apply_lateral_modifier(join_expr)
    return join_expr


@trait
class JoinClauseMixin:
    """Mixin providing JOIN clause methods for SELECT builders.

    ``_expression`` is populated by the base builder class so the mixin can append JOINs without initializing the underlying SELECT expression.
    """

    __slots__ = ()

    _expression: exp.Expr | None

    def join(
        self,
        table: str | exp.Expr | Any,
        on: Union[str, exp.Expr, "SQL"] | None = None,
        alias: str | None = None,
        join_type: str = "INNER",
        lateral: bool = False,
        as_of: Any | None = None,
        as_of_type: str | None = None,
    ) -> Self:
        """Add a JOIN clause to the SELECT expression.

        ``as_of`` attaches a temporal version clause by copying the inner table, honoring existing aliases, and updating the JOIN target without mutating shared expressions.
        """
        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "JOIN clause is only supported for SELECT statements."
            raise SQLBuilderError(msg)

        if isinstance(table, exp.Join):
            builder._expression = builder._expression.join(table, copy=False)
            return cast("Self", builder)

        join_expr = build_join_clause(builder, table, on, alias, join_type, lateral=lateral)

        if as_of is not None:
            join_expr.set("this", _attach_as_of_version(join_expr.this, alias, as_of, as_of_type))

        builder._expression = builder._expression.join(join_expr, copy=False)
        return cast("Self", builder)

    def inner_join(
        self,
        table: str | exp.Expr | Any,
        on: Union[str, exp.Expr, "SQL"],
        alias: str | None = None,
        as_of: Any | None = None,
    ) -> Self:
        return self.join(table, on, alias, "INNER", as_of=as_of)

    def left_join(
        self,
        table: str | exp.Expr | Any,
        on: Union[str, exp.Expr, "SQL"],
        alias: str | None = None,
        as_of: Any | None = None,
    ) -> Self:
        return self.join(table, on, alias, "LEFT", as_of=as_of)

    def right_join(
        self,
        table: str | exp.Expr | Any,
        on: Union[str, exp.Expr, "SQL"],
        alias: str | None = None,
        as_of: Any | None = None,
    ) -> Self:
        return self.join(table, on, alias, "RIGHT", as_of=as_of)

    def full_join(
        self,
        table: str | exp.Expr | Any,
        on: Union[str, exp.Expr, "SQL"],
        alias: str | None = None,
        as_of: Any | None = None,
    ) -> Self:
        return self.join(table, on, alias, "FULL", as_of=as_of)

    def cross_join(
        self,
        table: str | exp.Expr | Any,
        alias: str | None = None,
        as_of: Any | None = None,
        as_of_type: str | None = None,
    ) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "Cannot add cross join to a non-SELECT expression."
            raise SQLBuilderError(msg)
        table_expr = _parse_join_table(builder, table, alias)

        if as_of is not None:
            table_expr = _attach_as_of_version(table_expr, alias, as_of, as_of_type)

        join_expr = exp.Join(this=table_expr, kind="CROSS")
        builder._expression = builder._expression.join(join_expr, copy=False)
        return cast("Self", builder)

    def lateral_join(
        self, table: str | exp.Expr | Any, on: Union[str, exp.Expr, "SQL"] | None = None, alias: str | None = None
    ) -> Self:
        """Create a LATERAL JOIN.

        Args:
            table: Table, subquery, or table function to join
            on: Optional join condition (for LATERAL JOINs with ON clause)
            alias: Optional alias for the joined table/subquery

        Returns:
            Self for method chaining
        """
        return self.join(table, on=on, alias=alias, join_type="INNER", lateral=True)

    def left_lateral_join(
        self, table: str | exp.Expr | Any, on: Union[str, exp.Expr, "SQL"] | None = None, alias: str | None = None
    ) -> Self:
        """Create a LEFT LATERAL JOIN.

        Args:
            table: Table, subquery, or table function to join
            on: Optional join condition
            alias: Optional alias for the joined table/subquery

        Returns:
            Self for method chaining
        """
        return self.join(table, on=on, alias=alias, join_type="LEFT", lateral=True)

    def cross_lateral_join(self, table: str | exp.Expr | Any, alias: str | None = None) -> Self:
        """Create a CROSS LATERAL JOIN (no ON condition).

        Args:
            table: Table, subquery, or table function to join
            alias: Optional alias for the joined table/subquery

        Returns:
            Self for method chaining
        """
        return self.join(table, on=None, alias=alias, join_type="CROSS", lateral=True)


@final
class JoinBuilder:
    """Builder for JOIN operations with fluent syntax."""

    __slots__ = ("_alias", "_as_of", "_as_of_type", "_join_type", "_lateral", "_table")

    def __init__(self, join_type: str, lateral: bool = False) -> None:
        """Initialize the join builder.

        Args:
            join_type: Type of join (inner, left, right, full, cross, lateral)
            lateral: Whether this is a LATERAL join
        """
        self._join_type = join_type.upper()
        self._lateral = lateral
        self._table: str | exp.Expr | None = None
        self._alias: str | None = None
        self._as_of: Any | None = None
        self._as_of_type: str | None = None

    def __call__(self, table: str | exp.Expr, alias: str | None = None) -> Self:
        """Set the table to join.

        Args:
            table: Table name or expression to join
            alias: Optional alias for the table

        Returns:
            Self for method chaining
        """
        self._table = table
        self._alias = alias
        return self

    def as_of(self, time_expr: Any, kind: str | None = None) -> Self:
        """Set AS OF clause for the join (Time Travel/Flashback).

        Args:
            time_expr: Timestamp or system time expression
            kind: Type of AS OF clause (SYSTEM TIME, TIMESTAMP). If None, defaults based on dialect.

        Returns:
            Self for method chaining
        """
        self._as_of = time_expr
        self._as_of_type = kind
        return self

    def on(self, condition: str | exp.Expr) -> exp.Expr:
        """Set the join condition and build the JOIN expression.

        Args:
            condition: JOIN condition

        Returns:
            Complete JOIN expression
        """
        if not self._table:
            msg = "Table must be set before calling .on()"
            raise SQLBuilderError(msg)

        condition_expr: exp.Expr
        if isinstance(condition, str):
            parsed: exp.Expr | None = exp.maybe_parse(condition)
            condition_expr = parsed or exp.condition(condition)
        else:
            condition_expr = condition

        table_expr: exp.Expr
        if isinstance(self._table, str):
            table_expr = exp.to_table(self._table)
            if self._alias:
                table_expr = exp.alias_(table_expr, self._alias)
        else:
            table_expr = self._table
            if self._alias:
                table_expr = exp.alias_(table_expr, self._alias)

        if self._as_of is not None:
            table_expr = _attach_as_of_version(table_expr, self._alias, self._as_of, self._as_of_type)

        normalized_join_type = self._join_type.removesuffix(" JOIN")
        if normalized_join_type == "LATERAL":
            normalized_join_type = "INNER"
        if normalized_join_type in {"INNER", "LEFT", "RIGHT", "FULL", "CROSS"}:
            join_expr = _join_for_type(table_expr, condition_expr, normalized_join_type)
        else:
            join_expr = exp.Join(this=table_expr, on=condition_expr)

        if self._lateral or self._join_type == "LATERAL JOIN":
            _apply_lateral_modifier(join_expr)

        return join_expr


def create_join_builder(join_type: str, lateral: bool = False) -> "JoinBuilder":
    """Create a JoinBuilder without tripping trait instantiation errors.

    This guards against runtime environments where a trait-decorated JoinBuilder
    may raise on direct construction.
    """
    try:
        return JoinBuilder(join_type, lateral=lateral)
    except TypeError as exc:
        if "traits may not be directly created" not in str(exc):
            raise
        builder = object.__new__(JoinBuilder)
        builder._join_type = join_type.upper()
        builder._lateral = lateral
        builder._table = None
        builder._alias = None
        builder._as_of = None
        builder._as_of_type = None
        return builder
