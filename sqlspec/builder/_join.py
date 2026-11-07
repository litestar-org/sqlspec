# pyright: reportPrivateUsage=false, reportPrivateImportUsage=false
"""JOIN operation mixins.

Provides mixins for JOIN operations in SELECT statements.
"""

from typing import TYPE_CHECKING, Any, Self, Union, cast, final

from mypy_extensions import trait
from sqlglot import exp

from sqlspec.builder._base import BuiltQuery, QueryBuilder, register_subquery_fragment
from sqlspec.builder._parsing_utils import (
    extract_sql_object_expression,
    parse_table_expression,
    parse_table_function,
    resolve_value_expression,
    unwrap_expression_source,
)
from sqlspec.builder._temporal import create_temporal_version, register_version_generators
from sqlspec.core import SQL
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_parameter_builder

if TYPE_CHECKING:
    from sqlspec.protocols import HasParameterBuilderProtocol, SQLBuilderProtocol

__all__ = ("JoinBuilder", "JoinClauseMixin", "create_join_builder")


def build_join_clause(
    builder: "SQLBuilderProtocol",
    table: str | exp.Expr | Any,
    on: Union[str, exp.Expr, "SQL"] | None,
    alias: str | None,
    join_type: str,
    *,
    lateral: bool = False,
) -> exp.Join:
    table_expr = _parse_join_table(builder, table, alias, lateral)
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

        A string ``table`` is a table name with an optional alias. Pass function sources and subqueries as
        expressions or builders. Table text must be trusted, because engines may resolve names to files or
        external sources. With ``lateral=True`` a string may also be one function call, which runs as named;
        its arguments cannot hold subqueries, statements, or comments.
        """
        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "JOIN clause is only supported for SELECT statements."
            raise SQLBuilderError(msg)

        if isinstance(table, exp.Join):
            merged_join = (
                cast("exp.Join", builder._merge_subquery_fragments(table))
                if isinstance(builder, QueryBuilder)
                else table
            )
            builder._expression = builder._expression.join(merged_join, copy=False)
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
            table: Table, subquery, or table function to join. String text may be one function call,
                which runs as named and can read files or run SQL text on some engines, so that text
                must be trusted.
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
            table: Table, subquery, or table function to join. String text may be one function call,
                which runs as named and can read files or run SQL text on some engines, so that text
                must be trusted.
            on: Optional join condition
            alias: Optional alias for the joined table/subquery

        Returns:
            Self for method chaining
        """
        return self.join(table, on=on, alias=alias, join_type="LEFT", lateral=True)

    def cross_lateral_join(self, table: str | exp.Expr | Any, alias: str | None = None) -> Self:
        """Create a CROSS LATERAL JOIN (no ON condition).

        Args:
            table: Table, subquery, or table function to join. String text may be one function call,
                which runs as named and can read files or run SQL text on some engines, so that text
                must be trusted.
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
        self._table: Any = None
        self._alias: str | None = None
        self._as_of: Any | None = None
        self._as_of_type: str | None = None

    def __call__(self, table: Any, alias: str | None = None) -> Self:
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

    def on(self, condition: Any) -> exp.Expr:
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
        unwrapped_condition = unwrap_expression_source(condition)
        if isinstance(condition, str):
            parsed: exp.Expr | None = exp.maybe_parse(condition)
            condition_expr = parsed or exp.condition(condition)
        elif unwrapped_condition is not None:
            condition_expr = unwrapped_condition
        elif isinstance(condition, SQL):
            condition_expr = extract_sql_object_expression(condition)
            if condition.named_parameters:
                condition_expr = register_subquery_fragment(condition_expr, condition.named_parameters)
        else:
            condition_expr = condition

        table_expr: exp.Expr
        unwrapped_table = unwrap_expression_source(self._table)
        if isinstance(self._table, str):
            table_expr = _parse_join_text(
                self._table, self._alias, None, self._lateral or self._join_type == "LATERAL JOIN"
            )
        elif unwrapped_table is not None:
            table_expr = exp.alias_(unwrapped_table, self._alias) if self._alias else unwrapped_table
        elif isinstance(self._table, SQL):
            table_expr = extract_sql_object_expression(self._table)
            if self._alias:
                table_expr = exp.alias_(table_expr, self._alias)
            if self._table.named_parameters:
                table_expr = register_subquery_fragment(table_expr, self._table.named_parameters)
        elif isinstance(self._table, QueryBuilder):
            subquery_expr = exp.paren(self._table._build_final_expression(copy=True))
            table_expr = exp.alias_(subquery_expr, self._alias) if self._alias else subquery_expr
            if self._table.parameters:
                table_expr = register_subquery_fragment(table_expr, self._table.parameters)
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
    """Create a JoinBuilder instance."""
    return JoinBuilder(join_type, lateral=lateral)


def _parse_join_condition(builder: "SQLBuilderProtocol", on: Union[str, exp.Expr, "SQL"] | None) -> exp.Expr | None:
    if on is None:
        return None
    if isinstance(on, str):
        return exp.condition(on)
    if isinstance(on, SQL):

        def parse_join_condition(sql_text: str) -> exp.Expr:
            return exp.maybe_parse(sql_text, dialect=builder.dialect) or exp.condition(sql_text)

        return extract_sql_object_expression(on, builder=builder, parse_sql=parse_join_condition)
    if isinstance(on, exp.Expr):
        return on.copy()
    resolved = resolve_value_expression(on, builder, "join_on")
    if resolved is None:
        msg = f"Unsupported join condition type: {type(on).__name__}"
        raise SQLBuilderError(msg)
    return resolved


def _table_from_builder(table: Any, alias: str | None, builder: "SQLBuilderProtocol") -> exp.Expr:
    subquery_expression: exp.Expr
    if isinstance(builder, QueryBuilder):
        subquery_expression = builder._embed_subquery(table, alias_hint=alias or "join_src")
    else:
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


def _parse_join_text(table: str, alias: str | None, dialect: Any, lateral: bool) -> exp.Expr:
    if lateral and "(" in table:
        return parse_table_function(table, alias, dialect=dialect)
    return parse_table_expression(table, alias, dialect=dialect)


def _parse_join_table(
    builder: "SQLBuilderProtocol", table: str | exp.Expr | Any, alias: str | None, lateral: bool = False
) -> exp.Expr:
    if isinstance(table, str):
        return _parse_join_text(table, alias, builder.dialect, lateral)
    if has_parameter_builder(table):
        return _table_from_builder(table, alias, builder)
    unwrapped = unwrap_expression_source(table)
    if unwrapped is not None:
        return exp.alias_(unwrapped, alias) if alias else unwrapped
    if isinstance(table, SQL):
        expression = extract_sql_object_expression(table, builder=builder)
        return exp.alias_(expression, alias) if alias else expression
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
    """Turn a join into a LATERAL join.

    The join target is wrapped in ``exp.Lateral`` so sqlglot renders the standard
    ``JOIN LATERAL`` / ``LEFT JOIN LATERAL`` / ``CROSS JOIN LATERAL`` form accepted by
    PostgreSQL, MySQL, DuckDB, and Snowflake. Non-cross lateral joins without a
    condition get ``ON TRUE`` because PostgreSQL requires a join qualifier for them.
    """
    join_expr.set("this", _lateral_source(join_expr.this))
    if join_expr.args.get("kind") != "CROSS" and join_expr.args.get("on") is None:
        join_expr.set("on", exp.true())


def _lateral_source(table_expr: exp.Expr) -> exp.Expr:
    """Wrap a join target in ``exp.Lateral``, moving its alias onto the LATERAL node.

    Args:
        table_expr: Parsed join target (aliased subquery, table function, or table).

    Returns:
        The ``exp.Lateral`` join target.
    """
    if isinstance(table_expr, exp.Lateral):
        return table_expr

    source = table_expr
    alias_node: exp.Expr | None = None
    if isinstance(source, exp.Alias):
        alias_node = exp.TableAlias(this=exp.to_identifier(source.alias))
        source = source.this
    else:
        raw_alias = source.args.get("alias")
        if raw_alias is not None:
            source = source.copy()
            source.set("alias", None)
            if isinstance(raw_alias, exp.TableAlias):
                alias_node = raw_alias.copy()
            else:
                alias_identifier = (
                    raw_alias.copy() if isinstance(raw_alias, exp.Identifier) else exp.to_identifier(str(raw_alias))
                )
                alias_node = exp.TableAlias(this=alias_identifier)

    if isinstance(source, exp.Paren) and isinstance(source.this, exp.Query):
        source = exp.Subquery(this=source.this)
    elif isinstance(source, exp.Table) and isinstance(source.this, exp.Func) and not source.args.get("db"):
        source = source.this

    return exp.Lateral(this=source, alias=alias_node)


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
        table_alias = inner_table.alias
        if table_alias:
            target_alias = table_alias
            inner_table.set("alias", None)

    version = create_temporal_version(this=as_of_type, expression=exp.convert(as_of))
    inner_table.set("version", version)
    return exp.alias_(inner_table, target_alias) if target_alias else inner_table
