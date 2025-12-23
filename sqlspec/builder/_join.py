# pyright: reportPrivateUsage=false
"""JOIN operation mixins.

Provides mixins for JOIN operations in SELECT statements.
"""

from typing import TYPE_CHECKING, Any, Union, cast

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._parsing_utils import parse_table_expression
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import (
    has_attr,
    has_expression_and_parameters,
    has_expression_and_sql,
    has_parameter_builder,
)

if TYPE_CHECKING:
    from sqlspec.core import SQL
    from sqlspec.protocols import SQLBuilderProtocol

__all__ = ("JoinBuilder", "JoinClauseMixin")


def _handle_sql_object_condition(on: Any, builder: "SQLBuilderProtocol") -> exp.Expression:
    if has_expression_and_parameters(on) and on.expression is not None:
        for param_name, param_value in on.parameters.items():
            builder.add_parameter(param_value, name=param_name)
        return cast("exp.Expression", on.expression)
    if has_expression_and_parameters(on):
        for param_name, param_value in on.parameters.items():
            builder.add_parameter(param_value, name=param_name)
    parsed_expr = exp.maybe_parse(on.sql, dialect=builder.dialect)  # pyright: ignore[reportAttributeAccessIssue]
    return parsed_expr if parsed_expr is not None else exp.condition(str(on.sql))  # pyright: ignore[reportAttributeAccessIssue]


def _parse_join_condition(
    builder: "SQLBuilderProtocol", on: Union[str, exp.Expression, "SQL"] | None
) -> exp.Expression | None:
    if on is None:
        return None
    if isinstance(on, str):
        return exp.condition(on)
    if has_expression_and_sql(on):
        return _handle_sql_object_condition(on, builder)
    if isinstance(on, exp.Expression):
        return on
    return exp.condition(str(on))


def _handle_query_builder_table(table: Any, alias: str | None, builder: "SQLBuilderProtocol") -> exp.Expression:
    subquery_expression: exp.Expression
    parameters: dict[str, Any] | None = None
    table_parameters = getattr(table, "parameters", None)
    if isinstance(table_parameters, dict):
        parameters = table_parameters

    if has_attr(table, "_build_final_expression") and callable(table._build_final_expression):
        subquery_expression = cast("exp.Expression", table._build_final_expression(copy=True))
    else:
        subquery_result = table.build()
        sql_text = subquery_result.sql if has_attr(subquery_result, "sql") else str(subquery_result)
        subquery_expression = exp.maybe_parse(sql_text, dialect=builder.dialect) or exp.convert(sql_text)
        if parameters is None and has_attr(subquery_result, "parameters"):
            result_parameters = subquery_result.parameters
            if isinstance(result_parameters, dict):
                parameters = result_parameters

    if parameters:
        for param_name, param_value in parameters.items():
            builder.add_parameter(param_value, name=param_name)

    subquery_exp = exp.paren(subquery_expression)
    return exp.alias_(subquery_exp, alias) if alias else subquery_exp


def _parse_join_table(
    builder: "SQLBuilderProtocol", table: str | exp.Expression | Any, alias: str | None
) -> exp.Expression:
    if isinstance(table, str):
        return parse_table_expression(table, alias)
    if has_parameter_builder(table):
        return _handle_query_builder_table(table, alias, builder)
    if isinstance(table, exp.Expression):
        return table
    return cast("exp.Expression", table)


def _create_join_expression(table_expr: exp.Expression, on_expr: exp.Expression | None, join_type: str) -> exp.Join:
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


def build_join_clause(
    builder: "SQLBuilderProtocol",
    table: str | exp.Expression | Any,
    on: Union[str, exp.Expression, "SQL"] | None,
    alias: str | None,
    join_type: str,
    *,
    lateral: bool = False,
) -> exp.Join:
    table_expr = _parse_join_table(builder, table, alias)
    on_expr = _parse_join_condition(builder, on)
    join_expr = _create_join_expression(table_expr, on_expr, join_type)
    if lateral:
        _apply_lateral_modifier(join_expr)
    return join_expr


@trait
class JoinClauseMixin:
    """Mixin providing JOIN clause methods for SELECT builders."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: exp.Expression | None

    def join(
        self,
        table: str | exp.Expression | Any,
        on: Union[str, exp.Expression, "SQL"] | None = None,
        alias: str | None = None,
        join_type: str = "INNER",
        lateral: bool = False,
    ) -> Self:
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
        builder._expression = builder._expression.join(join_expr, copy=False)
        return cast("Self", builder)

    def inner_join(
        self, table: str | exp.Expression | Any, on: Union[str, exp.Expression, "SQL"], alias: str | None = None
    ) -> Self:
        return self.join(table, on, alias, "INNER")

    def left_join(
        self, table: str | exp.Expression | Any, on: Union[str, exp.Expression, "SQL"], alias: str | None = None
    ) -> Self:
        return self.join(table, on, alias, "LEFT")

    def right_join(
        self, table: str | exp.Expression | Any, on: Union[str, exp.Expression, "SQL"], alias: str | None = None
    ) -> Self:
        return self.join(table, on, alias, "RIGHT")

    def full_join(
        self, table: str | exp.Expression | Any, on: Union[str, exp.Expression, "SQL"], alias: str | None = None
    ) -> Self:
        return self.join(table, on, alias, "FULL")

    def cross_join(self, table: str | exp.Expression | Any, alias: str | None = None) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "Cannot add cross join to a non-SELECT expression."
            raise SQLBuilderError(msg)
        table_expr = _parse_join_table(builder, table, alias)
        join_expr = exp.Join(this=table_expr, kind="CROSS")
        builder._expression = builder._expression.join(join_expr, copy=False)
        return cast("Self", builder)

    def lateral_join(
        self,
        table: str | exp.Expression | Any,
        on: Union[str, exp.Expression, "SQL"] | None = None,
        alias: str | None = None,
    ) -> Self:
        """Create a LATERAL JOIN.

        Args:
            table: Table, subquery, or table function to join
            on: Optional join condition (for LATERAL JOINs with ON clause)
            alias: Optional alias for the joined table/subquery

        Returns:
            Self for method chaining

        Example:
            ```python
            query = (
                sql
                .select("u.name", "arr.value")
                .from_("users u")
                .lateral_join("UNNEST(u.tags)", alias="arr")
            )
            ```
        """
        return self.join(table, on=on, alias=alias, join_type="INNER", lateral=True)

    def left_lateral_join(
        self,
        table: str | exp.Expression | Any,
        on: Union[str, exp.Expression, "SQL"] | None = None,
        alias: str | None = None,
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

    def cross_lateral_join(self, table: str | exp.Expression | Any, alias: str | None = None) -> Self:
        """Create a CROSS LATERAL JOIN (no ON condition).

        Args:
            table: Table, subquery, or table function to join
            alias: Optional alias for the joined table/subquery

        Returns:
            Self for method chaining
        """
        return self.join(table, on=None, alias=alias, join_type="CROSS", lateral=True)


@trait
class JoinBuilder:
    """Builder for JOIN operations with fluent syntax.

    Example:
        ```python
        from sqlspec import sql

        # sql.left_join_("posts").on("users.id = posts.user_id")
        join_clause = sql.left_join_("posts").on(
            "users.id = posts.user_id"
        )

        # Or with query builder
        query = (
            sql
            .select("users.name", "posts.title")
            .from_("users")
            .join(
                sql.left_join_("posts").on(
                    "users.id = posts.user_id"
                )
            )
        )
        ```
    """

    def __init__(self, join_type: str, lateral: bool = False) -> None:
        """Initialize the join builder.

        Args:
            join_type: Type of join (inner, left, right, full, cross, lateral)
            lateral: Whether this is a LATERAL join
        """
        self._join_type = join_type.upper()
        self._lateral = lateral
        self._table: str | exp.Expression | None = None
        self._condition: exp.Expression | None = None
        self._alias: str | None = None

    def __call__(self, table: str | exp.Expression, alias: str | None = None) -> Self:
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

    def on(self, condition: str | exp.Expression) -> exp.Expression:
        """Set the join condition and build the JOIN expression.

        Args:
            condition: JOIN condition (e.g., "users.id = posts.user_id")

        Returns:
            Complete JOIN expression
        """
        if not self._table:
            msg = "Table must be set before calling .on()"
            raise SQLBuilderError(msg)

        # Parse the condition
        condition_expr: exp.Expression
        if isinstance(condition, str):
            parsed: exp.Expression | None = exp.maybe_parse(condition)
            condition_expr = parsed or exp.condition(condition)
        else:
            condition_expr = condition

        # Build table expression
        table_expr: exp.Expression
        if isinstance(self._table, str):
            table_expr = exp.to_table(self._table)
            if self._alias:
                table_expr = exp.alias_(table_expr, self._alias)
        else:
            table_expr = self._table
            if self._alias:
                table_expr = exp.alias_(table_expr, self._alias)

        # Create the appropriate join type using same pattern as existing JoinClauseMixin
        if self._join_type in {"INNER JOIN", "INNER", "LATERAL JOIN"}:
            join_expr = exp.Join(this=table_expr, on=condition_expr)
        elif self._join_type in {"LEFT JOIN", "LEFT"}:
            join_expr = exp.Join(this=table_expr, on=condition_expr, side="LEFT")
        elif self._join_type in {"RIGHT JOIN", "RIGHT"}:
            join_expr = exp.Join(this=table_expr, on=condition_expr, side="RIGHT")
        elif self._join_type in {"FULL JOIN", "FULL"}:
            join_expr = exp.Join(this=table_expr, on=condition_expr, side="FULL", kind="OUTER")
        elif self._join_type in {"CROSS JOIN", "CROSS"}:
            # CROSS JOIN doesn't use ON condition
            join_expr = exp.Join(this=table_expr, kind="CROSS")
        else:
            join_expr = exp.Join(this=table_expr, on=condition_expr)

        if self._lateral or self._join_type == "LATERAL JOIN":
            current_kind = join_expr.args.get("kind")
            current_side = join_expr.args.get("side")

            if current_kind == "CROSS":
                join_expr.set("kind", "CROSS LATERAL")
            elif current_kind == "OUTER" and current_side == "FULL":
                join_expr.set("side", "FULL")  # Keep side
                join_expr.set("kind", "OUTER LATERAL")
            elif current_side:
                join_expr.set("kind", f"{current_side} LATERAL")
                join_expr.set("side", None)  # Clear side to avoid duplication
            else:
                join_expr.set("kind", "LATERAL")

        return join_expr
