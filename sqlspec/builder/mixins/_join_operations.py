# pyright: reportPrivateUsage=false
"""JOIN operation mixins.

Provides mixins for JOIN operations in SELECT statements.
"""

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

__all__ = ("JoinBuilder", "JoinClauseMixin")


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
        lateral: bool = False,
    ) -> Self:
        builder = cast("SQLBuilderProtocol", self)
        self._validate_join_context(builder)

        # Handle Join expressions directly (from JoinBuilder.on() calls)
        if isinstance(table, exp.Join):
            if builder._expression is not None and isinstance(builder._expression, exp.Select):
                builder._expression = builder._expression.join(table, copy=False)
            return cast("Self", builder)

        table_expr = self._parse_table_expression(table, alias, builder)
        on_expr = self._parse_on_condition(on, builder)
        join_expr = self._create_join_expression(table_expr, on_expr, join_type)

        if lateral:
            self._apply_lateral_modifier(join_expr)

        if builder._expression is not None and isinstance(builder._expression, exp.Select):
            builder._expression = builder._expression.join(join_expr, copy=False)
        return cast("Self", builder)

    def _validate_join_context(self, builder: "SQLBuilderProtocol") -> None:
        """Validate that the join can be applied to the current expression."""
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "JOIN clause is only supported for SELECT statements."
            raise SQLBuilderError(msg)

    def _parse_table_expression(
        self, table: Union[str, exp.Expression, Any], alias: Optional[str], builder: "SQLBuilderProtocol"
    ) -> exp.Expression:
        """Parse table parameter into a SQLGlot expression."""
        if isinstance(table, str):
            return parse_table_expression(table, alias)
        if has_query_builder_parameters(table):
            return self._handle_query_builder_table(table, alias, builder)
        if isinstance(table, exp.Expression):
            return table
        return cast("exp.Expression", table)

    def _handle_query_builder_table(
        self, table: Any, alias: Optional[str], builder: "SQLBuilderProtocol"
    ) -> exp.Expression:
        """Handle table parameters that are query builders."""
        if hasattr(table, "_expression") and getattr(table, "_expression", None) is not None:
            table_expr_value = getattr(table, "_expression", None)
            if table_expr_value is not None:
                subquery_exp = exp.paren(table_expr_value)
            else:
                subquery_exp = exp.paren(exp.Anonymous(this=""))
            return exp.alias_(subquery_exp, alias) if alias else subquery_exp
        subquery = table.build()
        sql_str = subquery.sql if hasattr(subquery, "sql") and not callable(subquery.sql) else str(subquery)
        subquery_exp = exp.paren(exp.maybe_parse(sql_str, dialect=getattr(builder, "dialect", None)))
        return exp.alias_(subquery_exp, alias) if alias else subquery_exp

    def _parse_on_condition(
        self, on: Optional[Union[str, exp.Expression, "SQL"]], builder: "SQLBuilderProtocol"
    ) -> Optional[exp.Expression]:
        """Parse ON condition into a SQLGlot expression."""
        if on is None:
            return None

        if isinstance(on, str):
            return exp.condition(on)
        if hasattr(on, "expression") and hasattr(on, "sql"):
            return self._handle_sql_object_condition(on, builder)
        if isinstance(on, exp.Expression):
            return on
        # Last resort - convert to string and parse
        return exp.condition(str(on))

    def _handle_sql_object_condition(self, on: Any, builder: "SQLBuilderProtocol") -> exp.Expression:
        """Handle SQL object conditions with parameter binding."""
        expression = getattr(on, "expression", None)
        if expression is not None and isinstance(expression, exp.Expression):
            # Merge parameters from SQL object into builder
            if hasattr(on, "parameters") and hasattr(builder, "add_parameter"):
                sql_parameters = getattr(on, "parameters", {})
                for param_name, param_value in sql_parameters.items():
                    builder.add_parameter(param_value, name=param_name)
            return cast("exp.Expression", expression)
        # If expression is None, fall back to parsing the raw SQL
        sql_text = getattr(on, "sql", "")
        # Merge parameters even when parsing raw SQL
        if hasattr(on, "parameters") and hasattr(builder, "add_parameter"):
            sql_parameters = getattr(on, "parameters", {})
            for param_name, param_value in sql_parameters.items():
                builder.add_parameter(param_value, name=param_name)
        parsed_expr = exp.maybe_parse(sql_text)
        return parsed_expr if parsed_expr is not None else exp.condition(str(sql_text))

    def _create_join_expression(
        self, table_expr: exp.Expression, on_expr: Optional[exp.Expression], join_type: str
    ) -> exp.Join:
        """Create the appropriate JOIN expression based on join type."""
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

    def _apply_lateral_modifier(self, join_expr: exp.Join) -> None:
        """Apply LATERAL modifier to the join expression."""
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

    def lateral_join(
        self,
        table: Union[str, exp.Expression, Any],
        on: Optional[Union[str, exp.Expression, "SQL"]] = None,
        alias: Optional[str] = None,
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
                sql.select("u.name", "arr.value")
                .from_("users u")
                .lateral_join("UNNEST(u.tags)", alias="arr")
            )
            ```
        """
        return self.join(table, on=on, alias=alias, join_type="INNER", lateral=True)

    def left_lateral_join(
        self,
        table: Union[str, exp.Expression, Any],
        on: Optional[Union[str, exp.Expression, "SQL"]] = None,
        alias: Optional[str] = None,
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

    def cross_lateral_join(self, table: Union[str, exp.Expression, Any], alias: Optional[str] = None) -> Self:
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
            sql.select("users.name", "posts.title")
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
        self._table: Optional[Union[str, exp.Expression]] = None
        self._condition: Optional[exp.Expression] = None
        self._alias: Optional[str] = None

    def __call__(self, table: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
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

    def on(self, condition: Union[str, exp.Expression]) -> exp.Expression:
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
            parsed: Optional[exp.Expression] = exp.maybe_parse(condition)
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
