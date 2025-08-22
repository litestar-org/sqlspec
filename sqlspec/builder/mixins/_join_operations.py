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
    from sqlspec.builder._column import ColumnExpression
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

    def __init__(self, join_type: str) -> None:
        """Initialize the join builder.

        Args:
            join_type: Type of join (inner, left, right, full, cross)
        """
        self._join_type = join_type.upper()
        self._table: Optional[Union[str, exp.Expression]] = None
        self._condition: Optional[exp.Expression] = None
        self._alias: Optional[str] = None

    def __eq__(self, other: object) -> "ColumnExpression":  # type: ignore[override]
        """Equal to (==) - not typically used but needed for type consistency."""
        from sqlspec.builder._column import ColumnExpression

        # JoinBuilder doesn't have a direct expression, so this is a placeholder
        # In practice, this shouldn't be called as joins are used differently
        placeholder_expr = exp.Literal.string(f"join_{self._join_type.lower()}")
        if other is None:
            return ColumnExpression(exp.Is(this=placeholder_expr, expression=exp.Null()))
        return ColumnExpression(exp.EQ(this=placeholder_expr, expression=exp.convert(other)))

    def __hash__(self) -> int:
        """Make JoinBuilder hashable."""
        return hash(id(self))

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
        if self._join_type == "INNER JOIN":
            return exp.Join(this=table_expr, on=condition_expr)
        if self._join_type == "LEFT JOIN":
            return exp.Join(this=table_expr, on=condition_expr, side="LEFT")
        if self._join_type == "RIGHT JOIN":
            return exp.Join(this=table_expr, on=condition_expr, side="RIGHT")
        if self._join_type == "FULL JOIN":
            return exp.Join(this=table_expr, on=condition_expr, side="FULL", kind="OUTER")
        if self._join_type == "CROSS JOIN":
            # CROSS JOIN doesn't use ON condition
            return exp.Join(this=table_expr, kind="CROSS")
        return exp.Join(this=table_expr, on=condition_expr)
