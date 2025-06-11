from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError

if TYPE_CHECKING:
    from sqlspec.statement.builder.protocols import BuilderProtocol

__all__ = ("JoinClauseMixin",)


def _parse_table_and_alias(table_str: str, explicit_alias: Optional[str] = None) -> exp.Table:
    """Parse a table string that may contain an alias using SQLGlot's parser.

    Args:
        table_str: Table string like 'users' or 'users u' or 'users AS u'
        explicit_alias: Explicit alias to use (overrides any alias in table_str)

    Returns:
        exp.Table: Table expression with proper alias handling
    """
    if explicit_alias:
        # If explicit alias provided, use table_str as table name only
        return exp.table_(table_str, alias=explicit_alias)

    # Use SQLGlot's parser to handle table expressions with aliases
    try:
        import sqlglot

        # Parse as FROM clause and extract the table
        parsed = sqlglot.parse_one(f"FROM {table_str}")
        table_expr = parsed.find(exp.Table)
        if table_expr:
            return table_expr
    except Exception:  # noqa: S110
        # Join table parsing failed, will use basic identifier
        pass

    # Fallback: just table name
    return exp.table_(table_str)


class JoinClauseMixin:
    """Mixin providing JOIN clause methods for SELECT builders."""

    def join(
        self,
        table: Union[str, exp.Expression, Any],
        on: Optional[Union[str, exp.Expression]] = None,
        alias: Optional[str] = None,
        join_type: str = "INNER",
    ) -> Any:
        builder = cast("BuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "Cannot add join to a non-SELECT expression."
            raise SQLBuilderError(msg)
        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = _parse_table_and_alias(table, alias)
        elif hasattr(table, "build"):
            # Handle builder objects with build() method
            # Work directly with AST when possible to avoid string parsing
            if hasattr(table, "_expression") and getattr(table, "_expression", None) is not None:
                subquery_exp = exp.paren(table._expression.copy())
                table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
            else:
                # Fallback to string parsing
                subquery = table.build()
                subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=getattr(builder, "dialect", None)))
                table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
            # Parameter merging logic can be added here if needed
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
        return builder

    def inner_join(
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression], alias: Optional[str] = None
    ) -> Any:
        return self.join(table, on, alias, "INNER")

    def left_join(
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression], alias: Optional[str] = None
    ) -> Any:
        return self.join(table, on, alias, "LEFT")

    def right_join(
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression], alias: Optional[str] = None
    ) -> Any:
        return self.join(table, on, alias, "RIGHT")

    def full_join(
        self, table: Union[str, exp.Expression, Any], on: Union[str, exp.Expression], alias: Optional[str] = None
    ) -> Any:
        return self.join(table, on, alias, "FULL")

    def cross_join(self, table: Union[str, exp.Expression, Any], alias: Optional[str] = None) -> Any:
        builder = cast("BuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "Cannot add cross join to a non-SELECT expression."
            raise SQLBuilderError(msg)
        table_expr: exp.Expression
        if isinstance(table, str):
            table_expr = _parse_table_and_alias(table, alias)
        elif hasattr(table, "build"):
            # Handle builder objects with build() method
            if hasattr(table, "_expression") and getattr(table, "_expression", None) is not None:
                subquery_exp = exp.paren(table._expression.copy())
                table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
            else:
                # Fallback to string parsing
                subquery = table.build()
                subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=getattr(builder, "dialect", None)))
                table_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
        else:
            table_expr = table
        join_expr = exp.Join(this=table_expr, kind="CROSS")
        builder._expression = builder._expression.join(join_expr, copy=False)
        return builder
