"""Shared AST transformation and rendering helpers for Sybase ASE and SAP SQL Anywhere dialects."""

from typing import Any

from sqlglot import exp

__all__ = (
    "clone_with_detached_pagination",
    "is_literal_integer_limit",
    "render_char_length",
    "render_charindex",
    "render_ilike",
)


def is_literal_integer_limit(limit_expr: Any) -> bool:
    """Return True when limit_expr is a literal integer exp.Limit without parameters."""
    if not isinstance(limit_expr, exp.Limit):
        return False
    count_expr = limit_expr.expression
    return isinstance(count_expr, exp.Literal) and not count_expr.is_string


def clone_with_detached_pagination(expression: exp.Expr) -> "tuple[exp.Expr, exp.Limit | None, exp.Offset | None]":
    """Return a shallow copy of expression with limit and offset removed if present."""
    limit_arg = expression.args.get("limit")
    offset_arg = expression.args.get("offset")
    if limit_arg is None and offset_arg is None:
        return expression, None, None
    cloned = expression.copy()
    cloned.set("limit", None)
    cloned.set("offset", None)
    limit_node = limit_arg if isinstance(limit_arg, exp.Limit) else None
    offset_node = offset_arg if isinstance(offset_arg, exp.Offset) else None
    return cloned, limit_node, offset_node


def render_char_length(generator: Any, expression: exp.Length) -> str:
    """Render exp.Length as CHAR_LENGTH(this)."""
    return f"CHAR_LENGTH({generator.sql(expression, 'this')})"


def render_charindex(generator: Any, expression: exp.StrPosition) -> str:
    """Render exp.StrPosition as CHARINDEX(substr, this[, position])."""
    this_sql = generator.sql(expression, "this")
    substr_sql = generator.sql(expression, "substr")
    position = expression.args.get("position")
    if position is not None:
        return f"CHARINDEX({substr_sql}, {this_sql}, {generator.sql(position)})"
    return f"CHARINDEX({substr_sql}, {this_sql})"


def render_ilike(generator: Any, expression: exp.ILike) -> str:
    """Render case-insensitive ILIKE using LOWER(this) LIKE LOWER(expression)."""
    return f"LOWER({generator.sql(expression, 'this')}) LIKE LOWER({generator.sql(expression, 'expression')})"
