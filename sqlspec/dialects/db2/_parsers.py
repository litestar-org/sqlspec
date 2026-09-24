"""Db2 AST normalisation helpers."""

from sqlglot import exp

__all__ = ("normalize_db2_expression",)

_POSSTR_ARGUMENT_COUNT = 2


def _posstr_to_str_position(node: exp.Expr) -> exp.Expr:
    if (
        isinstance(node, exp.Anonymous)
        and str(node.this).upper() == "POSSTR"
        and len(node.expressions) == _POSSTR_ARGUMENT_COUNT
    ):
        haystack, needle = node.expressions
        return exp.StrPosition(this=haystack, substr=needle)
    return node


def normalize_db2_expression(expression: exp.Expr) -> exp.Expr:
    """Rewrite Db2-only function spellings into canonical sqlglot expressions.

    ``POSSTR(haystack, needle)`` becomes ``exp.StrPosition``.

    Args:
        expression: Expression parsed from Db2 SQL.

    Returns:
        The normalised expression.
    """
    return expression.transform(_posstr_to_str_position, copy=False)
