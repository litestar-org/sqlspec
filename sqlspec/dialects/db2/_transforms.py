"""Db2 render helper functions."""

import re
from typing import Any, Final

from sqlglot import exp

__all__ = (
    "add_sysibm_dual",
    "render_anonymous",
    "render_concat",
    "render_date_add",
    "render_duration_amount",
    "render_ilike",
    "render_mod",
    "render_posstr",
    "render_varchar_format",
)


def add_sysibm_dual(expression: exp.Select) -> exp.Select:
    """Add FROM SYSIBM.SYSDUMMY1 to SELECT statements lacking a FROM clause."""
    if expression.args.get("from_") is None:
        expression = expression.copy()
        expression.set(
            "from_", exp.From(this=exp.Table(this=exp.to_identifier("SYSDUMMY1"), db=exp.to_identifier("SYSIBM")))
        )
    return expression


_EXPECTED_DATEADD_ARGS_LEN = 3
_DURATION_LITERAL_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\s*([+-]?\d+(?:\.\d+)?)\s*([A-Za-z]+)?\s*$")
_ATOMIC_DURATION_OPERANDS: Final[tuple[type[exp.Expr], ...]] = (
    exp.Column,
    exp.Placeholder,
    exp.Parameter,
    exp.Paren,
    exp.Func,
)


def render_duration_amount(generator: Any, node: exp.Expr) -> "tuple[str, str | None]":
    """Render the amount of a Db2 labeled duration.

    A string literal holding a number, optionally followed by a unit
    (``'1'``, ``'1 day'``), renders as the bare number together with the
    embedded unit. Any other string literal is reported as unsupported and
    rendered as a quoted literal. Other operands render unchanged when they are
    a number, column, placeholder, function call or parenthesised expression,
    and are parenthesised otherwise.

    Args:
        generator: Generator rendering the expression.
        node: Duration amount expression.

    Returns:
        The rendered amount and the unit embedded in a string literal, if any.
    """
    if isinstance(node, exp.Literal):
        if not node.is_string:
            return generator.sql(node), None
        match = _DURATION_LITERAL_PATTERN.match(node.name)
        if match:
            unit = match.group(2)
            return match.group(1), unit.upper() if unit else None
        generator.unsupported("Db2 labeled durations need a numeric amount")
        return generator.sql(node), None
    rendered = generator.sql(node)
    if isinstance(node, _ATOMIC_DURATION_OPERANDS):
        return rendered, None
    return f"({rendered})", None


def render_anonymous(generator: Any, expression: exp.Anonymous) -> str:
    """Transform anonymous functions such as DATEADD into Db2 syntax."""
    if expression.this.upper() == "DATEADD" and len(expression.expressions) == _EXPECTED_DATEADD_ARGS_LEN:
        unit = generator.sql(expression.expressions[0]).upper()
        amount = generator.sql(expression.expressions[1])
        this = generator.sql(expression.expressions[2])
        op = "+"
        if amount.startswith("-"):
            op = "-"
            amount = amount[1:].lstrip()
        return f"{this} {op} {amount} {unit}"
    return str(generator.anonymous_sql(expression))


def render_date_add(generator: Any, expression: exp.DateAdd | exp.DateSub | exp.DatetimeAdd | exp.DatetimeSub) -> str:
    """Transform date addition and subtraction to Db2 labeled duration syntax."""
    this = generator.sql(expression, "this")
    unit = expression.args.get("unit")
    unit_str = generator.sql(unit).upper() if unit else "DAY"
    amount = expression.expression
    if isinstance(amount, exp.Interval):
        amount_sql = generator.sql(amount.this)
        interval_unit = amount.args.get("unit")
        if interval_unit:
            unit_str = generator.sql(interval_unit).upper()
    else:
        amount_sql = generator.sql(amount)

    if isinstance(amount, exp.Literal) and amount.is_string:
        try:
            val = int(amount.name)
            amount_sql = str(val)
        except ValueError:
            amount_sql = amount.name

    op = "-" if isinstance(expression, (exp.DateSub, exp.DatetimeSub)) else "+"
    if amount_sql.startswith("-"):
        amount_sql = amount_sql[1:].lstrip()
        op = "+" if op == "-" else "-"

    return f"{this} {op} {amount_sql} {unit_str}"


def render_concat(generator: Any, expression: exp.Concat) -> str:
    """Render Concat expressions using Db2 string concatenation operator."""
    return " || ".join(generator.sql(e) for e in expression.expressions)


def render_mod(generator: Any, expression: exp.Mod) -> str:
    """Render Mod expressions as Db2 MOD(this, expression) function."""
    this = generator.sql(expression, "this")
    exp_arg = generator.sql(expression, "expression")
    return f"MOD({this}, {exp_arg})"


def render_ilike(generator: Any, expression: exp.ILike) -> str:
    """Render ILike expressions using LOWER(this) LIKE LOWER(expression)."""
    this = generator.sql(expression, "this")
    exp_arg = generator.sql(expression, "expression")
    return f"LOWER({this}) LIKE LOWER({exp_arg})"


def render_posstr(generator: Any, expression: exp.StrPosition) -> str:
    """Map string position functions to Db2 POSSTR(haystack, needle)."""
    this = generator.sql(expression, "this")
    substr = generator.sql(expression, "substr")
    return f"POSSTR({this}, {substr})"


def render_varchar_format(generator: Any, expression: exp.TimeToStr | exp.ToChar) -> str:
    """Map datetime formatting functions to Db2 VARCHAR_FORMAT(ts, fmt)."""
    this = generator.sql(expression, "this")
    if isinstance(expression, exp.TimeToStr):
        fmt = generator.format_time(expression)
    else:
        fmt = generator.sql(expression, "format")
    if fmt:
        return f"VARCHAR_FORMAT({this}, {fmt})"
    return f"VARCHAR_FORMAT({this})"
