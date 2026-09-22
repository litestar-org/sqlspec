"""Db2 dialect AST transformations."""

from typing import Any

from sqlglot import exp

__all__ = (
    "_add_sysibm_dual",
    "_transform_anonymous",
    "_transform_date_add",
    "_transform_posstr",
    "_transform_varchar_format",
)


def _add_sysibm_dual(expression: exp.Select) -> exp.Select:
    """Add FROM SYSIBM.SYSDUMMY1 to SELECT statements lacking a FROM clause."""
    if expression.args.get("from_") is None:
        expression = expression.copy()
        expression.set(
            "from_", exp.From(this=exp.Table(this=exp.to_identifier("SYSDUMMY1"), db=exp.to_identifier("SYSIBM")))
        )
    return expression


_EXPECTED_DATEADD_ARGS_LEN = 3


def _transform_anonymous(generator: Any, expression: exp.Anonymous) -> str:
    """Transform anonymous functions such as DATEADD into Db2 syntax."""
    if expression.this.upper() == "DATEADD" and len(expression.expressions) == _EXPECTED_DATEADD_ARGS_LEN:
        unit = generator.sql(expression.expressions[0]).upper()
        amount = generator.sql(expression.expressions[1])
        this = generator.sql(expression.expressions[2])
        return f"{this} + {amount} {unit}"
    return str(generator.function_fallback_sql(expression))


def _transform_date_add(
    generator: Any,
    expression: exp.DateAdd | exp.DateSub | exp.DatetimeAdd | exp.DatetimeSub,
) -> str:
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
    return f"{this} {op} {amount_sql} {unit_str}"


def _transform_posstr(generator: Any, expression: exp.StrPosition) -> str:
    """Map string position functions to Db2 POSSTR(haystack, needle)."""
    this = generator.sql(expression, "this")
    substr = generator.sql(expression, "substr")
    return f"POSSTR({this}, {substr})"


def _transform_varchar_format(generator: Any, expression: exp.TimeToStr | exp.ToChar) -> str:
    """Map datetime formatting functions to Db2 VARCHAR_FORMAT(ts, fmt)."""
    this = generator.sql(expression, "this")
    if isinstance(expression, exp.TimeToStr):
        fmt = generator.format_time(expression)
    else:
        fmt = generator.sql(expression, "format")
    if fmt:
        return f"VARCHAR_FORMAT({this}, {fmt})"
    return f"VARCHAR_FORMAT({this})"
