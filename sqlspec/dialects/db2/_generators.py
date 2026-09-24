"""Db2 render handlers layered onto a per-instance sqlglot generator dispatch.

Each handler takes explicit ``(generator, expression)`` arguments. ``db2_dispatch``
returns a dispatch table that combines a root generator's handlers with
``DB2_TRANSFORMS``; the Db2 dialect installs it on the generator instances it
creates, so no shared sqlglot class is modified.
"""

from collections.abc import Callable
from typing import Any, Final

from sqlglot import exp, generator

from sqlspec.dialects.db2._transforms import (
    add_sysibm_dual,
    render_anonymous,
    render_concat,
    render_date_add,
    render_ilike,
    render_mod,
    render_posstr,
    render_varchar_format,
)

__all__ = (
    "DB2_TRANSFORMS",
    "DB2_TYPE_MAPPING",
    "anonymous_sql",
    "concat_sql",
    "datatype_sql",
    "date_add_sql",
    "db2_dispatch",
    "ilike_sql",
    "interval_sql",
    "mod_sql",
    "offset_sql",
    "parameter_sql",
    "select_sql",
    "str_position_sql",
    "time_to_str_sql",
)

DB2_TYPE_MAPPING: dict[exp.DType, str] = {
    **generator.Generator.TYPE_MAPPING,
    exp.DType.BOOLEAN: "BOOLEAN",
    exp.DType.BLOB: "BLOB",
    exp.DType.TEXT: "CLOB",
    exp.DType.VARCHAR: "VARCHAR",
    exp.DType.NVARCHAR: "VARGRAPHIC",
    exp.DType.NCHAR: "GRAPHIC",
    exp.DType.TIMESTAMP: "TIMESTAMP",
    exp.DType.TIMESTAMPTZ: "TIMESTAMP",
    exp.DType.TIMESTAMPNTZ: "TIMESTAMP",
    exp.DType.DATE: "DATE",
    exp.DType.TIME: "TIME",
    exp.DType.SMALLINT: "SMALLINT",
    exp.DType.INT: "INTEGER",
    exp.DType.BIGINT: "BIGINT",
    exp.DType.FLOAT: "DOUBLE",
    exp.DType.DOUBLE: "DOUBLE",
    exp.DType.DECIMAL: "DECIMAL",
    exp.DType.BINARY: "BLOB",
    exp.DType.VARBINARY: "BLOB",
}

_DECFLOAT = getattr(exp.DType, "DECFLOAT", None)
if _DECFLOAT is not None:
    DB2_TYPE_MAPPING[_DECFLOAT] = "DECFLOAT"

_DBCLOB = getattr(exp.DType, "DBCLOB", None)
if _DBCLOB is not None:
    DB2_TYPE_MAPPING[_DBCLOB] = "DBCLOB"


def select_sql(generator: "generator.Generator", expression: exp.Select) -> str:
    """Render a SELECT with a Db2 dummy table and FETCH pagination."""
    expression = add_sysibm_dual(expression)
    limit = expression.args.get("limit")
    if isinstance(limit, exp.Limit):
        expression = expression.copy()
        direction = "NEXT" if expression.args.get("offset") else "FIRST"
        fetch = exp.Fetch(direction=direction, count=exp.maybe_copy(limit.expression))
        expression.set("limit", fetch)
    return generator.select_sql(expression)


def offset_sql(generator: "generator.Generator", expression: exp.Offset) -> str:
    """Render OFFSET with the ROWS keyword Db2 requires."""
    return f"{generator.offset_sql(expression)} ROWS"


def datatype_sql(generator: "generator.Generator", expression: exp.DataType) -> str:
    """Render a data type using the Db2 type name."""
    type_str = DB2_TYPE_MAPPING.get(expression.this)
    if type_str:
        if expression.expressions:
            params = ", ".join(generator.sql(e) for e in expression.expressions)
            return f"{type_str}({params})"
        return type_str
    return generator.datatype_sql(expression)


def interval_sql(generator: "generator.Generator", expression: exp.Interval) -> str:
    """Render an interval as a Db2 labeled duration."""
    unit = generator.sql(expression, "unit")
    this = generator.sql(expression, "this")
    if isinstance(expression.this, exp.Literal) and expression.this.is_string:
        this = expression.this.name
    unit_str = f" {unit.upper()}" if unit else ""
    return f"{this}{unit_str}"


def date_add_sql(
    generator: "generator.Generator", expression: exp.DateAdd | exp.DateSub | exp.DatetimeAdd | exp.DatetimeSub
) -> str:
    """Render date arithmetic as a Db2 labeled duration."""
    return render_date_add(generator, expression)


def str_position_sql(generator: "generator.Generator", expression: exp.StrPosition) -> str:
    """Render a string position search with Db2 POSSTR."""
    return render_posstr(generator, expression)


def time_to_str_sql(generator: "generator.Generator", expression: exp.TimeToStr | exp.ToChar) -> str:
    """Render datetime formatting with Db2 VARCHAR_FORMAT."""
    return render_varchar_format(generator, expression)


def anonymous_sql(generator: "generator.Generator", expression: exp.Anonymous) -> str:
    """Render anonymous functions, rewriting DATEADD into a Db2 labeled duration."""
    return render_anonymous(generator, expression)


def parameter_sql(generator: "generator.Generator", expression: exp.Parameter) -> str:
    """Render a parameter as a positional question-mark placeholder."""
    return "?"


def concat_sql(generator: "generator.Generator", expression: exp.Concat) -> str:
    """Render concatenation with the Db2 ``||`` operator."""
    return render_concat(generator, expression)


def mod_sql(generator: "generator.Generator", expression: exp.Mod) -> str:
    """Render modulo as the Db2 MOD function."""
    return render_mod(generator, expression)


def ilike_sql(generator: "generator.Generator", expression: exp.ILike) -> str:
    """Render ILIKE as a case-insensitive LIKE over LOWER()."""
    return render_ilike(generator, expression)


DB2_TRANSFORMS: Final[dict[type[exp.Expr], Callable[[Any, Any], str]]] = {
    exp.Select: select_sql,
    exp.Offset: offset_sql,
    exp.DataType: datatype_sql,
    exp.Interval: interval_sql,
    exp.DateAdd: date_add_sql,
    exp.DateSub: date_add_sql,
    exp.DatetimeAdd: date_add_sql,
    exp.DatetimeSub: date_add_sql,
    exp.StrPosition: str_position_sql,
    exp.TimeToStr: time_to_str_sql,
    exp.ToChar: time_to_str_sql,
    exp.Anonymous: anonymous_sql,
    exp.Parameter: parameter_sql,
    exp.Concat: concat_sql,
    exp.Mod: mod_sql,
    exp.ILike: ilike_sql,
}

_overlay_cache: "tuple[dict[type[exp.Expr], Callable[..., str]], dict[type[exp.Expr], Callable[..., str]]] | None" = (
    None
)


def db2_dispatch(base: "dict[type[exp.Expr], Callable[..., str]]") -> "dict[type[exp.Expr], Callable[..., str]]":
    """Return the Db2 dispatch table layered over a root generator dispatch table.

    The combined table is cached for as long as ``base`` is the same object, so
    a rebuilt root dispatch table produces a fresh overlay.

    Args:
        base: Dispatch table of a root sqlglot generator instance.

    Returns:
        A dispatch table whose Db2 handlers take precedence over ``base``.
    """
    global _overlay_cache
    cache = _overlay_cache
    if cache is not None and cache[0] is base:
        return cache[1]
    overlay = {**base, **DB2_TRANSFORMS}
    _overlay_cache = (base, overlay)
    return overlay
