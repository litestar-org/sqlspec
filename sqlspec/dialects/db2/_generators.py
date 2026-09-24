"""Db2 render handlers layered onto a per-instance sqlglot generator dispatch.

Each handler takes explicit ``(generator, expression)`` arguments. ``db2_dispatch``
returns a dispatch table that combines a root generator's handlers with
``DB2_TRANSFORMS``; the Db2 dialect installs it on the generator instances it
creates, so no shared sqlglot class is modified.
"""

from collections.abc import Callable
from typing import Any, Final, cast

from sqlglot import exp, generator

from sqlspec.dialects.db2._parsers import DB2_SPECIAL_REGISTERS, DB2_TAIL_ARG_KEYS
from sqlspec.dialects.db2._transforms import (
    add_sysibm_dual,
    render_anonymous,
    render_concat,
    render_date_add,
    render_duration_amount,
    render_ilike,
    render_mod,
    render_posstr,
    render_varchar_format,
)

__all__ = (
    "DB2_DEFAULT_TYPE_LENGTHS",
    "DB2_TRANSFORMS",
    "DB2_TYPE_MAPPING",
    "anonymous_sql",
    "column_sql",
    "concat_sql",
    "current_date_sql",
    "current_schema_sql",
    "current_time_sql",
    "current_timestamp_sql",
    "current_user_sql",
    "datatype_sql",
    "date_add_sql",
    "db2_dispatch",
    "ilike_sql",
    "interval_sql",
    "mod_sql",
    "offset_sql",
    "parameter_sql",
    "render_statement_tail",
    "select_sql",
    "set_operation_sql",
    "str_position_sql",
    "time_to_str_sql",
)

DB2_TYPE_MAPPING: dict[exp.DType, str] = {
    **generator.Generator.TYPE_MAPPING,
    exp.DType.BOOLEAN: "BOOLEAN",
    exp.DType.BLOB: "BLOB",
    exp.DType.TEXT: "CLOB",
    exp.DType.JSON: "CLOB",
    exp.DType.JSONB: "CLOB",
    exp.DType.UUID: "VARCHAR(36)",
    exp.DType.VARCHAR: "VARCHAR",
    exp.DType.NVARCHAR: "VARGRAPHIC",
    exp.DType.NCHAR: "GRAPHIC",
    exp.DType.TIMESTAMP: "TIMESTAMP",
    exp.DType.TIMESTAMPTZ: "TIMESTAMP",
    exp.DType.TIMESTAMPLTZ: "TIMESTAMP",
    exp.DType.TIMESTAMPNTZ: "TIMESTAMP",
    exp.DType.DATETIME: "TIMESTAMP",
    exp.DType.DATE: "DATE",
    exp.DType.TIME: "TIME",
    exp.DType.TINYINT: "SMALLINT",
    exp.DType.SMALLINT: "SMALLINT",
    exp.DType.INT: "INTEGER",
    exp.DType.BIGINT: "BIGINT",
    exp.DType.FLOAT: "DOUBLE",
    exp.DType.DOUBLE: "DOUBLE",
    exp.DType.DECIMAL: "DECIMAL",
    exp.DType.DECFLOAT: "DECFLOAT",
    exp.DType.BINARY: "BINARY",
    exp.DType.VARBINARY: "VARBINARY",
}

DB2_DEFAULT_TYPE_LENGTHS: Final[dict[exp.DType, str]] = {
    exp.DType.VARCHAR: "VARCHAR(32672)",
    exp.DType.NVARCHAR: "VARGRAPHIC(16336)",
    exp.DType.VARBINARY: "VARBINARY(32672)",
}

_ZONED_TIMESTAMP_TYPES: Final[frozenset[exp.DType]] = frozenset({exp.DType.TIMESTAMPTZ, exp.DType.TIMESTAMPLTZ})


def _detach_statement_tail(expression: "exp.Query") -> "tuple[exp.Query, list[exp.Lock], dict[str, object]]":
    if not expression.args.get("locks") and not any(key in expression.args for key in DB2_TAIL_ARG_KEYS):
        return expression, [], {}
    expression = expression.copy()
    locks = list(expression.args.get("locks") or [])
    tail_args: dict[str, object] = {key: expression.args.get(key) for key in DB2_TAIL_ARG_KEYS}
    expression.set("locks", None)
    for key in DB2_TAIL_ARG_KEYS:
        expression.set(key, None)
    return expression, locks, tail_args


def render_statement_tail(
    generator: "generator.Generator", tail_args: "dict[str, object]", locks: "list[exp.Lock]"
) -> str:
    """Render the Db2 select-statement tail for a query.

    Locks parsed from Db2 SQL keep their ``FOR UPDATE [OF ...]`` spelling. Locks
    from other dialects or the query builder render as the Db2 isolation clause
    ``WITH RS USE AND KEEP UPDATE|SHARE LOCKS``, which is valid on read-only
    cursors. ``NOWAIT``/``WAIT n``, table lock targets and additional locking
    clauses are reported as unsupported and omitted.

    Args:
        generator: Generator rendering the query.
        tail_args: ``sqlspec_db2_*`` args detached from the query.
        locks: Locks detached from the query.

    Returns:
        The tail clauses separated by single spaces, or an empty string.
    """
    lock = locks[0] if locks else None
    if len(locks) > 1:
        generator.unsupported("Db2 accepts one locking clause")
    native_update = lock is not None and bool(lock.args.get("sqlspec_db2_native")) and bool(lock.args.get("update"))
    for candidate in locks:
        wait = candidate.args.get("wait")
        if wait is True or isinstance(wait, exp.Literal):
            generator.unsupported("Db2 has no per-statement NOWAIT/WAIT; set CURRENT LOCK TIMEOUT")
    if lock is not None and lock.expressions and not native_update:
        generator.unsupported("Db2 FOR UPDATE OF takes column names, not tables")

    parts: list[str] = []
    if lock is not None and native_update:
        columns = ", ".join(generator.sql(column) for column in lock.expressions)
        parts.append(f"FOR UPDATE OF {columns}" if columns else "FOR UPDATE")
    if tail_args.get("sqlspec_db2_read_only"):
        parts.append("FOR READ ONLY")
    optimize_rows = tail_args.get("sqlspec_db2_optimize_rows")
    if optimize_rows is not None:
        parts.append(f"OPTIMIZE FOR {optimize_rows} ROWS")
    isolation = tail_args.get("sqlspec_db2_isolation")
    if isolation:
        lock_request = tail_args.get("sqlspec_db2_lock_request")
        parts.append(f"WITH {isolation} USE AND KEEP {lock_request} LOCKS" if lock_request else f"WITH {isolation}")
    elif lock is not None and not native_update:
        lock_request = "UPDATE" if lock.args.get("update") else "SHARE"
        parts.append(f"WITH RS USE AND KEEP {lock_request} LOCKS")
    if tail_args.get("sqlspec_db2_skip_locked") or any(candidate.args.get("wait") is False for candidate in locks):
        parts.append("SKIP LOCKED DATA")
    return " ".join(parts)


def _with_statement_tail(sql: str, tail: str) -> str:
    return f"{sql} {tail}" if tail else sql


def select_sql(generator: "generator.Generator", expression: exp.Select) -> str:
    """Render a SELECT with a Db2 dummy table, FETCH pagination and statement tail."""
    detached, locks, tail_args = _detach_statement_tail(expression)
    select = add_sysibm_dual(cast("exp.Select", detached))
    limit = select.args.get("limit")
    if isinstance(limit, exp.Limit):
        select = select.copy()
        direction = "NEXT" if select.args.get("offset") else "FIRST"
        fetch = exp.Fetch(direction=direction, count=exp.maybe_copy(limit.expression))
        select.set("limit", fetch)
    return _with_statement_tail(generator.select_sql(select), render_statement_tail(generator, tail_args, locks))


def set_operation_sql(generator: "generator.Generator", expression: exp.SetOperation) -> str:
    """Render UNION, INTERSECT or EXCEPT followed by the Db2 statement tail."""
    detached, locks, tail_args = _detach_statement_tail(expression)
    return _with_statement_tail(
        generator.set_operations(cast("exp.SetOperation", detached)), render_statement_tail(generator, tail_args, locks)
    )


def offset_sql(generator: "generator.Generator", expression: exp.Offset) -> str:
    """Render OFFSET with the ROWS keyword Db2 requires."""
    return f"{generator.offset_sql(expression)} ROWS"


def datatype_sql(generator: "generator.Generator", expression: exp.DataType) -> str:
    """Render a data type using the Db2 type name.

    Variable-length types without a length get the Db2 maximum length, and
    zoned timestamps are reported as unsupported because Db2 TIMESTAMP stores
    no offset.
    """
    if expression.this in _ZONED_TIMESTAMP_TYPES:
        generator.unsupported("Db2 TIMESTAMP stores no time zone offset")
    type_str = DB2_TYPE_MAPPING.get(expression.this)
    if not type_str:
        return generator.datatype_sql(expression)
    if expression.expressions:
        params = ", ".join(generator.sql(e) for e in expression.expressions)
        return f"{type_str}({params})"
    return DB2_DEFAULT_TYPE_LENGTHS.get(expression.this, type_str)


def interval_sql(generator: "generator.Generator", expression: exp.Interval) -> str:
    """Render an interval as a Db2 labeled duration."""
    amount, embedded_unit = render_duration_amount(generator, expression.this)
    unit = generator.sql(expression, "unit") or embedded_unit
    return f"{amount} {unit.upper()}" if unit else amount


def current_timestamp_sql(generator: "generator.Generator", expression: exp.CurrentTimestamp) -> str:
    """Render the CURRENT TIMESTAMP special register."""
    precision = generator.sql(expression, "this")
    return f"CURRENT TIMESTAMP({precision})" if precision else "CURRENT TIMESTAMP"


def current_date_sql(generator: "generator.Generator", expression: exp.CurrentDate) -> str:
    """Render the CURRENT DATE special register."""
    return "CURRENT DATE"


def current_time_sql(generator: "generator.Generator", expression: exp.CurrentTime) -> str:
    """Render the CURRENT TIME special register."""
    return "CURRENT TIME"


def current_user_sql(generator: "generator.Generator", expression: exp.CurrentUser) -> str:
    """Render the CURRENT USER special register."""
    return "CURRENT USER"


def current_schema_sql(generator: "generator.Generator", expression: exp.CurrentSchema) -> str:
    """Render the CURRENT SCHEMA special register."""
    return "CURRENT SCHEMA"


def column_sql(generator: "generator.Generator", expression: exp.Column) -> str:
    """Render a column, emitting Db2 special registers verbatim."""
    identifier = expression.this
    if (
        not expression.table
        and isinstance(identifier, exp.Identifier)
        and not identifier.quoted
        and identifier.name.upper() in DB2_SPECIAL_REGISTERS
    ):
        return identifier.name.upper()
    return generator.column_sql(expression)


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
    exp.Union: set_operation_sql,
    exp.Intersect: set_operation_sql,
    exp.Except: set_operation_sql,
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
    exp.CurrentTimestamp: current_timestamp_sql,
    exp.CurrentDate: current_date_sql,
    exp.CurrentTime: current_time_sql,
    exp.CurrentUser: current_user_sql,
    exp.CurrentSchema: current_schema_sql,
    exp.Column: column_sql,
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
