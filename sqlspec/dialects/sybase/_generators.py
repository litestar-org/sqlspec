"""Sybase ASE and SAP SQL Anywhere render handlers layered onto per-instance sqlglot generator dispatch tables."""

from collections.abc import Callable
from typing import Any, Final, cast

from sqlglot import exp, generator

from sqlspec.dialects.sybase._transforms import (
    clone_with_detached_pagination,
    is_literal_integer_limit,
    render_char_length,
    render_charindex,
    render_ilike,
)

__all__ = (
    "SQLANYWHERE_DEFAULT_TYPE_LENGTHS",
    "SQLANYWHERE_TRANSFORMS",
    "SQLANYWHERE_TYPE_MAPPING",
    "SYBASE_DEFAULT_TYPE_LENGTHS",
    "SYBASE_TRANSFORMS",
    "SYBASE_TYPE_MAPPING",
    "char_length_sql",
    "charindex_sql",
    "ilike_sql",
    "sqlanywhere_create_sql",
    "sqlanywhere_current_timestamp_sql",
    "sqlanywhere_datatype_sql",
    "sqlanywhere_dispatch",
    "sqlanywhere_drop_sql",
    "sqlanywhere_select_sql",
    "sqlanywhere_set_operation_sql",
    "sybase_autoincrement_sql",
    "sybase_columndef_sql",
    "sybase_current_timestamp_sql",
    "sybase_datatype_sql",
    "sybase_dispatch",
    "sybase_generated_identity_sql",
    "sybase_returning_sql",
    "sybase_select_sql",
    "sybase_set_operation_sql",
)

SYBASE_TYPE_MAPPING: Final[dict[exp.DType, str]] = {
    **generator.Generator.TYPE_MAPPING,
    exp.DType.BOOLEAN: "BIT",
    exp.DType.BIT: "BIT",
    exp.DType.BLOB: "IMAGE",
    exp.DType.IMAGE: "IMAGE",
    exp.DType.TEXT: "TEXT",
    exp.DType.JSON: "TEXT",
    exp.DType.JSONB: "TEXT",
    exp.DType.UUID: "VARCHAR(36)",
    exp.DType.VARCHAR: "VARCHAR",
    exp.DType.NVARCHAR: "UNIVARCHAR",
    exp.DType.CHAR: "CHAR",
    exp.DType.NCHAR: "UNICHAR",
    exp.DType.BINARY: "BINARY",
    exp.DType.VARBINARY: "VARBINARY",
    exp.DType.TIMESTAMP: "BIGDATETIME",
    exp.DType.TIMESTAMPTZ: "BIGDATETIME",
    exp.DType.TIMESTAMPLTZ: "BIGDATETIME",
    exp.DType.TIMESTAMPNTZ: "BIGDATETIME",
    exp.DType.DATETIME: "BIGDATETIME",
    exp.DType.DATETIME2: "BIGDATETIME",
    exp.DType.DATE: "DATE",
    exp.DType.TIME: "BIGTIME",
    exp.DType.TIMETZ: "BIGTIME",
    exp.DType.TINYINT: "TINYINT",
    exp.DType.SMALLINT: "SMALLINT",
    exp.DType.INT: "INT",
    exp.DType.BIGINT: "BIGINT",
    exp.DType.FLOAT: "FLOAT",
    exp.DType.DOUBLE: "DOUBLE PRECISION",
    exp.DType.DECIMAL: "DECIMAL",
}

SYBASE_DEFAULT_TYPE_LENGTHS: Final[dict[exp.DType, str]] = {
    exp.DType.VARCHAR: "VARCHAR(16384)",
    exp.DType.NVARCHAR: "UNIVARCHAR(8192)",
    exp.DType.VARBINARY: "VARBINARY(16384)",
}

SQLANYWHERE_TYPE_MAPPING: Final[dict[exp.DType, str]] = {
    **generator.Generator.TYPE_MAPPING,
    exp.DType.BOOLEAN: "BIT",
    exp.DType.BIT: "BIT",
    exp.DType.BLOB: "LONG BINARY",
    exp.DType.IMAGE: "LONG BINARY",
    exp.DType.TEXT: "LONG VARCHAR",
    exp.DType.JSON: "LONG VARCHAR",
    exp.DType.JSONB: "LONG VARCHAR",
    exp.DType.UUID: "UNIQUEIDENTIFIER",
    exp.DType.VARCHAR: "VARCHAR",
    exp.DType.NVARCHAR: "NVARCHAR",
    exp.DType.CHAR: "CHAR",
    exp.DType.NCHAR: "NCHAR",
    exp.DType.BINARY: "BINARY",
    exp.DType.VARBINARY: "VARBINARY",
    exp.DType.TIMESTAMP: "TIMESTAMP",
    exp.DType.TIMESTAMPNTZ: "TIMESTAMP",
    exp.DType.DATETIME: "TIMESTAMP",
    exp.DType.DATETIME2: "TIMESTAMP",
    exp.DType.TIMESTAMPTZ: "TIMESTAMP WITH TIME ZONE",
    exp.DType.TIMESTAMPLTZ: "TIMESTAMP WITH TIME ZONE",
    exp.DType.DATE: "DATE",
    exp.DType.TIME: "TIME",
    exp.DType.TIMETZ: "TIME",
    exp.DType.TINYINT: "TINYINT",
    exp.DType.SMALLINT: "SMALLINT",
    exp.DType.INT: "INT",
    exp.DType.BIGINT: "BIGINT",
    exp.DType.FLOAT: "DOUBLE",
    exp.DType.DOUBLE: "DOUBLE",
    exp.DType.DECIMAL: "DECIMAL",
}

SQLANYWHERE_DEFAULT_TYPE_LENGTHS: Final[dict[exp.DType, str]] = {
    exp.DType.VARCHAR: "VARCHAR(32767)",
    exp.DType.NVARCHAR: "NVARCHAR(32767)",
    exp.DType.VARBINARY: "VARBINARY(32767)",
}

_BOOLEAN_TYPES: Final[frozenset[exp.DType]] = frozenset({exp.DType.BOOLEAN, exp.DType.BIT})


def sybase_select_sql(gen: "generator.Generator", expression: exp.Select) -> str:
    """Render a Sybase ASE SELECT with copy-on-write TOP or ROWS LIMIT/OFFSET pagination."""
    limit_arg = expression.args.get("limit")
    offset_arg = expression.args.get("offset")
    if offset_arg is None and is_literal_integer_limit(limit_arg):
        cloned = expression.copy()
        return generator.Generator.select_sql(gen, cloned)
    detached, limit_node, offset_node = clone_with_detached_pagination(expression)
    base_sql = generator.Generator.select_sql(gen, cast("exp.Select", detached))
    tail = _render_sybase_pagination_tail(gen, limit_node, offset_node)
    return f"{base_sql} {tail}" if tail else base_sql


def sybase_set_operation_sql(gen: "generator.Generator", expression: exp.SetOperation) -> str:
    """Render Sybase ASE set operations with copy-on-write ROWS LIMIT/OFFSET pagination."""
    detached, limit_node, offset_node = clone_with_detached_pagination(expression)
    base_sql = gen.set_operations(cast("exp.SetOperation", detached))
    tail = _render_sybase_pagination_tail(gen, limit_node, offset_node)
    return f"{base_sql} {tail}" if tail else base_sql


def sybase_datatype_sql(gen: "generator.Generator", expression: exp.DataType) -> str:
    """Render a data type using Sybase ASE type names and MAX / nullable BIT rules."""
    if _is_max_length_datatype(expression):
        if expression.this in {exp.DType.NVARCHAR, exp.DType.NCHAR}:
            return "UNITEXT"
        if expression.this in {exp.DType.VARBINARY, exp.DType.BINARY, exp.DType.BLOB}:
            return "IMAGE"
        return "TEXT"
    if expression.this in _BOOLEAN_TYPES and _is_explicitly_nullable_columndef(expression.parent):
        return "TINYINT"
    type_str = SYBASE_TYPE_MAPPING.get(expression.this)
    if not type_str:
        return generator.Generator.datatype_sql(gen, expression)
    if expression.expressions:
        params = ", ".join(gen.sql(e) for e in expression.expressions)
        return f"{type_str}({params})"
    return SYBASE_DEFAULT_TYPE_LENGTHS.get(expression.this, type_str)


def sybase_columndef_sql(gen: "generator.Generator", expression: exp.ColumnDef) -> str:
    """Render a Sybase ASE column definition."""
    return generator.Generator.columndef_sql(gen, expression)


def sybase_generated_identity_sql(
    gen: "generator.Generator", expression: exp.GeneratedAsIdentityColumnConstraint
) -> str:
    """Render GeneratedAsIdentityColumnConstraint as bare Sybase ASE IDENTITY."""
    _ = (gen, expression)
    return "IDENTITY"


def sybase_autoincrement_sql(gen: "generator.Generator", expression: exp.AutoIncrementColumnConstraint) -> str:
    """Render AutoIncrementColumnConstraint as bare Sybase ASE IDENTITY."""
    _ = (gen, expression)
    return "IDENTITY"


def sybase_current_timestamp_sql(gen: "generator.Generator", expression: exp.CurrentTimestamp) -> str:
    """Render CurrentTimestamp as GETUTCDATE()."""
    _ = (gen, expression)
    return "GETUTCDATE()"


def sybase_returning_sql(gen: "generator.Generator", expression: exp.Returning) -> str:
    """Omit RETURNING / OUTPUT clauses in Sybase ASE."""
    _ = expression
    gen.unsupported("Sybase ASE does not support RETURNING or OUTPUT clauses")
    return ""


def sqlanywhere_select_sql(gen: "generator.Generator", expression: exp.Select) -> str:
    """Render a SAP SQL Anywhere SELECT with copy-on-write trailing LIMIT/OFFSET pagination."""
    detached, limit_node, offset_node = clone_with_detached_pagination(expression)
    base_sql = generator.Generator.select_sql(gen, cast("exp.Select", detached))
    tail = _render_sqlanywhere_pagination_tail(gen, limit_node, offset_node)
    return f"{base_sql} {tail}" if tail else base_sql


def sqlanywhere_set_operation_sql(gen: "generator.Generator", expression: exp.SetOperation) -> str:
    """Render SAP SQL Anywhere set operations with copy-on-write trailing LIMIT/OFFSET pagination."""
    detached, limit_node, offset_node = clone_with_detached_pagination(expression)
    base_sql = gen.set_operations(cast("exp.SetOperation", detached))
    tail = _render_sqlanywhere_pagination_tail(gen, limit_node, offset_node)
    return f"{base_sql} {tail}" if tail else base_sql


def sqlanywhere_datatype_sql(gen: "generator.Generator", expression: exp.DataType) -> str:
    """Render a data type using SAP SQL Anywhere type names."""
    if _is_max_length_datatype(expression):
        if expression.this in {exp.DType.NVARCHAR, exp.DType.NCHAR}:
            return "LONG NVARCHAR"
        if expression.this in {exp.DType.VARBINARY, exp.DType.BINARY, exp.DType.BLOB}:
            return "LONG BINARY"
        return "LONG VARCHAR"
    type_str = SQLANYWHERE_TYPE_MAPPING.get(expression.this)
    if not type_str:
        return generator.Generator.datatype_sql(gen, expression)
    if expression.expressions:
        params = ", ".join(gen.sql(e) for e in expression.expressions)
        return f"{type_str}({params})"
    return SQLANYWHERE_DEFAULT_TYPE_LENGTHS.get(expression.this, type_str)


def sqlanywhere_create_sql(gen: "generator.Generator", expression: exp.Create) -> str:
    """Render CREATE statements using native SQL Anywhere IF NOT EXISTS syntax."""
    return generator.Generator.create_sql(gen, expression)


def sqlanywhere_drop_sql(gen: "generator.Generator", expression: exp.Drop) -> str:
    """Render DROP statements using native SQL Anywhere IF EXISTS syntax."""
    return generator.Generator.drop_sql(gen, expression)


def sqlanywhere_current_timestamp_sql(gen: "generator.Generator", expression: exp.CurrentTimestamp) -> str:
    """Render CurrentTimestamp as CURRENT UTC TIMESTAMP."""
    _ = (gen, expression)
    return "CURRENT UTC TIMESTAMP"


def char_length_sql(gen: "generator.Generator", expression: exp.Length) -> str:
    """Render exp.Length as CHAR_LENGTH(this)."""
    return render_char_length(gen, expression)


def charindex_sql(gen: "generator.Generator", expression: exp.StrPosition) -> str:
    """Render exp.StrPosition as CHARINDEX(substr, this[, position])."""
    return render_charindex(gen, expression)


def ilike_sql(gen: "generator.Generator", expression: exp.ILike) -> str:
    """Render exp.ILike as LOWER(this) LIKE LOWER(expression)."""
    return render_ilike(gen, expression)


SYBASE_TRANSFORMS: Final[dict[type[exp.Expr], Callable[[Any, Any], str]]] = {
    exp.Select: sybase_select_sql,
    exp.Union: sybase_set_operation_sql,
    exp.Intersect: sybase_set_operation_sql,
    exp.Except: sybase_set_operation_sql,
    exp.DataType: sybase_datatype_sql,
    exp.ColumnDef: sybase_columndef_sql,
    exp.GeneratedAsIdentityColumnConstraint: sybase_generated_identity_sql,
    exp.AutoIncrementColumnConstraint: sybase_autoincrement_sql,
    exp.CurrentTimestamp: sybase_current_timestamp_sql,
    exp.Returning: sybase_returning_sql,
    exp.Length: char_length_sql,
    exp.StrPosition: charindex_sql,
    exp.ILike: ilike_sql,
}

SQLANYWHERE_TRANSFORMS: Final[dict[type[exp.Expr], Callable[[Any, Any], str]]] = {
    exp.Select: sqlanywhere_select_sql,
    exp.Union: sqlanywhere_set_operation_sql,
    exp.Intersect: sqlanywhere_set_operation_sql,
    exp.Except: sqlanywhere_set_operation_sql,
    exp.DataType: sqlanywhere_datatype_sql,
    exp.Create: sqlanywhere_create_sql,
    exp.Drop: sqlanywhere_drop_sql,
    exp.CurrentTimestamp: sqlanywhere_current_timestamp_sql,
    exp.Length: char_length_sql,
    exp.StrPosition: charindex_sql,
    exp.ILike: ilike_sql,
}

_sybase_overlay_cache: "tuple[dict[type[exp.Expr], Callable[..., str]], dict[type[exp.Expr], Callable[..., str]]] | None" = None
_sqlanywhere_overlay_cache: "tuple[dict[type[exp.Expr], Callable[..., str]], dict[type[exp.Expr], Callable[..., str]]] | None" = None


def sybase_dispatch(base: "dict[type[exp.Expr], Callable[..., str]]") -> "dict[type[exp.Expr], Callable[..., str]]":
    """Return the Sybase ASE dispatch table layered over a root generator dispatch table."""
    global _sybase_overlay_cache
    cache = _sybase_overlay_cache
    if cache is not None and cache[0] is base:
        return cache[1]
    overlay = {**base, **SYBASE_TRANSFORMS}
    _sybase_overlay_cache = (base, overlay)
    return overlay


def sqlanywhere_dispatch(
    base: "dict[type[exp.Expr], Callable[..., str]]",
) -> "dict[type[exp.Expr], Callable[..., str]]":
    """Return the SAP SQL Anywhere dispatch table layered over a root generator dispatch table."""
    global _sqlanywhere_overlay_cache
    cache = _sqlanywhere_overlay_cache
    if cache is not None and cache[0] is base:
        return cache[1]
    overlay = {**base, **SQLANYWHERE_TRANSFORMS}
    _sqlanywhere_overlay_cache = (base, overlay)
    return overlay


def _is_max_length_datatype(expression: exp.DataType) -> bool:
    if len(expression.expressions) != 1:
        return False
    first = expression.expressions[0]
    return str(first).upper() == "MAX" or (isinstance(first, exp.DataTypeParam) and str(first.this).upper() == "MAX")


def _is_explicitly_nullable_columndef(parent: exp.Expr | None) -> bool:
    if not isinstance(parent, exp.ColumnDef):
        return False
    for constraint in parent.constraints:
        kind = constraint.kind
        if isinstance(kind, exp.NotNullColumnConstraint) and bool(kind.args.get("allow_null")):
            return True
    return False


def _render_sybase_pagination_tail(
    gen: "generator.Generator", limit_node: exp.Limit | None, offset_node: exp.Offset | None
) -> str:
    limit_sql = gen.sql(limit_node, "expression") if limit_node is not None else ""
    offset_sql = gen.sql(offset_node, "expression") if offset_node is not None else ""
    if limit_sql and offset_sql:
        return f"ROWS LIMIT {limit_sql} OFFSET {offset_sql}"
    if limit_sql:
        return f"ROWS LIMIT {limit_sql}"
    if offset_sql:
        return f"ROWS OFFSET {offset_sql}"
    return ""


def _render_sqlanywhere_pagination_tail(
    gen: "generator.Generator", limit_node: exp.Limit | None, offset_node: exp.Offset | None
) -> str:
    limit_sql = gen.sql(limit_node, "expression") if limit_node is not None else ""
    offset_sql = gen.sql(offset_node, "expression") if offset_node is not None else ""
    if limit_sql and offset_sql:
        return f"LIMIT {limit_sql} OFFSET {offset_sql}"
    if limit_sql:
        return f"LIMIT {limit_sql}"
    if offset_sql:
        return f"OFFSET {offset_sql}"
    return ""
