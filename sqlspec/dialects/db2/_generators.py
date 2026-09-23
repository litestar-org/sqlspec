"""Db2 dialect generator."""

from typing import Any, Final

from sqlglot import exp, generator

from sqlspec.builder._generation import invalidate_generator_dispatch
from sqlspec.dialects.db2._transforms import (
    _add_sysibm_dual,
    _transform_anonymous,
    _transform_concat,
    _transform_date_add,
    _transform_ilike,
    _transform_mod,
    _transform_posstr,
    _transform_varchar_format,
)

__all__ = ("DB2Generator",)

_DB2_DIALECT_NAME: Final[str] = "DB2"


def _is_db2(gen: Any) -> bool:
    """Return True if generator target dialect is Db2."""
    dialect_class = getattr(gen.dialect, "__class__", None)
    return dialect_class is not None and dialect_class.__name__ == _DB2_DIALECT_NAME


_DB2_TYPE_MAPPING: dict[exp.DType, str] = {
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
    _DB2_TYPE_MAPPING[_DECFLOAT] = "DECFLOAT"

_DBCLOB = getattr(exp.DType, "DBCLOB", None)
if _DBCLOB is not None:
    _DB2_TYPE_MAPPING[_DBCLOB] = "DBCLOB"

_orig_select = generator.Generator.TRANSFORMS.get(exp.Select)
_orig_offset = generator.Generator.TRANSFORMS.get(exp.Offset)
_orig_datatype = generator.Generator.TRANSFORMS.get(exp.DataType)
_orig_interval = generator.Generator.TRANSFORMS.get(exp.Interval)
_orig_dateadd = generator.Generator.TRANSFORMS.get(exp.DateAdd)
_orig_datesub = generator.Generator.TRANSFORMS.get(exp.DateSub)
_orig_datetimeadd = generator.Generator.TRANSFORMS.get(exp.DatetimeAdd)
_orig_datetimesub = generator.Generator.TRANSFORMS.get(exp.DatetimeSub)
_orig_strposition = generator.Generator.TRANSFORMS.get(exp.StrPosition)
_orig_timetostr = generator.Generator.TRANSFORMS.get(exp.TimeToStr)
_orig_tochar = generator.Generator.TRANSFORMS.get(exp.ToChar)
_orig_anonymous = generator.Generator.TRANSFORMS.get(exp.Anonymous)
_orig_parameter = generator.Generator.TRANSFORMS.get(exp.Parameter)
_orig_concat = generator.Generator.TRANSFORMS.get(exp.Concat)
_orig_mod = generator.Generator.TRANSFORMS.get(exp.Mod)
_orig_ilike = generator.Generator.TRANSFORMS.get(exp.ILike)


def _db2_select_transform(gen: Any, expression: exp.Select) -> str:
    """Transform Select nodes for Db2, adding dual table and pagination fetch."""
    if _is_db2(gen):
        expression = _add_sysibm_dual(expression)
        limit = expression.args.get("limit")
        if isinstance(limit, exp.Limit):
            expression = expression.copy()
            direction = "NEXT" if expression.args.get("offset") else "FIRST"
            fetch = exp.Fetch(direction=direction, count=exp.maybe_copy(limit.expression))
            expression.set("limit", fetch)
    if _orig_select:
        return str(_orig_select(gen, expression))
    return str(gen.select_sql(expression))


def _db2_offset_transform(gen: Any, expression: exp.Offset) -> str:
    """Transform Offset nodes to include ROWS suffix required by Db2."""
    if _is_db2(gen):
        return f"{gen.offset_sql(expression)} ROWS"
    if _orig_offset:
        return str(_orig_offset(gen, expression))
    return str(gen.offset_sql(expression))


def _db2_datatype_transform(gen: Any, expression: exp.DataType) -> str:
    """Map generic DataType expressions to Db2 native type declarations."""
    if _is_db2(gen):
        type_str = _DB2_TYPE_MAPPING.get(expression.this)
        if type_str:
            if expression.expressions:
                params = ", ".join(gen.sql(e) for e in expression.expressions)
                return f"{type_str}({params})"
            return type_str
    if _orig_datatype:
        return str(_orig_datatype(gen, expression))
    return str(gen.datatype_sql(expression))


def _db2_interval_transform(gen: Any, expression: exp.Interval) -> str:
    """Render Interval expressions in Db2 labeled duration syntax."""
    if _is_db2(gen):
        unit = gen.sql(expression, "unit")
        this = gen.sql(expression, "this")
        if isinstance(expression.this, exp.Literal) and expression.this.is_string:
            this = expression.this.name
        unit_str = f" {unit.upper()}" if unit else ""
        return f"{this}{unit_str}"
    if _orig_interval:
        return str(_orig_interval(gen, expression))
    return str(gen.interval_sql(expression))


def _db2_dateadd_transform(gen: Any, expression: exp.DateAdd) -> str:
    """Render DateAdd expressions in Db2 labeled duration syntax."""
    if _is_db2(gen):
        return _transform_date_add(gen, expression)
    if _orig_dateadd:
        return str(_orig_dateadd(gen, expression))
    return str(gen.dateadd_sql(expression))


def _db2_datesub_transform(gen: Any, expression: exp.DateSub) -> str:
    """Render DateSub expressions in Db2 labeled duration syntax."""
    if _is_db2(gen):
        return _transform_date_add(gen, expression)
    if _orig_datesub:
        return str(_orig_datesub(gen, expression))
    return str(gen.datesub_sql(expression))


def _db2_datetimeadd_transform(gen: Any, expression: exp.DatetimeAdd) -> str:
    """Render DatetimeAdd expressions in Db2 labeled duration syntax."""
    if _is_db2(gen):
        return _transform_date_add(gen, expression)
    if _orig_datetimeadd:
        return str(_orig_datetimeadd(gen, expression))
    return str(gen.datetimeadd_sql(expression))


def _db2_datetimesub_transform(gen: Any, expression: exp.DatetimeSub) -> str:
    """Render DatetimeSub expressions in Db2 labeled duration syntax."""
    if _is_db2(gen):
        return _transform_date_add(gen, expression)
    if _orig_datetimesub:
        return str(_orig_datetimesub(gen, expression))
    return str(gen.datetimesub_sql(expression))


def _db2_strposition_transform(gen: Any, expression: exp.StrPosition) -> str:
    """Render StrPosition expressions using Db2 POSSTR."""
    if _is_db2(gen):
        return _transform_posstr(gen, expression)
    if _orig_strposition:
        return str(_orig_strposition(gen, expression))
    return str(gen.strposition_sql(expression))


def _db2_timetostr_transform(gen: Any, expression: exp.TimeToStr) -> str:
    """Render TimeToStr expressions using Db2 VARCHAR_FORMAT."""
    if _is_db2(gen):
        return _transform_varchar_format(gen, expression)
    if _orig_timetostr:
        return str(_orig_timetostr(gen, expression))
    return str(gen.timetostr_sql(expression))


def _db2_tochar_transform(gen: Any, expression: exp.ToChar) -> str:
    """Render ToChar expressions using Db2 VARCHAR_FORMAT."""
    if _is_db2(gen):
        return _transform_varchar_format(gen, expression)
    if _orig_tochar:
        return str(_orig_tochar(gen, expression))
    return str(gen.tochar_sql(expression))


def _db2_anonymous_transform(gen: Any, expression: exp.Anonymous) -> str:
    """Render anonymous functions including DATEADD into Db2 duration syntax."""
    if _is_db2(gen):
        return _transform_anonymous(gen, expression)
    if _orig_anonymous:
        return str(_orig_anonymous(gen, expression))
    return str(gen.anonymous_sql(expression))


def _db2_parameter_transform(gen: Any, expression: exp.Parameter) -> str:
    """Render Parameter expressions as positional question mark placeholders."""
    if _is_db2(gen):
        return "?"
    if _orig_parameter:
        return str(_orig_parameter(gen, expression))
    return str(gen.parameter_sql(expression))


def _db2_concat_transform(gen: Any, expression: exp.Concat) -> str:
    """Render Concat expressions using Db2 string concatenation operator."""
    if _is_db2(gen):
        return _transform_concat(gen, expression)
    if _orig_concat:
        return str(_orig_concat(gen, expression))
    return str(gen.concat_sql(expression))


def _db2_mod_transform(gen: Any, expression: exp.Mod) -> str:
    """Render Mod expressions as Db2 MOD function."""
    if _is_db2(gen):
        return _transform_mod(gen, expression)
    if _orig_mod:
        return str(_orig_mod(gen, expression))
    return str(gen.mod_sql(expression))


def _db2_ilike_transform(gen: Any, expression: exp.ILike) -> str:
    """Render ILike expressions using LOWER() LIKE LOWER()."""
    if _is_db2(gen):
        return _transform_ilike(gen, expression)
    if _orig_ilike:
        return str(_orig_ilike(gen, expression))
    return str(gen.ilike_sql(expression))


generator.Generator.TRANSFORMS[exp.Select] = _db2_select_transform
generator.Generator.TRANSFORMS[exp.Offset] = _db2_offset_transform
generator.Generator.TRANSFORMS[exp.DataType] = _db2_datatype_transform
generator.Generator.TRANSFORMS[exp.Interval] = _db2_interval_transform
generator.Generator.TRANSFORMS[exp.DateAdd] = _db2_dateadd_transform
generator.Generator.TRANSFORMS[exp.DateSub] = _db2_datesub_transform
generator.Generator.TRANSFORMS[exp.DatetimeAdd] = _db2_datetimeadd_transform
generator.Generator.TRANSFORMS[exp.DatetimeSub] = _db2_datetimesub_transform
generator.Generator.TRANSFORMS[exp.StrPosition] = _db2_strposition_transform
generator.Generator.TRANSFORMS[exp.TimeToStr] = _db2_timetostr_transform
generator.Generator.TRANSFORMS[exp.ToChar] = _db2_tochar_transform
generator.Generator.TRANSFORMS[exp.Anonymous] = _db2_anonymous_transform
generator.Generator.TRANSFORMS[exp.Parameter] = _db2_parameter_transform
generator.Generator.TRANSFORMS[exp.Concat] = _db2_concat_transform
generator.Generator.TRANSFORMS[exp.Mod] = _db2_mod_transform
generator.Generator.TRANSFORMS[exp.ILike] = _db2_ilike_transform

invalidate_generator_dispatch(generator.Generator)

DB2Generator: type[generator.Generator] = generator.Generator
