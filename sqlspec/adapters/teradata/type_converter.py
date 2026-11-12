"""Teradata scalar and Arrow metadata conversion."""

from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

__all__ = ("coerce_teradata_row", "coerce_teradata_value", "extract_arrow_column_types", "prepare_teradata_parameters")


_MAX_DECIMAL_PRECISION = 76


def coerce_teradata_value(value: Any, *, type_code: Any = None) -> Any:
    """Normalize native binary, LOB, numeric, and PERIOD values."""
    type_name = str(type_code).upper()
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if callable(getattr(value, "read", None)):
        value = value.read()
        return (
            value.decode()
            if isinstance(value, bytes) and any(t in type_name for t in ("CLOB", "JSON", "XML"))
            else value
        )
    if isinstance(value, str) and any(t in type_name for t in ("DECIMAL", "NUMBER", "NUMERIC")):
        return Decimal(value)
    if value is None or isinstance(value, (int, float, str, bytes, Decimal, date, time)):
        return value
    return str(value)


def coerce_teradata_row(row: Sequence[Any], *, description: Sequence[Any] | None = None) -> tuple[Any, ...]:
    """Coerce a DB-API row while preserving clean tuples."""
    values = tuple(
        coerce_teradata_value(v, type_code=description[i][1] if description else None) for i, v in enumerate(row)
    )
    return row if isinstance(row, tuple) and all(a is b for a, b in zip(row, values, strict=False)) else values


def _prepare(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (memoryview, bytearray)):
        return bytes(value)
    return value


def prepare_teradata_parameters(parameters: Any, *, is_many: bool = False) -> Any:
    """Prepare native positional parameters without losing Decimal precision."""
    if is_many:
        return [prepare_teradata_parameters(row) for row in parameters or ()]
    if isinstance(parameters, Mapping):
        return {k: _prepare(v) for k, v in parameters.items()}
    return tuple(_prepare(v) for v in parameters or ())


def extract_arrow_column_types(
    description: Sequence[Any] | None, *, column_names: Sequence[str] | None = None
) -> dict[str, str]:
    """Map native DB-API metadata to Arrow type tokens."""
    result = {}
    for i, col in enumerate(description or ()):
        code = col[1]
        type_name = getattr(code, "__name__", str(code)).upper()
        if code is bool or type_name in {"BOOL", "BOOLEAN"}:
            kind = "bool"
        elif code is int or type_name in {"INT", "INTEGER", "BIGINT", "SMALLINT", "BYTEINT"}:
            kind = "int64"
        elif code is Decimal or type_name in {"DECIMAL", "NUMBER", "NUMERIC"}:
            p, s = col[4:6]
            kind = (
                f"decimal({p},{s})"
                if isinstance(p, int) and isinstance(s, int) and 1 <= p <= _MAX_DECIMAL_PRECISION and 0 <= s <= p
                else "decimal"
            )
        elif code is float or type_name in {"FLOAT", "DOUBLE", "REAL"}:
            kind = "float64"
        elif code in (bytes, bytearray, memoryview) or type_name in {"BYTE", "VARBYTE", "BLOB", "BINARY", "VARBINARY"}:
            kind = "binary"
        elif "TIMESTAMP" in type_name and ("TIME ZONE" in type_name or "TZ" in type_name):
            kind = "timestamptz"
        elif code is datetime or "TIMESTAMP" in type_name:
            kind = "timestamp"
        elif code is date or type_name == "DATE":
            kind = "date"
        elif code is time or type_name.startswith("TIME"):
            kind = "time"
        else:
            kind = "string"
        result[column_names[i] if column_names else col[0]] = kind
    return result
