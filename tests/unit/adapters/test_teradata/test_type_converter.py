from datetime import date, datetime, time
from decimal import Decimal
from io import BytesIO, StringIO
from uuid import UUID

import pytest

from sqlspec.adapters.teradata.type_converter import (
    coerce_teradata_row,
    coerce_teradata_value,
    extract_arrow_column_types,
    prepare_teradata_parameters,
)


@pytest.mark.parametrize(
    ("value", "type_code", "expected"),
    [
        (bytearray(b"abc"), "VARBYTE", b"abc"),
        (memoryview(b"abc"), "BLOB", b"abc"),
        (BytesIO(b"hello"), "CLOB", "hello"),
        (BytesIO(b"\x00\xff"), "BLOB", b"\x00\xff"),
        (StringIO("hello"), "CLOB", "hello"),
        ("123.456", "DECIMAL", Decimal("123.456")),
        (None, "INTEGER", None),
        (date(2026, 1, 2), "DATE", date(2026, 1, 2)),
        (UUID(int=0), "PERIOD", "00000000-0000-0000-0000-000000000000"),
    ],
)
def test_native_scalar_conversion(value: object, type_code: str, expected: object) -> None:
    assert coerce_teradata_value(value, type_code=type_code) == expected


def test_native_row_conversion_and_parameters() -> None:
    assert coerce_teradata_row([memoryview(b"abc"), "1.25"], description=[("raw", "BLOB"), ("amount", "DECIMAL")]) == (
        b"abc", Decimal("1.25")
    )
    assert prepare_teradata_parameters({"enabled": True, "id": UUID(int=0)}) == {
        "enabled": 1, "id": "00000000-0000-0000-0000-000000000000"
    }
    assert prepare_teradata_parameters([[True, bytearray(b"abc")], [False, None]], is_many=True) == [(1, b"abc"), (0, None)]
    assert prepare_teradata_parameters(None) == ()
    assert coerce_teradata_row((1, "hello")) == (1, "hello")


@pytest.mark.parametrize(
    ("type_code", "precision", "scale", "expected"),
    [(bool, None, None, "bool"), (int, None, None, "int64"), (float, None, None, "float64"),
     (Decimal, 12, 2, "decimal(12,2)"), (Decimal, None, None, "decimal"),
     (bytes, None, None, "binary"), ("TIMESTAMP WITH TIME ZONE", None, None, "timestamptz"),
     (datetime, None, None, "timestamp"), (date, None, None, "date"), (time, None, None, "time"),
     ("JSON", None, None, "string")],
)
def test_native_arrow_metadata(type_code: object, precision: int | None, scale: int | None, expected: str) -> None:
    description = [("COL", type_code, None, None, precision, scale, True)]
    assert extract_arrow_column_types(description) == {"COL": expected}
    assert extract_arrow_column_types(description, column_names=["col"]) == {"col": expected}


def test_empty_arrow_metadata() -> None:
    assert extract_arrow_column_types(None) == {}
