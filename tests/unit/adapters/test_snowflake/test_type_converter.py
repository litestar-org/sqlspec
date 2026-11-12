"""Snowflake value conversion tests."""

from datetime import UTC, date, datetime, time
from decimal import Decimal

import pytest

from sqlspec.adapters.snowflake.type_converter import SnowflakeOutputConverter


@pytest.mark.parametrize(
    ("value", "type_code", "expected"),
    [
        ('{"tier":"gold","tags":[1,2]}', 5, {"tier": "gold", "tags": [1, 2]}),
        (b"[true,null]", 10, [True, None]),
        ("null", 5, None),
        ('"text"', 5, "text"),
        ("unquoted", 5, "unquoted"),
        ({"native": 1}, 9, {"native": 1}),
    ],
)
def test_variant_conversion(value: object, type_code: int, expected: object) -> None:
    assert SnowflakeOutputConverter().convert_value(value, type_code=type_code) == expected


@pytest.mark.parametrize(
    "value",
    [
        Decimal("12345678901234567890.123456"),
        datetime(2026, 1, 1, tzinfo=UTC),
        date(2026, 1, 1),
        time(12, 30),
        b"bytes",
        True,
        123,
        1.25,
        "text",
    ],
)
def test_native_scalars_are_preserved(value: object) -> None:
    assert SnowflakeOutputConverter().convert_value(value) is value


def test_custom_variant_deserializer() -> None:
    converter = SnowflakeOutputConverter(json_deserializer=lambda value: {"decoded": value})
    assert converter.convert_variant("  data  ") == {"decoded": "data"}
