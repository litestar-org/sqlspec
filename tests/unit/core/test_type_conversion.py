"""Tests for centralized scalar conversion helpers."""

from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import UUID

import pytest

import sqlspec.utils.serializers._json as json_serialization
from sqlspec.core import (
    convert_decimal,
    convert_iso_date,
    convert_iso_datetime,
    convert_iso_time,
    convert_json,
    convert_uuid,
    format_datetime_rfc3339,
    parse_datetime_rfc3339,
)


def test_convert_uuid() -> None:
    """Test UUID conversion function."""
    uuid_str = "123e4567-e89b-12d3-a456-426614174000"
    result = convert_uuid(uuid_str)
    assert isinstance(result, UUID)
    assert str(result) == uuid_str


def test_convert_iso_datetime() -> None:
    """Test ISO datetime conversion."""
    dt_str = "2023-12-25T10:30:00Z"
    result = convert_iso_datetime(dt_str)
    assert isinstance(result, datetime)


def test_convert_iso_datetime_with_space() -> None:
    """Test ISO datetime with space separator."""
    dt_str = "2023-12-25 10:30:00"
    result = convert_iso_datetime(dt_str)
    assert isinstance(result, datetime)


def test_convert_iso_date() -> None:
    """Test ISO date conversion."""
    date_str = "2023-12-25"
    result = convert_iso_date(date_str)
    assert isinstance(result, date)


def test_convert_iso_time() -> None:
    """Test ISO time conversion."""
    time_str = "10:30:00"
    result = convert_iso_time(time_str)
    assert isinstance(result, time)


def test_convert_json() -> None:
    """Test JSON conversion."""
    json_str = '{"key": "value"}'
    result = convert_json(json_str)
    assert isinstance(result, dict)
    assert result["key"] == "value"


def test_convert_json_avoids_serializer_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    """JSON coercion should not bounce through serializer selection."""

    def fail_get_default_serializer() -> None:
        raise AssertionError("convert_json should not call serializer selection")

    monkeypatch.setattr(json_serialization, "get_default_serializer", fail_get_default_serializer)

    result = convert_json('{"key": "value"}')

    assert result == {"key": "value"}


def test_convert_decimal() -> None:
    """Test decimal conversion."""
    decimal_str = "123.456"
    result = convert_decimal(decimal_str)
    assert isinstance(result, Decimal)
    assert result == Decimal("123.456")


def test_format_datetime_rfc3339() -> None:
    """Test RFC 3339 datetime formatting."""
    dt = datetime(2023, 12, 25, 10, 30, 0, tzinfo=timezone.utc)
    formatted = format_datetime_rfc3339(dt)
    assert formatted == "2023-12-25T10:30:00+00:00"


def test_format_datetime_rfc3339_naive() -> None:
    """Test RFC 3339 formatting with naive datetime."""
    dt = datetime(2023, 12, 25, 10, 30, 0)
    formatted = format_datetime_rfc3339(dt)
    assert "+00:00" in formatted or "Z" in formatted


def test_parse_datetime_rfc3339() -> None:
    """Test RFC 3339 datetime parsing."""
    dt_str = "2023-12-25T10:30:00+00:00"
    parsed = parse_datetime_rfc3339(dt_str)
    assert isinstance(parsed, datetime)
    assert parsed.year == 2023


def test_parse_datetime_rfc3339_z_suffix() -> None:
    """Test RFC 3339 parsing with Z suffix."""
    dt_str = "2023-12-25T10:30:00Z"
    parsed = parse_datetime_rfc3339(dt_str)
    assert isinstance(parsed, datetime)
    assert parsed.year == 2023


def test_parse_datetime_rfc3339_delegates_to_iso_converter(monkeypatch: pytest.MonkeyPatch) -> None:
    """RFC3339 parsing should reuse the ISO datetime converter."""

    import sqlspec.core.type_converter as type_converter

    expected = datetime(2023, 12, 25, 10, 30, 0, tzinfo=timezone.utc)

    def fake_convert(value: str) -> datetime:
        assert value == "2023-12-25T10:30:00+00:00"
        return expected

    monkeypatch.setattr(type_converter, "convert_iso_datetime", fake_convert)

    assert type_converter.parse_datetime_rfc3339("2023-12-25T10:30:00Z") is expected


def test_datetime_round_trip() -> None:
    """Test datetime formatting and parsing round trip."""
    original = datetime(2023, 12, 25, 10, 30, 0, tzinfo=timezone.utc)
    formatted = format_datetime_rfc3339(original)
    parsed = parse_datetime_rfc3339(formatted)

    assert abs((original - parsed).total_seconds()) < 1


def test_invalid_uuid() -> None:
    """Test invalid UUID handling."""
    invalid_uuid = "not-a-valid-uuid"
    with pytest.raises(ValueError):
        convert_uuid(invalid_uuid)


def test_invalid_datetime() -> None:
    """Test invalid datetime handling."""
    invalid_dt = "not-a-valid-datetime"
    with pytest.raises(ValueError):
        convert_iso_datetime(invalid_dt)


def test_invalid_json() -> None:
    """Test invalid JSON handling."""
    invalid_json = "not valid json"
    with pytest.raises((ValueError, Exception)):
        convert_json(invalid_json)
