"""Tests for centralized type conversion functionality.

Tests for the BaseTypeConverter class and related conversion utilities,
ensuring consistent type handling across all database adapters.
"""

from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import cast
from uuid import UUID, uuid4

import pytest

from sqlspec.core import (
    BaseTypeConverter,
    convert_decimal,
    convert_iso_date,
    convert_iso_datetime,
    convert_iso_time,
    convert_json,
    convert_uuid,
    format_datetime_rfc3339,
    parse_datetime_rfc3339,
)
from tests.conftest import requires_interpreted


@pytest.fixture
def detector() -> "BaseTypeConverter":
    """Provide a fresh type converter for each test."""
    return BaseTypeConverter()


def test_uuid_detection(detector: "BaseTypeConverter") -> None:
    """Test UUID string detection."""
    test_uuid = str(uuid4())
    detected = detector.detect_type(test_uuid)
    assert detected == "uuid"


def test_uuid_conversion(detector: "BaseTypeConverter") -> None:
    """Test UUID string conversion."""
    test_uuid_str = "123e4567-e89b-12d3-a456-426614174000"
    detected = detector.detect_type(test_uuid_str)
    assert detected == "uuid"

    if detected is not None:
        converted = detector.convert_value(test_uuid_str, detected)
        assert isinstance(converted, UUID)
        assert str(converted) == test_uuid_str


def test_datetime_detection(detector: "BaseTypeConverter") -> None:
    """Test ISO datetime detection."""
    test_cases = [
        "2023-12-25T10:30:00Z",
        "2023-12-25T10:30:00+00:00",
        "2023-12-25T10:30:00.123456Z",
        "2023-12-25 10:30:00",
    ]

    for dt_str in test_cases:
        detected = detector.detect_type(dt_str)
        assert detected == "iso_datetime", f"Failed for: {dt_str}"


def test_datetime_conversion(detector: "BaseTypeConverter") -> None:
    """Test ISO datetime conversion."""
    dt_str = "2023-12-25T10:30:00Z"
    detected = detector.detect_type(dt_str)
    assert detected == "iso_datetime"

    if detected is not None:
        converted = detector.convert_value(dt_str, detected)
        assert isinstance(converted, datetime)
        assert converted.year == 2023
        assert converted.month == 12
        assert converted.day == 25


def test_date_detection(detector: "BaseTypeConverter") -> None:
    """Test ISO date detection."""
    date_str = "2023-12-25"
    detected = detector.detect_type(date_str)
    assert detected == "iso_date"


def test_date_conversion(detector: "BaseTypeConverter") -> None:
    """Test ISO date conversion."""
    date_str = "2023-12-25"
    detected = detector.detect_type(date_str)
    assert detected == "iso_date"

    if detected is not None:
        converted = detector.convert_value(date_str, detected)
        assert isinstance(converted, date)
        assert converted.year == 2023
        assert converted.month == 12
        assert converted.day == 25


def test_time_detection(detector: "BaseTypeConverter") -> None:
    """Test ISO time detection."""
    time_str = "10:30:00"
    detected = detector.detect_type(time_str)
    assert detected == "iso_time"


def test_time_conversion(detector: "BaseTypeConverter") -> None:
    """Test ISO time conversion."""
    time_str = "10:30:45"
    detected = detector.detect_type(time_str)
    assert detected == "iso_time"

    if detected is not None:
        converted = detector.convert_value(time_str, detected)
        assert isinstance(converted, time)
        assert converted.hour == 10
        assert converted.minute == 30
        assert converted.second == 45


def test_json_detection(detector: "BaseTypeConverter") -> None:
    """Test JSON string detection."""
    test_cases = ['{"key": "value"}', "[1, 2, 3]", '{"nested": {"data": true}}']

    for json_str in test_cases:
        detected = detector.detect_type(json_str)
        assert detected == "json", f"Failed for: {json_str}"


def test_json_conversion(detector: "BaseTypeConverter") -> None:
    """Test JSON string conversion."""
    json_str = '{"key": "value", "number": 42}'
    detected = detector.detect_type(json_str)
    assert detected == "json"

    if detected is not None:
        converted = detector.convert_value(json_str, detected)
        assert isinstance(converted, dict)
        assert converted["key"] == "value"
        assert converted["number"] == 42


def test_ipv4_detection(detector: "BaseTypeConverter") -> None:
    """Test IPv4 address detection."""
    ip_str = "192.168.1.1"
    detected = detector.detect_type(ip_str)
    assert detected == "ipv4"


def test_ipv6_detection(detector: "BaseTypeConverter") -> None:
    """Test IPv6 address detection."""
    ip_str = "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
    detected = detector.detect_type(ip_str)
    assert detected == "ipv6"


def test_mac_address_detection(detector: "BaseTypeConverter") -> None:
    """Test MAC address detection."""
    mac_str = "00:1B:44:11:3A:B7"
    detected = detector.detect_type(mac_str)
    assert detected == "mac"


def test_non_special_type(detector: "BaseTypeConverter") -> None:
    """Test that regular strings are not detected as special types."""
    regular_strings = ["hello world", "123", "not-a-uuid", "invalid-date"]

    for string in regular_strings:
        detected = detector.detect_type(string)
        assert detected is None, f"Incorrectly detected: {string}"


@requires_interpreted
def test_none_input(detector: "BaseTypeConverter") -> None:
    """Test that None input is handled correctly."""
    detected = detector.detect_type(cast("str", None))
    assert detected is None


@requires_interpreted
def test_non_string_input(detector: "BaseTypeConverter") -> None:
    """Test that non-string input is handled correctly."""
    non_strings = [123, [], {}, True]

    for value in non_strings:
        detected = detector.detect_type(cast("str", value))
        assert detected is None


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


def test_datetime_round_trip(detector: "BaseTypeConverter") -> None:
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


def test_empty_string(detector: "BaseTypeConverter") -> None:
    """Test empty string handling."""
    detected = detector.detect_type("")
    assert detected is None


def test_whitespace_string(detector: "BaseTypeConverter") -> None:
    """Test whitespace-only string."""
    detected = detector.detect_type("   ")
    assert detected is None


def test_case_insensitive_patterns(detector: "BaseTypeConverter") -> None:
    """Test that patterns work case-insensitively where appropriate."""
    uuid_lower = "123e4567-e89b-12d3-a456-426614174000"
    uuid_upper = "123E4567-E89B-12D3-A456-426614174000"

    assert detector.detect_type(uuid_lower) == "uuid"
    assert detector.detect_type(uuid_upper) == "uuid"
