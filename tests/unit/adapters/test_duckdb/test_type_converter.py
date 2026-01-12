"""Unit tests for DuckDB type converter UUID conversion control."""

import uuid

from sqlspec.adapters.duckdb.type_converter import DuckDBOutputConverter


def test_uuid_conversion_enabled_by_default() -> None:
    """Test that UUID conversion is enabled by default."""
    converter = DuckDBOutputConverter()
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"

    result = converter.handle_uuid(uuid_str)

    assert isinstance(result, uuid.UUID)
    assert str(result) == uuid_str


def test_uuid_conversion_can_be_disabled() -> None:
    """Test that UUID conversion can be disabled."""
    converter = DuckDBOutputConverter(enable_uuid_conversion=False)
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"

    result = converter.handle_uuid(uuid_str)

    assert isinstance(result, str)
    assert result == uuid_str


def test_uuid_objects_pass_through_regardless_of_flag() -> None:
    """Test that UUID objects pass through unchanged regardless of conversion flag."""
    converter_enabled = DuckDBOutputConverter(enable_uuid_conversion=True)
    converter_disabled = DuckDBOutputConverter(enable_uuid_conversion=False)
    uuid_obj = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")

    result_enabled = converter_enabled.handle_uuid(uuid_obj)
    result_disabled = converter_disabled.handle_uuid(uuid_obj)

    assert result_enabled is uuid_obj
    assert result_disabled is uuid_obj


def test_convert_respects_uuid_flag() -> None:
    """Test that convert respects UUID conversion flag."""
    converter_enabled = DuckDBOutputConverter(enable_uuid_conversion=True)
    converter_disabled = DuckDBOutputConverter(enable_uuid_conversion=False)
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"

    result_enabled = converter_enabled.convert(uuid_str)
    result_disabled = converter_disabled.convert(uuid_str)

    assert isinstance(result_enabled, uuid.UUID)
    assert isinstance(result_disabled, str)
    assert result_disabled == uuid_str


def test_non_uuid_strings_unaffected_by_flag() -> None:
    """Test that non-UUID strings are unaffected by the conversion flag."""
    converter_enabled = DuckDBOutputConverter(enable_uuid_conversion=True)
    converter_disabled = DuckDBOutputConverter(enable_uuid_conversion=False)
    regular_str = "just a regular string"

    result_enabled = converter_enabled.convert(regular_str)
    result_disabled = converter_disabled.convert(regular_str)

    assert result_enabled == regular_str
    assert result_disabled == regular_str


def test_datetime_conversion_unaffected_by_uuid_flag() -> None:
    """Test that datetime conversion works regardless of UUID flag."""
    converter_enabled = DuckDBOutputConverter(enable_uuid_conversion=True)
    converter_disabled = DuckDBOutputConverter(enable_uuid_conversion=False)
    datetime_str = "2024-01-15T10:30:00"

    result_enabled = converter_enabled.convert(datetime_str)
    result_disabled = converter_disabled.convert(datetime_str)

    from datetime import datetime

    assert isinstance(result_enabled, datetime)
    assert isinstance(result_disabled, datetime)
