from uuid import UUID

from sqlspec.adapters.spanner.type_converter import SpannerOutputConverter


def test_uuid_conversion() -> None:
    """Test UUID string auto-conversion."""
    converter = SpannerOutputConverter(enable_uuid_conversion=True)
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    result = converter.convert(uuid_str)
    assert isinstance(result, UUID)
    assert str(result) == uuid_str


def test_json_detection() -> None:
    """Test JSON string auto-detection."""
    converter = SpannerOutputConverter()
    json_str = '{"key": "value"}'
    result = converter.convert(json_str)
    assert isinstance(result, dict)
    assert result == {"key": "value"}


def test_disabled_uuid_conversion() -> None:
    """Test UUID conversion when disabled."""
    converter = SpannerOutputConverter(enable_uuid_conversion=False)
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    result = converter.convert(uuid_str)
    assert isinstance(result, str)
    assert result == uuid_str
