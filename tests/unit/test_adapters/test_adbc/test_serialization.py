"""Unit tests for ADBC JSON serializer configuration."""

from typing import Any

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.utils.serializers import to_json


def test_default_json_serializer() -> None:
    """Test that ADBC config defaults to standard to_json serializer."""
    config = AdbcConfig()

    assert "json_serializer" in config.driver_features
    assert config.driver_features["json_serializer"] is to_json


def test_custom_json_serializer_function() -> None:
    """Test that custom JSON serializer function can be provided."""

    def custom_serializer(data: Any) -> str:
        """Custom JSON serializer for testing."""
        return f"custom:{data}"

    config = AdbcConfig(driver_features={"json_serializer": custom_serializer})

    assert config.driver_features["json_serializer"] is custom_serializer
    assert config.driver_features["json_serializer"]({"key": "value"}) == "custom:{'key': 'value'}"


def test_custom_json_serializer_orjson() -> None:
    """Test that orjson can be used as custom JSON serializer."""
    pytest.importorskip("orjson")
    import orjson

    def orjson_serializer(data: Any) -> str:
        """Wrapper for orjson serializer."""
        return orjson.dumps(data).decode("utf-8")

    config = AdbcConfig(driver_features={"json_serializer": orjson_serializer})

    test_data = {"key": "value", "number": 42}
    result = config.driver_features["json_serializer"](test_data)

    assert isinstance(result, str)
    assert "key" in result
    assert "value" in result


def test_custom_json_serializer_msgspec() -> None:
    """Test that msgspec can be used as custom JSON serializer."""
    pytest.importorskip("msgspec")
    import msgspec.json

    def msgspec_serializer(data: Any) -> str:
        """Wrapper for msgspec JSON serializer."""
        return msgspec.json.encode(data).decode("utf-8")

    config = AdbcConfig(driver_features={"json_serializer": msgspec_serializer})

    test_data = {"key": "value", "number": 42}
    result = config.driver_features["json_serializer"](test_data)

    assert isinstance(result, str)
    assert "key" in result
    assert "value" in result


def test_json_serializer_type_annotation() -> None:
    """Test that json_serializer accepts Callable[[Any], str]."""
    from collections.abc import Callable

    def typed_serializer(data: Any) -> str:
        """Typed JSON serializer for testing."""
        return str(data)

    config = AdbcConfig(driver_features={"json_serializer": typed_serializer})

    serializer: Callable[[Any], str] = config.driver_features["json_serializer"]
    assert callable(serializer)
    assert serializer({"test": "data"}) == "{'test': 'data'}"


def test_driver_features_defaults() -> None:
    """Test that all driver features have appropriate defaults."""
    config = AdbcConfig()

    assert config.driver_features["json_serializer"] is to_json
    assert config.driver_features["enable_cast_detection"] is True
    assert config.driver_features["strict_type_coercion"] is False
    assert config.driver_features["arrow_extension_types"] is True


def test_driver_features_override() -> None:
    """Test that driver features can be overridden selectively."""

    def custom_serializer(data: Any) -> str:
        return "custom"

    config = AdbcConfig(
        driver_features={
            "json_serializer": custom_serializer,
            "enable_cast_detection": False,
            "strict_type_coercion": True,
        }
    )

    assert config.driver_features["json_serializer"] is custom_serializer
    assert config.driver_features["enable_cast_detection"] is False
    assert config.driver_features["strict_type_coercion"] is True
    assert config.driver_features["arrow_extension_types"] is True


def test_backward_compatibility_no_serializer() -> None:
    """Test backward compatibility when no json_serializer is provided."""
    config = AdbcConfig(driver_features={"enable_cast_detection": True})

    assert "json_serializer" in config.driver_features
    assert config.driver_features["json_serializer"] is to_json
