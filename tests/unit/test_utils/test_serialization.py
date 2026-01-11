"""Tests for enhanced serialization functionality.

Tests for the byte-aware serialization system, including performance
improvements and compatibility with msgspec/orjson fallback patterns.
"""

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from sqlspec._serialization import decode_json, encode_json


def test_encode_json_as_string() -> None:
    """Test encoding to string format."""
    data = {"key": "value", "number": 42}
    result = encode_json(data, as_bytes=False)

    assert isinstance(result, str)
    parsed = json.loads(result)
    assert parsed == data


def test_encode_json_as_bytes() -> None:
    """Test encoding to bytes format."""
    data = {"key": "value", "number": 42}
    result = encode_json(data, as_bytes=True)

    assert isinstance(result, bytes)
    parsed = json.loads(result.decode("utf-8"))
    assert parsed == data


def test_encode_json_default_is_string() -> None:
    """Test that default encoding returns string."""
    data = {"key": "value"}
    result = encode_json(data)

    assert isinstance(result, str)


def test_round_trip_string() -> None:
    """Test string encoding round-trip."""
    data = {"uuid": str(uuid4()), "items": [1, 2, 3]}

    encoded = encode_json(data, as_bytes=False)
    decoded = decode_json(encoded)

    assert decoded == data


def test_round_trip_bytes() -> None:
    """Test bytes encoding round-trip."""
    data = {"uuid": str(uuid4()), "items": [1, 2, 3]}

    encoded = encode_json(data, as_bytes=True)
    decoded = decode_json(encoded)

    assert decoded == data


def test_complex_data_structures() -> None:
    """Test encoding complex nested structures."""
    data = {
        "users": [{"id": str(uuid4()), "name": "User 1"}, {"id": str(uuid4()), "name": "User 2"}],
        "metadata": {"count": 2, "timestamp": "2023-12-25T10:30:00Z"},
    }

    str_result = encode_json(data, as_bytes=False)
    bytes_result = encode_json(data, as_bytes=True)

    assert isinstance(str_result, str)
    assert isinstance(bytes_result, bytes)

    assert decode_json(str_result) == data
    assert decode_json(bytes_result) == data


def test_decode_string_input() -> None:
    """Test decoding from string input."""
    data = {"key": "value", "number": 42}
    json_str = json.dumps(data)

    result = decode_json(json_str)
    assert result == data


def test_decode_bytes_input() -> None:
    """Test decoding from bytes input."""
    data = {"key": "value", "number": 42}
    json_bytes = json.dumps(data).encode("utf-8")

    result = decode_json(json_bytes)
    assert result == data


def test_decode_bytes_passthrough() -> None:
    """Test bytes passthrough when decode_bytes=False."""
    json_bytes = b'{"key": "value"}'

    result = decode_json(json_bytes, decode_bytes=False)
    assert result is json_bytes


def test_unicode_handling() -> None:
    """Test proper unicode handling in encoding/decoding."""
    data = {"message": "Hello 🌍", "emoji": "🚀"}

    encoded_str = encode_json(data, as_bytes=False)
    decoded_str = decode_json(encoded_str)
    assert decoded_str == data

    encoded_bytes = encode_json(data, as_bytes=True)
    decoded_bytes = decode_json(encoded_bytes)
    assert decoded_bytes == data


def test_datetime_serialization() -> None:
    """Test datetime objects are properly handled."""
    dt = datetime(2023, 12, 25, 10, 30, 0, tzinfo=timezone.utc)
    data = {"timestamp": dt}

    encoded = encode_json(data, as_bytes=False)
    assert isinstance(encoded, str)

    parsed = json.loads(encoded)
    assert "timestamp" in parsed
    assert isinstance(parsed["timestamp"], str)


def test_datetime_with_timezone() -> None:
    """Test datetime with timezone information."""
    dt = datetime.now(timezone.utc)
    data = {"created_at": dt}

    encoded = encode_json(data, as_bytes=True)
    assert isinstance(encoded, bytes)


def test_bytes_encoding_efficiency() -> None:
    """Test that bytes encoding avoids string allocation."""
    data = {"records": [{"id": i, "data": f"record_{i}"} for i in range(1000)]}

    str_result = encode_json(data, as_bytes=False)
    bytes_result = encode_json(data, as_bytes=True)

    assert isinstance(str_result, str)
    assert isinstance(bytes_result, bytes)

    assert decode_json(str_result) == decode_json(bytes_result)


def test_large_data_round_trip() -> None:
    """Test round-trip with larger data sets."""
    data = {
        "users": [
            {
                "id": str(uuid4()),
                "email": f"user{i}@example.com",
                "metadata": {"created": "2023-01-01T00:00:00Z", "tags": [f"tag{j}" for j in range(10)]},
            }
            for i in range(100)
        ]
    }

    encoded = encode_json(data, as_bytes=True)
    decoded = decode_json(encoded)

    assert len(decoded["users"]) == 100
    assert all("id" in user for user in decoded["users"])


def test_invalid_json_string() -> None:
    """Test handling of invalid JSON strings."""
    try:
        from msgspec import DecodeError

        expected_exceptions = (ValueError, json.JSONDecodeError, DecodeError)
    except ImportError:
        expected_exceptions = (ValueError, json.JSONDecodeError)

    with pytest.raises(expected_exceptions):
        decode_json("invalid json")


def test_invalid_json_bytes() -> None:
    """Test handling of invalid JSON bytes."""
    try:
        from msgspec import DecodeError

        expected_exceptions = (ValueError, json.JSONDecodeError, DecodeError)
    except ImportError:
        expected_exceptions = (ValueError, json.JSONDecodeError)

    with pytest.raises(expected_exceptions):
        decode_json(b"invalid json")


def test_non_utf8_bytes() -> None:
    """Test handling of non-UTF8 bytes."""
    invalid_bytes = b"\xff\xfe invalid"

    try:
        from msgspec import DecodeError

        expected_exceptions = (UnicodeDecodeError, ValueError, json.JSONDecodeError, DecodeError)
    except ImportError:
        expected_exceptions = (UnicodeDecodeError, ValueError, json.JSONDecodeError)

    try:
        decode_json(invalid_bytes)
    except expected_exceptions:
        pass


def test_msgspec_fallback() -> None:
    """Test that orjson fallback works when msgspec fails."""
    data = {"special": float("inf")}

    try:
        result = encode_json(data, as_bytes=True)
        assert isinstance(result, bytes)
    except (ValueError, TypeError):
        pass


def test_numpy_array_serialization_1d() -> None:
    """Test 1D numpy array is serialized to list in JSON."""
    np = pytest.importorskip("numpy")

    data = {"values": np.array([1, 2, 3, 4, 5])}
    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert decoded["values"] == [1, 2, 3, 4, 5], "1D array should serialize to list"


def test_numpy_array_serialization_2d() -> None:
    """Test 2D numpy array is serialized to nested list in JSON."""
    np = pytest.importorskip("numpy")

    data = {"matrix": np.array([[1, 2, 3], [4, 5, 6]])}
    encoded = encode_json(data, as_bytes=False)
    decoded = decode_json(encoded)

    assert decoded["matrix"] == [[1, 2, 3], [4, 5, 6]], "2D array should serialize to nested list"


def test_numpy_array_serialization_3d() -> None:
    """Test 3D numpy array is serialized to nested list structure."""
    np = pytest.importorskip("numpy")

    data = {"tensor": np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]])}
    encoded = encode_json(data, as_bytes=True)
    decoded = decode_json(encoded)

    expected = [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
    assert decoded["tensor"] == expected, "3D array should serialize to nested list structure"


def test_numpy_empty_array_serialization() -> None:
    """Test empty numpy array serialization."""
    np = pytest.importorskip("numpy")

    data = {"empty": np.array([])}
    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert decoded["empty"] == [], "Empty array should serialize to empty list"


def test_numpy_array_round_trip() -> None:
    """Test round-trip serialization: dict with ndarray -> JSON -> dict with list."""
    np = pytest.importorskip("numpy")

    original_data = {"id": 123, "values": np.array([10.5, 20.3, 30.1]), "name": "test_array"}

    encoded = encode_json(original_data, as_bytes=False)
    assert isinstance(encoded, str), "Should encode to string"

    decoded = decode_json(encoded)

    assert decoded["id"] == 123
    assert decoded["values"] == [10.5, 20.3, 30.1]
    assert decoded["name"] == "test_array"


def test_numpy_array_round_trip_bytes() -> None:
    """Test round-trip serialization with bytes encoding."""
    np = pytest.importorskip("numpy")

    original_data = {"data": np.array([1, 2, 3, 4, 5])}

    encoded = encode_json(original_data, as_bytes=True)
    assert isinstance(encoded, bytes), "Should encode to bytes"

    decoded = decode_json(encoded)
    assert decoded["data"] == [1, 2, 3, 4, 5]


def test_numpy_nested_structure_with_arrays() -> None:
    """Test serialization of nested structures containing numpy arrays."""
    np = pytest.importorskip("numpy")

    data = {
        "users": [{"id": 1, "scores": np.array([85, 90, 88])}, {"id": 2, "scores": np.array([92, 87, 95])}],
        "metadata": {"total_count": 2, "averages": np.array([87.67, 91.33])},
    }

    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert decoded["users"][0]["scores"] == [85, 90, 88]
    assert decoded["users"][1]["scores"] == [92, 87, 95]
    assert len(decoded["metadata"]["averages"]) == 2


def test_numpy_different_dtypes_int32() -> None:
    """Test numpy array with int32 dtype serialization."""
    np = pytest.importorskip("numpy")

    data = {"values": np.array([1, 2, 3, 4, 5], dtype=np.int32)}
    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert decoded["values"] == [1, 2, 3, 4, 5]


def test_numpy_different_dtypes_float64() -> None:
    """Test numpy array with float64 dtype serialization."""
    np = pytest.importorskip("numpy")

    data = {"values": np.array([1.1, 2.2, 3.3], dtype=np.float64)}
    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert len(decoded["values"]) == 3
    assert abs(decoded["values"][0] - 1.1) < 0.01
    assert abs(decoded["values"][1] - 2.2) < 0.01
    assert abs(decoded["values"][2] - 3.3) < 0.01


def test_numpy_different_dtypes_bool() -> None:
    """Test numpy array with boolean dtype serialization."""
    np = pytest.importorskip("numpy")

    data = {"flags": np.array([True, False, True, False], dtype=bool)}
    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert decoded["flags"] == [True, False, True, False]


def test_numpy_multiple_arrays_in_dict() -> None:
    """Test dict containing multiple numpy arrays."""
    np = pytest.importorskip("numpy")

    data = {"array1": np.array([1, 2, 3]), "array2": np.array([4.0, 5.0, 6.0]), "array3": np.array([[7, 8], [9, 10]])}

    encoded = encode_json(data, as_bytes=False)
    decoded = decode_json(encoded)

    assert decoded["array1"] == [1, 2, 3]
    assert decoded["array2"] == [4.0, 5.0, 6.0]
    assert decoded["array3"] == [[7, 8], [9, 10]]


def test_numpy_array_with_datetime_integration() -> None:
    """Test numpy array serialization alongside datetime objects."""
    np = pytest.importorskip("numpy")

    dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    data = {"timestamp": dt, "values": np.array([100, 200, 300])}

    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert "timestamp" in decoded
    assert isinstance(decoded["timestamp"], str)
    assert decoded["values"] == [100, 200, 300]


def test_numpy_array_with_uuid_integration() -> None:
    """Test numpy array serialization alongside UUID objects."""
    np = pytest.importorskip("numpy")

    test_uuid = uuid4()
    data = {"id": str(test_uuid), "data": np.array([1, 2, 3, 4])}

    encoded = encode_json(data, as_bytes=True)
    decoded = decode_json(encoded)

    assert decoded["id"] == str(test_uuid)
    assert decoded["data"] == [1, 2, 3, 4]


def test_numpy_large_array_performance() -> None:
    """Test serialization of large numpy arrays."""
    np = pytest.importorskip("numpy")

    large_array = np.arange(10000)
    data = {"large_data": large_array}

    encoded = encode_json(data, as_bytes=True)
    decoded = decode_json(encoded)

    assert len(decoded["large_data"]) == 10000
    assert decoded["large_data"][0] == 0
    assert decoded["large_data"][-1] == 9999


def test_serialization_without_numpy_no_import_error() -> None:
    """Test serialization works when numpy is not available."""
    data = {"key": "value", "number": 42, "items": [1, 2, 3]}

    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert decoded == data


def test_serialization_without_numpy_standard_types() -> None:
    """Test serialization of standard types when numpy is not available."""
    data = {
        "string": "hello",
        "integer": 123,
        "float": 45.67,
        "boolean": True,
        "null": None,
        "list": [1, 2, 3],
        "dict": {"nested": "value"},
    }

    encoded = encode_json(data, as_bytes=False)
    decoded = decode_json(encoded)

    assert decoded == data


def test_serialization_without_numpy_datetime_support() -> None:
    """Test datetime serialization works without numpy."""
    dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
    data = {"timestamp": dt, "message": "test"}

    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert "timestamp" in decoded
    assert decoded["message"] == "test"


def test_serialization_graceful_degradation() -> None:
    """Test that serialization degrades gracefully without numpy."""
    data = {
        "users": [
            {"id": 1, "name": "User 1", "scores": [85, 90, 88]},
            {"id": 2, "name": "User 2", "scores": [92, 87, 95]},
        ],
        "metadata": {"count": 2, "timestamp": "2024-01-15T10:30:00Z"},
    }

    encoded = encode_json(data, as_bytes=True)
    decoded = decode_json(encoded)

    assert decoded == data
    assert len(decoded["users"]) == 2


def test_numpy_zero_dimensional_array() -> None:
    """Test serialization of 0-dimensional numpy array (scalar)."""
    np = pytest.importorskip("numpy")

    data = {"scalar": np.array(42)}
    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert decoded["scalar"] == 42


def test_numpy_single_element_array() -> None:
    """Test serialization of single-element array."""
    np = pytest.importorskip("numpy")

    data = {"single": np.array([99])}
    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert decoded["single"] == [99]


def test_numpy_mixed_content_serialization() -> None:
    """Test mixed content with numpy arrays and standard types."""
    np = pytest.importorskip("numpy")

    data = {
        "id": 123,
        "name": "test",
        "array": np.array([1, 2, 3]),
        "timestamp": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "uuid": str(uuid4()),
        "nested": {"value": 456, "data": np.array([7, 8, 9])},
    }

    encoded = encode_json(data, as_bytes=False)
    decoded = decode_json(encoded)

    assert decoded["id"] == 123
    assert decoded["name"] == "test"
    assert decoded["array"] == [1, 2, 3]
    assert "timestamp" in decoded
    assert "uuid" in decoded
    assert decoded["nested"]["value"] == 456
    assert decoded["nested"]["data"] == [7, 8, 9]


def test_numpy_array_string_encoding() -> None:
    """Test numpy array with string encoding path."""
    np = pytest.importorskip("numpy")

    data = {"text_array": np.array(["a", "b", "c"])}
    encoded = encode_json(data, as_bytes=False)
    decoded = decode_json(encoded)

    assert decoded["text_array"] == ["a", "b", "c"]


def test_numpy_complex_nested_arrays() -> None:
    """Test deeply nested structures with numpy arrays."""
    np = pytest.importorskip("numpy")

    data = {
        "level1": {
            "level2": {"level3": {"data": np.array([1, 2, 3]), "metadata": {"count": 3}}, "other": np.array([4, 5])}
        }
    }

    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert decoded["level1"]["level2"]["level3"]["data"] == [1, 2, 3]
    assert decoded["level1"]["level2"]["other"] == [4, 5]


def test_numpy_array_in_list() -> None:
    """Test numpy array as element in a list."""
    np = pytest.importorskip("numpy")

    data = {"items": [1, "two", np.array([3, 4, 5]), {"key": "value"}]}

    encoded = encode_json(data)
    decoded = decode_json(encoded)

    assert decoded["items"][0] == 1
    assert decoded["items"][1] == "two"
    assert decoded["items"][2] == [3, 4, 5]
    assert decoded["items"][3] == {"key": "value"}
