"""Tests for sqlspec.utils.serializers module.

Tests for JSON serialization utilities that are re-exported from sqlspec._serialization.
Covers all serialization scenarios including edge cases and type handling.
"""

import json
import math
from typing import Any

import pytest

from sqlspec.core.filters import OffsetPagination
from sqlspec.utils.serializers import (
    __all__,
    from_json,
    numpy_array_dec_hook,
    numpy_array_enc_hook,
    numpy_array_predicate,
    to_json,
)

pytestmark = pytest.mark.xdist_group("utils")


def test_to_json_basic_types() -> None:
    """Test serialization of basic Python types."""

    assert to_json("hello") == '"hello"'

    assert to_json(42) == "42"

    assert to_json(True) == "true"
    assert to_json(False) == "false"

    assert to_json(None) == "null"


def test_to_json_collections() -> None:
    """Test serialization of collections."""

    list_result = to_json([1, 2, 3])
    assert list_result in {"[1,2,3]", "[1, 2, 3]"}
    assert to_json([]) == "[]"

    result = to_json({"key": "value", "num": 42})

    parsed = json.loads(result)
    assert parsed == {"key": "value", "num": 42}

    assert to_json({}) == "{}"


def test_to_json_nested_structures() -> None:
    """Test serialization of nested data structures."""
    nested = {
        "users": [{"id": 1, "name": "Alice", "active": True}, {"id": 2, "name": "Bob", "active": False}],
        "metadata": {"total": 2, "page": 1},
    }

    result = to_json(nested)

    parsed = json.loads(result)
    assert parsed == nested


def test_to_json_unicode_strings() -> None:
    """Test serialization of Unicode strings."""
    unicode_text = "Hello 世界 🌍 café naïve résumé"
    result = to_json(unicode_text)

    parsed = json.loads(result)
    assert parsed == unicode_text


def test_to_json_special_characters() -> None:
    """Test serialization of strings with special characters."""
    special_chars = "Line1\nLine2\tTabbed\"Quoted'Single\\Backslash"
    result = to_json(special_chars)

    parsed = json.loads(result)
    assert parsed == special_chars


def test_to_json_offset_pagination() -> None:
    """Test serialization of OffsetPagination containers."""
    pagination = OffsetPagination([{"id": 1}], limit=10, offset=5, total=20)

    result = to_json(pagination)

    parsed = json.loads(result)
    assert parsed == {"items": [{"id": 1}], "limit": 10, "offset": 5, "total": 20}


def test_to_json_numeric_edge_cases() -> None:
    """Test serialization of edge case numeric values."""

    large_int = 9223372036854775807
    assert to_json(large_int) == str(large_int)

    assert to_json(-42) == "-42"

    assert to_json(0) == "0"
    assert to_json(0.0) == "0.0"


def test_to_json_empty_collections() -> None:
    """Test serialization of empty collections."""
    assert to_json([]) == "[]"
    assert to_json({}) == "{}"
    assert to_json(()) == "[]"


def test_to_json_tuple_serialization() -> None:
    """Test that tuples are serialized as JSON arrays."""
    tuple_result = to_json((1, 2, 3))
    assert tuple_result in {"[1,2,3]", "[1, 2, 3]"}
    assert to_json(()) == "[]"

    nested_tuple = ((1, 2), (3, 4))
    result = to_json(nested_tuple)
    parsed = json.loads(result)
    assert parsed == [[1, 2], [3, 4]]


def test_to_json_none_in_collections() -> None:
    """Test serialization of None values within collections."""
    data_with_none = {"value": None, "items": [1, None, "text"], "nested": {"null_field": None}}

    result = to_json(data_with_none)
    parsed = json.loads(result)
    assert parsed == data_with_none


def test_to_json_mixed_type_collections() -> None:
    """Test serialization of collections with mixed types."""
    mixed_list = [1, "string", True, None, {"nested": "dict"}, [1, 2]]
    result = to_json(mixed_list)
    parsed = json.loads(result)
    assert parsed == mixed_list


def test_from_json_basic_types() -> None:
    """Test deserialization of basic JSON types."""

    assert from_json('"hello"') == "hello"

    assert from_json("42") == 42
    assert from_json("3.14") == pytest.approx(3.14)

    assert from_json("true") is True
    assert from_json("false") is False

    assert from_json("null") is None


def test_from_json_collections() -> None:
    """Test deserialization of JSON collections."""

    assert from_json("[1, 2, 3]") == [1, 2, 3]
    assert from_json("[]") == []

    result = from_json('{"key": "value", "num": 42}')
    assert result == {"key": "value", "num": 42}

    assert from_json("{}") == {}


def test_from_json_nested_structures() -> None:
    """Test deserialization of nested JSON structures."""
    json_string = """
    {
        "users": [
            {"id": 1, "name": "Alice", "active": true},
            {"id": 2, "name": "Bob", "active": false}
        ],
        "metadata": {
            "total": 2,
            "page": 1
        }
    }
    """

    result = from_json(json_string)
    expected = {
        "users": [{"id": 1, "name": "Alice", "active": True}, {"id": 2, "name": "Bob", "active": False}],
        "metadata": {"total": 2, "page": 1},
    }
    assert result == expected


def test_from_json_unicode_strings() -> None:
    """Test deserialization of Unicode strings."""
    unicode_json = '"Hello 世界 🌍 café naïve résumé"'
    result = from_json(unicode_json)
    assert result == "Hello 世界 🌍 café naïve résumé"


def test_from_json_escaped_characters() -> None:
    """Test deserialization of strings with escaped characters."""
    escaped_json = '"Line1\\nLine2\\tTabbed\\"Quoted\'Single\\\\Backslash"'
    result = from_json(escaped_json)
    expected = "Line1\nLine2\tTabbed\"Quoted'Single\\Backslash"
    assert result == expected


def test_from_json_numeric_edge_cases() -> None:
    """Test deserialization of edge case numeric values."""

    assert from_json("9223372036854775807") == 9223372036854775807

    assert from_json("-42") == -42
    assert from_json("-3.14") == pytest.approx(-3.14)

    assert from_json("0") == 0
    assert from_json("0.0") == 0.0


def test_from_json_scientific_notation() -> None:
    """Test deserialization of scientific notation numbers."""
    assert from_json("1e5") == 100000.0
    assert from_json("1.5e-3") == 0.0015
    assert from_json("-2.5e2") == -250.0


def test_from_json_whitespace_handling() -> None:
    """Test that whitespace in JSON is handled correctly."""

    assert from_json('  "hello"  ') == "hello"
    assert from_json('\n\t{\n\t  "key": "value"\n\t}\n') == {"key": "value"}


def test_from_json_invalid_json_raises_error() -> None:
    """Test that invalid JSON raises appropriate errors."""

    try:
        import msgspec

        expected_errors = (ValueError, json.JSONDecodeError, msgspec.DecodeError)
    except ImportError:
        expected_errors = (ValueError, json.JSONDecodeError)

    with pytest.raises(expected_errors):
        from_json("invalid json")

    with pytest.raises(expected_errors):
        from_json('{"unclosed": "object"')

    with pytest.raises(expected_errors):
        from_json('["unclosed array"')

    with pytest.raises(expected_errors):
        from_json("")


def test_from_json_trailing_commas_error() -> None:
    """Test that trailing commas cause errors (strict JSON)."""
    try:
        import msgspec

        expected_errors = (ValueError, json.JSONDecodeError, msgspec.DecodeError)
    except ImportError:
        expected_errors = (ValueError, json.JSONDecodeError)

    with pytest.raises(expected_errors):
        from_json('{"key": "value",}')

    with pytest.raises(expected_errors):
        from_json("[1, 2, 3,]")


def test_round_trip_basic() -> None:
    """Test round-trip with basic data types."""
    test_data = ["string", 42, math.pi, True, False, None, [], {}]

    for data in test_data:
        serialized = to_json(data)
        deserialized = from_json(serialized)
        assert deserialized == data


def test_round_trip_complex() -> None:
    """Test round-trip with complex nested structures."""
    complex_data = {
        "string": "hello world",
        "number": 42,
        "float": 123.456,
        "boolean": True,
        "null": None,
        "array": [1, 2, 3, "mixed", {"nested": True}],
        "object": {
            "nested_string": "value",
            "nested_number": 100,
            "deeply_nested": {"level": 3, "items": ["a", "b", "c"]},
        },
    }

    serialized = to_json(complex_data)
    deserialized = from_json(serialized)
    assert deserialized == complex_data


def test_round_trip_unicode() -> None:
    """Test round-trip with Unicode data."""
    unicode_data = {
        "english": "hello",
        "chinese": "你好",
        "japanese": "こんにちは",
        "emoji": "🌍🚀💻",
        "accented": "café résumé naïve",
        "special": 'quotes"backslash\\newline\n',
    }

    serialized = to_json(unicode_data)
    deserialized = from_json(serialized)
    assert deserialized == unicode_data


def test_round_trip_numeric_precision() -> None:
    """Test that numeric precision is maintained in round-trip."""
    numeric_data = {
        "integer": 123456789,
        "float": 123.456,
        "negative": -987.654,
        "zero": 0,
        "large": 9223372036854775807,
    }

    serialized = to_json(numeric_data)
    deserialized = from_json(serialized)
    assert deserialized == numeric_data


def test_round_trip_empty_structures() -> None:
    """Test round-trip with empty data structures."""
    empty_data = {
        "empty_list": [],
        "empty_dict": {},
        "list_with_empty": [[], {}],
        "dict_with_empty": {"empty_list": [], "empty_dict": {}},
    }

    serialized = to_json(empty_data)
    deserialized = from_json(serialized)
    assert deserialized == empty_data


def test_edge_case_very_long_strings() -> None:
    """Test serialization of very long strings."""
    long_string = "x" * 10000
    serialized = to_json(long_string)
    deserialized = from_json(serialized)
    assert deserialized == long_string


def test_edge_case_deeply_nested_structures() -> None:
    """Test deeply nested data structures."""

    nested = "base"
    for i in range(100):
        nested = {"level": i, "data": nested}

    serialized = to_json(nested)
    deserialized = from_json(serialized)

    current = deserialized
    for i in range(99, -1, -1):
        assert current["level"] == i
        current = current["data"]
    assert current == "base"


def test_edge_case_large_arrays() -> None:
    """Test serialization of large arrays."""
    large_array = list(range(10000))
    serialized = to_json(large_array)
    deserialized = from_json(serialized)
    assert deserialized == large_array


def test_edge_case_dict_with_numeric_keys() -> None:
    """Test that dict keys are properly handled."""

    data = {"1": "one", "2": "two", "key": "value"}
    serialized = to_json(data)
    deserialized = from_json(serialized)
    assert deserialized == data


def test_edge_case_special_float_values() -> None:
    """Test handling of special float values."""

    special_values = [float("inf"), float("-inf"), float("nan")]

    for value in special_values:
        try:
            serialized = to_json(value)

            json.loads(serialized)
        except (ValueError, OverflowError):
            pass


def test_compatibility_produces_valid_json() -> None:
    """Test that to_json produces JSON that can be parsed by stdlib json."""
    test_data = {"string": "hello", "number": 42, "array": [1, 2, 3], "nested": {"key": "value"}}

    serialized = to_json(test_data)

    stdlib_parsed = json.loads(serialized)
    assert stdlib_parsed == test_data


def test_compatibility_parses_stdlib_json_output() -> None:
    """Test that from_json can parse output from stdlib json."""
    test_data = {"string": "hello", "number": 42, "array": [1, 2, 3], "nested": {"key": "value"}}

    stdlib_serialized = json.dumps(test_data)
    our_parsed = from_json(stdlib_serialized)
    assert our_parsed == test_data


def test_compatibility_consistent_formatting() -> None:
    """Test that formatting is consistent with expectations."""

    simple_data = {"key": "value", "num": 42}

    our_output = to_json(simple_data)
    stdlib_output = json.dumps(simple_data)

    assert from_json(our_output) == json.loads(stdlib_output) == simple_data


@pytest.mark.parametrize(
    "test_input",
    [
        "simple string",
        42,
        math.pi,
        True,
        False,
        None,
        [],
        {},
        [1, 2, 3],
        {"key": "value"},
        {"mixed": [1, "two", {"three": 3}]},
    ],
)
def test_parametrized_round_trip(test_input: Any) -> None:
    """Parametrized test for round-trip serialization of various inputs."""
    serialized = to_json(test_input)
    deserialized = from_json(serialized)
    assert deserialized == test_input


def test_imports_work_correctly() -> None:
    """Test that the imports from _serialization module work correctly."""

    assert callable(to_json)
    assert callable(from_json)

    test_data = {"test": "import"}
    assert from_json(to_json(test_data)) == test_data


def test_module_all_exports() -> None:
    """Test that __all__ contains the expected exports."""

    expected = {
        "SchemaSerializer",
        "from_json",
        "get_collection_serializer",
        "get_serializer_metrics",
        "numpy_array_dec_hook",
        "numpy_array_enc_hook",
        "numpy_array_predicate",
        "reset_serializer_cache",
        "schema_dump",
        "serialize_collection",
        "to_json",
    }

    assert set(__all__) == expected


def test_error_messages_are_helpful() -> None:
    """Test that error messages from invalid JSON are helpful."""
    try:
        from_json("invalid json content")
        assert False, "Should have raised an exception"
    except Exception as e:
        error_msg = str(e).lower()

        assert any(word in error_msg for word in ["json", "decode", "parse", "invalid", "expect", "malformed"])


numpy_available = pytest.importorskip("numpy", reason="NumPy not installed")


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_array_enc_hook_basic() -> None:
    """Test basic NumPy array encoding to list."""
    import numpy as np

    arr = np.array([1.0, 2.0, 3.0])
    result = numpy_array_enc_hook(arr)

    assert result == [1.0, 2.0, 3.0]
    assert isinstance(result, list)


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_array_enc_hook_multidimensional() -> None:
    """Test NumPy array encoding for multi-dimensional arrays."""
    import numpy as np

    arr_2d = np.array([[1, 2], [3, 4]])
    result = numpy_array_enc_hook(arr_2d)

    assert result == [[1, 2], [3, 4]]

    arr_3d = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]])
    result_3d = numpy_array_enc_hook(arr_3d)

    assert result_3d == [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_array_enc_hook_empty() -> None:
    """Test NumPy array encoding for empty arrays."""
    import numpy as np

    empty_arr = np.array([])
    result = numpy_array_enc_hook(empty_arr)

    assert result == []


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_array_enc_hook_various_dtypes() -> None:
    """Test NumPy array encoding for various dtypes."""
    import numpy as np

    arr_float32 = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    assert numpy_array_enc_hook(arr_float32) == [1.0, 2.0, 3.0]

    arr_float64 = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    assert numpy_array_enc_hook(arr_float64) == [1.0, 2.0, 3.0]

    arr_int64 = np.array([1, 2, 3], dtype=np.int64)
    assert numpy_array_enc_hook(arr_int64) == [1, 2, 3]

    arr_uint8 = np.array([1, 2, 3], dtype=np.uint8)
    assert numpy_array_enc_hook(arr_uint8) == [1, 2, 3]


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_array_enc_hook_non_array() -> None:
    """Test that non-array values are passed through unchanged."""

    assert numpy_array_enc_hook([1, 2, 3]) == [1, 2, 3]
    assert numpy_array_enc_hook("string") == "string"
    assert numpy_array_enc_hook(42) == 42
    assert numpy_array_enc_hook(None) is None


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_array_dec_hook_basic() -> None:
    """Test basic list decoding to NumPy array."""
    import numpy as np

    result = numpy_array_dec_hook([1.0, 2.0, 3.0])

    assert isinstance(result, np.ndarray)
    assert np.array_equal(result, np.array([1.0, 2.0, 3.0]))


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_array_dec_hook_multidimensional() -> None:
    """Test list decoding for multi-dimensional arrays."""
    import numpy as np

    result_2d = numpy_array_dec_hook([[1, 2], [3, 4]])
    expected_2d = np.array([[1, 2], [3, 4]])

    assert isinstance(result_2d, np.ndarray)
    assert np.array_equal(result_2d, expected_2d)


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_array_dec_hook_empty() -> None:
    """Test list decoding for empty lists."""
    import numpy as np

    result = numpy_array_dec_hook([])

    assert isinstance(result, np.ndarray)
    assert len(result) == 0


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_array_dec_hook_non_list() -> None:
    """Test that non-list values are passed through unchanged."""

    assert numpy_array_dec_hook("string") == "string"
    assert numpy_array_dec_hook(42) == 42
    assert numpy_array_dec_hook(None) is None


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_array_predicate_basic() -> None:
    """Test NumPy array predicate for type checking."""
    import numpy as np

    arr = np.array([1, 2, 3])
    assert numpy_array_predicate(arr) is True

    assert numpy_array_predicate([1, 2, 3]) is False
    assert numpy_array_predicate("string") is False
    assert numpy_array_predicate(42) is False
    assert numpy_array_predicate(None) is False


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_round_trip() -> None:
    """Test round-trip NumPy array serialization."""
    import numpy as np

    original = np.array([1.5, 2.5, 3.5])

    encoded = numpy_array_enc_hook(original)
    decoded = numpy_array_dec_hook(encoded)

    assert isinstance(encoded, list)
    assert isinstance(decoded, np.ndarray)
    assert np.array_equal(decoded, original)


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_round_trip_multidimensional() -> None:
    """Test round-trip for multi-dimensional NumPy arrays."""
    import numpy as np

    original = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])

    encoded = numpy_array_enc_hook(original)
    decoded = numpy_array_dec_hook(encoded)

    assert isinstance(encoded, list)
    assert isinstance(decoded, np.ndarray)
    assert np.array_equal(decoded, original)


@pytest.mark.skipif(not numpy_available, reason="NumPy not installed")
def test_numpy_serialization_with_to_json() -> None:
    """Test that NumPy arrays can be serialized with to_json via hook."""
    import numpy as np

    arr = np.array([1.0, 2.0, 3.0])

    encoded = numpy_array_enc_hook(arr)
    json_str = to_json(encoded)

    assert isinstance(json_str, str)

    decoded_list = from_json(json_str)
    assert decoded_list == [1.0, 2.0, 3.0]
