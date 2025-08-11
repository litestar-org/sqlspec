"""Comprehensive tests for sqlspec.utils.serializers module.

Tests for JSON serialization utilities that are re-exported from sqlspec._serialization.
Covers all serialization scenarios including edge cases and type handling.
"""

import json
from typing import Any

import pytest

from sqlspec.utils.serializers import from_json, to_json


def test_to_json_basic_types() -> None:
    """Test serialization of basic Python types."""
    # String
    assert to_json("hello") == '"hello"'

    # Number
    assert to_json(42) == "42"

    # Boolean
    assert to_json(True) == "true"
    assert to_json(False) == "false"

    # None
    assert to_json(None) == "null"


def test_to_json_collections() -> None:
    """Test serialization of collections."""
    # List - msgspec/orjson use compact formatting
    list_result = to_json([1, 2, 3])
    assert list_result in {"[1,2,3]", "[1, 2, 3]"}  # Accept both formats
    assert to_json([]) == "[]"

    # Dict
    result = to_json({"key": "value", "num": 42})
    # Parse back to verify correctness (order may vary)
    parsed = json.loads(result)
    assert parsed == {"key": "value", "num": 42}

    # Empty dict
    assert to_json({}) == "{}"


def test_to_json_nested_structures() -> None:
    """Test serialization of nested data structures."""
    nested = {
        "users": [{"id": 1, "name": "Alice", "active": True}, {"id": 2, "name": "Bob", "active": False}],
        "metadata": {"total": 2, "page": 1},
    }

    result = to_json(nested)
    # Verify by parsing back
    parsed = json.loads(result)
    assert parsed == nested


def test_to_json_unicode_strings() -> None:
    """Test serialization of Unicode strings."""
    unicode_text = "Hello 世界 🌍 café naïve résumé"
    result = to_json(unicode_text)

    # Should be valid JSON
    parsed = json.loads(result)
    assert parsed == unicode_text


def test_to_json_special_characters() -> None:
    """Test serialization of strings with special characters."""
    special_chars = "Line1\nLine2\tTabbed\"Quoted'Single\\Backslash"
    result = to_json(special_chars)

    # Should properly escape and be parseable
    parsed = json.loads(result)
    assert parsed == special_chars


def test_to_json_numeric_edge_cases() -> None:
    """Test serialization of edge case numeric values."""
    # Large integers
    large_int = 9223372036854775807  # Max 64-bit signed int
    assert to_json(large_int) == str(large_int)

    # Negative numbers
    assert to_json(-42) == "-42"

    # Zero
    assert to_json(0) == "0"
    assert to_json(0.0) == "0.0"


def test_to_json_empty_collections() -> None:
    """Test serialization of empty collections."""
    assert to_json([]) == "[]"
    assert to_json({}) == "{}"
    assert to_json(()) == "[]"  # Tuple serializes as array


def test_to_json_tuple_serialization() -> None:
    """Test that tuples are serialized as JSON arrays."""
    tuple_result = to_json((1, 2, 3))
    assert tuple_result in {"[1,2,3]", "[1, 2, 3]"}  # Accept both formats
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
    # String
    assert from_json('"hello"') == "hello"

    # Number
    assert from_json("42") == 42
    assert from_json("3.14") == 3.14

    # Boolean
    assert from_json("true") is True
    assert from_json("false") is False

    # None
    assert from_json("null") is None


def test_from_json_collections() -> None:
    """Test deserialization of JSON collections."""
    # Array
    assert from_json("[1, 2, 3]") == [1, 2, 3]
    assert from_json("[]") == []

    # Object
    result = from_json('{"key": "value", "num": 42}')
    assert result == {"key": "value", "num": 42}

    # Empty object
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
    # Large integers
    assert from_json("9223372036854775807") == 9223372036854775807

    # Negative numbers
    assert from_json("-42") == -42
    assert from_json("-3.14") == -3.14

    # Zero
    assert from_json("0") == 0
    assert from_json("0.0") == 0.0


def test_from_json_scientific_notation() -> None:
    """Test deserialization of scientific notation numbers."""
    assert from_json("1e5") == 100000.0
    assert from_json("1.5e-3") == 0.0015
    assert from_json("-2.5e2") == -250.0


def test_from_json_whitespace_handling() -> None:
    """Test that whitespace in JSON is handled correctly."""
    # Extra whitespace should be ignored
    assert from_json('  "hello"  ') == "hello"
    assert from_json('\n\t{\n\t  "key": "value"\n\t}\n') == {"key": "value"}


def test_from_json_invalid_json_raises_error() -> None:
    """Test that invalid JSON raises appropriate errors."""
    # Need to handle msgspec.DecodeError which is different from json.JSONDecodeError
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
    test_data = ["string", 42, 3.14, True, False, None, [], {}]

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
    long_string = "x" * 10000  # 10KB string
    serialized = to_json(long_string)
    deserialized = from_json(serialized)
    assert deserialized == long_string


def test_edge_case_deeply_nested_structures() -> None:
    """Test deeply nested data structures."""
    # Create deeply nested structure
    nested = "base"
    for i in range(100):
        nested = {"level": i, "data": nested}

    serialized = to_json(nested)
    deserialized = from_json(serialized)

    # Navigate back to verify structure
    current = deserialized
    for i in range(99, -1, -1):
        assert current["level"] == i
        current = current["data"]
    assert current == "base"


def test_edge_case_large_arrays() -> None:
    """Test serialization of large arrays."""
    large_array = list(range(10000))  # 10K element array
    serialized = to_json(large_array)
    deserialized = from_json(serialized)
    assert deserialized == large_array


def test_edge_case_dict_with_numeric_keys() -> None:
    """Test that dict keys are properly handled."""
    # JSON requires string keys, numeric keys should be converted
    data = {"1": "one", "2": "two", "key": "value"}
    serialized = to_json(data)
    deserialized = from_json(serialized)
    assert deserialized == data


def test_edge_case_special_float_values() -> None:
    """Test handling of special float values."""
    # Note: JSON doesn't support inf, -inf, or nan
    # These should either be handled gracefully or raise appropriate errors
    special_values = [float("inf"), float("-inf"), float("nan")]

    for value in special_values:
        # Depending on implementation, this might raise an error or convert to null
        try:
            serialized = to_json(value)
            # If it doesn't raise, it should produce valid JSON
            json.loads(serialized)
        except (ValueError, OverflowError):
            # It's acceptable to raise an error for these values
            pass


def test_compatibility_produces_valid_json() -> None:
    """Test that to_json produces JSON that can be parsed by stdlib json."""
    test_data = {"string": "hello", "number": 42, "array": [1, 2, 3], "nested": {"key": "value"}}

    serialized = to_json(test_data)

    # Should be parseable by standard json module
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
    # Simple data should produce similar output to stdlib
    simple_data = {"key": "value", "num": 42}

    our_output = to_json(simple_data)
    stdlib_output = json.dumps(simple_data)

    # Both should parse to the same data
    assert from_json(our_output) == json.loads(stdlib_output) == simple_data


@pytest.mark.parametrize(
    "test_input",
    [
        "simple string",
        42,
        3.14,
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
    # This test ensures the re-exports are working
    assert callable(to_json)
    assert callable(from_json)

    # Basic functionality test
    test_data = {"test": "import"}
    assert from_json(to_json(test_data)) == test_data


def test_module_all_exports() -> None:
    """Test that __all__ contains the expected exports."""
    from sqlspec.utils.serializers import __all__

    assert "from_json" in __all__
    assert "to_json" in __all__
    assert len(__all__) == 2


def test_error_messages_are_helpful() -> None:
    """Test that error messages from invalid JSON are helpful."""
    try:
        from_json("invalid json content")
        assert False, "Should have raised an exception"
    except Exception as e:
        # Should contain some indication of what went wrong
        error_msg = str(e).lower()
        # msgspec uses "malformed" instead of "invalid", so check for various terms
        assert any(word in error_msg for word in ["json", "decode", "parse", "invalid", "expect", "malformed"])
