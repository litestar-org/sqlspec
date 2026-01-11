# pyright: reportPrivateUsage=false
"""Tests for to_schema functionality from CommonDriverAttributesMixin.

Tests numpy array handling, msgspec deserialization, and type conversion functionality.
Uses function-based pytest approach as per AGENTS.md requirements.
"""

from typing import Any
from unittest.mock import Mock, patch

import msgspec
import pytest
from typing_extensions import TypedDict

from sqlspec.driver import CommonDriverAttributesMixin
from sqlspec.typing import NUMPY_INSTALLED
from sqlspec.utils.schema import (
    _DEFAULT_TYPE_DECODERS,
    _convert_numpy_to_list,
    _default_msgspec_deserializer,
    _is_list_type_target,
)

pytestmark = pytest.mark.xdist_group("driver")


# Test helper classes
class SampleMsgspecStruct(msgspec.Struct):
    """Sample msgspec struct for testing."""

    name: str
    embedding: "list[float]"
    metadata: "dict[str, Any] | None" = None


class SampleMsgspecStructWithIntList(msgspec.Struct):
    """Sample msgspec struct with int list for testing."""

    name: str
    values: "list[int]"


class SampleTypedDict(TypedDict):
    """Sample TypedDict for testing."""

    name: str
    age: int
    optional_field: "str | None"


# Test _is_list_type_target function
def test_is_list_type_target_with_list_types() -> None:
    """Test detection of list type targets."""
    # Test list[float]
    assert _is_list_type_target(list[float]) is True

    # Test list[int]
    assert _is_list_type_target(list[int]) is True

    # Test list[str]
    assert _is_list_type_target(list[str]) is True

    # Test list[Any]
    assert _is_list_type_target(list[Any]) is True


def test_is_list_type_target_with_non_list_types() -> None:
    """Test that non-list types are not detected as list targets."""
    assert _is_list_type_target(str) is False
    assert _is_list_type_target(int) is False
    assert _is_list_type_target(float) is False
    assert _is_list_type_target(dict) is False
    assert _is_list_type_target(tuple) is False


def test_is_list_type_target_with_none_and_invalid_types() -> None:
    """Test edge cases with None and invalid types."""
    assert _is_list_type_target(None) is False
    assert _is_list_type_target("not_a_type") is False
    assert _is_list_type_target(42) is False


# Test _convert_numpy_to_list function
@pytest.mark.skipif(not NUMPY_INSTALLED, reason="numpy not installed")
def test_convert_numpy_to_list_with_numpy_available() -> None:
    """Test numpy array conversion when numpy is available."""
    import numpy as np

    # Test with list[float] target type
    numpy_array = np.array([1.0, 2.0, 3.0])
    result = _convert_numpy_to_list(list[float], numpy_array)
    assert result == [1.0, 2.0, 3.0]
    assert isinstance(result, list)

    # Test with list[int] target type
    numpy_int_array = np.array([1, 2, 3])
    result = _convert_numpy_to_list(list[int], numpy_int_array)
    assert result == [1, 2, 3]
    assert isinstance(result, list)


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="numpy not installed")
def test_convert_numpy_to_list_with_non_list_target() -> None:
    """Test that numpy arrays are not converted for non-list targets."""
    import numpy as np

    numpy_array = np.array([1.0, 2.0, 3.0])

    # Should return original numpy array for non-list targets
    result = _convert_numpy_to_list(str, numpy_array)
    assert np.array_equal(result, numpy_array)

    result = _convert_numpy_to_list(int, numpy_array)
    assert np.array_equal(result, numpy_array)


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="numpy not installed")
def test_convert_numpy_to_list_with_non_numpy_value() -> None:
    """Test that non-numpy values are returned unchanged."""
    regular_list = [1.0, 2.0, 3.0]
    result = _convert_numpy_to_list(list[float], regular_list)
    assert result == regular_list

    string_value = "test"
    result = _convert_numpy_to_list(str, string_value)
    assert result == string_value


def test_convert_numpy_to_list_when_numpy_not_installed() -> None:
    """Test graceful fallback when numpy not available."""
    with patch("sqlspec.typing.NUMPY_INSTALLED", False):
        # Should return the value unchanged
        test_value = [1.0, 2.0, 3.0]  # Regular list
        result = _convert_numpy_to_list(list[float], test_value)
        assert result == test_value


# Test _default_msgspec_deserializer function
@pytest.mark.skipif(not NUMPY_INSTALLED, reason="numpy not installed")
def test_default_msgspec_deserializer_with_numpy_array() -> None:
    """Test that msgspec deserializer handles numpy arrays correctly."""
    import numpy as np

    numpy_array = np.array([1.0, 2.5, 3.7])

    # Should convert numpy array to list for list[float] target
    result = _default_msgspec_deserializer(list[float], numpy_array)
    assert result == [1.0, 2.5, 3.7]
    assert isinstance(result, list)


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="numpy not installed")
def test_default_msgspec_deserializer_with_numpy_array_non_list_target() -> None:
    """Test msgspec deserializer with numpy array for non-list target."""
    import numpy as np

    numpy_array = np.array([1.0, 2.0, 3.0])

    # Should return original numpy array for non-list targets
    result = _default_msgspec_deserializer(str, numpy_array)
    assert np.array_equal(result, numpy_array)


def test_default_msgspec_deserializer_with_type_decoders() -> None:
    """Test that type decoders are processed correctly."""
    # Test with the default UUID decoder
    from uuid import UUID

    test_uuid = UUID("12345678-1234-5678-1234-567812345678")

    result = _default_msgspec_deserializer(UUID, test_uuid, type_decoders=_DEFAULT_TYPE_DECODERS)
    # UUID is handled by the UUID-specific logic, not the type decoders
    # So it returns the original UUID, not the hex
    assert result == test_uuid


def test_default_msgspec_deserializer_with_regular_values() -> None:
    """Test that regular values are handled correctly."""
    # Test string
    result = _default_msgspec_deserializer(str, "test_string")
    assert result == "test_string"

    # Test int
    result = _default_msgspec_deserializer(int, 42)
    assert result == 42

    # Test list
    test_list = [1, 2, 3]
    result = _default_msgspec_deserializer(list[int], test_list)
    assert result == test_list


# Test CommonDriverAttributesMixin integration
@pytest.mark.skipif(not NUMPY_INSTALLED, reason="numpy not installed")
def test_to_schema_mixin_with_numpy_array_single_record() -> None:
    """Test CommonDriverAttributesMixin.to_schema with numpy array in single record."""
    import numpy as np

    # Create test data with numpy array
    test_data = {"name": "test_embedding", "embedding": np.array([1.0, 2.0, 3.0]), "metadata": {"model": "test"}}

    # First, let's test direct dec_hook functionality
    from functools import partial

    deserializer = partial(_default_msgspec_deserializer, type_decoders=_DEFAULT_TYPE_DECODERS)

    # Test the dec_hook directly
    embedding_result = deserializer(list[float], test_data["embedding"])
    assert embedding_result == [1.0, 2.0, 3.0]
    assert isinstance(embedding_result, list)

    # Now test the full conversion
    result = CommonDriverAttributesMixin.to_schema(test_data, schema_type=SampleMsgspecStruct)

    assert isinstance(result, SampleMsgspecStruct)
    assert result.name == "test_embedding"
    assert result.embedding == [1.0, 2.0, 3.0]  # Should be converted to list
    assert isinstance(result.embedding, list)
    assert result.metadata == {"model": "test"}


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="numpy not installed")
def test_to_schema_mixin_with_numpy_array_multiple_records() -> None:
    """Test CommonDriverAttributesMixin.to_schema with numpy arrays in multiple records."""
    import numpy as np

    # Create test data with multiple records containing numpy arrays
    test_data: list[dict[str, Any]] = [
        {"name": "embedding_1", "embedding": np.array([1.0, 2.0, 3.0]), "metadata": None},
        {"name": "embedding_2", "embedding": np.array([4.0, 5.0, 6.0]), "metadata": {"source": "test"}},
    ]

    # Convert to schema
    result = CommonDriverAttributesMixin.to_schema(test_data, schema_type=SampleMsgspecStruct)

    assert isinstance(result, list)
    assert len(result) == 2

    # Check first record
    assert isinstance(result[0], SampleMsgspecStruct)
    assert result[0].name == "embedding_1"
    assert result[0].embedding == [1.0, 2.0, 3.0]
    assert isinstance(result[0].embedding, list)
    assert result[0].metadata is None

    # Check second record
    assert isinstance(result[1], SampleMsgspecStruct)
    assert result[1].name == "embedding_2"
    assert result[1].embedding == [4.0, 5.0, 6.0]
    assert isinstance(result[1].embedding, list)
    assert result[1].metadata == {"source": "test"}


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="numpy not installed")
def test_to_schema_mixin_with_different_numpy_dtypes() -> None:
    """Test CommonDriverAttributesMixin with different numpy array dtypes."""
    import numpy as np

    # Test int32 array
    int_data = {"name": "int_test", "values": np.array([1, 2, 3], dtype=np.int32)}

    result = CommonDriverAttributesMixin.to_schema(int_data, schema_type=SampleMsgspecStructWithIntList)
    assert isinstance(result, SampleMsgspecStructWithIntList)
    assert result.values == [1, 2, 3]
    assert isinstance(result.values, list)

    # Test float64 array
    float_data = {"name": "float_test", "embedding": np.array([1.1, 2.2, 3.3], dtype=np.float64)}

    result = CommonDriverAttributesMixin.to_schema(float_data, schema_type=SampleMsgspecStruct)
    assert isinstance(result, SampleMsgspecStruct)
    assert result.embedding == [1.1, 2.2, 3.3]
    assert isinstance(result.embedding, list)


def test_to_schema_mixin_with_regular_lists() -> None:
    """Test that regular lists still work correctly."""
    # Test with regular Python list (no numpy)
    test_data = {
        "name": "regular_list",
        "embedding": [1.0, 2.0, 3.0],  # Regular list
        "metadata": {"type": "manual"},
    }

    result = CommonDriverAttributesMixin.to_schema(test_data, schema_type=SampleMsgspecStruct)

    assert isinstance(result, SampleMsgspecStruct)
    assert result.name == "regular_list"
    assert result.embedding == [1.0, 2.0, 3.0]
    assert isinstance(result.embedding, list)
    assert result.metadata == {"type": "manual"}


def test_to_schema_mixin_without_schema_type() -> None:
    """Test that data is returned unchanged when no schema_type is provided."""
    test_data = {"name": "test", "values": [1, 2, 3]}

    result = CommonDriverAttributesMixin.to_schema(test_data)
    assert result == test_data


def test_to_schema_mixin_with_typeddict_single_record() -> None:
    """Test CommonDriverAttributesMixin.to_schema with TypedDict for single record."""
    test_data = {"name": "test_user", "age": 30, "optional_field": "value"}

    result = CommonDriverAttributesMixin.to_schema(test_data, schema_type=SampleTypedDict)

    assert result == test_data
    assert isinstance(result, dict)


def test_to_schema_mixin_with_typeddict_multiple_records() -> None:
    """Test CommonDriverAttributesMixin.to_schema with TypedDict for multiple records."""
    test_data = [
        {"name": "user1", "age": 25, "optional_field": "value1"},
        {"name": "user2", "age": 30, "optional_field": "value2"},
    ]

    result = CommonDriverAttributesMixin.to_schema(test_data, schema_type=SampleTypedDict)

    assert isinstance(result, list)
    assert len(result) == 2
    for item in result:
        assert isinstance(item, dict)
    assert result == test_data


def test_to_schema_mixin_with_typeddict_mixed_data() -> None:
    """Test CommonDriverAttributesMixin.to_schema with TypedDict filters non-dict items."""
    test_data = [
        {"name": "user1", "age": 25, "optional_field": "value1"},
        "not_a_dict",  # This should be filtered out
        {"name": "user2", "age": 30, "optional_field": "value2"},
    ]

    result = CommonDriverAttributesMixin.to_schema(test_data, schema_type=SampleTypedDict)

    assert isinstance(result, list)
    assert len(result) == 2  # Only dict items should be included
    for item in result:
        assert isinstance(item, dict)


def test_to_schema_mixin_with_typeddict_non_dict_data() -> None:
    """Test CommonDriverAttributesMixin.to_schema with TypedDict returns non-dict data unchanged."""
    test_data = "not_a_dict"

    result = CommonDriverAttributesMixin.to_schema(test_data, schema_type=SampleTypedDict)

    assert result == test_data


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="numpy not installed")
def test_numpy_array_conversion_edge_cases() -> None:
    """Test edge cases for numpy array conversion."""
    import numpy as np

    # Empty numpy array
    empty_array = np.array([])
    result = _convert_numpy_to_list(list[float], empty_array)
    assert result == []
    assert isinstance(result, list)

    # Single element array
    single_element = np.array([42.0])
    result = _convert_numpy_to_list(list[float], single_element)
    assert result == [42.0]
    assert isinstance(result, list)

    # Multi-dimensional array (should still convert)
    multi_dim = np.array([[1.0, 2.0], [3.0, 4.0]])
    result = _convert_numpy_to_list(list[float], multi_dim)
    assert result == [[1.0, 2.0], [3.0, 4.0]]
    assert isinstance(result, list)


def test_default_type_decoders_includes_numpy_converter() -> None:
    """Test that numpy converter is included in default type decoders."""
    # Check that our numpy converter function is in the decoders
    decoder_functions = [decoder for predicate, decoder in _DEFAULT_TYPE_DECODERS]
    assert _convert_numpy_to_list in decoder_functions

    # Check that the predicate function is there too
    predicate_functions = [predicate for predicate, decoder in _DEFAULT_TYPE_DECODERS]
    assert _is_list_type_target in predicate_functions


def test_integration_with_mock_numpy_when_not_installed() -> None:
    """Test behavior when numpy is not installed but we try to process numpy-like objects."""

    # Mock object that looks like it could be a numpy array
    mock_array = Mock()
    mock_array.tolist = Mock(return_value=[1.0, 2.0, 3.0])

    with patch("sqlspec.typing.NUMPY_INSTALLED", False):
        # Should return the mock object unchanged since numpy is "not installed"
        result = _convert_numpy_to_list(list[float], mock_array)
        assert result == mock_array
        # tolist should not have been called
        mock_array.tolist.assert_not_called()


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="numpy not installed")
def test_performance_no_conversion_when_not_needed() -> None:
    """Test that conversion only happens when needed."""
    import numpy as np

    # Create numpy array
    numpy_array = np.array([1.0, 2.0, 3.0])

    # When target is not a list type, should return original array
    result = _convert_numpy_to_list(str, numpy_array)

    # Should be the same object (no conversion overhead)
    assert result is numpy_array
