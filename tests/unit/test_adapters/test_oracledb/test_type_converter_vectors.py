"""Unit tests for Oracle type converter NumPy vector methods."""

import array

import pytest

from sqlspec.adapters.oracledb.type_converter import OracleOutputConverter
from sqlspec.typing import NUMPY_INSTALLED

pytestmark = pytest.mark.skipif(not NUMPY_INSTALLED, reason="NumPy not installed")


def test_convert_vector_to_numpy_with_float32_array() -> None:
    """Test converting Oracle array.array to NumPy float32 array."""
    import numpy as np

    converter = OracleOutputConverter()
    oracle_array = array.array("f", [1.0, 2.0, 3.0])

    result = converter.convert_vector_to_numpy(oracle_array)

    assert isinstance(result, np.ndarray)
    assert result.dtype.kind == "f"
    np.testing.assert_array_equal(result, [1.0, 2.0, 3.0])


def test_convert_vector_to_numpy_with_float64_array() -> None:
    """Test converting Oracle array.array to NumPy float64 array."""
    import numpy as np

    converter = OracleOutputConverter()
    oracle_array = array.array("d", [1.0, 2.0, 3.0])

    result = converter.convert_vector_to_numpy(oracle_array)

    assert isinstance(result, np.ndarray)
    assert result.dtype.kind == "f"
    np.testing.assert_array_equal(result, [1.0, 2.0, 3.0])


def test_convert_vector_to_numpy_with_uint8_array() -> None:
    """Test converting Oracle array.array to NumPy uint8 array."""
    import numpy as np

    converter = OracleOutputConverter()
    oracle_array = array.array("B", [1, 2, 3])

    result = converter.convert_vector_to_numpy(oracle_array)

    assert isinstance(result, np.ndarray)
    np.testing.assert_array_equal(result, [1, 2, 3])


def test_convert_vector_to_numpy_with_int8_array() -> None:
    """Test converting Oracle array.array to NumPy int8 array."""
    import numpy as np

    converter = OracleOutputConverter()
    oracle_array = array.array("b", [-1, 2, -3])

    result = converter.convert_vector_to_numpy(oracle_array)

    assert isinstance(result, np.ndarray)
    np.testing.assert_array_equal(result, [-1, 2, -3])


def test_convert_vector_to_numpy_returns_non_array_unchanged() -> None:
    """Test that non-array values are returned unchanged."""
    converter = OracleOutputConverter()

    assert converter.convert_vector_to_numpy("not an array") == "not an array"
    assert converter.convert_vector_to_numpy(42) == 42
    assert converter.convert_vector_to_numpy(None) is None


def test_convert_numpy_to_vector_with_float32() -> None:
    """Test converting NumPy float32 array to Oracle array."""
    import numpy as np

    converter = OracleOutputConverter()
    np_array = np.array([1.0, 2.0, 3.0], dtype=np.float32)

    result = converter.convert_numpy_to_vector(np_array)

    assert isinstance(result, array.array)
    assert result.typecode == "f"
    assert list(result) == [1.0, 2.0, 3.0]


def test_convert_numpy_to_vector_with_float64() -> None:
    """Test converting NumPy float64 array to Oracle array."""
    import numpy as np

    converter = OracleOutputConverter()
    np_array = np.array([1.0, 2.0, 3.0], dtype=np.float64)

    result = converter.convert_numpy_to_vector(np_array)

    assert isinstance(result, array.array)
    assert result.typecode == "d"
    assert list(result) == [1.0, 2.0, 3.0]


def test_convert_numpy_to_vector_with_uint8() -> None:
    """Test converting NumPy uint8 array to Oracle array."""
    import numpy as np

    converter = OracleOutputConverter()
    np_array = np.array([1, 2, 3], dtype=np.uint8)

    result = converter.convert_numpy_to_vector(np_array)

    assert isinstance(result, array.array)
    assert result.typecode == "B"
    assert list(result) == [1, 2, 3]


def test_convert_numpy_to_vector_with_int8() -> None:
    """Test converting NumPy int8 array to Oracle array."""
    import numpy as np

    converter = OracleOutputConverter()
    np_array = np.array([-1, 2, -3], dtype=np.int8)

    result = converter.convert_numpy_to_vector(np_array)

    assert isinstance(result, array.array)
    assert result.typecode == "b"
    assert list(result) == [-1, 2, -3]


def test_convert_numpy_to_vector_with_unsupported_dtype_raises_type_error() -> None:
    """Test that unsupported NumPy dtype raises TypeError."""
    import numpy as np

    converter = OracleOutputConverter()
    np_array = np.array([1.0, 2.0, 3.0], dtype=np.float16)

    with pytest.raises(TypeError, match=r"Unsupported NumPy dtype.*float16"):
        converter.convert_numpy_to_vector(np_array)


def test_convert_numpy_to_vector_returns_non_numpy_unchanged() -> None:
    """Test that non-NumPy values are returned unchanged."""
    converter = OracleOutputConverter()

    assert converter.convert_numpy_to_vector("not numpy") == "not numpy"
    assert converter.convert_numpy_to_vector(42) == 42
    assert converter.convert_numpy_to_vector(None) is None


def test_convert_vector_to_numpy_round_trip() -> None:
    """Test round-trip conversion NumPy → Oracle → NumPy."""
    import numpy as np

    converter = OracleOutputConverter()
    original = np.array([1.5, 2.5, 3.5], dtype=np.float32)

    oracle_array = converter.convert_numpy_to_vector(original)
    result = converter.convert_vector_to_numpy(oracle_array)

    assert isinstance(result, np.ndarray)
    np.testing.assert_array_almost_equal(result, original)


def test_converter_methods_with_numpy_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test converter methods gracefully handle NumPy not installed."""
    import sqlspec.adapters.oracledb.type_converter

    monkeypatch.setattr(sqlspec.adapters.oracledb.type_converter, "NUMPY_INSTALLED", False)

    converter = OracleOutputConverter()
    oracle_array = array.array("f", [1.0, 2.0, 3.0])

    result = converter.convert_vector_to_numpy(oracle_array)
    assert result is oracle_array

    result = converter.convert_numpy_to_vector("some value")
    assert result == "some value"


def test_convert_vector_to_numpy_uses_copy_true() -> None:
    """Test that convert_vector_to_numpy uses copy=True for safety."""

    converter = OracleOutputConverter()
    oracle_array = array.array("f", [1.0, 2.0, 3.0])

    result = converter.convert_vector_to_numpy(oracle_array)

    oracle_array[0] = 999.0

    assert result[0] == 1.0
