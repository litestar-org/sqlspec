"""Unit tests for Oracle NumPy vector type handlers."""

import array
from unittest.mock import MagicMock, Mock

import pytest

from sqlspec.typing import NUMPY_INSTALLED

pytestmark = pytest.mark.skipif(not NUMPY_INSTALLED, reason="NumPy not installed")


def test_dtype_to_array_code_mapping() -> None:
    """Test dtype to array code mapping constant."""
    from sqlspec.adapters.oracledb import DTYPE_TO_ARRAY_CODE

    expected = {"float64": "d", "float32": "f", "uint8": "B", "int8": "b"}
    assert DTYPE_TO_ARRAY_CODE == expected


def test_numpy_converter_in_float32() -> None:
    """Test NumPy float32 array conversion to Oracle array."""
    import numpy as np

    from sqlspec.adapters.oracledb import numpy_converter_in

    arr = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    result = numpy_converter_in(arr)

    assert isinstance(result, array.array)
    assert result.typecode == "f"
    assert list(result) == [1.0, 2.0, 3.0]


def test_numpy_converter_in_float64() -> None:
    """Test NumPy float64 array conversion to Oracle array."""
    import numpy as np

    from sqlspec.adapters.oracledb import numpy_converter_in

    arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    result = numpy_converter_in(arr)

    assert isinstance(result, array.array)
    assert result.typecode == "d"
    assert list(result) == [1.0, 2.0, 3.0]


def test_numpy_converter_in_uint8() -> None:
    """Test NumPy uint8 array conversion to Oracle array."""
    import numpy as np

    from sqlspec.adapters.oracledb import numpy_converter_in

    arr = np.array([1, 2, 3], dtype=np.uint8)
    result = numpy_converter_in(arr)

    assert isinstance(result, array.array)
    assert result.typecode == "B"
    assert list(result) == [1, 2, 3]


def test_numpy_converter_in_int8() -> None:
    """Test NumPy int8 array conversion to Oracle array."""
    import numpy as np

    from sqlspec.adapters.oracledb import numpy_converter_in

    arr = np.array([1, -2, 3], dtype=np.int8)
    result = numpy_converter_in(arr)

    assert isinstance(result, array.array)
    assert result.typecode == "b"
    assert list(result) == [1, -2, 3]


def test_numpy_converter_in_unsupported_dtype_raises_type_error() -> None:
    """Test that unsupported NumPy dtype raises TypeError."""
    import numpy as np

    from sqlspec.adapters.oracledb import numpy_converter_in

    arr = np.array([1.0, 2.0, 3.0], dtype=np.float16)

    with pytest.raises(TypeError, match=r"Unsupported NumPy dtype.*float16.*Supported"):
        numpy_converter_in(arr)


def test_numpy_converter_out_float32() -> None:
    """Test Oracle array conversion to NumPy float32 array."""
    import numpy as np

    from sqlspec.adapters.oracledb import numpy_converter_out

    oracle_array = array.array("f", [1.0, 2.0, 3.0])
    result = numpy_converter_out(oracle_array)

    assert isinstance(result, np.ndarray)
    assert result.dtype.kind == "f"
    np.testing.assert_array_equal(result, [1.0, 2.0, 3.0])


def test_numpy_converter_out_float64() -> None:
    """Test Oracle array conversion to NumPy float64 array."""
    import numpy as np

    from sqlspec.adapters.oracledb import numpy_converter_out

    oracle_array = array.array("d", [1.0, 2.0, 3.0])
    result = numpy_converter_out(oracle_array)

    assert isinstance(result, np.ndarray)
    assert result.dtype.kind == "f"
    np.testing.assert_array_equal(result, [1.0, 2.0, 3.0])


def test_numpy_converter_out_uses_copy_true() -> None:
    """Test that numpy_converter_out uses copy=True for safety."""

    from sqlspec.adapters.oracledb import numpy_converter_out

    oracle_array = array.array("f", [1.0, 2.0, 3.0])
    result = numpy_converter_out(oracle_array)

    oracle_array[0] = 999.0

    assert result[0] == 1.0


def test_input_type_handler_registers_numpy_converter() -> None:
    """Test input type handler correctly registers NumPy converter."""
    import numpy as np

    from sqlspec.adapters.oracledb import numpy_input_type_handler

    mock_cursor = MagicMock()
    mock_var = MagicMock()
    mock_cursor.var.return_value = mock_var

    arr = np.array([1.0, 2.0], dtype=np.float32)
    result = numpy_input_type_handler(mock_cursor, arr, 1)

    assert result == mock_var
    mock_cursor.var.assert_called_once()
    call_args = mock_cursor.var.call_args

    assert call_args.kwargs["arraysize"] == 1
    assert callable(call_args.kwargs["inconverter"])


def test_input_type_handler_returns_none_for_non_numpy() -> None:
    """Test input type handler returns None for non-NumPy values."""
    from sqlspec.adapters.oracledb import numpy_input_type_handler

    mock_cursor = MagicMock()
    result = numpy_input_type_handler(mock_cursor, "not an array", 1)

    assert result is None
    mock_cursor.var.assert_not_called()


def test_output_type_handler_registers_numpy_converter() -> None:
    """Test output type handler correctly registers NumPy converter."""
    from sqlspec.adapters.oracledb import numpy_output_type_handler

    mock_cursor = MagicMock()
    mock_cursor.arraysize = 10
    mock_var = MagicMock()
    mock_cursor.var.return_value = mock_var

    mock_metadata = Mock()

    import oracledb

    mock_metadata.type_code = oracledb.DB_TYPE_VECTOR

    result = numpy_output_type_handler(mock_cursor, mock_metadata)

    assert result == mock_var
    mock_cursor.var.assert_called_once()
    call_args = mock_cursor.var.call_args

    assert call_args.kwargs["arraysize"] == 10
    assert callable(call_args.kwargs["outconverter"])


def test_output_type_handler_returns_none_for_non_vector() -> None:
    """Test output type handler returns None for non-VECTOR columns."""
    from sqlspec.adapters.oracledb import numpy_output_type_handler

    mock_cursor = MagicMock()
    mock_metadata = Mock()

    import oracledb

    mock_metadata.type_code = oracledb.DB_TYPE_VARCHAR

    result = numpy_output_type_handler(mock_cursor, mock_metadata)

    assert result is None
    mock_cursor.var.assert_not_called()


def test_register_numpy_handlers_sets_connection_handlers() -> None:
    """Test register_numpy_handlers sets both input and output handlers."""
    from sqlspec.adapters.oracledb import register_numpy_handlers

    mock_connection = MagicMock()
    register_numpy_handlers(mock_connection)

    assert mock_connection.inputtypehandler is not None
    assert mock_connection.outputtypehandler is not None
    assert callable(mock_connection.inputtypehandler)
    assert callable(mock_connection.outputtypehandler)


def test_register_numpy_handlers_with_numpy_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test register_numpy_handlers gracefully handles NumPy not installed."""
    import sqlspec.adapters.oracledb as oracledb_module

    monkeypatch.setattr(oracledb_module.numpy_handlers, "NUMPY_INSTALLED", False)

    from sqlspec.adapters.oracledb import register_numpy_handlers

    mock_connection = MagicMock(spec=[])
    register_numpy_handlers(mock_connection)

    assert len(mock_connection.method_calls) == 0
