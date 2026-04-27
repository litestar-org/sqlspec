"""Unit tests for Oracle vector input/output type handlers.

Mirrors the dispatch-matrix style of ``test_json_handlers.py`` (C1).
Covers:
    - Input handler claims numpy ndarray, list/tuple of float|int, array.array.
    - Input handler rejects empty list, list[str], list[bool], list[dict], dict.
    - Output handler honours ``connection._sqlspec_vector_return_format``.
    - Output handler raises ``ValueError`` on invalid format.
    - Output handler raises ``RuntimeError`` for ``"numpy"`` without numpy.
"""

import array
from unittest.mock import MagicMock, Mock

import pytest

from sqlspec.typing import NUMPY_INSTALLED


def _mock_cursor() -> Mock:
    cursor = MagicMock()
    cursor.arraysize = 100
    return cursor


def _mock_cursor_with_format(fmt: "str | None") -> Mock:
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.connection = MagicMock()
    if fmt is None:
        if hasattr(cursor.connection, "_sqlspec_vector_return_format"):
            del cursor.connection._sqlspec_vector_return_format
    else:
        cursor.connection._sqlspec_vector_return_format = fmt
    return cursor


def _mock_metadata(type_code: object) -> Mock:
    md = Mock()
    md.type_code = type_code
    return md


def test_input_handler_claims_list_of_float() -> None:
    """``list[float]`` is auto-packed as float32 and bound to DB_TYPE_VECTOR."""
    from sqlspec.adapters.oracledb._vector_handlers import _input_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor()
    cursor.var.return_value = "var-token"

    result = _input_type_handler(cursor, [1.0, 2.0, 3.0], 1)

    assert result == "var-token"
    cursor.var.assert_called_once()
    kwargs = cursor.var.call_args.kwargs
    assert kwargs["arraysize"] == 1
    assert callable(kwargs["inconverter"])
    packed = kwargs["inconverter"](None)
    assert isinstance(packed, array.array)
    assert packed.typecode == "f"
    assert list(packed) == [1.0, 2.0, 3.0]


def test_input_handler_claims_tuple_of_float() -> None:
    """``tuple[float, ...]`` is treated identically to ``list[float]``."""
    from sqlspec.adapters.oracledb._vector_handlers import _input_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor()
    cursor.var.return_value = "var-token"

    result = _input_type_handler(cursor, (0.5, 0.25, 0.125), 1)

    assert result == "var-token"
    packed = cursor.var.call_args.kwargs["inconverter"](None)
    assert packed.typecode == "f"
    assert list(packed) == [0.5, 0.25, 0.125]


def test_input_handler_packs_int_in_int8_range_as_int8() -> None:
    """``list[int]`` with all values in [-128, 127] is packed as int8."""
    from sqlspec.adapters.oracledb._vector_handlers import _input_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor()
    cursor.var.return_value = "var-token"

    _input_type_handler(cursor, [-128, 0, 64, 127], 1)

    packed = cursor.var.call_args.kwargs["inconverter"](None)
    assert packed.typecode == "b"
    assert list(packed) == [-128, 0, 64, 127]


def test_input_handler_packs_int_outside_int8_range_as_float32() -> None:
    """``list[int]`` with any out-of-range value falls back to float32."""
    from sqlspec.adapters.oracledb._vector_handlers import _input_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor()
    cursor.var.return_value = "var-token"

    _input_type_handler(cursor, [0, 200], 1)

    packed = cursor.var.call_args.kwargs["inconverter"](None)
    assert packed.typecode == "f"
    assert list(packed) == [0.0, 200.0]


def test_input_handler_passes_array_array_through() -> None:
    """``array.array`` is bound directly without an inconverter."""
    from sqlspec.adapters.oracledb._vector_handlers import _input_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor()
    cursor.var.return_value = "var-token"

    payload = array.array("f", [1.0, 2.0])
    _input_type_handler(cursor, payload, 1)

    cursor.var.assert_called_once()
    kwargs = cursor.var.call_args.kwargs
    assert "inconverter" not in kwargs


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="NumPy not installed")
def test_input_handler_claims_ndarray() -> None:
    """``np.ndarray`` continues to bind via the existing ``numpy_converter_in`` path."""
    import numpy as np

    from sqlspec.adapters.oracledb._vector_handlers import _input_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor()
    cursor.var.return_value = "var-token"

    arr = np.array([1.0, 2.0], dtype=np.float32)
    _input_type_handler(cursor, arr, 1)

    cursor.var.assert_called_once()
    assert callable(cursor.var.call_args.kwargs["inconverter"])


def test_input_handler_rejects_empty_list() -> None:
    """Empty list yields ``None`` so JSON / generic handlers can claim it."""
    from sqlspec.adapters.oracledb._vector_handlers import _input_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor()

    assert _input_type_handler(cursor, [], 1) is None
    cursor.var.assert_not_called()


def test_input_handler_rejects_list_of_str() -> None:
    """A list of strings is not a numeric vector — falls through."""
    from sqlspec.adapters.oracledb._vector_handlers import _input_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor()

    assert _input_type_handler(cursor, ["a", "b"], 1) is None
    cursor.var.assert_not_called()


def test_input_handler_rejects_list_of_bool() -> None:
    """``bool`` is a subclass of ``int``; the predicate must explicitly reject it."""
    from sqlspec.adapters.oracledb._vector_handlers import _input_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor()

    assert _input_type_handler(cursor, [True, False], 1) is None
    cursor.var.assert_not_called()


def test_input_handler_rejects_list_of_dict() -> None:
    """``list[dict]`` belongs to the JSON handler, not vectors."""
    from sqlspec.adapters.oracledb._vector_handlers import _input_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor()

    assert _input_type_handler(cursor, [{"a": 1}, {"b": 2}], 1) is None
    cursor.var.assert_not_called()


def test_input_handler_rejects_dict() -> None:
    """A bare dict is owned by the JSON handler."""
    from sqlspec.adapters.oracledb._vector_handlers import _input_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor()

    assert _input_type_handler(cursor, {"a": 1}, 1) is None
    cursor.var.assert_not_called()


def test_output_handler_returns_none_for_non_vector_column() -> None:
    """Non-VECTOR columns are not claimed by the vector output handler."""
    import oracledb

    from sqlspec.adapters.oracledb._vector_handlers import _output_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor_with_format("numpy")
    metadata = _mock_metadata(oracledb.DB_TYPE_VARCHAR)

    assert _output_type_handler(cursor, metadata) is None


@pytest.mark.skipif(not NUMPY_INSTALLED, reason="NumPy not installed")
def test_output_handler_numpy_format_uses_numpy_outconverter() -> None:
    """``vector_return_format="numpy"`` registers the numpy outconverter."""
    import oracledb

    from sqlspec.adapters.oracledb._vector_handlers import (
        _output_type_handler,  # pyright: ignore[reportPrivateUsage]
        numpy_converter_out,
    )

    cursor = _mock_cursor_with_format("numpy")
    cursor.var.return_value = "var-token"
    metadata = _mock_metadata(oracledb.DB_TYPE_VECTOR)

    result = _output_type_handler(cursor, metadata)

    assert result == "var-token"
    kwargs = cursor.var.call_args.kwargs
    assert kwargs["arraysize"] == cursor.arraysize
    assert kwargs["outconverter"] is numpy_converter_out


def test_output_handler_list_format_uses_list_outconverter() -> None:
    """``vector_return_format="list"`` registers ``list`` as the outconverter."""
    import oracledb

    from sqlspec.adapters.oracledb._vector_handlers import _output_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor_with_format("list")
    cursor.var.return_value = "var-token"
    metadata = _mock_metadata(oracledb.DB_TYPE_VECTOR)

    result = _output_type_handler(cursor, metadata)

    assert result == "var-token"
    kwargs = cursor.var.call_args.kwargs
    assert kwargs["outconverter"] is list


def test_output_handler_array_format_passes_through() -> None:
    """``vector_return_format="array"`` returns ``None`` (oracledb default)."""
    import oracledb

    from sqlspec.adapters.oracledb._vector_handlers import _output_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor_with_format("array")
    metadata = _mock_metadata(oracledb.DB_TYPE_VECTOR)

    assert _output_type_handler(cursor, metadata) is None
    cursor.var.assert_not_called()


def test_output_handler_invalid_format_raises_value_error() -> None:
    """An unknown ``vector_return_format`` raises a clear ``ValueError``."""
    import oracledb

    from sqlspec.adapters.oracledb._vector_handlers import _output_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = _mock_cursor_with_format("yaml")
    metadata = _mock_metadata(oracledb.DB_TYPE_VECTOR)

    with pytest.raises(ValueError, match="Invalid vector_return_format"):
        _output_type_handler(cursor, metadata)


def test_output_handler_numpy_without_numpy_raises_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """``"numpy"`` format with NumPy missing raises a friendly ``RuntimeError``."""
    import oracledb

    import sqlspec.adapters.oracledb._vector_handlers as vh

    monkeypatch.setattr(vh, "NUMPY_INSTALLED", False)

    cursor = _mock_cursor_with_format("numpy")
    metadata = _mock_metadata(oracledb.DB_TYPE_VECTOR)

    with pytest.raises(RuntimeError, match="vector_return_format='numpy' requires numpy"):
        vh._output_type_handler(cursor, metadata)  # pyright: ignore[reportPrivateUsage]


def test_output_handler_default_format_when_attr_missing() -> None:
    """When the connection lacks ``_sqlspec_vector_return_format``, fall back to default."""
    import oracledb

    from sqlspec.adapters.oracledb._vector_handlers import _output_type_handler  # pyright: ignore[reportPrivateUsage]

    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.connection = Mock(spec=[])
    cursor.var.return_value = "var-token"

    metadata = _mock_metadata(oracledb.DB_TYPE_VECTOR)
    result = _output_type_handler(cursor, metadata)

    assert result == "var-token"
    cursor.var.assert_called_once()
