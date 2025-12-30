"""Tests for arrow_helpers conversion utilities."""

import math
from typing import Any

import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import PYARROW_INSTALLED
from sqlspec.utils.arrow_helpers import convert_dict_to_arrow

pytestmark = pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")


def test_convert_empty_data_to_table() -> None:
    """Test converting empty data to Arrow Table."""

    result = convert_dict_to_arrow([], return_format="table")

    assert result.num_rows == 0
    assert result.num_columns == 0


def test_convert_empty_data_to_batch() -> None:
    """Test converting empty data to RecordBatch."""

    result = convert_dict_to_arrow([], return_format="batch")

    assert result.num_rows == 0
    assert result.num_columns == 0


def test_convert_single_row_to_table() -> None:
    """Test converting single row to Arrow Table."""

    data = [{"id": 1, "name": "Alice", "age": 30}]
    result = convert_dict_to_arrow(data, return_format="table")

    assert result.num_rows == 1
    assert result.num_columns == 3
    assert result.column_names == ["id", "name", "age"]


def test_convert_multiple_rows_to_table() -> None:
    """Test converting multiple rows to Arrow Table."""

    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
    ]
    result = convert_dict_to_arrow(data, return_format="table")

    assert result.num_rows == 3
    assert result.num_columns == 3
    assert result.column_names == ["id", "name", "age"]


def test_convert_to_record_batch() -> None:
    """Test converting data to RecordBatch."""

    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    result = convert_dict_to_arrow(data, return_format="batch")

    assert result.num_rows == 2
    assert result.num_columns == 2


def test_convert_with_null_values() -> None:
    """Test converting data with NULL/None values."""

    data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": None},
        {"id": 3, "name": "Charlie", "email": None},
    ]
    result = convert_dict_to_arrow(data, return_format="table")

    assert result.num_rows == 3
    assert result.num_columns == 3

    # Check that NULL values are preserved
    pydict = result.to_pydict()
    assert pydict["email"][1] is None
    assert pydict["email"][2] is None


def test_convert_with_various_types() -> None:
    """Test converting data with various Python types."""

    data = [{"int_col": 42, "float_col": math.pi, "str_col": "hello", "bool_col": True, "none_col": None}]
    result = convert_dict_to_arrow(data, return_format="table")

    assert result.num_rows == 1
    assert result.num_columns == 5

    # Verify types are inferred correctly by pyarrow
    pydict = result.to_pydict()
    assert isinstance(pydict["int_col"][0], int)
    assert isinstance(pydict["float_col"][0], float)
    assert isinstance(pydict["str_col"][0], str)
    assert isinstance(pydict["bool_col"][0], bool)
    assert pydict["none_col"][0] is None


def test_convert_preserves_column_order() -> None:
    """Test that column order is preserved during conversion."""

    data = [{"z_col": 1, "a_col": 2, "m_col": 3}]
    result = convert_dict_to_arrow(data, return_format="table")

    # Dictionary order should be preserved (Python 3.7+)
    assert result.column_names == ["z_col", "a_col", "m_col"]


def test_convert_without_pyarrow_raises_import_error() -> None:
    """Test that MissingDependencyError is raised when pyarrow is not available."""

    if PYARROW_INSTALLED:
        pytest.skip("pyarrow is installed")

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        convert_dict_to_arrow([{"id": 1}])


def test_convert_with_missing_keys_in_some_rows() -> None:
    """Test converting data where some rows are missing keys."""

    # First row has all keys, subsequent rows may be missing some
    data: list[dict[str, Any]] = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob"},  # missing 'email'
        {"id": 3},  # missing 'name' and 'email'
    ]

    result = convert_dict_to_arrow(data, return_format="table")

    assert result.num_rows == 3
    # All columns from first row should be present
    assert result.num_columns == 3

    pydict = result.to_pydict()
    assert pydict["id"] == [1, 2, 3]
    assert pydict["name"] == ["Alice", "Bob", None]
    assert pydict["email"] == ["alice@example.com", None, None]
