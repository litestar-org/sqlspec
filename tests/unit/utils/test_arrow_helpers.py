"""Tests for arrow_helpers conversion utilities."""

import math
from typing import Any

import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import PYARROW_INSTALLED
from sqlspec.utils.arrow_helpers import coerce_arrow_table, convert_dict_to_arrow

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


def test_all_null_column_falls_back_to_string_type() -> None:
    """All-NULL columns must not collapse to Arrow ``null`` type (issue #631)."""
    import pyarrow as pa

    data = [{"id": 1, "label": None}, {"id": 2, "label": None}]
    result = convert_dict_to_arrow(data, return_format="table")

    label_type = result.schema.field("label").type
    assert not pa.types.is_null(label_type)
    assert label_type == pa.string()
    assert result.schema.field("id").type == pa.int64()
    assert result.to_pydict()["label"] == [None, None]


def test_all_null_column_fallback_applies_to_reader_format() -> None:
    """The null-type fallback must also flow through non-table return formats."""
    import pyarrow as pa

    data = [{"label": None}, {"label": None}]
    reader = convert_dict_to_arrow(data, return_format="reader")

    assert not pa.types.is_null(reader.schema.field("label").type)
    assert reader.schema.field("label").type == pa.string()


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


def test_coerce_arrow_table_accepts_record_batch() -> None:
    import pyarrow as pa

    batch = pa.RecordBatch.from_pylist([{"id": 1}, {"id": 2}])

    table = coerce_arrow_table(batch)

    assert table.num_rows == 2
    assert table.column_names == ["id"]


def test_coerce_arrow_table_accepts_iterable_rows() -> None:
    table = coerce_arrow_table(iter([{"id": 1}, {"id": 2}]))

    assert table.num_rows == 2
    assert table.column_names == ["id"]


def test_coerce_arrow_table_accepts_column_mapping() -> None:
    table = coerce_arrow_table({"id": [1, 2], "name": ["a", "b"]})

    assert table.num_rows == 2
    assert table.column_names == ["id", "name"]
    assert table.to_pydict() == {"id": [1, 2], "name": ["a", "b"]}


def test_arrow_reader_with_deferred_close_fires_on_exhaustion() -> None:
    import pyarrow as pa

    from sqlspec.utils.arrow_helpers import arrow_reader_with_deferred_close

    batches = [pa.record_batch({"id": pa.array([i, i + 1])}) for i in (0, 2, 4)]
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    closed: list[bool] = []

    wrapped = arrow_reader_with_deferred_close(reader, lambda: closed.append(True))
    collected = list(wrapped)

    assert sum(batch.num_rows for batch in collected) == 6
    assert closed == [True]
    assert wrapped.read_all().num_rows == 0
    assert closed == [True]


def test_arrow_reader_with_deferred_close_fires_on_error() -> None:
    import pyarrow as pa

    from sqlspec.utils.arrow_helpers import arrow_reader_with_deferred_close

    class _RaisingBatches:
        def __init__(self, first: Any) -> None:
            self._first = first
            self._calls = 0

        def __iter__(self) -> "_RaisingBatches":
            return self

        def __next__(self) -> Any:
            self._calls += 1
            if self._calls == 1:
                return self._first
            raise ValueError("batch boom")

    first = pa.record_batch({"id": pa.array([0, 1])})
    reader = pa.RecordBatchReader.from_batches(first.schema, _RaisingBatches(first))
    closed: list[bool] = []

    wrapped = arrow_reader_with_deferred_close(reader, lambda: closed.append(True))
    with pytest.raises(Exception):
        list(wrapped)

    assert closed == [True]


def test_arrow_reader_to_return_format_reader_returns_minus_one() -> None:
    import pyarrow as pa

    from sqlspec.utils.arrow_helpers import arrow_reader_to_return_format

    reader = pa.table({"id": pa.array([0, 1, 2])}).to_reader()
    data, rows = arrow_reader_to_return_format(reader, return_format="reader")

    assert isinstance(data, pa.RecordBatchReader)
    assert rows == -1


def test_arrow_reader_to_return_format_batches_sums_rows() -> None:
    import pyarrow as pa

    from sqlspec.utils.arrow_helpers import arrow_reader_to_return_format

    reader = pa.table({"id": pa.array(range(5))}).to_reader()
    data, rows = arrow_reader_to_return_format(reader, return_format="batches")

    assert isinstance(data, list)
    assert all(isinstance(batch, pa.RecordBatch) for batch in data)
    assert rows == 5


def test_arrow_reader_to_return_format_table_materializes() -> None:
    import pyarrow as pa

    from sqlspec.utils.arrow_helpers import arrow_reader_to_return_format

    reader = pa.table({"id": pa.array(range(4))}).to_reader()
    data, rows = arrow_reader_to_return_format(reader, return_format="table")

    assert isinstance(data, pa.Table)
    assert data.num_rows == 4
    assert rows == 4


def test_arrow_reader_to_return_format_casts_schema() -> None:
    import pyarrow as pa

    from sqlspec.utils.arrow_helpers import arrow_reader_to_return_format

    reader = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())}).to_reader()
    schema = pa.schema([pa.field("id", pa.int32())])
    data, _ = arrow_reader_to_return_format(reader, return_format="reader", arrow_schema=schema)

    assert data.schema.field("id").type == pa.int32()


def test_arrow_reader_to_return_format_rejects_non_reader() -> None:
    from sqlspec.utils.arrow_helpers import arrow_reader_to_return_format

    with pytest.raises(TypeError):
        arrow_reader_to_return_format(object(), return_format="reader")
