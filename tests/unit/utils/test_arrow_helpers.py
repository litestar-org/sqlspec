"""Tests for arrow_helpers conversion utilities."""

import logging
import math
from collections.abc import AsyncIterator, Iterator
from decimal import Decimal
from typing import Any, Literal

import pytest

from sqlspec.exceptions import MissingDependencyError, SQLBuilderError
from sqlspec.typing import PYARROW_INSTALLED
from sqlspec.utils.arrow_helpers import (
    arrow_batches_from_async_rows,
    arrow_batches_from_rows,
    arrow_table_to_return_format,
    arrow_type_from_token,
    coerce_arrow_table,
    convert_dict_to_arrow,
    convert_dict_to_arrow_with_schema,
    sql_type_for_category,
    sql_type_from_arrow_type,
)

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


@pytest.mark.parametrize("return_format", ("table", "batch", "reader"))
@pytest.mark.parametrize("use_schema", (False, True))
def test_empty_arrow_conversion_preserves_projected_schema(
    return_format: Literal["table", "batch", "reader"], use_schema: bool
) -> None:
    import pyarrow as pa

    schema = pa.schema((pa.field("id", pa.int32(), metadata={b"source": b"explicit"}), pa.field("name", pa.string())))
    expected = schema if use_schema else pa.schema((("id", pa.int64()), ("name", pa.null())))
    result = convert_dict_to_arrow_with_schema(
        [],
        return_format=return_format,
        arrow_schema=schema if use_schema else None,
        column_types={"id": "int64"},
        column_names=["id", "name"],
    )

    assert isinstance(result, (pa.Table, pa.RecordBatch, pa.RecordBatchReader))
    assert result.schema.equals(expected, check_metadata=True)
    if isinstance(result, pa.RecordBatchReader):
        result = result.read_all()
    assert result.num_rows == 0


def test_empty_arrow_schema_takes_precedence_over_projected_names() -> None:
    import pyarrow as pa

    schema = pa.schema([("explicit", pa.int64())])
    result = convert_dict_to_arrow_with_schema([], arrow_schema=schema, column_names=["projected"])

    assert isinstance(result, pa.Table)
    assert result.schema == schema
    assert convert_dict_to_arrow_with_schema([], return_format="batches", arrow_schema=schema) == []


def test_empty_native_arrow_batch_keeps_full_schema() -> None:
    import pyarrow as pa

    schema = pa.schema([pa.field("id", pa.int64(), metadata={b"source": b"native"})], metadata={b"table": b"items"})
    table = pa.Table.from_pylist([], schema=schema)
    batch = arrow_table_to_return_format(table, return_format="batch")

    assert isinstance(batch, pa.RecordBatch)
    assert batch.schema.equals(schema, check_metadata=True)
    assert batch.num_rows == 0


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


def test_convert_empty_data_to_batches_returns_empty_list() -> None:
    """convert_dict_to_arrow([], return_format='batches') returns an empty list rather than a single RecordBatch."""
    result = convert_dict_to_arrow([], return_format="batches")
    assert result == []


def test_deferred_close_batch_iterator_closes_on_explicit_close_or_gc() -> None:
    """_DeferredCloseBatchIterator invokes close_callback when closed early before full iteration."""
    import pyarrow as pa

    from sqlspec.utils.arrow_helpers import _DeferredCloseBatchIterator

    batches = [pa.record_batch({"id": pa.array([1, 2])}), pa.record_batch({"id": pa.array([3, 4])})]
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    closed: list[bool] = []

    iterator = _DeferredCloseBatchIterator(reader, lambda: closed.append(True))
    first = next(iterator)
    assert first.num_rows == 2
    assert closed == []

    iterator.close()
    assert closed == [True]
    iterator.close()
    assert closed == [True]


@pytest.mark.parametrize(
    ("category", "dialect", "expected"),
    [
        ("double", "duckdb", "DOUBLE"),
        ("double", "spanner", "FLOAT64"),
        ("double", None, "DOUBLE PRECISION"),
        ("timestamp", "postgres", "TIMESTAMP"),
        ("timestamptz", "postgres", "TIMESTAMP WITH TIME ZONE"),
        ("timestamp", "mssql", "DATETIME2(6)"),
        ("timestamptz", "mssql", "DATETIMEOFFSET(6)"),
        ("timestamptz", "sqlite", "TIMESTAMP"),
        ("key_text", "mysql", "VARCHAR(255)"),
        ("key_text", "oracle", "VARCHAR2(255)"),
        ("key_text", "mssql", "NVARCHAR(255)"),
        ("key_text", "postgres", "TEXT"),
        ("text", "oracle", "CLOB"),
    ],
)
def test_sql_type_for_category_is_shared_by_arrow_and_value_inference(
    category: str, dialect: "str | None", expected: str
) -> None:
    """The neutral category mapping resolves engine-specific types for both inference paths."""
    assert sql_type_for_category(category, dialect) == expected


def test_sql_type_for_category_decimal_limits() -> None:
    """Decimals keep exact precision, collapse to Spanner NUMERIC, and refuse widths an engine cannot store."""
    assert sql_type_for_category("decimal", "postgres", precision=50, scale=4) == "DECIMAL(50, 4)"
    assert sql_type_for_category("decimal", "spanner", precision=38, scale=9) == "NUMERIC"
    with pytest.raises(SQLBuilderError, match="maximum precision 38 supported by duckdb"):
        sql_type_for_category("decimal", "duckdb", precision=50, scale=4)
    with pytest.raises(SQLBuilderError, match="does not fit Spanner NUMERIC"):
        sql_type_for_category("decimal", "spanner", precision=50, scale=4)
    with pytest.raises(SQLBuilderError, match="Unknown SQL type category"):
        sql_type_for_category("geometry", "postgres")


def test_coerce_arrow_batches_streams_record_batch_reader_lazily() -> None:
    """coerce_arrow_batches yields RecordBatches lazily from a RecordBatchReader without calling read_all()."""
    import pyarrow as pa

    from sqlspec.utils.arrow_helpers import coerce_arrow_batches

    yielded_indices: list[int] = []

    def _batch_gen() -> Any:
        for idx in range(3):
            yielded_indices.append(idx)
            yield pa.record_batch({"id": pa.array([idx * 2, idx * 2 + 1], type=pa.int64())})

    schema = pa.schema([pa.field("id", pa.int64())])
    reader = pa.RecordBatchReader.from_batches(schema, _batch_gen())

    columns, batch_iter = coerce_arrow_batches(reader)
    assert columns == ["id"]
    assert yielded_indices == []

    first_batch = next(batch_iter)
    assert first_batch.num_rows == 2
    assert yielded_indices == [0]

    remaining = list(batch_iter)
    assert len(remaining) == 2
    assert yielded_indices == [0, 1, 2]


def test_coerce_arrow_batches_accepts_table_record_batch_and_arrow_result() -> None:
    """coerce_arrow_batches handles Table, RecordBatch, ArrowResult, and Arrow C stream sources."""
    import pyarrow as pa

    from sqlspec.core.result import ArrowResult
    from sqlspec.core.statement import SQL
    from sqlspec.utils.arrow_helpers import arrow_batch_to_rows, coerce_arrow_batches

    table = pa.table({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]})
    cols, batches = coerce_arrow_batches(table, batch_size=2)
    batch_list = list(batches)
    assert cols == ["id", "name"]
    assert [b.num_rows for b in batch_list] == [2, 2, 1]
    assert arrow_batch_to_rows(batch_list[0]) == [(1, "a"), (2, "b")]
    assert arrow_batch_to_rows(batch_list[0], ["name"]) == [("a",), ("b",)]

    single_batch = batch_list[0]
    cols_single, iter_single = coerce_arrow_batches(single_batch)
    assert cols_single == ["id", "name"]
    assert len(list(iter_single)) == 1

    arrow_result = ArrowResult(statement=SQL("SELECT 1"), data=table.to_reader(max_chunksize=2))
    cols_res, iter_res = coerce_arrow_batches(arrow_result)
    assert cols_res == ["id", "name"]
    assert [b.num_rows for b in iter_res] == [2, 2, 1]


@pytest.mark.parametrize(
    ("dialect", "timezone", "expected"),
    [
        ("sybase", None, "BIGDATETIME"),
        ("sybase", "UTC", "BIGDATETIME"),
        ("sqlanywhere", None, "TIMESTAMP"),
        ("sqlanywhere", "UTC", "TIMESTAMP WITH TIME ZONE"),
    ],
)
def test_sap_arrow_timestamp_types(dialect: str, timezone: str | None, expected: str) -> None:
    import pyarrow as pa

    arrow_type = pa.timestamp("us", tz=timezone) if timezone is not None else pa.timestamp("us")
    assert sql_type_from_arrow_type(arrow_type, dialect) == expected
    assert sql_type_for_category("timestamptz" if timezone else "timestamp", dialect) == expected


@pytest.mark.parametrize("dialect", ["sybase", "sqlanywhere"])
def test_sap_key_text_is_bounded_and_indexable(dialect: str) -> None:
    assert sql_type_for_category("key_text", dialect) == "VARCHAR(255)"


@pytest.mark.parametrize(("dialect", "maximum"), [("sybase", 38), ("sqlanywhere", 127)])
def test_sap_decimal_precision_keeps_exact_values_and_rejects_overflow(dialect: str, maximum: int) -> None:
    import pyarrow as pa

    assert sql_type_for_category("decimal", dialect, precision=maximum, scale=4) == f"DECIMAL({maximum}, 4)"
    assert sql_type_from_arrow_type(pa.decimal128(38, 4), dialect) == "DECIMAL(38, 4)"
    with pytest.raises(SQLBuilderError, match=f"maximum precision {maximum} supported by {dialect}"):
        sql_type_for_category("decimal", dialect, precision=maximum + 1, scale=4)
    if dialect == "sybase":
        with pytest.raises(SQLBuilderError, match="maximum precision 38 supported by sybase"):
            sql_type_from_arrow_type(pa.decimal256(50, 4), dialect)


class _TrackingSyncRowSource:
    """In-process row iterator tracking consumption count, close calls, and late column_types."""

    def __init__(self, rows: list[dict[str, Any]], column_types: dict[str, str] | None = None) -> None:
        self._rows = list(rows)
        self._declared = column_types
        self._index = 0
        self.consumed = 0
        self.closed = False
        self.column_types: dict[str, str] | None = None

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self

    def __next__(self) -> dict[str, Any]:
        if self._index == 0:
            self.column_types = self._declared
        if self._index >= len(self._rows):
            raise StopIteration
        row = self._rows[self._index]
        self._index += 1
        self.consumed += 1
        return row

    def close(self) -> None:
        self.closed = True


class _TrackingAsyncRowSource:
    """Async in-process row iterator tracking consumption count, aclose calls, and late column_types."""

    def __init__(self, rows: list[dict[str, Any]], column_types: dict[str, str] | None = None) -> None:
        self._rows = list(rows)
        self._declared = column_types
        self._index = 0
        self.consumed = 0
        self.closed = False
        self.column_types: dict[str, str] | None = None

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self

    async def __anext__(self) -> dict[str, Any]:
        if self._index == 0:
            self.column_types = self._declared
        if self._index >= len(self._rows):
            raise StopAsyncIteration
        row = self._rows[self._index]
        self._index += 1
        self.consumed += 1
        return row

    async def aclose(self) -> None:
        self.closed = True


async def _read_all_async_batches(batches: AsyncIterator[Any]) -> Any:
    """Collect an async batch iterator into a single PyArrow Table."""
    import pyarrow as pa

    collected = [batch async for batch in batches]
    return pa.Table.from_batches(collected)


def test_decimal_token_parsing() -> None:
    """arrow_type_from_token parses exact decimal(P,S) tokens and falls back to string on invalid input."""
    import pyarrow as pa

    assert arrow_type_from_token("decimal(10,2)") == pa.decimal128(10, 2)
    assert arrow_type_from_token("decimal(65,30)") == pa.decimal256(65, 30)
    assert arrow_type_from_token("decimal(2,5)") == pa.string()
    assert arrow_type_from_token("decimal(0,0)") == pa.string()
    assert arrow_type_from_token("decimal(80,2)") == pa.string()
    assert arrow_type_from_token("decimal(bad)") == pa.string()


@pytest.mark.parametrize("batch_size", [0, -1])
async def test_arrow_batches_rejects_non_positive_batch_size(batch_size: int) -> None:
    """Both row batch helpers reject batch_size < 1."""
    with pytest.raises(ValueError, match="batch_size must be greater than or equal to 1"):
        arrow_batches_from_rows(iter([{"a": 1}]), batch_size=batch_size)
    with pytest.raises(ValueError, match="batch_size must be greater than or equal to 1"):
        await arrow_batches_from_async_rows(_TrackingAsyncRowSource([{"a": 1}]), batch_size=batch_size)


async def test_arrow_batches_empty_returns_none_and_closes_source() -> None:
    """Empty row sources return None and close immediately."""
    sync_src = _TrackingSyncRowSource([])
    assert arrow_batches_from_rows(sync_src, batch_size=2) is None
    assert sync_src.closed

    async_src = _TrackingAsyncRowSource([])
    assert await arrow_batches_from_async_rows(async_src, batch_size=2) is None
    assert async_src.closed


async def test_arrow_batches_streams_lazily_with_pinned_schema() -> None:
    """Settled columns consume at most batch_size rows upfront and pin schema without read-ahead."""
    import pyarrow as pa

    rows = [{"a": idx} for idx in range(5)]
    sync_src = _TrackingSyncRowSource(rows)
    reader = arrow_batches_from_rows(sync_src, batch_size=2)
    assert reader is not None
    assert sync_src.consumed <= 2
    assert reader.schema.field("a").type == pa.int64()

    first = reader.read_next_batch()
    assert first.to_pylist() == [{"a": 0}, {"a": 1}]
    assert sync_src.consumed <= 4
    rest = [first, *list(reader)]
    assert pa.Table.from_batches(rest).to_pylist() == rows
    assert sync_src.closed

    async_src = _TrackingAsyncRowSource([{"a": 1}, {"a": 2}])
    async_batches = await arrow_batches_from_async_rows(async_src, batch_size=1)
    assert async_batches is not None
    assert async_src.consumed <= 1
    first_async = await anext(async_batches)
    assert first_async.schema.field("a").type == pa.int64()
    aclose = getattr(async_batches, "aclose", None)
    assert aclose is not None
    await aclose()
    assert async_src.closed


async def test_arrow_batches_null_and_declared_column_types() -> None:
    """Safe declared tokens pin null columns lazily while unsafe or undeclared null columns read ahead."""
    import pyarrow as pa

    untyped = arrow_batches_from_rows(_TrackingSyncRowSource([{"a": None}, {"a": 7}]), batch_size=1)
    assert untyped is not None
    untyped_table = untyped.read_all()
    assert untyped_table.schema.field("a").type == pa.int64()
    assert untyped_table.to_pydict()["a"] == [None, 7]

    all_null = arrow_batches_from_rows(
        _TrackingSyncRowSource([{"a": None}, {"a": None}], column_types={"a": "int32"}), batch_size=1
    )
    assert all_null is not None
    assert all_null.schema.field("a").type == pa.int32()

    safe_seen: list[tuple[str, ...]] = []
    safe_src = _TrackingSyncRowSource([{"a": None}, {"a": 7}], column_types={"a": "int32"})
    safe_reader = arrow_batches_from_rows(safe_src, batch_size=1, on_read_ahead=safe_seen.append)
    assert safe_reader is not None
    assert safe_src.consumed <= 1
    assert safe_seen == []
    assert safe_reader.schema.field("a").type == pa.int32()
    assert safe_reader.read_all().to_pydict()["a"] == [None, 7]

    unsafe_seen: list[tuple[str, ...]] = []
    unsafe_reader = arrow_batches_from_rows(
        _TrackingSyncRowSource([{"a": None}], column_types={"a": "binary"}),
        batch_size=1,
        on_read_ahead=unsafe_seen.append,
    )
    assert unsafe_reader is not None
    assert unsafe_seen == [("a",)]
    assert unsafe_reader.schema.field("a").type == pa.binary()


async def test_arrow_batches_list_and_vector_settlement() -> None:
    """Float vectors settle from chunk one while integer and struct lists read ahead unless arrow_schema is set."""
    import pyarrow as pa

    vec_seen: list[tuple[str, ...]] = []
    vec_src = _TrackingSyncRowSource([{"v": [1.0, 2.0]}, {"v": [3.0]}])
    vec_reader = arrow_batches_from_rows(vec_src, batch_size=1, on_read_ahead=vec_seen.append)
    assert vec_reader is not None
    assert vec_src.consumed <= 1
    assert vec_seen == []
    assert vec_reader.schema.field("v").type == pa.list_(pa.float64())
    assert vec_reader.read_all().to_pydict()["v"] == [[1.0, 2.0], [3.0]]

    int_list_seen: list[tuple[str, ...]] = []
    int_list_rows: list[dict[str, Any]] = [{"v": [1, 2]}, {"v": [1.5]}]
    int_list_reader = arrow_batches_from_rows(
        _TrackingSyncRowSource(int_list_rows), batch_size=1, on_read_ahead=int_list_seen.append
    )
    assert int_list_reader is not None
    assert int_list_seen == [("v",)]
    int_list_table = int_list_reader.read_all()
    assert int_list_table.schema.field("v").type == pa.list_(pa.float64())
    assert int_list_table.to_pydict()["v"] == [[1.0, 2.0], [1.5]]
    assert int_list_table == convert_dict_to_arrow_with_schema(int_list_rows, return_format="table")

    nested_seen: list[tuple[str, ...]] = []
    nested_reader = arrow_batches_from_rows(
        _TrackingSyncRowSource([{"v": [[1]]}, {"v": [[1.5]]}]), batch_size=1, on_read_ahead=nested_seen.append
    )
    assert nested_reader is not None
    assert nested_seen == [("v",)]
    assert nested_reader.schema.field("v").type == pa.list_(pa.list_(pa.float64()))

    explicit_seen: list[tuple[str, ...]] = []
    explicit_schema = pa.schema([("v", pa.list_(pa.int64()))])
    explicit_reader = arrow_batches_from_rows(
        _TrackingSyncRowSource([{"v": [1]}]),
        batch_size=1,
        arrow_schema=explicit_schema,
        on_read_ahead=explicit_seen.append,
    )
    assert explicit_reader is not None
    assert explicit_seen == []
    assert explicit_reader.schema.field("v").type == pa.list_(pa.int64())

    struct_list_seen: list[tuple[str, ...]] = []
    struct_list_reader = arrow_batches_from_rows(
        _TrackingSyncRowSource([{"v": [{"x": 1}]}, {"v": [{"x": 1, "y": 2}]}]),
        batch_size=1,
        on_read_ahead=struct_list_seen.append,
    )
    assert struct_list_reader is not None
    assert struct_list_seen == [("v",)]
    struct_list_type = struct_list_reader.schema.field("v").type
    assert pa.types.is_list(struct_list_type)
    assert pa.types.is_struct(struct_list_type.value_type)
    assert [f.name for f in struct_list_type.value_type] == ["x", "y"]


@pytest.mark.parametrize(
    "all_rows",
    [
        [{"n": Decimal("1.50")}, {"n": Decimal("100.25")}],
        [{"n": Decimal("1.5")}, {"n": Decimal("1.255")}],
        [{"s": {"x": 1}}, {"s": {"x": 1, "y": "z"}}],
        [{"s": {"y": 2, "x": 1}}, {"s": {"x": 3, "y": 4}}],
        [{"v": [{"x": 1}]}, {"v": [{"x": 1, "y": 2}]}],
        [{"v": [1, 2]}, {"v": [1.5]}],
        [{"a": None}, {"a": 7}],
    ],
)
async def test_arrow_batches_whole_result_parity(all_rows: list[dict[str, Any]]) -> None:
    """Read-ahead conversion matches convert_dict_to_arrow_with_schema across sync and async helpers."""
    import pyarrow as pa

    expected = convert_dict_to_arrow_with_schema(all_rows, return_format="table")
    assert isinstance(expected, pa.Table)

    sync_reader = arrow_batches_from_rows(_TrackingSyncRowSource(all_rows), batch_size=1)
    assert sync_reader is not None
    sync_table = sync_reader.read_all()
    assert sync_table.schema == expected.schema
    assert sync_table == expected

    async_batches = await arrow_batches_from_async_rows(_TrackingAsyncRowSource(all_rows), batch_size=1)
    assert async_batches is not None
    async_table = await _read_all_async_batches(async_batches)
    assert async_table.schema == expected.schema
    assert async_table == expected


async def test_arrow_batches_exact_decimal_and_read_ahead_reporting(caplog: pytest.LogCaptureFixture) -> None:
    """Exact decimal tokens pin without read-ahead, overflow raises ArrowInvalid, and read-ahead logs columns."""
    import pyarrow as pa

    decimal_rows = [{"n": Decimal("1.50")}, {"n": Decimal("100.25")}]
    exact_seen: list[tuple[str, ...]] = []
    exact_src = _TrackingSyncRowSource(decimal_rows, column_types={"n": "decimal(10,2)"})
    exact_reader = arrow_batches_from_rows(exact_src, batch_size=1, on_read_ahead=exact_seen.append)
    assert exact_reader is not None
    assert exact_src.consumed <= 1
    assert exact_seen == []
    assert exact_reader.schema.field("n").type == pa.decimal128(10, 2)
    assert exact_reader.read_all().to_pydict()["n"] == [Decimal("1.50"), Decimal("100.25")]

    overflow_reader = arrow_batches_from_rows(
        _TrackingSyncRowSource(decimal_rows, column_types={"n": "decimal(3,2)"}), batch_size=1
    )
    assert overflow_reader is not None
    with pytest.raises(pa.ArrowInvalid):
        overflow_reader.read_all()

    seen: list[tuple[str, ...]] = []
    with caplog.at_level(logging.INFO, logger="sqlspec.utils.arrow_helpers"):
        reported_reader = arrow_batches_from_rows(
            _TrackingSyncRowSource([{"n": Decimal("1.5"), "a": 1}]), batch_size=2, on_read_ahead=seen.append
        )
    assert reported_reader is not None
    assert seen == [("n",)]
    events = [rec for rec in caplog.records if rec.getMessage() == "storage.export.read_ahead"]
    assert len(events) == 1
    assert events[0].__dict__["extra_fields"] == {"columns": ["n"], "mode": "sync"}


async def test_arrow_batches_integer_widening_and_family_conflicts() -> None:
    """Pinned schemas reject type-family changes while allowing integer-to-float and null chunks."""
    import pyarrow as pa

    widened_seen: list[tuple[str, ...]] = []
    widened_reader = arrow_batches_from_rows(
        _TrackingSyncRowSource([{"a": 1}, {"a": 1.5}]),
        batch_size=1,
        integer_may_widen=True,
        on_read_ahead=widened_seen.append,
    )
    assert widened_reader is not None
    assert widened_seen == [("a",)]
    widened_table = widened_reader.read_all()
    assert widened_table.schema.field("a").type == pa.float64()
    assert widened_table.to_pydict()["a"] == [1.0, 1.5]

    bad_cases: tuple[list[dict[str, Any]], ...] = (
        [{"a": "x"}, {"a": 1}],
        [{"a": 1}, {"a": 1.5}],
        [{"a": 1}, {"a": "x"}],
    )
    for bad_rows in bad_cases:
        bad_reader = arrow_batches_from_rows(_TrackingSyncRowSource(bad_rows), batch_size=1)
        assert bad_reader is not None
        with pytest.raises(pa.ArrowTypeError, match="'a'"):
            bad_reader.read_all()

        bad_async = await arrow_batches_from_async_rows(_TrackingAsyncRowSource(bad_rows), batch_size=1)
        assert bad_async is not None
        with pytest.raises(pa.ArrowTypeError, match="'a'"):
            await _read_all_async_batches(bad_async)

    float_reader = arrow_batches_from_rows(_TrackingSyncRowSource([{"a": 1.5}, {"a": 2}]), batch_size=1)
    assert float_reader is not None
    float_table = float_reader.read_all()
    assert float_table.schema.field("a").type == pa.float64()
    assert float_table.to_pydict()["a"] == [1.5, 2.0]

    null_later_reader = arrow_batches_from_rows(_TrackingSyncRowSource([{"a": 1}, {"a": None}]), batch_size=1)
    assert null_later_reader is not None
    null_later_table = null_later_reader.read_all()
    assert null_later_table.schema.field("a").type == pa.int64()
    assert null_later_table.to_pydict()["a"] == [1, None]


async def test_arrow_batches_explicit_schema_and_struct_field_loss_guards() -> None:
    """Explicit arrow_schema wins without read-ahead and refuses silent struct key drops."""
    import pyarrow as pa

    struct_schema = pa.schema([("s", pa.struct([("x", pa.int64())]))])

    later_loss_reader = arrow_batches_from_rows(
        _TrackingSyncRowSource([{"s": {"x": 1}}, {"s": {"x": 1, "y": "z"}}]), batch_size=1, arrow_schema=struct_schema
    )
    assert later_loss_reader is not None
    with pytest.raises(pa.ArrowTypeError, match="'s'"):
        later_loss_reader.read_all()

    first_loss_src = _TrackingSyncRowSource([{"s": {"x": 1, "y": "z"}}])
    with pytest.raises(pa.ArrowTypeError, match="'s'"):
        arrow_batches_from_rows(first_loss_src, batch_size=1, arrow_schema=struct_schema)
    assert first_loss_src.closed

    seen: list[tuple[str, ...]] = []
    explicit_null_reader = arrow_batches_from_rows(
        _TrackingSyncRowSource([{"a": None}]),
        batch_size=1,
        arrow_schema=pa.schema([("a", pa.string())]),
        on_read_ahead=seen.append,
    )
    assert explicit_null_reader is not None
    assert seen == []
    assert explicit_null_reader.schema.field("a").type == pa.string()


@pytest.mark.parametrize("dialect", ["snowflake", "sf"])
def test_sql_type_from_arrow_type_snowflake(dialect: str) -> None:
    """Snowflake Arrow import maps nested and timezone-aware values natively."""
    import pyarrow as pa

    assert sql_type_from_arrow_type(pa.binary(), dialect) == "BINARY"
    assert sql_type_from_arrow_type(pa.struct([("k", pa.int64())]), dialect) == "VARIANT"
    assert sql_type_from_arrow_type(pa.uuid(), dialect) == "VARCHAR(36)"
    assert sql_type_from_arrow_type(pa.timestamp("us", tz="UTC"), dialect) == "TIMESTAMP_TZ"
    with pytest.raises(SQLBuilderError, match="precision"):
        sql_type_for_category("decimal", dialect, precision=39, scale=0)
