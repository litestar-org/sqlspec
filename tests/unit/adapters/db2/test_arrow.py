"""Unit tests for Db2Driver Apache Arrow result conversion."""

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest

from sqlspec.adapters.db2.core import default_statement_config
from sqlspec.adapters.db2.driver import Db2Driver
from sqlspec.exceptions import ImproperConfigurationError

pa = pytest.importorskip("pyarrow")


class FakeCursor:
    """Fake Db2 cursor for driver testing."""

    def __init__(
        self, rows: list[Any] | None = None, description: Sequence[tuple[str, ...]] | None = None, rowcount: int = 0
    ) -> None:
        self.rows = list(rows) if rows is not None else []
        self.description = description
        self.rowcount = rowcount
        self.calls: list[tuple[str, Any]] = []
        self.closed = False

    def execute(self, operation: str, parameters: Any = None) -> Any:
        self.calls.append((operation, parameters))
        return self

    def fetchall(self) -> list[Any]:
        return list(self.rows)

    def fetchmany(self, size: int = 1) -> list[Any]:
        chunk = self.rows[:size]
        self.rows = self.rows[size:]
        return chunk

    def fetchone(self) -> Any:
        return self.rows.pop(0) if self.rows else None

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    """Fake Db2 connection for driver testing."""

    def __init__(self, cursor: FakeCursor | None = None) -> None:
        self.cursor_instance = cursor or FakeCursor()
        self.autocommit_state = True
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def test_select_to_arrow_table_default() -> None:
    """Verify select_to_arrow returns Arrow Table by default with matching column data."""
    rows = [(1, "Ada"), (2, "Grace")]
    cursor = FakeCursor(rows=rows, description=[("id",), ("name",)])
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)

    result = driver.select_to_arrow("SELECT id, name FROM users")
    table = result.get_data()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert table.column_names == ["id", "name"]
    assert table["id"].to_pylist() == [1, 2]
    assert table["name"].to_pylist() == ["Ada", "Grace"]


def test_select_to_arrow_batch_format() -> None:
    """Verify select_to_arrow supports return_format='batch' producing single RecordBatch."""
    rows = [(10, "Admin"), (20, "User")]
    cursor = FakeCursor(rows=rows, description=[("role_id",), ("role_name",)])
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)

    result = driver.select_to_arrow("SELECT role_id, role_name FROM roles", return_format="batch")
    batch = result.get_data()

    assert isinstance(batch, pa.RecordBatch)
    assert batch.num_rows == 2
    assert batch.schema.names == ["role_id", "role_name"]


def test_select_to_arrow_batches_format() -> None:
    """Verify select_to_arrow supports return_format='batches' with chunk sizing."""
    rows = [(i, f"item_{i}") for i in range(5)]
    cursor = FakeCursor(rows=rows, description=[("id",), ("item",)])
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)

    result = driver.select_to_arrow("SELECT id, item FROM inventory", return_format="batches", batch_size=2)
    batches = result.get_data()

    assert isinstance(batches, list)
    assert len(batches) == 3
    assert batches[0].num_rows == 2
    assert batches[1].num_rows == 2
    assert batches[2].num_rows == 1


def test_select_to_arrow_reader_format() -> None:
    """Verify select_to_arrow supports return_format='reader' returning RecordBatchReader."""
    rows = [(1, "a"), (2, "b")]
    cursor = FakeCursor(rows=rows, description=[("k",), ("v",)])
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)

    result = driver.select_to_arrow("SELECT k, v FROM kv", return_format="reader", batch_size=1)
    reader = result.get_data()

    assert isinstance(reader, pa.RecordBatchReader)
    materialized = list(reader)
    assert len(materialized) == 2


def test_select_to_arrow_db2_data_types() -> None:
    """Verify select_to_arrow converts complex Db2 types including DECFLOAT, timestamps, and binary blobs."""
    rows = [
        (
            1,
            Decimal("12345.6789"),
            3.14159,
            "sample text",
            b"\x00\x01\x02\xff",
            date(2026, 9, 22),
            datetime(2026, 9, 22, 12, 0, 0),
            True,
        )
    ]
    description = [
        ("id",),
        ("decfloat_val",),
        ("double_val",),
        ("varchar_val",),
        ("blob_val",),
        ("date_val",),
        ("timestamp_val",),
        ("bool_val",),
    ]
    cursor = FakeCursor(rows=rows, description=description)
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)

    result = driver.select_to_arrow("SELECT * FROM complex_table")
    table = result.get_data()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table["id"].to_pylist() == [1]
    assert table["decfloat_val"].to_pylist() == [Decimal("12345.6789")]
    assert table["double_val"].to_pylist() == [3.14159]
    assert table["varchar_val"].to_pylist() == ["sample text"]
    assert table["blob_val"].to_pylist() == [b"\x00\x01\x02\xff"]
    assert table["date_val"].to_pylist() == [date(2026, 9, 22)]
    assert table["timestamp_val"].to_pylist() == [datetime(2026, 9, 22, 12, 0, 0)]
    assert table["bool_val"].to_pylist() == [True]


def test_select_to_arrow_null_values() -> None:
    """Verify select_to_arrow properly converts columns with null values."""
    rows = [(1, None, None), (2, "some_str", Decimal("10.5"))]
    description = [("id",), ("nullable_str",), ("nullable_dec",)]
    cursor = FakeCursor(rows=rows, description=description)
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)

    result = driver.select_to_arrow("SELECT id, nullable_str, nullable_dec FROM nullable_table")
    table = result.get_data()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert table["nullable_str"].to_pylist() == [None, "some_str"]
    assert table["nullable_dec"].to_pylist() == [None, Decimal("10.5")]


def test_select_to_arrow_empty_result() -> None:
    """Verify select_to_arrow on empty result returns empty table and honors optional schema."""
    rows: list[Any] = []
    description = [("col_a",), ("col_b",)]
    cursor = FakeCursor(rows=rows, description=description)
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)

    result = driver.select_to_arrow("SELECT col_a, col_b FROM empty_table")
    table = result.get_data()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 0
    assert table.column_names == []

    schema = pa.schema([("col_a", pa.int64()), ("col_b", pa.string())])
    result_with_schema = driver.select_to_arrow("SELECT col_a, col_b FROM empty_table", arrow_schema=schema)
    table_with_schema = result_with_schema.get_data()

    assert isinstance(table_with_schema, pa.Table)
    assert table_with_schema.num_rows == 0
    assert table_with_schema.column_names == ["col_a", "col_b"]


def test_select_to_arrow_native_only_raises() -> None:
    """Verify select_to_arrow with native_only=True raises ImproperConfigurationError."""
    cursor = FakeCursor(rows=[(1,)], description=[("id",)])
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)

    with pytest.raises(ImproperConfigurationError, match="does not support native Arrow"):
        driver.select_to_arrow("SELECT id FROM users", native_only=True)


def test_select_to_arrow_with_parameters() -> None:
    """Verify select_to_arrow passes bound parameters to underlying execution."""
    rows = [(42, "Found")]
    cursor = FakeCursor(rows=rows, description=[("id",), ("name",)])
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)

    result = driver.select_to_arrow("SELECT id, name FROM users WHERE id = ?", 42)
    table = result.get_data()

    assert table.num_rows == 1
    assert table["id"].to_pylist() == [42]
    assert len(cursor.calls) == 1
    assert cursor.calls[0][1] == (42,)


def test_select_to_arrow_result_methods_and_conversions() -> None:
    """Verify ArrowResult metadata properties and downstream conversion methods."""
    rows = [(1, "Alice"), (2, "Bob")]
    cursor = FakeCursor(rows=rows, description=[("id",), ("name",)])
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)

    result = driver.select_to_arrow("SELECT id, name FROM users")

    assert result.is_success() is True
    assert result.column_names == ["id", "name"]
    assert result.num_rows == 2
    assert result.num_columns == 2

    table = result.get_data()
    batches = table.to_batches()
    assert len(batches) >= 1
    assert batches[0].num_rows == 2

    pydict = table.to_pydict()
    assert pydict == {"id": [1, 2], "name": ["Alice", "Bob"]}

    df = table.to_pandas()
    assert list(df["name"]) == ["Alice", "Bob"]
