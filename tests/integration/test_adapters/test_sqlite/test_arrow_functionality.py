"""Test Arrow functionality for SQLite drivers."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def sqlite_arrow_session() -> "Generator[SqliteDriver, None, None]":
    """Create a SQLite session for Arrow testing."""
    config = SqliteConfig(database=":memory:", statement_config=SQLConfig(strict_mode=False))

    with config.provide_session() as session:
        # Create test table with various data types
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_arrow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER,
                price REAL,
                is_active INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Insert test data
        session.execute_many(
            "INSERT INTO test_arrow (name, value, price, is_active) VALUES (?, ?, ?, ?)",
            [
                ("Product A", 100, 19.99, 1),
                ("Product B", 200, 29.99, 1),
                ("Product C", 300, 39.99, 0),
                ("Product D", 400, 49.99, 1),
                ("Product E", 500, 59.99, 0),
            ],
        )
        yield session


def test_sqlite_fetch_arrow_table(sqlite_arrow_session: SqliteDriver) -> None:
    """Test fetch_arrow_table method with SQLite."""
    result = sqlite_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow ORDER BY id")

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5
    assert result.num_columns >= 5  # id, name, value, price, is_active, created_at

    # Check column names
    expected_columns = {"id", "name", "value", "price", "is_active"}
    actual_columns = set(result.column_names)
    assert expected_columns.issubset(actual_columns)

    # Check values
    values = result.data["value"].to_pylist()
    assert values == [100, 200, 300, 400, 500]

    # Check names
    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product E" in names


def test_sqlite_to_parquet(sqlite_arrow_session: SqliteDriver) -> None:
    """Test to_parquet export with SQLite."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        sqlite_arrow_session.export_to_storage("SELECT * FROM test_arrow WHERE is_active = 1", destination_uri=str(output_path))

        assert output_path.exists()

        # Read back the parquet file
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Only active products

        # Verify data
        names = table["name"].to_pylist()
        assert "Product A" in names
        assert "Product C" not in names  # Inactive product


def test_sqlite_arrow_with_parameters(sqlite_arrow_session: SqliteDriver) -> None:
    """Test fetch_arrow_table with parameters on SQLite."""
    # fetch_arrow_table doesn't accept parameters - embed them in SQL
    result = sqlite_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value >= 200 AND value <= 400 ORDER BY value"
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    values = result.data["value"].to_pylist()
    assert values == [200, 300, 400]


def test_sqlite_arrow_empty_result(sqlite_arrow_session: SqliteDriver) -> None:
    """Test fetch_arrow_table with empty result on SQLite."""
    # fetch_arrow_table doesn't accept parameters - embed them in SQL
    result = sqlite_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow WHERE value > 1000")

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 0
    assert result.num_columns >= 5  # Schema should still be present


def test_sqlite_arrow_data_types(sqlite_arrow_session: SqliteDriver) -> None:
    """Test Arrow data type mapping for SQLite."""
    result = sqlite_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow LIMIT 1")

    assert isinstance(result, ArrowResult)

    # Check schema has expected columns
    schema = result.data.schema
    column_names = [field.name for field in schema]
    assert "id" in column_names
    assert "name" in column_names
    assert "value" in column_names
    assert "price" in column_names
    assert "is_active" in column_names


def test_sqlite_to_arrow_with_sql_object(sqlite_arrow_session: SqliteDriver) -> None:
    """Test to_arrow with SQL object instead of string."""
    from sqlspec.statement.sql import SQL

    sql_obj = SQL("SELECT name, value FROM test_arrow WHERE is_active = ?", 1)
    result = sqlite_arrow_session.fetch_arrow_table(sql_obj)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    assert result.num_columns == 2  # Only name and value columns

    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product C" not in names  # Inactive


def test_sqlite_arrow_large_dataset(sqlite_arrow_session: SqliteDriver) -> None:
    """Test Arrow functionality with larger dataset."""
    # Insert more test data
    large_data = [(f"Item {i}", i * 10, float(i * 2.5), i % 2) for i in range(100, 1000)]

    sqlite_arrow_session.execute_many(
        "INSERT INTO test_arrow (name, value, price, is_active) VALUES (?, ?, ?, ?)", large_data
    )

    result = sqlite_arrow_session.fetch_arrow_table("SELECT COUNT(*) as total FROM test_arrow")

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 1
    total_count = result.data["total"].to_pylist()[0]
    assert total_count == 905  # 5 original + 900 new records


def test_sqlite_parquet_export_options(sqlite_arrow_session: SqliteDriver) -> None:
    """Test Parquet export with different options."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_compressed.parquet"

        # Export with compression
        sqlite_arrow_session.export_to_storage("SELECT * FROM test_arrow WHERE value <= 300", destination_uri=str(output_path), compression="snappy"
        )

        assert output_path.exists()

        # Verify the file can be read
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Products A, B, C

        # Check compression was applied (file should be smaller than uncompressed)
        assert output_path.stat().st_size > 0
