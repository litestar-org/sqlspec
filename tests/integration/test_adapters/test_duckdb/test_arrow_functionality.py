"""Test Arrow functionality for DuckDB drivers."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.duckdb import DuckDbConfig, DuckDbDriver
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def duckdb_arrow_session() -> "Generator[DuckDbDriver, None, None]":
    """Create a DuckDB session for Arrow testing."""
    config = DuckDbConfig(
        connection_config={
            "database": ":memory:",
        },
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        # Create test table with various data types
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_arrow (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                value INTEGER,
                price DECIMAL(10, 2),
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Insert test data
        session.execute_many(
            "INSERT INTO test_arrow (id, name, value, price, is_active) VALUES (?, ?, ?, ?, ?)",
            [
                (1, "Product A", 100, 19.99, True),
                (2, "Product B", 200, 29.99, True),
                (3, "Product C", 300, 39.99, False),
                (4, "Product D", 400, 49.99, True),
                (5, "Product E", 500, 59.99, False),
            ],
        )
        yield session


def test_duckdb_fetch_arrow_table(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test fetch_arrow_table method with DuckDB."""
    result = duckdb_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow ORDER BY id")

    assert isinstance(result, ArrowResult)
    assert isinstance(result.data, pa.Table)
    assert result.data.num_rows == 5
    assert result.data.num_columns >= 5  # id, name, value, price, is_active, created_at

    # Check column names
    expected_columns = {"id", "name", "value", "price", "is_active"}
    actual_columns = set(result.data.column_names)
    assert expected_columns.issubset(actual_columns)

    # Check values
    values = result.data["value"].to_pylist()
    assert values == [100, 200, 300, 400, 500]

    # Check names
    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product E" in names


def test_duckdb_to_parquet(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test to_parquet export with DuckDB."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        duckdb_arrow_session.export_to_storage(
            "SELECT * FROM test_arrow WHERE is_active = true",
            str(output_path),
        )

        assert output_path.exists()

        # Read back the parquet file
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Only active products

        # Verify data
        names = table["name"].to_pylist()
        assert "Product A" in names
        assert "Product C" not in names  # Inactive product


def test_duckdb_arrow_with_parameters(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test fetch_arrow_table with parameters on DuckDB."""
    result = duckdb_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value >= ? AND value <= ? ORDER BY value",
        (200, 400),
    )

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 3
    values = result.data["value"].to_pylist()
    assert values == [200, 300, 400]


def test_duckdb_arrow_empty_result(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test fetch_arrow_table with empty result on DuckDB."""
    result = duckdb_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value > ?",
        (1000,),
    )

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 0
    assert result.data.num_columns >= 5  # Schema should still be present


def test_duckdb_arrow_data_types(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test Arrow data type mapping for DuckDB."""
    result = duckdb_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow LIMIT 1")

    assert isinstance(result, ArrowResult)

    # Check schema has expected columns
    schema = result.data.schema
    column_names = [field.name for field in schema]
    assert "id" in column_names
    assert "name" in column_names
    assert "value" in column_names
    assert "price" in column_names
    assert "is_active" in column_names

    # Verify DuckDB-specific type mappings
    assert pa.types.is_integer(result.data.schema.field("id").type)
    assert pa.types.is_string(result.data.schema.field("name").type)
    assert pa.types.is_boolean(result.data.schema.field("is_active").type)


def test_duckdb_to_arrow_with_sql_object(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test to_arrow with SQL object instead of string."""
    from sqlspec.statement.sql import SQL

    sql_obj = SQL("SELECT name, value FROM test_arrow WHERE is_active = ?", parameters=[True])
    result = duckdb_arrow_session.fetch_arrow_table(sql_obj)

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 3
    assert result.data.num_columns == 2  # Only name and value columns

    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product C" not in names  # Inactive


def test_duckdb_arrow_large_dataset(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test Arrow functionality with larger dataset."""
    # Insert more test data
    large_data = [(i, f"Item {i}", i * 10, float(i * 2.5), i % 2 == 0) for i in range(100, 1000)]

    duckdb_arrow_session.execute_many(
        "INSERT INTO test_arrow (id, name, value, price, is_active) VALUES (?, ?, ?, ?, ?)",
        large_data,
    )

    result = duckdb_arrow_session.fetch_arrow_table("SELECT COUNT(*) as total FROM test_arrow")

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 1
    total_count = result.data["total"].to_pylist()[0]
    assert total_count == 905  # 5 original + 900 new records


def test_duckdb_parquet_export_options(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test Parquet export with different options."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_compressed.parquet"

        # Export with compression
        duckdb_arrow_session.export_to_storage(
            "SELECT * FROM test_arrow WHERE value <= 300",
            str(output_path),
            compression="snappy",
        )

        assert output_path.exists()

        # Verify the file can be read
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Products A, B, C

        # Check compression was applied (file should be smaller than uncompressed)
        assert output_path.stat().st_size > 0


def test_duckdb_arrow_analytics_functions(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test Arrow functionality with DuckDB analytics functions."""
    result = duckdb_arrow_session.fetch_arrow_table("""
        SELECT
            name,
            value,
            price,
            LAG(value) OVER (ORDER BY id) as prev_value,
            ROW_NUMBER() OVER (ORDER BY value DESC) as rank_by_value
        FROM test_arrow
        ORDER BY id
    """)

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 5
    assert "prev_value" in result.data.column_names
    assert "rank_by_value" in result.data.column_names

    # Check window function results
    ranks = result.data["rank_by_value"].to_pylist()
    assert len(set(ranks)) == 5  # All ranks should be unique


def test_duckdb_arrow_with_json_data(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test Arrow functionality with JSON data in DuckDB."""
    # Create table with JSON column
    duckdb_arrow_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_json (
            id INTEGER,
            data JSON
        )
    """)

    # Insert JSON data
    duckdb_arrow_session.execute_many(
        "INSERT INTO test_json (id, data) VALUES (?, ?)",
        [
            (1, '{"name": "Alice", "age": 30}'),
            (2, '{"name": "Bob", "age": 25}'),
            (3, '{"name": "Charlie", "age": 35}'),
        ],
    )

    # Query with JSON extraction
    result = duckdb_arrow_session.fetch_arrow_table("""
        SELECT
            id,
            data->>'$.name' as name,
            CAST(data->>'$.age' AS INTEGER) as age
        FROM test_json
        ORDER BY id
    """)

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 3
    assert "name" in result.data.column_names
    assert "age" in result.data.column_names

    names = result.data["name"].to_pylist()
    assert "Alice" in names
    assert "Charlie" in names


def test_duckdb_arrow_with_aggregation(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test Arrow functionality with aggregation queries."""
    result = duckdb_arrow_session.fetch_arrow_table("""
        SELECT
            is_active,
            COUNT(*) as count,
            AVG(value) as avg_value,
            SUM(price) as total_price,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM test_arrow
        GROUP BY is_active
        ORDER BY is_active
    """)

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 2  # True and False groups
    assert "count" in result.data.column_names
    assert "avg_value" in result.data.column_names
    assert "total_price" in result.data.column_names

    # Verify aggregation results
    counts = result.data["count"].to_pylist()
    assert sum(counts) == 5  # Total should be 5 records


def test_duckdb_arrow_with_parquet_integration(duckdb_arrow_session: DuckDbDriver) -> None:
    """Test Arrow functionality with DuckDB's native Parquet integration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = Path(tmpdir) / "source_data.parquet"

        # First export to Parquet
        duckdb_arrow_session.export_to_storage(
            "SELECT * FROM test_arrow WHERE is_active = true",
            str(parquet_path),
        )

        # Then query the Parquet file directly in DuckDB
        result = duckdb_arrow_session.fetch_arrow_table(f"""
            SELECT
                name,
                value * 2 as doubled_value,
                price
            FROM read_parquet('{parquet_path}')
            ORDER BY value
        """)

        assert isinstance(result, ArrowResult)
        assert result.data.num_rows == 3  # Only active products
        assert "doubled_value" in result.data.column_names

        # Verify the doubling calculation
        doubled_values = result.data["doubled_value"].to_pylist()
        original_values = [100, 200, 400]  # Active products A, B, D
        expected_doubled = [v * 2 for v in original_values]
        assert doubled_values == expected_doubled
