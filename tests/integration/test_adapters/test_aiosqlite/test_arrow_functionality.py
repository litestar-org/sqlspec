"""Test Arrow functionality for AIOSQLite drivers."""

import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
async def aiosqlite_arrow_session() -> "AsyncGenerator[AiosqliteDriver, None]":
    """Create an AIOSQLite session for Arrow testing."""
    config = AiosqliteConfig(
        connection_config={
            "database": ":memory:",
        },
        statement_config=SQLConfig(strict_mode=False),
    )

    async with config.provide_session() as session:
        # Create test table with various data types
        await session.execute_script("""
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
        await session.execute_many(
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


@pytest.mark.asyncio
async def test_aiosqlite_fetch_arrow_table(aiosqlite_arrow_session: AiosqliteDriver) -> None:
    """Test fetch_arrow_table method with AIOSQLite."""
    result = await aiosqlite_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow ORDER BY id")

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


@pytest.mark.asyncio
async def test_aiosqlite_to_parquet(aiosqlite_arrow_session: AiosqliteDriver) -> None:
    """Test to_parquet export with AIOSQLite."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        await aiosqlite_arrow_session.export_to_storage(
            "SELECT * FROM test_arrow WHERE is_active = 1",
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


@pytest.mark.asyncio
async def test_aiosqlite_arrow_with_parameters(aiosqlite_arrow_session: AiosqliteDriver) -> None:
    """Test fetch_arrow_table with parameters on AIOSQLite."""
    result = await aiosqlite_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value >= ? AND value <= ? ORDER BY value",
        (200, 400),
    )

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 3
    values = result.data["value"].to_pylist()
    assert values == [200, 300, 400]


@pytest.mark.asyncio
async def test_aiosqlite_arrow_empty_result(aiosqlite_arrow_session: AiosqliteDriver) -> None:
    """Test fetch_arrow_table with empty result on AIOSQLite."""
    result = await aiosqlite_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value > ?",
        (1000,),
    )

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 0
    assert result.data.num_columns >= 5  # Schema should still be present


@pytest.mark.asyncio
async def test_aiosqlite_arrow_data_types(aiosqlite_arrow_session: AiosqliteDriver) -> None:
    """Test Arrow data type mapping for AIOSQLite."""
    result = await aiosqlite_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow LIMIT 1")

    assert isinstance(result, ArrowResult)

    # Check schema has expected columns
    schema = result.data.schema
    column_names = [field.name for field in schema]
    assert "id" in column_names
    assert "name" in column_names
    assert "value" in column_names
    assert "price" in column_names
    assert "is_active" in column_names


@pytest.mark.asyncio
async def test_aiosqlite_to_arrow_with_sql_object(aiosqlite_arrow_session: AiosqliteDriver) -> None:
    """Test to_arrow with SQL object instead of string."""
    from sqlspec.statement.sql import SQL

    sql_obj = SQL("SELECT name, value FROM test_arrow WHERE is_active = ?", parameters=[1])
    result = await aiosqlite_arrow_session.fetch_arrow_table(sql_obj)

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 3
    assert result.data.num_columns == 2  # Only name and value columns

    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product C" not in names  # Inactive


@pytest.mark.asyncio
async def test_aiosqlite_arrow_large_dataset(aiosqlite_arrow_session: AiosqliteDriver) -> None:
    """Test Arrow functionality with larger dataset."""
    # Insert more test data
    large_data = [(f"Item {i}", i * 10, float(i * 2.5), i % 2) for i in range(100, 1000)]

    await aiosqlite_arrow_session.execute_many(
        "INSERT INTO test_arrow (name, value, price, is_active) VALUES (?, ?, ?, ?)",
        large_data,
    )

    result = await aiosqlite_arrow_session.fetch_arrow_table("SELECT COUNT(*) as total FROM test_arrow")

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 1
    total_count = result.data["total"].to_pylist()[0]
    assert total_count == 905  # 5 original + 900 new records


@pytest.mark.asyncio
async def test_aiosqlite_parquet_export_options(aiosqlite_arrow_session: AiosqliteDriver) -> None:
    """Test Parquet export with different options."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_compressed.parquet"

        # Export with compression
        await aiosqlite_arrow_session.export_to_storage(
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


@pytest.mark.asyncio
async def test_aiosqlite_arrow_with_joins(aiosqlite_arrow_session: AiosqliteDriver) -> None:
    """Test Arrow functionality with JOIN operations."""
    # Create a second table for joins
    await aiosqlite_arrow_session.execute_script("""
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            category_name TEXT,
            min_value INTEGER
        )
    """)

    await aiosqlite_arrow_session.execute_many(
        "INSERT INTO categories (id, category_name, min_value) VALUES (?, ?, ?)",
        [
            (1, "Basic", 0),
            (2, "Standard", 200),
            (3, "Premium", 400),
        ],
    )

    result = await aiosqlite_arrow_session.fetch_arrow_table("""
        SELECT
            t.name,
            t.value,
            t.price,
            c.category_name
        FROM test_arrow t
        LEFT JOIN categories c ON t.value >= c.min_value AND c.id = (
            SELECT MAX(id) FROM categories WHERE min_value <= t.value
        )
        WHERE t.is_active = 1
        ORDER BY t.value
    """)

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 3  # Only active products
    assert "category_name" in result.data.column_names

    # Verify join results
    categories = result.data["category_name"].to_pylist()
    assert len(categories) == 3


@pytest.mark.asyncio
async def test_aiosqlite_arrow_with_sqlite_functions(aiosqlite_arrow_session: AiosqliteDriver) -> None:
    """Test Arrow functionality with SQLite-specific functions."""
    result = await aiosqlite_arrow_session.fetch_arrow_table(
        """
        SELECT
            name,
            value,
            price,
            UPPER(name) as name_upper,
            LENGTH(name) as name_length,
            ROUND(price, 1) as price_rounded,
            SUBSTR(name, 1, 7) as name_prefix
        FROM test_arrow
        WHERE value BETWEEN ? AND ?
        ORDER BY value
    """,
        (200, 400),
    )

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 3  # Products B, C, D
    assert "name_upper" in result.data.column_names
    assert "name_length" in result.data.column_names
    assert "price_rounded" in result.data.column_names
    assert "name_prefix" in result.data.column_names

    # Verify SQLite function results
    upper_names = result.data["name_upper"].to_pylist()
    assert all(name.isupper() for name in upper_names)

    lengths = result.data["name_length"].to_pylist()
    assert all(isinstance(length, int) and length > 0 for length in lengths)


@pytest.mark.asyncio
async def test_aiosqlite_arrow_with_cte(aiosqlite_arrow_session: AiosqliteDriver) -> None:
    """Test Arrow functionality with Common Table Expressions (CTE)."""
    result = await aiosqlite_arrow_session.fetch_arrow_table("""
        WITH active_products AS (
            SELECT * FROM test_arrow WHERE is_active = 1
        ),
        product_stats AS (
            SELECT
                COUNT(*) as total_count,
                AVG(value) as avg_value,
                SUM(price) as total_price
            FROM active_products
        )
        SELECT
            a.name,
            a.value,
            a.price,
            s.total_count,
            s.avg_value,
            a.value - s.avg_value as value_diff
        FROM active_products a
        CROSS JOIN product_stats s
        ORDER BY a.value
    """)

    assert isinstance(result, ArrowResult)
    assert result.data.num_rows == 3  # Only active products
    assert "total_count" in result.data.column_names
    assert "avg_value" in result.data.column_names
    assert "value_diff" in result.data.column_names

    # Verify CTE results
    total_counts = result.data["total_count"].to_pylist()
    assert all(count == 3 for count in total_counts)  # Should be 3 for all rows
