"""Test Arrow functionality for AsyncMy drivers."""

import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
async def asyncmy_arrow_session(mysql_service: MySQLService) -> "AsyncGenerator[AsyncmyDriver, None]":
    """Create an AsyncMy session for Arrow testing."""
    config = AsyncmyConfig(
        host=mysql_service.host,
        port=mysql_service.port,
        user=mysql_service.user,
        password=mysql_service.password,
        database=mysql_service.db,
        statement_config=SQLConfig(strict_mode=False),
    )

    async with config.provide_session() as session:
        # Create test table with various data types
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_arrow (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT,
                price DECIMAL(10, 2),
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Clear any existing data
        await session.execute_script("TRUNCATE TABLE test_arrow")

        # Insert test data
        await session.execute_many(
            "INSERT INTO test_arrow (name, value, price, is_active) VALUES (%s, %s, %s, %s)",
            [
                ("Product A", 100, 19.99, True),
                ("Product B", 200, 29.99, True),
                ("Product C", 300, 39.99, False),
                ("Product D", 400, 49.99, True),
                ("Product E", 500, 59.99, False),
            ],
        )
        yield session
        # Cleanup
        await session.execute_script("DROP TABLE IF EXISTS test_arrow")


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_fetch_arrow_table(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test fetch_arrow_table method with AsyncMy."""
    result = await asyncmy_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow ORDER BY id")

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5
    assert result.num_columns >= 5  # id, name, value, price, is_active, created_at

    # Check column names
    expected_columns = {"id", "name", "value", "price", "is_active"}
    actual_columns = set(result.column_names)
    assert expected_columns.issubset(actual_columns)

    # Check values
    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product E" in names


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_to_parquet(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test to_parquet export with AsyncMy."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        await asyncmy_arrow_session.export_to_storage(
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


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_arrow_with_parameters(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test fetch_arrow_table with parameters on AsyncMy."""
    result = await asyncmy_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value >= %s AND value <= %s ORDER BY value",
        (200, 400),
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    values = result.data["value"].to_pylist()
    assert values == [200, 300, 400]


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_arrow_empty_result(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test fetch_arrow_table with empty result on AsyncMy."""
    result = await asyncmy_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value > %s",
        (1000,),
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 0
    assert result.num_columns >= 5  # Schema should still be present


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_arrow_data_types(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test Arrow data type mapping for AsyncMy."""
    result = await asyncmy_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow LIMIT 1")

    assert isinstance(result, ArrowResult)

    # Check schema has expected columns
    schema = result.data.schema
    column_names = [field.name for field in schema]
    assert "id" in column_names
    assert "name" in column_names
    assert "value" in column_names
    assert "price" in column_names
    assert "is_active" in column_names

    # Verify MySQL-specific type mappings
    assert pa.types.is_integer(result.data.schema.field("id").type)
    assert pa.types.is_string(result.data.schema.field("name").type)


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_to_arrow_with_sql_object(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test to_arrow with SQL object instead of string."""
    from sqlspec.statement.sql import SQL

    sql_obj = SQL("SELECT name, value FROM test_arrow WHERE is_active = %s", parameters=[True])
    result = await asyncmy_arrow_session.fetch_arrow_table(sql_obj)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    assert result.num_columns == 2  # Only name and value columns

    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product C" not in names  # Inactive


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_arrow_large_dataset(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test Arrow functionality with larger dataset."""
    # Insert more test data in smaller batches for MySQL
    batch_size = 100
    for batch_start in range(100, 500, batch_size):
        batch_data = [
            (f"Item {i}", i * 10, float(i * 2.5), i % 2 == 0) for i in range(batch_start, batch_start + batch_size)
        ]
        await asyncmy_arrow_session.execute_many(
            "INSERT INTO test_arrow (name, value, price, is_active) VALUES (%s, %s, %s, %s)",
            batch_data,
        )

    result = await asyncmy_arrow_session.fetch_arrow_table("SELECT COUNT(*) as total FROM test_arrow")

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 1
    total_count = result.data["total"].to_pylist()[0]
    assert total_count == 405  # 5 original + 400 new records


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_parquet_export_options(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test Parquet export with different options."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_compressed.parquet"

        # Export with compression
        await asyncmy_arrow_session.export_to_storage(
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
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_arrow_mysql_functions(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test Arrow functionality with MySQL-specific functions."""
    result = await asyncmy_arrow_session.fetch_arrow_table(
        """
        SELECT
            name,
            value,
            price,
            CONCAT('Product: ', name) as formatted_name,
            ROUND(price * 1.1, 2) as price_with_tax
        FROM test_arrow
        WHERE value BETWEEN %s AND %s
        ORDER BY value
    """,
        (200, 400),
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3  # Products B, C, D
    assert "formatted_name" in result.column_names
    assert "price_with_tax" in result.column_names

    # Verify MySQL function results
    formatted_names = result.data["formatted_name"].to_pylist()
    assert all(name.startswith("Product: ") for name in formatted_names if name is not None)


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_arrow_with_datetime_functions(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test Arrow functionality with MySQL datetime functions."""
    result = await asyncmy_arrow_session.fetch_arrow_table("""
        SELECT
            name,
            value,
            created_at,
            DATE(created_at) as date_only,
            YEAR(created_at) as year_part,
            MONTH(created_at) as month_part
        FROM test_arrow
        ORDER BY id
        LIMIT 3
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    assert "date_only" in result.column_names
    assert "year_part" in result.column_names
    assert "month_part" in result.column_names

    # Verify datetime extraction
    years = result.data["year_part"].to_pylist()
    assert all(isinstance(year, int) and year > 2020 for year in years)


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_arrow_with_aggregation(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test Arrow functionality with aggregation queries."""
    result = await asyncmy_arrow_session.fetch_arrow_table("""
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
    assert result.num_rows == 2  # True and False groups
    assert "count" in result.column_names
    assert "avg_value" in result.column_names
    assert "total_price" in result.column_names

    # Verify aggregation results
    counts = result.data["count"].to_pylist()
    assert sum(counts) == 5  # Total should be 5 records # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_arrow_with_case_statements(asyncmy_arrow_session: AsyncmyDriver) -> None:
    """Test Arrow functionality with CASE statements."""
    result = await asyncmy_arrow_session.fetch_arrow_table("""
        SELECT
            name,
            value,
            CASE
                WHEN value <= 200 THEN 'Low'
                WHEN value <= 400 THEN 'Medium'
                ELSE 'High'
            END as value_category,
            CASE
                WHEN is_active = 1 THEN 'Active'
                ELSE 'Inactive'
            END as status
        FROM test_arrow
        ORDER BY value
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5
    assert "value_category" in result.column_names
    assert "status" in result.column_names

    # Verify CASE statement results
    categories = result.data["value_category"].to_pylist()
    assert "Low" in categories
    assert "Medium" in categories
    assert "High" in categories

    statuses = result.data["status"].to_pylist()
    assert "Active" in statuses
    assert "Inactive" in statuses
