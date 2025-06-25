"""Test Arrow functionality for PSQLPy drivers."""

import tempfile
from collections.abc import AsyncGenerator
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from psqlpy.extra_types import JSON, JSONB, Int32Array, NumericArray, TextArray
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
async def psqlpy_arrow_session(postgres_service: PostgresService) -> "AsyncGenerator[PsqlpyDriver, None]":
    """Create a PSQLPy session for Arrow testing."""
    config = PsqlpyConfig(
        host=postgres_service.host,
        port=postgres_service.port,
        username=postgres_service.user,
        password=postgres_service.password,
        db_name=postgres_service.database,
        statement_config=SQLConfig(strict_mode=False),
    )

    async with config.provide_session() as session:
        # Create test table with various data types
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_arrow (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER,
                price DECIMAL(10, 2),
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Clear any existing data
        await session.execute_script("TRUNCATE TABLE test_arrow RESTART IDENTITY")

        # Insert test data
        await session.execute_many(
            "INSERT INTO test_arrow (name, value, price, is_active) VALUES ($1, $2, $3, $4)",
            [
                ("Product A", 100, Decimal("19.99"), True),
                ("Product B", 200, Decimal("29.99"), True),
                ("Product C", 300, Decimal("39.99"), False),
                ("Product D", 400, Decimal("49.99"), True),
                ("Product E", 500, Decimal("59.99"), False),
            ],
        )
        yield session
        # Cleanup
        await session.execute_script("DROP TABLE IF EXISTS test_arrow")


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_psqlpy_fetch_arrow_table(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test fetch_arrow_table method with PSQLPy."""
    result = await psqlpy_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow ORDER BY id")

    assert isinstance(result, ArrowResult)
    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5
    assert result.data.num_columns >= 5  # id, name, value, price, is_active, created_at

    # Check column names
    expected_columns = {"id", "name", "value", "price", "is_active"}
    actual_columns = set(result.column_names)
    assert expected_columns.issubset(actual_columns)

    # Check data types
    assert pa.types.is_integer(result.data.schema.field("value").type)
    assert pa.types.is_string(result.data.schema.field("name").type)
    assert pa.types.is_boolean(result.data.schema.field("is_active").type)

    # Check values
    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product E" in names


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_psqlpy_to_parquet(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test to_parquet export with PSQLPy."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        await psqlpy_arrow_session.export_to_storage(
            "SELECT * FROM test_arrow WHERE is_active = true", destination_uri=str(output_path)
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
@pytest.mark.xdist_group("postgres")
async def test_psqlpy_arrow_with_parameters(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test fetch_arrow_table with parameters on PSQLPy."""
    result = await psqlpy_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value >= $1 AND value <= $2 ORDER BY value", (200, 400)
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    values = result.data["value"].to_pylist()
    assert values == [200, 300, 400]


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_psqlpy_arrow_empty_result(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test fetch_arrow_table with empty result on PSQLPy."""
    result = await psqlpy_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow WHERE value > $1", (1000,))

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 0
    # PSQLPy limitation: empty results don't include schema information
    assert result.data.num_columns == 0


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_psqlpy_arrow_data_types(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test Arrow data type mapping for PSQLPy."""
    result = await psqlpy_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow LIMIT 1")

    assert isinstance(result, ArrowResult)

    # Check schema has expected columns
    schema = result.data.schema
    column_names = [field.name for field in schema]
    assert "id" in column_names
    assert "name" in column_names
    assert "value" in column_names
    assert "price" in column_names
    assert "is_active" in column_names

    # Verify PostgreSQL-specific type mappings
    assert pa.types.is_integer(result.data.schema.field("id").type)
    assert pa.types.is_string(result.data.schema.field("name").type)
    assert pa.types.is_boolean(result.data.schema.field("is_active").type)


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_psqlpy_to_arrow_with_sql_object(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test to_arrow with SQL object instead of string."""
    from sqlspec.statement.sql import SQL

    sql_obj = SQL("SELECT name, value FROM test_arrow WHERE is_active = $1", parameters=[True])
    result = await psqlpy_arrow_session.fetch_arrow_table(sql_obj)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    assert result.data.num_columns == 2  # Only name and value columns

    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product C" not in names  # Inactive


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_psqlpy_arrow_large_dataset(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test Arrow functionality with larger dataset."""
    # Insert more test data
    large_data = [(f"Item {i}", i * 10, Decimal(str(i * 2.5)), i % 2 == 0) for i in range(100, 1000)]

    await psqlpy_arrow_session.execute_many(
        "INSERT INTO test_arrow (name, value, price, is_active) VALUES ($1, $2, $3, $4)", large_data
    )

    result = await psqlpy_arrow_session.fetch_arrow_table("SELECT COUNT(*) as total FROM test_arrow")

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 1
    total_count = result.data["total"].to_pylist()[0]
    assert total_count == 905  # 5 original + 900 new records


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_psqlpy_parquet_export_options(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test Parquet export with different options."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_compressed.parquet"

        # Export with compression
        await psqlpy_arrow_session.export_to_storage(
            "SELECT * FROM test_arrow WHERE value <= 300", destination_uri=str(output_path), compression="snappy"
        )

        assert output_path.exists()

        # Verify the file can be read
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Products A, B, C

        # Check compression was applied (file should be smaller than uncompressed)
        assert output_path.stat().st_size > 0


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
@pytest.mark.skip(reason="SQLglot issue with array_length function parameters")
async def test_psqlpy_arrow_with_postgresql_arrays(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test Arrow functionality with PostgreSQL array types."""
    # Create table with array columns
    await psqlpy_arrow_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_arrays (
            id SERIAL PRIMARY KEY,
            tags TEXT[],
            scores INTEGER[],
            ratings DECIMAL[]
        )
    """)

    await psqlpy_arrow_session.execute_many(
        "INSERT INTO test_arrays (tags, scores, ratings) VALUES ($1, $2, $3)",
        [
            (
                TextArray(["electronics", "laptop"]),
                Int32Array([95, 87, 92]),
                NumericArray([Decimal("4.5"), Decimal("4.2"), Decimal("4.8")]),
            ),
            (TextArray(["mobile", "phone"]), Int32Array([88, 91]), NumericArray([Decimal("4.1"), Decimal("4.6")])),
            (TextArray(["accessories"]), Int32Array([75]), NumericArray([Decimal("3.9")])),
        ],
    )

    result = await psqlpy_arrow_session.fetch_arrow_table(
        "SELECT id, tags, scores, ratings, array_length(tags, 1) as tag_count, array_to_string(tags, $1) as tags_string FROM test_arrays ORDER BY id",
        (", ",),
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    assert "tags" in result.column_names
    assert "scores" in result.column_names
    assert "tag_count" in result.column_names

    # Verify array handling
    tag_counts = result.data["tag_count"].to_pylist()
    assert tag_counts == [2, 2, 1]


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_psqlpy_arrow_with_json_operations(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test Arrow functionality with PostgreSQL JSON operations."""
    # Drop table if exists and create fresh - execute separately due to psqlpy limitation
    await psqlpy_arrow_session.execute("DROP TABLE IF EXISTS test_json")
    await psqlpy_arrow_session.execute("""
        CREATE TABLE test_json (
            id SERIAL PRIMARY KEY,
            metadata JSONB,
            settings JSON
        )
    """)

    await psqlpy_arrow_session.execute_many(
        "INSERT INTO test_json (metadata, settings) VALUES ($1, $2)",
        [
            (
                JSONB({"name": "Product A", "category": "electronics", "price": 19.99}),
                JSON({"theme": "dark", "notifications": True}),
            ),
            (
                JSONB({"name": "Product B", "category": "books", "price": 29.99}),
                JSON({"theme": "light", "notifications": False}),
            ),
            (
                JSONB({"name": "Product C", "category": "electronics", "price": 39.99}),
                JSON({"theme": "auto", "notifications": True}),
            ),
        ],
    )

    result = await psqlpy_arrow_session.fetch_arrow_table(
        """
        SELECT
            id,
            metadata->>'name' as product_name,
            metadata->>'category' as category,
            (metadata->>'price')::DECIMAL as price,
            settings->>'theme' as theme,
            (settings->>'notifications')::BOOLEAN as notifications_enabled
        FROM test_json
        WHERE metadata->>'category' = $1
        ORDER BY id
    """,
        ("electronics",),
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 2  # Only electronics products
    assert "product_name" in result.column_names
    assert "category" in result.column_names
    assert "theme" in result.column_names

    # Verify JSON extraction
    categories = result.data["category"].to_pylist()
    assert all(cat == "electronics" for cat in categories)

    themes = result.data["theme"].to_pylist()
    assert "dark" in themes or "auto" in themes


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_psqlpy_arrow_with_window_functions(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test Arrow functionality with PostgreSQL window functions."""
    result = await psqlpy_arrow_session.fetch_arrow_table("""
        SELECT
            name,
            value,
            price,
            ROW_NUMBER() OVER (ORDER BY value DESC) as value_rank,
            RANK() OVER (ORDER BY price DESC) as price_rank,
            LAG(value) OVER (ORDER BY id) as prev_value,
            LEAD(value) OVER (ORDER BY id) as next_value,
            SUM(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total,
            AVG(price) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_avg_price
        FROM test_arrow
        ORDER BY id
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5
    assert "value_rank" in result.column_names
    assert "price_rank" in result.column_names
    assert "prev_value" in result.column_names
    assert "running_total" in result.column_names

    # Verify window function results
    ranks = result.data["value_rank"].to_pylist()
    assert len(set(ranks)) == 5  # All ranks should be unique

    running_totals = result.data["running_total"].to_pylist()
    # Running total should be monotonically increasing
    assert all(running_totals[i] <= running_totals[i + 1] for i in range(len(running_totals) - 1))  # type: ignore[operator]


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
@pytest.mark.skip(
    reason="Literal parameterization causes type inference issues in recursive CTEs - see .bugs/literal-parameterizer-type-inference.md"
)
async def test_psqlpy_arrow_with_cte_and_recursive(psqlpy_arrow_session: PsqlpyDriver) -> None:
    """Test Arrow functionality with PostgreSQL CTEs and recursive queries."""
    result = await psqlpy_arrow_session.fetch_arrow_table("""
        WITH RECURSIVE value_sequence AS (
            -- Base case: start with minimum value
            SELECT
                name,
                value,
                price,
                1 as level,
                value as sequence_value
            FROM test_arrow
            WHERE value = (SELECT MIN(value) FROM test_arrow)

            UNION ALL

            -- Recursive case: find next higher value
            SELECT
                t.name,
                t.value,
                t.price,
                vs.level + 1,
                t.value
            FROM test_arrow t
            INNER JOIN value_sequence vs ON t.value > vs.sequence_value
            WHERE t.value = (
                SELECT MIN(value)
                FROM test_arrow
                WHERE value > vs.sequence_value
            )
            AND vs.level < 5  -- Limit recursion depth
        )
        SELECT
            name,
            value,
            price,
            level,
            LAG(value) OVER (ORDER BY level) as prev_sequence_value
        FROM value_sequence
        ORDER BY level
    """)

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5  # All products in sequence
    assert "level" in result.column_names
    assert "prev_sequence_value" in result.column_names

    # Verify recursive sequence
    levels = result.data["level"].to_pylist()
    assert levels == [1, 2, 3, 4, 5]  # Sequential levels

    values = result.data["value"].to_pylist()
    assert values == [100, 200, 300, 400, 500]  # Should be in ascending order
