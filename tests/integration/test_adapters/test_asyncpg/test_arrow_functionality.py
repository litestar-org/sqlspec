"""Test Arrow functionality for AsyncPG drivers."""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.statement.result import ArrowResult


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_fetch_arrow_table(asyncpg_arrow_session: AsyncpgDriver) -> None:
    """Test fetch_arrow_table method with AsyncPG."""
    result = await asyncpg_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow ORDER BY id")

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 5
    assert result.num_columns >= 5  # id, name, value, price, is_active, created_at

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
async def test_asyncpg_to_parquet(asyncpg_arrow_session: AsyncpgDriver) -> None:
    """Test to_parquet export with AsyncPG."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        await asyncpg_arrow_session.export_to_storage(
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
async def test_asyncpg_arrow_with_parameters(asyncpg_arrow_session: AsyncpgDriver) -> None:
    """Test fetch_arrow_table with parameters on AsyncPG."""
    # fetch_arrow_table doesn't accept parameters - embed them in SQL
    result = await asyncpg_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value >= 200 AND value <= 400 ORDER BY value"
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    values = result.data["value"].to_pylist()
    assert values == [200, 300, 400]


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_arrow_empty_result(asyncpg_arrow_session: AsyncpgDriver) -> None:
    """Test fetch_arrow_table with empty result on AsyncPG."""
    # fetch_arrow_table doesn't accept parameters - embed them in SQL
    result = await asyncpg_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow WHERE value > 1000")

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 0
    # AsyncPG limitation: schema information is not available for empty result sets
    assert result.num_columns == 0


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_arrow_data_types(asyncpg_arrow_session: AsyncpgDriver) -> None:
    """Test Arrow data type mapping for AsyncPG."""
    result = await asyncpg_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow LIMIT 1")

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
async def test_asyncpg_to_arrow_with_sql_object(asyncpg_arrow_session: AsyncpgDriver) -> None:
    """Test to_arrow with SQL object instead of string."""

    # fetch_arrow_table expects a SQL string, not a SQL object
    result = await asyncpg_arrow_session.fetch_arrow_table("SELECT name, value FROM test_arrow WHERE is_active = true")

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    assert result.num_columns == 2  # Only name and value columns

    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product C" not in names  # Inactive


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_arrow_large_dataset(asyncpg_arrow_session: AsyncpgDriver) -> None:
    """Test Arrow functionality with larger dataset."""
    # Insert more test data
    large_data = [(f"Item {i}", i * 10, float(i * 2.5), i % 2 == 0) for i in range(100, 1000)]

    await asyncpg_arrow_session.execute_many(
        "INSERT INTO test_arrow (name, value, price, is_active) VALUES ($1, $2, $3, $4)", large_data
    )

    result = await asyncpg_arrow_session.fetch_arrow_table("SELECT COUNT(*) as total FROM test_arrow")

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 1
    total_count = result.data["total"].to_pylist()[0]
    assert total_count == 905  # 5 original + 900 new records


@pytest.mark.asyncio
@pytest.mark.xdist_group("postgres")
async def test_asyncpg_parquet_export_options(asyncpg_arrow_session: AsyncpgDriver) -> None:
    """Test Parquet export with different options."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_compressed.parquet"

        # Export with compression
        await asyncpg_arrow_session.export_to_storage(
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
async def test_asyncpg_arrow_complex_query(asyncpg_arrow_session: AsyncpgDriver) -> None:
    """Test Arrow functionality with complex SQL queries."""
    result = await asyncpg_arrow_session.fetch_arrow_table(
        """
        SELECT
            name,
            value,
            price,
            CASE WHEN is_active THEN 'Active' ELSE 'Inactive' END as status,
            value * price as total_value
        FROM test_arrow
        WHERE value BETWEEN 200 AND 500
        ORDER BY total_value DESC
    """
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 4  # Products B, C, D, E
    assert "status" in result.column_names
    assert "total_value" in result.column_names

    # Verify calculated column
    total_values = result.data["total_value"].to_pylist()
    assert len(total_values) == 4
    # Should be ordered by total_value DESC
    assert total_values is not None
    assert total_values == sorted(total_values, reverse=True)  # type: ignore[type-var]
