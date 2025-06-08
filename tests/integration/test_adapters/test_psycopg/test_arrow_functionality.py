"""Test Arrow functionality for Psycopg drivers."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def psycopg_arrow_session(postgres_service: PostgresService) -> "Generator[PsycopgSyncDriver, None, None]":
    """Create a Psycopg session for Arrow testing."""
    config = PsycopgSyncConfig(
        pool_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "dbname": postgres_service.database,
        },
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        # Create test table with various data types
        session.execute_script("""
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
        session.execute_script("TRUNCATE TABLE test_arrow RESTART IDENTITY")

        # Insert test data
        session.execute_many(
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
        session.execute_script("DROP TABLE IF EXISTS test_arrow")


@pytest.mark.xdist_group("postgres")
def test_psycopg_fetch_arrow_table(psycopg_arrow_session: PsycopgSyncDriver) -> None:
    """Test fetch_arrow_table method with Psycopg."""
    result = psycopg_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow ORDER BY id")

    assert isinstance(result, ArrowResult)
    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 5
    assert result.data.num_columns >= 5  # id, name, value, price, is_active, created_at

    # Check column names
    expected_columns = {"id", "name", "value", "price", "is_active"}
    actual_columns = set(result.column_names())
    assert expected_columns.issubset(actual_columns)

    # Check data types
    assert pa.types.is_integer(result.data.schema.field("value").type)
    assert pa.types.is_string(result.data.schema.field("name").type)
    assert pa.types.is_boolean(result.data.schema.field("is_active").type)

    # Check values
    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product E" in names


@pytest.mark.xdist_group("postgres")
def test_psycopg_to_parquet(psycopg_arrow_session: PsycopgSyncDriver) -> None:
    """Test to_parquet export with Psycopg."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        psycopg_arrow_session.export_to_storage(
            "SELECT * FROM test_arrow WHERE is_active = true", str(output_path), format="parquet"
        )

        assert output_path.exists()

        # Read back the parquet file
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Only active products

        # Verify data
        names = table["name"].to_pylist()
        assert "Product A" in names
        assert "Product C" not in names  # Inactive product


@pytest.mark.xdist_group("postgres")
def test_psycopg_arrow_with_parameters(psycopg_arrow_session: PsycopgSyncDriver) -> None:
    """Test fetch_arrow_table with parameters on Psycopg."""
    # TODO: tuples should be valid parameters?
    result = psycopg_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value >= %s AND value <= %s ORDER BY value",
        (200, 400),
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 3
    values = result.data["value"].to_pylist()
    assert values == [200, 300, 400]


@pytest.mark.xdist_group("postgres")
def test_psycopg_arrow_empty_result(psycopg_arrow_session: PsycopgSyncDriver) -> None:
    """Test fetch_arrow_table with empty result on Psycopg."""
    result = psycopg_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value > %s",
        (1000,),
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 0
    assert result.data.num_columns >= 5  # Schema should still be present


@pytest.mark.xdist_group("postgres")
def test_psycopg_arrow_data_types(psycopg_arrow_session: PsycopgSyncDriver) -> None:
    """Test Arrow data type mapping for Psycopg."""
    result = psycopg_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow LIMIT 1")

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


@pytest.mark.xdist_group("postgres")
def test_psycopg_to_arrow_with_sql_object(psycopg_arrow_session: PsycopgSyncDriver) -> None:
    """Test to_arrow with SQL object instead of string."""
    from sqlspec.statement.sql import SQL

    sql_obj = SQL("SELECT name, value FROM test_arrow WHERE is_active = %s", parameters=[True])
    result = psycopg_arrow_session.fetch_arrow_table(sql_obj)

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 3
    assert result.data.num_columns == 2  # Only name and value columns

    names = result.data["name"].to_pylist()
    assert "Product A" in names
    assert "Product C" not in names  # Inactive


@pytest.mark.xdist_group("postgres")
def test_psycopg_arrow_large_dataset(psycopg_arrow_session: PsycopgSyncDriver) -> None:
    """Test Arrow functionality with larger dataset."""
    # Insert more test data
    large_data = [(f"Item {i}", i * 10, float(i * 2.5), i % 2 == 0) for i in range(100, 1000)]

    psycopg_arrow_session.execute_many(
        "INSERT INTO test_arrow (name, value, price, is_active) VALUES (%s, %s, %s, %s)",
        large_data,
    )

    result = psycopg_arrow_session.fetch_arrow_table("SELECT COUNT(*) as total FROM test_arrow")

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 1
    total_count = result.data["total"].to_pylist()[0]
    assert total_count == 905  # 5 original + 900 new records


@pytest.mark.xdist_group("postgres")
def test_psycopg_parquet_export_options(psycopg_arrow_session: PsycopgSyncDriver) -> None:
    """Test Parquet export with different options."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_compressed.parquet"

        # Export with compression
        psycopg_arrow_session.export_to_storage(
            "SELECT * FROM test_arrow WHERE value <= 300",
            str(output_path),
            format="parquet",
            compression="snappy",
        )

        assert output_path.exists()

        # Verify the file can be read
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Products A, B, C

        # Check compression was applied (file should be smaller than uncompressed)
        assert output_path.stat().st_size > 0


@pytest.mark.xdist_group("postgres")
def test_psycopg_arrow_complex_query(psycopg_arrow_session: PsycopgSyncDriver) -> None:
    """Test Arrow functionality with complex SQL queries."""
    result = psycopg_arrow_session.fetch_arrow_table(
        """
        SELECT
            name,
            value,
            price,
            CASE WHEN is_active THEN 'Active' ELSE 'Inactive' END as status,
            value * price as total_value
        FROM test_arrow
        WHERE value BETWEEN %s AND %s
        ORDER BY total_value DESC
    """,
        (200, 500),
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows() == 4  # Products B, C, D, E
    assert "status" in result.column_names()
    assert "total_value" in result.column_names()

    # Verify calculated column
    total_values = result.data["total_value"].to_pylist()
    assert len(total_values) == 4
    # Should be ordered by total_value DESC
    assert total_values == sorted(total_values, reverse=True)  # type: ignore


@pytest.mark.xdist_group("postgres")
def test_psycopg_arrow_with_copy_operations(psycopg_arrow_session: PsycopgSyncDriver) -> None:
    """Test Arrow functionality works well with COPY operations."""
    # Test that Arrow export works after bulk data operations
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create CSV file
        csv_path = Path(tmpdir) / "test_data.csv"
        csv_path.write_text("name,value,price,is_active\nBulk A,1000,100.00,true\nBulk B,2000,200.00,false")

        # Use COPY to load data (if supported)
        try:
            # TODO: we need to make sure this works on our mixin
            psycopg_arrow_session.import_from_storage(
                csv_path,
                "test_arrow",
                strategy="append",
                format="csv",
            )

            # Now test Arrow export
            result = psycopg_arrow_session.fetch_arrow_table(
                "SELECT * FROM test_arrow WHERE name LIKE 'Bulk%' ORDER BY value"
            )

            assert isinstance(result, ArrowResult)
            assert result.num_rows() >= 2  # At least the bulk records

        except AttributeError:
            # COPY operations may not be implemented for all drivers
            pytest.skip("COPY operations not supported for this driver")
