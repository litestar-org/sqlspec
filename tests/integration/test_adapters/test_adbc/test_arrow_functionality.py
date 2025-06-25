"""Test Arrow functionality for ADBC drivers."""

from __future__ import annotations

import tempfile
from collections.abc import Generator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_postgresql_arrow_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session for Arrow testing."""
    config = AdbcConfig(
        uri=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="adbc_driver_postgresql",
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
        # Insert test data
        session.execute_many(
            "INSERT INTO test_arrow (name, value, price, is_active) VALUES ($1, $2, $3, $4)",
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


@pytest.fixture
def adbc_sqlite_arrow_session() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC SQLite session for Arrow testing."""
    config = AdbcConfig(uri=":memory:", driver_name="adbc_driver_sqlite", statement_config=SQLConfig(strict_mode=False))

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


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_fetch_arrow_table(adbc_postgresql_arrow_session: AdbcDriver) -> None:
    """Test fetch_arrow_table method with PostgreSQL."""
    result = adbc_postgresql_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow ORDER BY id")

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


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_fetch_arrow_table(adbc_sqlite_arrow_session: AdbcDriver) -> None:
    """Test fetch_arrow_table method with SQLite."""
    result = adbc_sqlite_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow ORDER BY id")

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


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_to_parquet(adbc_postgresql_arrow_session: AdbcDriver) -> None:
    """Test to_parquet export with PostgreSQL."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        adbc_postgresql_arrow_session.export_to_storage(
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


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_to_parquet(adbc_sqlite_arrow_session: AdbcDriver) -> None:
    """Test to_parquet export with SQLite."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test_output.parquet"

        adbc_sqlite_arrow_session.export_to_storage(
            "SELECT * FROM test_arrow WHERE is_active = 1", destination_uri=str(output_path)
        )

        assert output_path.exists()

        # Read back the parquet file
        table = pq.read_table(output_path)
        assert table.num_rows == 3  # Only active products


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_arrow_with_parameters(adbc_postgresql_arrow_session: AdbcDriver) -> None:
    """Test fetch_arrow_table with parameters on PostgreSQL."""
    result = adbc_postgresql_arrow_session.fetch_arrow_table(
        "SELECT * FROM test_arrow WHERE value >= $1 AND value <= $2 ORDER BY value", (200, 400)
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    values = result.data["value"].to_pylist()
    assert values == [200, 300, 400]


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_arrow_empty_result(adbc_postgresql_arrow_session: AdbcDriver) -> None:
    """Test fetch_arrow_table with empty result on PostgreSQL."""
    result = adbc_postgresql_arrow_session.fetch_arrow_table("SELECT * FROM test_arrow WHERE value > $1", (1000,))

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 0
    assert result.num_columns >= 5  # Schema should still be present
