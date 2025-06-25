"""Test data type handling for ADBC drivers."""

from __future__ import annotations

import datetime
import json
from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.statement.sql import SQLConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_postgresql_types_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session for data type testing."""
    config = AdbcConfig(
        uri=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="adbc_driver_postgresql",
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        # Create table with various data types
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_types (
                id SERIAL PRIMARY KEY,
                text_col TEXT,
                varchar_col VARCHAR(255),
                int_col INTEGER,
                bigint_col BIGINT,
                float_col FLOAT,
                decimal_col DECIMAL(10, 2),
                bool_col BOOLEAN,
                date_col DATE,
                time_col TIME,
                timestamp_col TIMESTAMP,
                json_col JSON,
                array_col INTEGER[]
            )
        """)
        yield session
        # Cleanup
        session.execute_script("DROP TABLE IF EXISTS test_types")


@pytest.fixture
def adbc_sqlite_types_session() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC SQLite session for data type testing."""
    config = AdbcConfig(uri=":memory:", driver_name="adbc_driver_sqlite", statement_config=SQLConfig(strict_mode=False))

    with config.provide_session() as session:
        # Create table with SQLite data types
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_types (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text_col TEXT,
                int_col INTEGER,
                real_col REAL,
                blob_col BLOB,
                numeric_col NUMERIC
            )
        """)
        yield session


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_basic_types(adbc_postgresql_types_session: AdbcDriver) -> None:
    """Test basic data types with PostgreSQL."""
    # Insert test data
    result = adbc_postgresql_types_session.execute(
        """
        INSERT INTO test_types
        (text_col, varchar_col, int_col, bigint_col, float_col, decimal_col, bool_col)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        """,
        ("Test text", "Test varchar", 42, 9876543210, 3.14159, 123.45, True),
    )

    inserted_id = result.data[0]["id"]

    # Retrieve and verify
    select_result = adbc_postgresql_types_session.execute("SELECT * FROM test_types WHERE id = $1", (inserted_id,))

    assert len(select_result.data) == 1
    row = select_result.data[0]

    assert row["text_col"] == "Test text"
    assert row["varchar_col"] == "Test varchar"
    assert row["int_col"] == 42
    assert row["bigint_col"] == 9876543210
    assert abs(row["float_col"] - 3.14159) < 0.00001
    assert float(row["decimal_col"]) == 123.45
    assert row["bool_col"] is True


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_basic_types(adbc_sqlite_types_session: AdbcDriver) -> None:
    """Test basic data types with SQLite."""
    # Insert test data
    adbc_sqlite_types_session.execute(
        """
        INSERT INTO test_types
        (text_col, int_col, real_col, numeric_col)
        VALUES (?, ?, ?, ?)
        """,
        ("Test text", 42, 3.14159, 123.45),
    )

    # Retrieve and verify
    select_result = adbc_sqlite_types_session.execute("SELECT * FROM test_types WHERE int_col = ?", (42,))

    assert len(select_result.data) == 1
    row = select_result.data[0]

    assert row["text_col"] == "Test text"
    assert row["int_col"] == 42
    assert abs(row["real_col"] - 3.14159) < 0.00001
    # SQLite may store numeric as float
    assert float(row["numeric_col"]) == 123.45


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_date_time_types(adbc_postgresql_types_session: AdbcDriver) -> None:
    """Test date and time types with PostgreSQL."""
    now = datetime.datetime.now()
    today = now.date()
    current_time = now.time()

    # Insert date/time data
    result = adbc_postgresql_types_session.execute(
        """
        INSERT INTO test_types
        (date_col, time_col, timestamp_col)
        VALUES ($1, $2, $3)
        RETURNING id
        """,
        (today, current_time, now),
    )

    inserted_id = result.data[0]["id"]

    # Retrieve and verify
    select_result = adbc_postgresql_types_session.execute(
        "SELECT date_col, time_col, timestamp_col FROM test_types WHERE id = $1", (inserted_id,)
    )

    row = select_result.data[0]

    # Date comparison
    assert row["date_col"] == today

    # Time comparison (may need tolerance for microseconds)
    retrieved_time = row["time_col"]
    if isinstance(retrieved_time, datetime.time):
        assert retrieved_time.hour == current_time.hour
        assert retrieved_time.minute == current_time.minute
        assert retrieved_time.second == current_time.second

    # Timestamp comparison
    assert isinstance(row["timestamp_col"], datetime.datetime)


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
@pytest.mark.xfail(reason="ADBC PostgreSQL driver has issues with null parameter handling")
def test_postgresql_null_values(adbc_postgresql_types_session: AdbcDriver) -> None:
    """Test NULL value handling with PostgreSQL."""
    # Insert row with NULL values
    result = adbc_postgresql_types_session.execute(
        """
        INSERT INTO test_types
        (text_col, int_col, bool_col, date_col)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        (None, None, None, None),
    )

    inserted_id = result.data[0]["id"]

    # Retrieve and verify NULLs
    select_result = adbc_postgresql_types_session.execute(
        "SELECT text_col, int_col, bool_col, date_col FROM test_types WHERE id = $1", (inserted_id,)
    )

    row = select_result.data[0]
    assert row["text_col"] is None
    assert row["int_col"] is None
    assert row["bool_col"] is None
    assert row["date_col"] is None


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_blob_type(adbc_sqlite_types_session: AdbcDriver) -> None:
    """Test BLOB data type with SQLite."""
    # Binary data
    binary_data = b"Hello, this is binary data!"

    # Insert BLOB
    adbc_sqlite_types_session.execute("INSERT INTO test_types (blob_col) VALUES (?)", (binary_data,))

    # Retrieve and verify
    select_result = adbc_sqlite_types_session.execute("SELECT blob_col FROM test_types WHERE blob_col IS NOT NULL")

    assert len(select_result.data) == 1
    retrieved_blob = select_result.data[0]["blob_col"]

    # ADBC might return as bytes or memoryview
    if isinstance(retrieved_blob, memoryview):
        retrieved_blob = bytes(retrieved_blob)

    assert retrieved_blob == binary_data


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_advanced_types(adbc_postgresql_types_session: AdbcDriver) -> None:
    """Test JSON and array types with PostgreSQL."""
    # Insert JSON and array data
    json_data = {"name": "Test", "value": 123, "nested": {"key": "value"}}
    array_data = [1, 2, 3, 4, 5]

    result = adbc_postgresql_types_session.execute(
        """
        INSERT INTO test_types
        (json_col, array_col)
        VALUES ($1::json, $2)
        RETURNING id
        """,
        (json.dumps(json_data), array_data),
    )

    inserted_id = result.data[0]["id"]

    # Retrieve and verify
    select_result = adbc_postgresql_types_session.execute(
        "SELECT json_col, array_col FROM test_types WHERE id = $1", (inserted_id,)
    )

    row = select_result.data[0]

    # JSON might be returned as string or dict
    json_col = row["json_col"]
    if isinstance(json_col, str):
        json_col = json.loads(json_col)
    assert json_col["name"] == "Test"
    assert json_col["value"] == 123

    # Array should be a list
    assert row["array_col"] == array_data
