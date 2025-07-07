"""Test NULL parameter handling for ADBC drivers."""

from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.statement.sql import SQLConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_postgresql_null_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session for NULL parameter testing."""
    config = AdbcConfig(
        uri=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="adbc_driver_postgresql",
        statement_config=SQLConfig(),
    )

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_null_params (
                id SERIAL PRIMARY KEY,
                col1 TEXT,
                col2 TEXT,
                col3 TEXT
            )
        """)
        yield session
        # Cleanup
        session.execute_script("DROP TABLE IF EXISTS test_null_params")


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_single_null_parameter(adbc_postgresql_null_session: AdbcDriver) -> None:
    """Test executing with a single NULL parameter."""
    result = adbc_postgresql_null_session.execute("INSERT INTO test_null_params (col1) VALUES ($1)", None)
    assert result.rows_affected in (-1, 1)

    # Verify data
    result = adbc_postgresql_null_session.execute("SELECT * FROM test_null_params")
    assert len(result.data) == 1
    assert result.data[0]["col1"] is None


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_all_null_parameters(adbc_postgresql_null_session: AdbcDriver) -> None:
    """Test executing with all NULL parameters."""
    result = adbc_postgresql_null_session.execute(
        "INSERT INTO test_null_params (col1, col2, col3) VALUES ($1, $2, $3)", None, None, None
    )
    assert result.rows_affected in (-1, 1)

    # Verify data
    result = adbc_postgresql_null_session.execute("SELECT * FROM test_null_params")
    assert len(result.data) == 1
    assert result.data[0]["col1"] is None
    assert result.data[0]["col2"] is None
    assert result.data[0]["col3"] is None


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_mixed_null_parameters(adbc_postgresql_null_session: AdbcDriver) -> None:
    """Test executing with mixed NULL and non-NULL parameters."""
    result = adbc_postgresql_null_session.execute(
        "INSERT INTO test_null_params (col1, col2, col3) VALUES ($1, $2, $3)", "value1", None, "value3"
    )
    assert result.rows_affected in (-1, 1)

    # Verify data
    result = adbc_postgresql_null_session.execute("SELECT * FROM test_null_params")
    assert len(result.data) == 1
    assert result.data[0]["col1"] == "value1"
    assert result.data[0]["col2"] is None
    assert result.data[0]["col3"] == "value3"


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_execute_many_with_nulls(adbc_postgresql_null_session: AdbcDriver) -> None:
    """Test execute_many with NULL parameters."""
    parameters = [
        ("row1", None, None),
        (None, "row2", None),
        (None, None, "row3"),
        (None, None, None),  # All NULL row
    ]

    result = adbc_postgresql_null_session.execute_many(
        "INSERT INTO test_null_params (col1, col2, col3) VALUES ($1, $2, $3)", parameters
    )
    assert result.rows_affected in (-1, 4, 1)

    # Verify data
    result = adbc_postgresql_null_session.execute("SELECT * FROM test_null_params ORDER BY id")
    assert len(result.data) == 4

    # Check each row
    assert result.data[0]["col1"] == "row1"
    assert result.data[0]["col2"] is None
    assert result.data[0]["col3"] is None

    assert result.data[1]["col1"] is None
    assert result.data[1]["col2"] == "row2"
    assert result.data[1]["col3"] is None

    assert result.data[2]["col1"] is None
    assert result.data[2]["col2"] is None
    assert result.data[2]["col3"] == "row3"

    assert result.data[3]["col1"] is None
    assert result.data[3]["col2"] is None
    assert result.data[3]["col3"] is None
