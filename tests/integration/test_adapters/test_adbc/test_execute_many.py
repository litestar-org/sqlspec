"""Test execute_many functionality for ADBC drivers."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQLConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_postgresql_batch_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session for batch operation testing."""
    config = AdbcConfig(
        uri=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="adbc_driver_postgresql",
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_batch (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                category TEXT
            )
        """)
        yield session
        # Cleanup
        session.execute_script("DROP TABLE IF EXISTS test_batch")


@pytest.fixture
def adbc_sqlite_batch_session() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC SQLite session for batch operation testing."""
    config = AdbcConfig(uri=":memory:", driver_name="adbc_driver_sqlite", statement_config=SQLConfig(strict_mode=False))

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_batch (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                category TEXT
            )
        """)
        yield session


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_execute_many_basic(adbc_postgresql_batch_session: AdbcDriver) -> None:
    """Test basic execute_many with PostgreSQL."""
    parameters = [
        ("Item 1", 100, "A"),
        ("Item 2", 200, "B"),
        ("Item 3", 300, "A"),
        ("Item 4", 400, "C"),
        ("Item 5", 500, "B"),
    ]

    result = adbc_postgresql_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", parameters
    )

    assert isinstance(result, SQLResult)
    # ADBC drivers may not accurately report rows affected for batch operations
    assert result.rows_affected in (-1, 5, 1)  # -1 for not supported, 5 for total, 1 for last

    # Verify data was inserted
    count_result = adbc_postgresql_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 5


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_execute_many_basic(adbc_sqlite_batch_session: AdbcDriver) -> None:
    """Test basic execute_many with SQLite."""
    parameters = [
        ("Item 1", 100, "A"),
        ("Item 2", 200, "B"),
        ("Item 3", 300, "A"),
        ("Item 4", 400, "C"),
        ("Item 5", 500, "B"),
    ]

    result = adbc_sqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)", parameters
    )

    assert isinstance(result, SQLResult)
    # ADBC drivers may not accurately report rows affected for batch operations
    assert result.rows_affected in (-1, 5, 1)

    # Verify data was inserted
    count_result = adbc_sqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 5


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_execute_many_update(adbc_postgresql_batch_session: AdbcDriver) -> None:
    """Test execute_many for UPDATE operations with PostgreSQL."""
    # First insert some data
    adbc_postgresql_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)",
        [("Update 1", 10, "X"), ("Update 2", 20, "Y"), ("Update 3", 30, "Z")],
    )

    # Now update with execute_many
    update_params = [(100, "Update 1"), (200, "Update 2"), (300, "Update 3")]

    result = adbc_postgresql_batch_session.execute_many(
        "UPDATE test_batch SET value = $1 WHERE name = $2", update_params
    )

    assert isinstance(result, SQLResult)

    # Verify updates
    check_result = adbc_postgresql_batch_session.execute("SELECT name, value FROM test_batch ORDER BY name")
    assert len(check_result.data) == 3
    assert all(row["value"] in (100, 200, 300) for row in check_result.data)


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_execute_many_empty(adbc_postgresql_batch_session: AdbcDriver) -> None:
    """Test execute_many with empty parameter list on PostgreSQL."""
    result = adbc_postgresql_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", []
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected in (-1, 0)

    # Verify no data was inserted
    count_result = adbc_postgresql_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 0


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_execute_many_mixed_types(adbc_sqlite_batch_session: AdbcDriver) -> None:
    """Test execute_many with mixed parameter types on SQLite."""
    parameters = [
        ("String Item", 123, "CAT1"),
        ("Another Item", 456, None),  # NULL category
        ("Third Item", 0, "CAT2"),
    ]

    result = adbc_sqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)", parameters
    )

    assert isinstance(result, SQLResult)

    # Verify data including NULL
    null_result = adbc_sqlite_batch_session.execute("SELECT * FROM test_batch WHERE category IS NULL")
    assert len(null_result.data) == 1
    assert null_result.data[0]["name"] == "Another Item"


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_execute_many_transaction(adbc_postgresql_batch_session: AdbcDriver) -> None:
    """Test execute_many within a transaction context on PostgreSQL."""
    # Get the connection to control transaction

    try:
        # Start transaction (if not auto-commit)
        parameters = [("Trans 1", 1000, "T"), ("Trans 2", 2000, "T"), ("Trans 3", 3000, "T")]

        adbc_postgresql_batch_session.execute_many(
            "INSERT INTO test_batch (name, value, category) VALUES ($1, $2, $3)", parameters
        )

        # Verify within transaction
        result = adbc_postgresql_batch_session.execute(
            "SELECT COUNT(*) as count FROM test_batch WHERE category = $1", ("T",)
        )
        assert result.data[0]["count"] == 3

    except Exception:
        # In case of error, the connection might handle rollback automatically
        raise
