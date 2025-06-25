"""Test RETURNING clause support for ADBC drivers."""

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
def adbc_postgresql_session_returning(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session with test table supporting RETURNING."""
    config = AdbcConfig(
        uri=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="adbc_driver_postgresql",
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_returning (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
        """)
        yield session
        # Cleanup
        session.execute_script("DROP TABLE IF EXISTS test_returning")


@pytest.fixture
def adbc_sqlite_session_returning() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC SQLite session with test table supporting RETURNING."""
    config = AdbcConfig(uri=":memory:", driver_name="adbc_driver_sqlite", statement_config=SQLConfig(strict_mode=False))

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_returning (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
        """)
        yield session


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_insert_returning(adbc_postgresql_session_returning: AdbcDriver) -> None:
    """Test INSERT with RETURNING clause on PostgreSQL."""
    result = adbc_postgresql_session_returning.execute(
        "INSERT INTO test_returning (name, value) VALUES ($1, $2) RETURNING id, name", ("test_user", 100)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_user"
    assert "id" in result.data[0]
    assert result.data[0]["id"] > 0


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_update_returning(adbc_postgresql_session_returning: AdbcDriver) -> None:
    """Test UPDATE with RETURNING clause on PostgreSQL."""
    # First insert a record
    adbc_postgresql_session_returning.execute(
        "INSERT INTO test_returning (name, value) VALUES ($1, $2)", ("update_test", 50)
    )

    # Update with RETURNING
    result = adbc_postgresql_session_returning.execute(
        "UPDATE test_returning SET value = $1 WHERE name = $2 RETURNING id, name, value", (200, "update_test")
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 200


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
@pytest.mark.xfail(reason="SQLite RETURNING clause support varies by version")
def test_sqlite_insert_returning(adbc_sqlite_session_returning: AdbcDriver) -> None:
    """Test INSERT with RETURNING clause on SQLite (requires SQLite 3.35.0+)."""
    result = adbc_sqlite_session_returning.execute(
        "INSERT INTO test_returning (name, value) VALUES (?, ?) RETURNING id, name", ("test_user", 100)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_user"
    assert "id" in result.data[0]
    assert result.data[0]["id"] > 0


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_delete_returning(adbc_postgresql_session_returning: AdbcDriver) -> None:
    """Test DELETE with RETURNING clause on PostgreSQL."""
    # First insert a record
    adbc_postgresql_session_returning.execute(
        "INSERT INTO test_returning (name, value) VALUES ($1, $2)", ("delete_test", 75)
    )

    # Delete with RETURNING
    result = adbc_postgresql_session_returning.execute(
        "DELETE FROM test_returning WHERE name = $1 RETURNING id, name, value", ("delete_test",)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "delete_test"
    assert result.data[0]["value"] == 75
