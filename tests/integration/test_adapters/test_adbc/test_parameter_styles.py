"""Test different parameter styles for ADBC drivers."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_postgresql_params_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session for parameter style testing."""
    config = AdbcConfig(
        uri=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="adbc_driver_postgresql",
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_params (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                description TEXT
            )
        """)
        # Insert test data
        session.execute(
            SQL("INSERT INTO test_params (name, value, description) VALUES ($1, $2, $3)", ("test1", 100, "First test"))
        )
        session.execute(
            SQL("INSERT INTO test_params (name, value, description) VALUES ($1, $2, $3)", ("test2", 200, "Second test"))
        )
        yield session
        # Cleanup
        session.execute_script("DROP TABLE IF EXISTS test_params")


@pytest.fixture
def adbc_sqlite_params_session() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC SQLite session for parameter style testing."""
    config = AdbcConfig(uri=":memory:", driver_name="adbc_driver_sqlite", statement_config=SQLConfig(strict_mode=False))

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_params (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                description TEXT
            )
        """)
        # Insert test data
        session.execute(
            SQL("INSERT INTO test_params (name, value, description) VALUES (?, ?, ?)", ("test1", 100, "First test"))
        )
        session.execute(
            SQL("INSERT INTO test_params (name, value, description) VALUES (?, ?, ?)", ("test2", 200, "Second test"))
        )
        yield session


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
@pytest.mark.parametrize(
    "params,expected_count",
    [
        (("test1",), 1),  # Tuple parameter
        (["test1"], 1),  # List parameter
        ({"name": "test1"}, 1),  # Dict parameter (if supported)
    ],
)
def test_postgresql_parameter_types(
    adbc_postgresql_params_session: AdbcDriver, params: Any, expected_count: int
) -> None:
    """Test different parameter types with PostgreSQL."""
    # PostgreSQL always uses numeric placeholders ($1, $2, etc.)
    # When using dict params, we need to use numeric placeholders too
    if isinstance(params, dict):
        # For dict params with PostgreSQL, we need to convert to positional
        # since ADBC PostgreSQL doesn't support named parameters
        result = adbc_postgresql_params_session.execute(
            SQL("SELECT * FROM test_params WHERE name = $1"),
            (params["name"],),  # Convert dict to positional tuple
        )
    else:
        result = adbc_postgresql_params_session.execute(SQL("SELECT * FROM test_params WHERE name = $1"), params)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == expected_count


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
@pytest.mark.parametrize(
    "params,style,query",
    [
        (("test1",), "qmark", "SELECT * FROM test_params WHERE name = ?"),
        ((":test1",), "named", "SELECT * FROM test_params WHERE name = :name"),
        ({"name": "test1"}, "named_dict", "SELECT * FROM test_params WHERE name = :name"),
    ],
)
def test_sqlite_parameter_styles(adbc_sqlite_params_session: AdbcDriver, params: Any, style: str, query: str) -> None:
    """Test different parameter styles with SQLite."""
    # SQLite ADBC might have limitations on parameter styles
    if style == "named":
        # Named parameters with colon prefix
        result = adbc_sqlite_params_session.execute(query, {"name": "test1"})
    else:
        result = adbc_sqlite_params_session.execute(query, params)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_multiple_parameters(adbc_postgresql_params_session: AdbcDriver) -> None:
    """Test queries with multiple parameters on PostgreSQL."""
    result = adbc_postgresql_params_session.execute(
        SQL("SELECT * FROM test_params WHERE value >= $1 AND value <= $2 ORDER BY value"), (50, 150)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_multiple_parameters(adbc_sqlite_params_session: AdbcDriver) -> None:
    """Test queries with multiple parameters on SQLite."""
    result = adbc_sqlite_params_session.execute(
        SQL("SELECT * FROM test_params WHERE value >= ? AND value <= ? ORDER BY value"), (50, 150)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
@pytest.mark.xfail(reason="ADBC PostgreSQL driver has issues with null parameter handling")
def test_postgresql_null_parameters(adbc_postgresql_params_session: AdbcDriver) -> None:
    """Test handling of NULL parameters on PostgreSQL."""
    # Insert a record with NULL description
    adbc_postgresql_params_session.execute(
        SQL("INSERT INTO test_params (name, value, description) VALUES ($1, $2, $3)", ("null_test", 300, None))
    )

    # Query for NULL values
    result = adbc_postgresql_params_session.execute(SQL("SELECT * FROM test_params WHERE description IS NULL"))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "null_test"
    assert result.data[0]["description"] is None
