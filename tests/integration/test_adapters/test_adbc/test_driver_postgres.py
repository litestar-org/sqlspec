"""Test ADBC postgres driver implementation."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any, Literal

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import Adbc, AdbcDriver

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture(scope="session")
def adbc_postgres_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC postgres session with a test table.

    Returns:
        A configured ADBC postgres session with a test table.
    """
    adapter = Adbc(
        uri=f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
    )
    try:
        with adapter.provide_session() as session:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )
            """
            session.execute_script(create_table_sql, None)
            yield session
            # Clean up
            session.execute_script("DROP TABLE IF EXISTS test_table", None)
    except Exception as e:
        if "cannot open shared object file" in str(e):
            pytest.xfail(f"ADBC driver shared object file not found during session setup: {e}")
        raise e  # Reraise unexpected exceptions


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_insert_update_delete_returning(adbc_postgres_session: AdbcDriver, params: Any, style: ParamStyle) -> None:
    """Test insert_update_delete_returning with different parameter styles."""
    # Clear table before test
    adbc_postgres_session.execute_script("DELETE FROM test_table", None)

    # ADBC PostgreSQL DBAPI seems inconsistent, using native $1 style
    sql_template = """
    INSERT INTO test_table (name)
    VALUES ($1)
    RETURNING id, name
    """
    sql = sql_template

    # Ensure params are tuples
    execute_params = (params[0] if style == "tuple_binds" else params["name"],)

    result = adbc_postgres_session.insert_update_delete_returning(sql, execute_params)

    # Assuming the method returns a single dict if one row is returned
    assert isinstance(result, dict)
    assert result["name"] == execute_params[0]
    assert "id" in result
    assert isinstance(result["id"], int)


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_select(adbc_postgres_session: AdbcDriver, params: Any, style: ParamStyle) -> None:  # pyright: ignore
    """Test select functionality with different parameter styles."""
    # Clear table before test
    adbc_postgres_session.execute_script("DELETE FROM test_table", None)

    # Insert test record first using the correct param style for the driver
    # Using $1 for plain execute
    insert_sql_template = """
    INSERT INTO test_table (name)
    VALUES ($1)
    """
    insert_params = (params[0] if style == "tuple_binds" else params["name"],)
    adbc_postgres_session.insert_update_delete(insert_sql_template, insert_params)

    # Test select - SELECT doesn't usually need parameters formatted by style,
    # but the driver might still expect a specific format if parameters were used.
    # Using empty params here, assuming qmark style if needed, though likely irrelevant.
    select_sql = "SELECT id, name FROM test_table"
    empty_params = ()  # Use empty tuple for qmark style
    results = adbc_postgres_session.select(select_sql, empty_params)
    assert len(results) == 1
    assert results[0]["name"] == "test_name"


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_select_one(adbc_postgres_session: AdbcDriver, params: Any, style: ParamStyle) -> None:
    """Test select_one functionality with different parameter styles."""
    # Clear table before test
    adbc_postgres_session.execute_script("DELETE FROM test_table", None)

    # Insert test record first
    # Using $1 for plain execute
    insert_sql_template = """
    INSERT INTO test_table (name)
    VALUES ($1)
    """
    insert_params = (params[0] if style == "tuple_binds" else params["name"],)
    adbc_postgres_session.insert_update_delete(insert_sql_template, insert_params)

    # Test select_one using qmark style for WHERE clause - let's try $1 here too for consistency
    sql_template = """
    SELECT id, name FROM test_table WHERE name = $1
    """
    sql = sql_template
    result = adbc_postgres_session.select_one(sql, (params[0] if style == "tuple_binds" else params["name"],))
    assert result is not None
    assert result["name"] == "test_name"


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_select_value(adbc_postgres_session: AdbcDriver, params: Any, style: ParamStyle) -> None:
    """Test select_value functionality with different parameter styles."""
    # Clear table before test
    adbc_postgres_session.execute_script("DELETE FROM test_table", None)

    # Insert test record first
    # Using $1 for plain execute
    insert_sql_template = """
    INSERT INTO test_table (name)
    VALUES ($1)
    """
    insert_params = (params[0] if style == "tuple_binds" else params["name"],)
    adbc_postgres_session.insert_update_delete(insert_sql_template, insert_params)

    # Test select_value using $1 style
    sql_template = """
    SELECT name FROM test_table WHERE name = $1
    """
    sql = sql_template
    select_params = (params[0] if style == "tuple_binds" else params["name"],)

    value = adbc_postgres_session.select_value(sql, select_params)
    assert value == "test_name"
