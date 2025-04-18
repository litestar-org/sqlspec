"""Test ADBC postgres driver implementation."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any, Literal

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import Adbc, AdbcDriver
from tests.fixtures.sql_utils import create_tuple_or_dict_params, format_sql

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


@pytest.fixture(autouse=True)
def cleanup_test_table(adbc_postgres_session: AdbcDriver) -> None:
    """Clean up the test table before and after each test."""
    adbc_postgres_session.execute_script("DELETE FROM test_table", None)


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_insert_update_delete_returning(adbc_postgres_session: AdbcDriver, params: Any, style: ParamStyle) -> None:
    """Test insert_update_delete_returning with different parameter styles."""
    sql_template = """
    INSERT INTO test_table (name)
    VALUES ({})
    RETURNING id, name
    """
    sql = format_sql(sql_template, ["name"], style, "postgres")

    result = adbc_postgres_session.insert_update_delete_returning(sql, params)
    assert result is not None
    assert result["name"] == "test_name"
    assert result["id"] is not None


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_select(adbc_postgres_session: AdbcDriver, params: Any, style: ParamStyle) -> None:
    """Test select functionality with different parameter styles."""
    # Insert test record
    sql_template = """
    INSERT INTO test_table (name)
    VALUES ({})
    """
    sql = format_sql(sql_template, ["name"], style, "postgres")
    adbc_postgres_session.insert_update_delete(sql, params)

    # Test select
    select_sql = "SELECT id, name FROM test_table"
    empty_params = create_tuple_or_dict_params([], [], style)
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
    # Insert test record first
    sql_template = """
    INSERT INTO test_table (name)
    VALUES ({})
    """
    sql = format_sql(sql_template, ["name"], style, "postgres")
    adbc_postgres_session.insert_update_delete(sql, params)

    # Test select_one
    sql_template = """
    SELECT id, name FROM test_table WHERE name = {}
    """
    sql = format_sql(sql_template, ["name"], style, "postgres")
    select_params = create_tuple_or_dict_params(
        [params[0] if style == "tuple_binds" else params["name"]], ["name"], style
    )
    result = adbc_postgres_session.select_one(sql, select_params)
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
    # Insert test record first
    sql_template = """
    INSERT INTO test_table (name)
    VALUES ({})
    """
    sql = format_sql(sql_template, ["name"], style, "postgres")
    adbc_postgres_session.insert_update_delete(sql, params)

    # Test select_value
    sql_template = """
    SELECT name FROM test_table WHERE name = {}
    """
    sql = format_sql(sql_template, ["name"], style, "postgres")
    select_params = create_tuple_or_dict_params(
        [params[0] if style == "tuple_binds" else params["name"]], ["name"], style
    )
    value = adbc_postgres_session.select_value(sql, select_params)
    assert value == "test_name"
