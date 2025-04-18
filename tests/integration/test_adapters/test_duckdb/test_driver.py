"""Test DuckDB driver implementation."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any, Literal

import pytest

from sqlspec.adapters.duckdb import DuckDB, DuckDBDriver
from tests.fixtures.sql_utils import create_tuple_or_dict_params, format_placeholder, format_sql

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture(scope="session")
def duckdb_session() -> Generator[DuckDBDriver, None, None]:
    """Create a DuckDB session with a test table.

    Returns:
        A DuckDB session with a test table.
    """
    adapter = DuckDB()
    with adapter.provide_session() as session:
        session.execute_script("CREATE SEQUENCE IF NOT EXISTS test_id_seq START 1", None)
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY DEFAULT nextval('test_id_seq'),
                name TEXT NOT NULL
            )
        """
        session.execute_script(create_table_sql, None)
        yield session
        # Clean up
        session.execute_script("DROP TABLE IF EXISTS test_table", None)
        session.execute_script("DROP SEQUENCE IF EXISTS test_id_seq", None)


@pytest.fixture(autouse=True)
def cleanup_table(duckdb_session: DuckDBDriver) -> None:
    """Clean up the test table before each test."""
    duckdb_session.execute_script("DELETE FROM test_table", None)


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param([("test_name", 1)], "tuple_binds", id="tuple_binds"),
        pytest.param([{"name": "test_name", "id": 1}], "dict_binds", id="dict_binds"),
    ],
)
def test_insert(duckdb_session: DuckDBDriver, params: list[Any], style: ParamStyle) -> None:
    """Test inserting data with different parameter styles."""
    # DuckDB supports multiple inserts at once
    sql_template = """
    INSERT INTO test_table (name, id)
    VALUES ({}, {})
    """
    sql = format_sql(sql_template, ["name", "id"], style, "duckdb")

    param = params[0]  # Get the first set of parameters
    duckdb_session.insert_update_delete(sql, param)

    # Verify insertion
    select_sql = "SELECT name, id FROM test_table"
    empty_params = create_tuple_or_dict_params([], [], style)
    results = duckdb_session.select(select_sql, empty_params)
    assert len(results) == 1
    assert results[0]["name"] == "test_name"
    assert results[0]["id"] == 1


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param([("test_name", 1)], "tuple_binds", id="tuple_binds"),
        pytest.param([{"name": "test_name", "id": 1}], "dict_binds", id="dict_binds"),
    ],
)
def test_select(duckdb_session: DuckDBDriver, params: list[Any], style: ParamStyle) -> None:
    """Test selecting data with different parameter styles."""
    # Insert test record
    sql_template = """
    INSERT INTO test_table (name, id)
    VALUES ({}, {})
    """
    sql = format_sql(sql_template, ["name", "id"], style, "duckdb")
    param = params[0]
    duckdb_session.insert_update_delete(sql, param)

    # Test select
    select_sql = "SELECT name, id FROM test_table"
    empty_params = create_tuple_or_dict_params([], [], style)
    results = duckdb_session.select(select_sql, empty_params)
    assert len(results) == 1
    assert results[0]["name"] == "test_name"
    assert results[0]["id"] == 1

    # Test select with a WHERE clause
    placeholder = format_placeholder("name", style, "duckdb")
    select_where_sql = f"""
    SELECT id FROM test_table WHERE name = {placeholder}
    """
    select_params = create_tuple_or_dict_params(["test_name"], ["name"], style)
    result = duckdb_session.select_one(select_where_sql, select_params)
    assert result is not None
    assert result["id"] == 1


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param([("test_name", 1)], "tuple_binds", id="tuple_binds"),
        pytest.param([{"name": "test_name", "id": 1}], "dict_binds", id="dict_binds"),
    ],
)
def test_select_value(duckdb_session: DuckDBDriver, params: list[Any], style: ParamStyle) -> None:
    """Test select_value with different parameter styles."""
    # Insert test record
    sql_template = """
    INSERT INTO test_table (name, id)
    VALUES ({}, {})
    """
    sql = format_sql(sql_template, ["name", "id"], style, "duckdb")
    param = params[0]
    duckdb_session.insert_update_delete(sql, param)

    # Test select_value
    placeholder = format_placeholder("id", style, "duckdb")
    value_sql = f"""
    SELECT name FROM test_table WHERE id = {placeholder}
    """
    value_params = create_tuple_or_dict_params([1], ["id"], style)
    value = duckdb_session.select_value(value_sql, value_params)
    assert value == "test_name"
