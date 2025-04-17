"""Test DuckDB driver implementation."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any, Literal

import pytest

from sqlspec.adapters.duckdb import DuckDB, DuckDBDriver

ParamStyle = Literal["tuple", "dict"]


@pytest.fixture(scope="session")
def duckdb_session() -> Generator[DuckDBDriver, None, None]:
    """Create a DuckDB session with a test table.

    Returns:
        A configured DuckDB session with a test table.
    """
    adapter = DuckDB()
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
        id INTEGER PRIMARY KEY,
        name VARCHAR NOT NULL
    )
    """
    with adapter.provide_session() as session:
        session.execute_script(create_table_sql)
        yield session
        # Clean up
        session.execute_script("DROP TABLE IF EXISTS test_table;")


@pytest.fixture(autouse=True)
def cleanup_table(duckdb_session: DuckDBDriver) -> None:
    """Clean up the test table before each test."""
    duckdb_session.execute_script("DELETE FROM test_table;")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name", 1), "tuple", id="tuple"),
        pytest.param({"name": "test_name", "id": 1}, "dict", id="dict"),
    ],
)
def test_insert_update_delete_returning(duckdb_session: DuckDBDriver, params: Any, style: ParamStyle) -> None:
    """Test insert_update_delete_returning with different parameter styles."""
    sql = """
    INSERT INTO test_table (name, id)
    VALUES (%s)
    RETURNING id, name
    """ % ("?, ?" if style == "tuple" else ":name, :id")

    result = duckdb_session.insert_update_delete_returning(sql, params)
    assert result is not None
    assert result["name"] == "test_name"
    assert result["id"] == 1


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name", 1), "tuple", id="tuple"),
        pytest.param({"name": "test_name", "id": 1}, "dict", id="dict"),
    ],
)
def test_select(duckdb_session: DuckDBDriver, params: Any, style: ParamStyle) -> None:
    """Test select functionality with different parameter styles."""
    # Insert test record
    insert_sql = """
    INSERT INTO test_table (name, id)
    VALUES (%s)
    """ % ("?, ?" if style == "tuple" else ":name, :id")
    duckdb_session.insert_update_delete(insert_sql, params)

    # Test select
    select_sql = "SELECT id, name FROM test_table"
    empty_params: tuple[()] | dict[str, Any] = () if style == "tuple" else {}
    results = duckdb_session.select(select_sql, empty_params)
    assert len(list(results)) == 1
    assert results[0]["name"] == "test_name"


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name", 1), "tuple", id="tuple"),
        pytest.param({"name": "test_name", "id": 1}, "dict", id="dict"),
    ],
)
def test_select_one(duckdb_session: DuckDBDriver, params: Any, style: ParamStyle) -> None:
    """Test select_one functionality with different parameter styles."""
    # Insert test record
    insert_sql = """
    INSERT INTO test_table (name, id)
    VALUES (%s)
    """ % ("?, ?" if style == "tuple" else ":name, :id")
    duckdb_session.insert_update_delete(insert_sql, params)

    # Test select_one
    select_one_sql = """
    SELECT id, name FROM test_table WHERE name = %s
    """ % ("?" if style == "tuple" else ":name")
    select_params = (params[0],) if style == "tuple" else {"name": params["name"]}
    result = duckdb_session.select_one(select_one_sql, select_params)
    assert result is not None
    assert result["name"] == "test_name"


@pytest.mark.parametrize(
    ("name_params", "id_params", "style"),
    [
        pytest.param(("test_name", 1), (1,), "tuple", id="tuple"),
        pytest.param({"name": "test_name", "id": 1}, {"id": 1}, "dict", id="dict"),
    ],
)
def test_select_value(
    duckdb_session: DuckDBDriver,
    name_params: Any,
    id_params: Any,
    style: ParamStyle,
) -> None:
    """Test select_value functionality with different parameter styles."""
    # Insert test record
    insert_sql = """
    INSERT INTO test_table (name, id)
    VALUES (%s)
    """ % ("?, ?" if style == "tuple" else ":name, :id")
    duckdb_session.insert_update_delete(insert_sql, name_params)

    # Test select_value
    value_sql = """
    SELECT name FROM test_table WHERE id = %s
    """ % ("?" if style == "tuple" else ":id")
    value = duckdb_session.select_value(value_sql, id_params)
    assert value == "test_name"
