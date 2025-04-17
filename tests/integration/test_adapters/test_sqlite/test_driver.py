"""Test SQLite driver implementation."""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from typing import Any, Literal

import pytest

from sqlspec.adapters.sqlite import Sqlite, SqliteDriver

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture(scope="session")
def sqlite_session() -> Generator[SqliteDriver, None, None]:
    """Create a SQLite session with a test table.

    Returns:
        A configured SQLite session with a test table.
    """
    adapter = Sqlite()
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    )
    """
    with adapter.provide_session() as session:
        session.execute_script(create_table_sql, None)
        yield session
        # Clean up
        session.execute_script("DROP TABLE IF EXISTS test_table", None)


@pytest.fixture(autouse=True)
def cleanup_table(sqlite_session: SqliteDriver) -> None:
    """Clean up the test table before each test."""
    sqlite_session.execute_script("DELETE FROM test_table", None)


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_insert_update_delete_returning(sqlite_session: SqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test insert_update_delete_returning with different parameter styles."""
    # Check SQLite version for RETURNING support (3.35.0+)
    sqlite_version = sqlite3.sqlite_version_info
    returning_supported = sqlite_version >= (3, 35, 0)

    if returning_supported:
        sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        RETURNING id, name
        """ % ("?" if style == "tuple_binds" else ":name")

        result = sqlite_session.insert_update_delete_returning(sql, params)
        assert result is not None
        assert result["name"] == "test_name"
        assert result["id"] is not None
    else:
        # Alternative for older SQLite: Insert and then get last row id
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        """ % ("?" if style == "tuple_binds" else ":name")

        sqlite_session.insert_update_delete(insert_sql, params)

        # Get the last inserted ID using select_value
        select_last_id_sql = "SELECT last_insert_rowid()"
        inserted_id = sqlite_session.select_value(select_last_id_sql)
        assert inserted_id is not None


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_select(sqlite_session: SqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test select functionality with different parameter styles."""
    # Insert test record
    insert_sql = """
    INSERT INTO test_table (name)
    VALUES (%s)
    """ % ("?" if style == "tuple_binds" else ":name")
    sqlite_session.insert_update_delete(insert_sql, params)

    # Test select
    select_sql = "SELECT id, name FROM test_table"
    empty_params: tuple[()] | dict[str, Any] = () if style == "tuple_binds" else {}
    results = sqlite_session.select(select_sql, empty_params)
    assert len(results) == 1
    assert results[0]["name"] == "test_name"


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_select_one(sqlite_session: SqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test select_one functionality with different parameter styles."""
    # Insert test record
    insert_sql = """
    INSERT INTO test_table (name)
    VALUES (%s)
    """ % ("?" if style == "tuple_binds" else ":name")
    sqlite_session.insert_update_delete(insert_sql, params)

    # Test select_one
    select_one_sql = """
    SELECT id, name FROM test_table WHERE name = %s
    """ % ("?" if style == "tuple_binds" else ":name")
    select_params = (params[0],) if style == "tuple_binds" else {"name": params["name"]}
    result = sqlite_session.select_one(select_one_sql, select_params)
    assert result is not None
    assert result["name"] == "test_name"


@pytest.mark.parametrize(
    ("name_params", "id_params", "style"),
    [
        pytest.param(("test_name",), (1,), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, {"id": 1}, "dict_binds", id="dict_binds"),
    ],
)
def test_select_value(
    sqlite_session: SqliteDriver,
    name_params: Any,
    id_params: Any,
    style: ParamStyle,
) -> None:
    """Test select_value functionality with different parameter styles."""
    # Insert test record and get the ID
    insert_sql = """
    INSERT INTO test_table (name)
    VALUES (%s)
    """ % ("?" if style == "tuple_binds" else ":name")
    sqlite_session.insert_update_delete(insert_sql, name_params)

    # Get the last inserted ID
    select_last_id_sql = "SELECT last_insert_rowid()"
    inserted_id = sqlite_session.select_value(select_last_id_sql)
    assert inserted_id is not None

    # Test select_value with the actual inserted ID
    value_sql = """
    SELECT name FROM test_table WHERE id = %s
    """ % ("?" if style == "tuple_binds" else ":id")
    test_id_params = (inserted_id,) if style == "tuple_binds" else {"id": inserted_id}
    value = sqlite_session.select_value(value_sql, test_id_params)
    assert value == "test_name"
