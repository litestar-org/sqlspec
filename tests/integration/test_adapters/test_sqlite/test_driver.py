"""Test SQLite driver implementation."""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from typing import Any, Literal

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.sql.result import ExecuteResult, SelectResult

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture(scope="session")
def sqlite_session() -> Generator[SqliteDriver, None, None]:
    """Create a SQLite session with a test table.

    Returns:
        A configured SQLite session with a test table.
    """
    adapter = SqliteConfig()
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    )
    """
    with adapter.provide_session() as session:
        session.execute_script(create_table_sql)
        yield session
        # Clean up
        session.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.fixture(autouse=True)
def cleanup_table(sqlite_session: SqliteDriver) -> None:
    """Clean up the test table before each test."""
    sqlite_session.execute_script("DELETE FROM test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("sqlite")
def test_insert_returning(sqlite_session: SqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test insert functionality with different parameter styles."""
    # Check SQLite version for RETURNING support (3.35.0+)
    sqlite_version = sqlite3.sqlite_version_info
    returning_supported = sqlite_version >= (3, 35, 0)

    if returning_supported:
        if style == "tuple_binds":
            sql = "INSERT INTO test_table (name) VALUES (?) RETURNING id, name"
        else:
            sql = "INSERT INTO test_table (name) VALUES (:name) RETURNING id, name"

        result = sqlite_session.execute(sql, params)
        assert isinstance(result, SelectResult)  # RETURNING makes this a SELECT result
        assert result.rows is not None
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "test_name"
        assert result.rows[0]["id"] is not None
    else:
        # Alternative for older SQLite: Insert and then get last row id
        if style == "tuple_binds":
            insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        else:
            insert_sql = "INSERT INTO test_table (name) VALUES (:name)"

        result = sqlite_session.execute(insert_sql, params)
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == 1
        assert result.last_inserted_id is not None


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("sqlite")
def test_select(sqlite_session: SqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test select functionality with different parameter styles."""
    # Insert test record first
    if style == "tuple_binds":
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    else:
        insert_sql = "INSERT INTO test_table (name) VALUES (:name)"

    insert_result = sqlite_session.execute(insert_sql, params)
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # Test select
    select_sql = "SELECT id, name FROM test_table"
    select_result = sqlite_session.execute(select_sql)
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert len(select_result.rows) == 1
    assert select_result.rows[0]["name"] == "test_name"


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("sqlite")
def test_select_one(sqlite_session: SqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test select one functionality with different parameter styles."""
    # Insert test record first
    if style == "tuple_binds":
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    else:
        insert_sql = "INSERT INTO test_table (name) VALUES (:name)"

    insert_result = sqlite_session.execute(insert_sql, params)
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # Test select with WHERE condition
    if style == "tuple_binds":
        select_sql = "SELECT id, name FROM test_table WHERE name = ?"
    else:
        select_sql = "SELECT id, name FROM test_table WHERE name = :name"

    select_result = sqlite_session.execute(select_sql, params)
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert len(select_result.rows) == 1
    assert select_result.rows[0]["name"] == "test_name"

    # Test the first row helper method
    first_row = select_result.get_first()
    assert first_row is not None
    assert first_row["name"] == "test_name"


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("sqlite")
def test_select_value(sqlite_session: SqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test select value functionality with different parameter styles."""
    # Insert test record first
    if style == "tuple_binds":
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    else:
        insert_sql = "INSERT INTO test_table (name) VALUES (:name)"

    insert_result = sqlite_session.execute(insert_sql, params)
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1
    inserted_id = insert_result.last_inserted_id

    # Test select single value by ID
    if style == "tuple_binds":
        value_sql = "SELECT name FROM test_table WHERE id = ?"
        value_params = (inserted_id,)
    else:
        value_sql = "SELECT name FROM test_table WHERE id = :id"
        value_params = {"id": inserted_id}

    value_result = sqlite_session.execute(value_sql, value_params)
    assert isinstance(value_result, SelectResult)
    assert value_result.rows is not None
    assert len(value_result.rows) == 1
    assert value_result.column_names is not None

    # Extract single value using column name
    value = value_result.rows[0][value_result.column_names[0]]
    assert value == "test_name"


@pytest.mark.xdist_group("sqlite")
def test_execute_many_insert(sqlite_session: SqliteDriver) -> None:
    """Test execute_many functionality for batch inserts."""
    insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    params_list = [("name1",), ("name2",), ("name3",)]

    result = sqlite_session.execute_many(insert_sql, params_list)
    assert isinstance(result, ExecuteResult)
    assert result.rows_affected == len(params_list)

    # Verify all records were inserted
    select_result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert select_result.rows[0]["count"] == len(params_list)


@pytest.mark.xdist_group("sqlite")
def test_execute_script(sqlite_session: SqliteDriver) -> None:
    """Test execute_script functionality for multi-statement scripts."""
    script = """
    INSERT INTO test_table (name) VALUES ('script_name1');
    INSERT INTO test_table (name) VALUES ('script_name2');
    """

    result = sqlite_session.execute_script(script)
    assert isinstance(result, str)
    assert result == "SCRIPT EXECUTED"

    # Verify script executed successfully
    select_result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert select_result.rows[0]["count"] == 2


@pytest.mark.xdist_group("sqlite")
def test_update_operation(sqlite_session: SqliteDriver) -> None:
    """Test UPDATE operations."""
    # Insert a record first
    insert_result = sqlite_session.execute("INSERT INTO test_table (name) VALUES (?)", ("original_name",))
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1
    inserted_id = insert_result.last_inserted_id

    # Update the record
    update_result = sqlite_session.execute("UPDATE test_table SET name = ? WHERE id = ?", ("updated_name", inserted_id))
    assert isinstance(update_result, ExecuteResult)
    assert update_result.rows_affected == 1

    # Verify the update
    select_result = sqlite_session.execute("SELECT name FROM test_table WHERE id = ?", (inserted_id,))
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert select_result.rows[0]["name"] == "updated_name"


@pytest.mark.xdist_group("sqlite")
def test_delete_operation(sqlite_session: SqliteDriver) -> None:
    """Test DELETE operations."""
    # Insert a record first
    insert_result = sqlite_session.execute("INSERT INTO test_table (name) VALUES (?)", ("to_delete",))
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1
    inserted_id = insert_result.last_inserted_id

    # Delete the record
    delete_result = sqlite_session.execute("DELETE FROM test_table WHERE id = ?", (inserted_id,))
    assert isinstance(delete_result, ExecuteResult)
    assert delete_result.rows_affected == 1

    # Verify the deletion
    select_result = sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert select_result.rows[0]["count"] == 0
