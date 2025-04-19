"""Test AioSQLite driver implementation."""

from __future__ import annotations

import sqlite3
from collections.abc import AsyncGenerator
from typing import Any, Literal

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from tests.fixtures.sql_utils import create_tuple_or_dict_params, format_sql

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture
async def aiosqlite_session() -> AsyncGenerator[AiosqliteDriver, None]:
    """Create a SQLite session with a test table.

    Returns:
        A configured SQLite session with a test table.
    """
    adapter = AiosqliteConfig()
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    )
    """
    async with adapter.provide_session() as session:
        await session.execute_script(create_table_sql, None)
        yield session
        # Clean up
        await session.execute_script("DROP TABLE IF EXISTS test_table", None)


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("sqlite")
@pytest.mark.asyncio
async def test_insert_update_delete_returning(
    aiosqlite_session: AiosqliteDriver, params: Any, style: ParamStyle
) -> None:
    """Test insert_update_delete_returning with different parameter styles."""
    # Check SQLite version for RETURNING support (3.35.0+)
    sqlite_version = sqlite3.sqlite_version_info
    returning_supported = sqlite_version >= (3, 35, 0)

    if returning_supported:
        sql_template = """
        INSERT INTO test_table (name)
        VALUES ({})
        RETURNING id, name
        """
        sql = format_sql(sql_template, ["name"], style, "aiosqlite")

        result = await aiosqlite_session.insert_update_delete_returning(sql, params)
        assert result is not None
        assert result["name"] == "test_name"
        assert result["id"] is not None
        await aiosqlite_session.execute_script("DELETE FROM test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("sqlite")
@pytest.mark.asyncio
async def test_select(aiosqlite_session: AiosqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test select functionality with different parameter styles."""
    # Insert test record
    sql_template = """
    INSERT INTO test_table (name)
    VALUES ({})
    """
    sql = format_sql(sql_template, ["name"], style, "aiosqlite")
    await aiosqlite_session.insert_update_delete(sql, params)

    # Test select
    select_sql = "SELECT id, name FROM test_table"
    empty_params = create_tuple_or_dict_params([], [], style)
    results = await aiosqlite_session.select(select_sql, empty_params)
    assert len(results) == 1
    assert results[0]["name"] == "test_name"
    await aiosqlite_session.execute_script("DELETE FROM test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("sqlite")
@pytest.mark.asyncio
async def test_select_one(aiosqlite_session: AiosqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test select_one functionality with different parameter styles."""
    # Insert test record
    sql_template = """
    INSERT INTO test_table (name)
    VALUES ({})
    """
    sql = format_sql(sql_template, ["name"], style, "aiosqlite")
    await aiosqlite_session.insert_update_delete(sql, params)

    # Test select_one
    sql_template = """
    SELECT id, name FROM test_table WHERE name = {}
    """
    sql = format_sql(sql_template, ["name"], style, "aiosqlite")
    select_params = create_tuple_or_dict_params(
        [params[0] if style == "tuple_binds" else params["name"]], ["name"], style
    )
    result = await aiosqlite_session.select_one(sql, select_params)
    assert result is not None
    assert result["name"] == "test_name"
    await aiosqlite_session.execute_script("DELETE FROM test_table")


@pytest.mark.parametrize(
    ("name_params", "id_params", "style"),
    [
        pytest.param(("test_name",), (1,), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, {"id": 1}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("sqlite")
@pytest.mark.asyncio
async def test_select_value(
    aiosqlite_session: AiosqliteDriver,
    name_params: Any,
    id_params: Any,
    style: ParamStyle,
) -> None:
    """Test select_value functionality with different parameter styles."""
    # Insert test record and get the ID
    sql_template = """
    INSERT INTO test_table (name)
    VALUES ({})
    """
    sql = format_sql(sql_template, ["name"], style, "aiosqlite")
    await aiosqlite_session.insert_update_delete(sql, name_params)

    # Get the last inserted ID
    select_last_id_sql = "SELECT last_insert_rowid()"
    inserted_id = await aiosqlite_session.select_value(select_last_id_sql)
    assert inserted_id is not None

    # Test select_value with the actual inserted ID
    sql_template = """
    SELECT name FROM test_table WHERE id = {}
    """
    sql = format_sql(sql_template, ["id"], style, "aiosqlite")
    test_id_params = create_tuple_or_dict_params([inserted_id], ["id"], style)
    value = await aiosqlite_session.select_value(sql, test_id_params)
    assert value == "test_name"
    await aiosqlite_session.execute_script("DELETE FROM test_table")
