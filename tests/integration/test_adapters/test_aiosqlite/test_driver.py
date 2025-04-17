"""Test aiosqlite driver implementation."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, Literal

import pytest

from sqlspec.adapters.aiosqlite import Aiosqlite, AiosqliteDriver

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture(scope="session")
async def aiosqlite_session() -> AsyncGenerator[AiosqliteDriver, None]:
    """Create an aiosqlite session with a test table.

    Returns:
        A configured aiosqlite session with a test table.
    """
    adapter = Aiosqlite()
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    )
    """
    async with adapter.provide_session() as session:
        await session.execute_script(create_table_sql, {})
        yield session
        # Clean up
        await session.execute_script("DROP TABLE IF EXISTS test_table", {})


@pytest.fixture(autouse=True)
async def cleanup_table(aiosqlite_session: AiosqliteDriver) -> None:
    """Clean up the test table before each test."""
    await aiosqlite_session.execute_script("DELETE FROM test_table", {})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
async def test_insert_update_delete_returning(
    aiosqlite_session: AiosqliteDriver, params: Any, style: ParamStyle
) -> None:
    """Test insert_update_delete_returning with different parameter styles."""
    sql = """
    INSERT INTO test_table (name)
    VALUES (%s)
    RETURNING id, name
    """ % ("?" if style == "tuple_binds" else ":name")

    result = await aiosqlite_session.insert_update_delete_returning(sql, params)
    assert result is not None
    assert result["name"] == "test_name"
    assert result["id"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
async def test_select(aiosqlite_session: AiosqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test select functionality with different parameter styles."""
    # Insert test record
    insert_sql = """
    INSERT INTO test_table (name)
    VALUES (%s)
    """ % ("?" if style == "tuple_binds" else ":name")
    await aiosqlite_session.insert_update_delete(insert_sql, params)

    # Test select
    select_sql = "SELECT id, name FROM test_table"
    empty_params: tuple[()] | dict[str, Any] = () if style == "tuple_binds" else {}
    results = await aiosqlite_session.select(select_sql, empty_params)
    assert len(results) == 1
    assert results[0]["name"] == "test_name"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
async def test_select_one(aiosqlite_session: AiosqliteDriver, params: Any, style: ParamStyle) -> None:
    """Test select_one functionality with different parameter styles."""
    # Insert test record
    insert_sql = """
    INSERT INTO test_table (name)
    VALUES (%s)
    """ % ("?" if style == "tuple_binds" else ":name")
    await aiosqlite_session.insert_update_delete(insert_sql, params)

    # Test select_one
    select_one_sql = """
    SELECT id, name FROM test_table WHERE name = %s
    """ % ("?" if style == "tuple_binds" else ":name")
    select_params = (params[0],) if style == "tuple_binds" else {"name": params["name"]}
    result = await aiosqlite_session.select_one(select_one_sql, select_params)
    assert result is not None
    assert result["name"] == "test_name"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name_params", "id_params", "style"),
    [
        pytest.param(("test_name",), (1,), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, {"id": 1}, "dict_binds", id="dict_binds"),
    ],
)
async def test_select_value(
    aiosqlite_session: AiosqliteDriver,
    name_params: Any,
    id_params: Any,
    style: ParamStyle,
) -> None:
    """Test select_value functionality with different parameter styles."""
    # Insert test record and get the ID
    insert_sql = """
    INSERT INTO test_table (name)
    VALUES (%s)
    RETURNING id
    """ % ("?" if style == "tuple_binds" else ":name")
    result = await aiosqlite_session.insert_update_delete_returning(insert_sql, name_params)
    assert result is not None
    inserted_id = result["id"]

    # Test select_value with the actual inserted ID
    value_sql = """
    SELECT name FROM test_table WHERE id = %s
    """ % ("?" if style == "tuple_binds" else ":id")
    test_id_params = (inserted_id,) if style == "tuple_binds" else {"id": inserted_id}
    value = await aiosqlite_session.select_value(value_sql, test_id_params)
    assert value == "test_name"
