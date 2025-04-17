"""Test aiosqlite driver implementation."""

import pytest

from sqlspec.adapters.aiosqlite import Aiosqlite


@pytest.mark.asyncio
async def test_driver() -> None:
    """Test driver components."""
    adapter = Aiosqlite()

    # Test execute_script
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    )
    """
    async with adapter.provide_session() as session:
        await session.execute_script(create_table_sql, {})

        try:
            # Test insert_update_delete
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            RETURNING id, name
            """
            result = await session.insert_update_delete_returning(insert_sql, {"name": "test_name"})
            assert result is not None
            assert isinstance(result, dict)
            assert result["name"] == "test_name"
            assert result["id"] is not None

            # Test select
            select_sql = "SELECT id, name FROM test_table"
            results = await session.select(select_sql)
            assert len(results) == 1
            assert results[0]["name"] == "test_name"

            # Test select_one
            select_one_sql = "SELECT id, name FROM test_table WHERE name = :name"
            result = await session.select_one(select_one_sql, {"name": "test_name"})
            assert result is not None
            assert isinstance(result, dict)
            assert result["name"] == "test_name"

            # Test select_value
            value_sql = "SELECT name FROM test_table WHERE id = :id"
            value = await session.select_value(value_sql, {"id": 1})
            assert value == "test_name"

        finally:
            # Clean up
            await session.execute_script("DROP TABLE IF EXISTS test_table", {})
