# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Integration tests for aiosqlite connection pooling."""

import os
import tempfile

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.aiosqlite.core import build_connection_config
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("sqlite")


async def test_shared_memory_pooling() -> None:
    """Test that shared memory databases allow pooling."""

    config = AiosqliteConfig(
        connection_config={
            "database": "file::memory:?cache=shared",
            "uri": True,
            "pool_min_size": 2,
            "pool_max_size": 5,
        }
    )

    try:
        async with config.provide_session() as session1:
            await session1.execute("DROP TABLE IF EXISTS shared_test")
            await session1.commit()

            await session1.execute_script("""
                CREATE TABLE shared_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                INSERT INTO shared_test (value) VALUES ('shared_data');
            """)
            await session1.commit()

        async with config.provide_session() as session2:
            result = await session2.execute("SELECT value FROM shared_test WHERE id = 1")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert len(result.data) == 1
            assert result.data[0]["value"] == "shared_data"

        async with config.provide_session() as session3:
            await session3.execute("DROP TABLE IF EXISTS shared_test")
            await session3.commit()

    finally:
        await config.close_pool()


async def test_regular_memory_auto_converted_pooling() -> None:
    """Test that regular memory databases are auto-converted and pooling works."""

    config = AiosqliteConfig(connection_config={"database": ":memory:", "pool_min_size": 5, "pool_max_size": 10})

    try:
        assert build_connection_config(config.connection_config)["database"] == "file::memory:?cache=shared"

        async with config.provide_session() as session1:
            await session1.execute("DROP TABLE IF EXISTS converted_test")
            await session1.commit()

            await session1.execute_script("""
                CREATE TABLE converted_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                INSERT INTO converted_test (value) VALUES ('converted_data');
            """)
            await session1.commit()

        async with config.provide_session() as session2:
            result = await session2.execute("SELECT value FROM converted_test WHERE id = 1")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert len(result.data) == 1
            assert result.data[0]["value"] == "converted_data"

        async with config.provide_session() as session3:
            await session3.execute("DROP TABLE IF EXISTS converted_test")
            await session3.commit()

    finally:
        await config.close_pool()


async def test_file_database_pooling_enabled() -> None:
    """Test that file-based databases allow pooling."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    config = AiosqliteConfig(connection_config={"database": db_path, "pool_min_size": 3, "pool_max_size": 8})

    try:
        async with config.provide_session() as session1:
            await session1.execute_script("""
                CREATE TABLE pool_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                INSERT INTO pool_test (value) VALUES ('test_data');
            """)
            await session1.commit()

        async with config.provide_session() as session2:
            result = await session2.execute("SELECT value FROM pool_test WHERE id = 1")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert len(result.data) == 1
            assert result.data[0]["value"] == "test_data"

    finally:
        await config.close_pool()
        try:
            os.unlink(db_path)
        except Exception:
            pass


async def test_pooling_with_core_round_3(aiosqlite_config: AiosqliteConfig) -> None:
    """Test pooling integration."""

    create_sql = SQL("""
        CREATE TABLE IF NOT EXISTS pool_core_test (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )
    """)

    insert_sql = SQL("INSERT INTO pool_core_test (data) VALUES (?)")
    select_sql = SQL("SELECT * FROM pool_core_test WHERE data = ?")

    async with aiosqlite_config.provide_session() as session1:
        create_result = await session1.execute_script(create_sql)
        assert isinstance(create_result, SQLResult)
        assert create_result.operation_type == "SCRIPT"

        insert_result = await session1.execute(insert_sql, ("pool_test_data",))
        assert isinstance(insert_result, SQLResult)
        assert insert_result.rows_affected == 1
        await session1.commit()

    async with aiosqlite_config.provide_session() as session2:
        select_result = await session2.execute(select_sql, ("pool_test_data",))
        assert isinstance(select_result, SQLResult)
        assert select_result.data is not None
        assert len(select_result.data) == 1
        assert select_result.data[0]["data"] == "pool_test_data"

        await session2.execute("DROP TABLE IF EXISTS pool_core_test")
        await session2.commit()


async def test_pool_concurrent_access(aiosqlite_config_file: AiosqliteConfig) -> None:
    """Test concurrent pool access with multiple sessions."""
    import asyncio

    async with aiosqlite_config_file.provide_session() as setup_session:
        await setup_session.execute_script("""
            CREATE TABLE IF NOT EXISTS concurrent_test (
                id INTEGER PRIMARY KEY,
                session_id TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await setup_session.commit()

    async def insert_data(session_id: str) -> None:
        """Insert data from a specific session."""
        async with aiosqlite_config_file.provide_session() as session:
            await session.execute("INSERT INTO concurrent_test (session_id) VALUES (?)", (session_id,))
            await session.commit()

    tasks = [insert_data(f"session_{i}") for i in range(5)]
    await asyncio.gather(*tasks)

    async with aiosqlite_config_file.provide_session() as verify_session:
        result = await verify_session.execute("SELECT COUNT(*) as count FROM concurrent_test")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["count"] == 5

        await verify_session.execute("DROP TABLE IF EXISTS concurrent_test")
        await verify_session.commit()
