# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Integration tests for aiosqlite connection pooling."""

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.aiosqlite.core import build_connection_config
from sqlspec.core import SQLResult

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
            assert result.get_data()[0]["value"] == "shared_data"

        async with config.provide_session() as session3:
            await session3.execute("DROP TABLE IF EXISTS shared_test")
            await session3.commit()

    finally:
        await config.close_pool()


async def test_regular_memory_auto_converted_pooling() -> None:
    """Test that regular memory databases are auto-converted and pooling works."""

    config = AiosqliteConfig(connection_config={"database": ":memory:", "pool_min_size": 5, "pool_max_size": 10})

    try:
        db_uri = build_connection_config(config.connection_config)["database"]
        assert db_uri.startswith("file:memory_") and "mode=memory" in db_uri and "cache=shared" in db_uri

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
            assert result.get_data()[0]["value"] == "converted_data"

        async with config.provide_session() as session3:
            await session3.execute("DROP TABLE IF EXISTS converted_test")
            await session3.commit()

    finally:
        await config.close_pool()


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
        assert result.get_data()[0]["count"] == 5

        await verify_session.execute("DROP TABLE IF EXISTS concurrent_test")
        await verify_session.commit()


async def test_sequential_configs_isolated_databases() -> None:
    """Regression test for #360: sequential configs must not share state.

    Two AiosqliteConfig instances created with default settings must have
    completely isolated in-memory databases.
    """
    config1 = AiosqliteConfig()
    config2 = AiosqliteConfig()
    try:
        # Create a table in config1's database
        async with config1.provide_session() as session1:
            await session1.execute_script("""
                CREATE TABLE isolation_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                )
            """)
            await session1.commit()

        # config2 must NOT see the table from config1
        async with config2.provide_session() as session2:
            result = await session2.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='isolation_test'"
            )
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert len(result.data) == 0, (
                "Table 'isolation_test' from config1 leaked into config2's database. See issue #360."
            )
    finally:
        await config1.close_pool()
        await config2.close_pool()


async def test_same_config_pool_shares_database() -> None:
    """Verify that connections within the same config share the database.

    This ensures the fix for #360 doesn't break legitimate pooling.
    """
    config = AiosqliteConfig()
    try:
        async with config.provide_session() as session1:
            await session1.execute_script("""
                CREATE TABLE pool_share_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                INSERT INTO pool_share_test (value) VALUES ('shared');
            """)
            await session1.commit()

        # A second session from the SAME config must see the table
        async with config.provide_session() as session2:
            result = await session2.execute("SELECT value FROM pool_share_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert len(result.data) == 1
            assert result.get_data()[0]["value"] == "shared"

            await session2.execute("DROP TABLE pool_share_test")
            await session2.commit()
    finally:
        await config.close_pool()
