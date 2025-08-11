"""Integration tests for aiosqlite connection pooling."""

from __future__ import annotations

import os
import tempfile

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.core.result import SQLResult


@pytest.mark.xdist_group("aiosqlite")
async def test_shared_memory_pooling() -> None:
    """Test that shared memory databases allow pooling."""
    # Create config with shared memory database
    config = AiosqliteConfig(
        pool_config={"database": "file::memory:?cache=shared", "uri": True, "pool_min_size": 2, "pool_max_size": 5}
    )

    try:
        # Test that multiple connections can access the same data
        async with config.provide_session() as session1:
            # Drop table if it exists from previous run
            await session1.execute("DROP TABLE IF EXISTS shared_test")
            await session1.commit()

            # Create table in first session
            await session1.execute_script("""
                CREATE TABLE shared_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                INSERT INTO shared_test (value) VALUES ('shared_data');
            """)
            await session1.commit()

        # Get data from another session in the pool
        async with config.provide_session() as session2:
            result = await session2.execute("SELECT value FROM shared_test WHERE id = 1")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert len(result.data) == 1
            assert result.data[0]["value"] == "shared_data"

        # Cleanup
        async with config.provide_session() as session3:
            await session3.execute("DROP TABLE IF EXISTS shared_test")
            await session3.commit()

    finally:
        await config.close_pool()


@pytest.mark.xdist_group("aiosqlite")
async def test_regular_memory_auto_converted_pooling() -> None:
    """Test that regular memory databases are auto-converted and pooling works."""
    # Create config with regular memory database
    config = AiosqliteConfig(pool_config={"database": ":memory:", "pool_min_size": 5, "pool_max_size": 10})

    try:
        # Verify database was auto-converted to shared cache format for pooling
        assert config._get_connection_config_dict()["database"] == "file::memory:?cache=shared"  # pyright: ignore[reportAttributeAccessIssue]

        # Test that multiple connections can access the same data (like shared memory test)
        async with config.provide_session() as session1:
            # Drop table if it exists from previous run
            await session1.execute("DROP TABLE IF EXISTS converted_test")
            await session1.commit()

            # Create table in first session
            await session1.execute_script("""
                CREATE TABLE converted_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                INSERT INTO converted_test (value) VALUES ('converted_data');
            """)
            await session1.commit()  # Commit to release locks

        # Get data from another session in the pool
        async with config.provide_session() as session2:
            result = await session2.execute("SELECT value FROM converted_test WHERE id = 1")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert len(result.data) == 1
            assert result.data[0]["value"] == "converted_data"

        # Cleanup
        async with config.provide_session() as session3:
            await session3.execute("DROP TABLE IF EXISTS converted_test")
            await session3.commit()

    finally:
        await config.close_pool()


@pytest.mark.xdist_group("aiosqlite")
async def test_file_database_pooling_enabled() -> None:
    """Test that file-based databases allow pooling."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    config = AiosqliteConfig(pool_config={"database": db_path, "pool_min_size": 3, "pool_max_size": 8})

    try:
        # Test that multiple connections work
        async with config.provide_session() as session1:
            await session1.execute_script("""
                CREATE TABLE pool_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                INSERT INTO pool_test (value) VALUES ('test_data');
            """)
            await session1.commit()  # Commit to persist data

        # Data persists across connections
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


@pytest.mark.xdist_group("aiosqlite")
async def test_pooling_with_core_round_3(aiosqlite_config: AiosqliteConfig) -> None:
    """Test pooling integration."""
    from sqlspec.core.statement import SQL

    # Create SQL object
    create_sql = SQL("""
        CREATE TABLE IF NOT EXISTS pool_core_test (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )
    """)

    insert_sql = SQL("INSERT INTO pool_core_test (data) VALUES (?)")
    select_sql = SQL("SELECT * FROM pool_core_test WHERE data = ?")

    # Test pooling with SQL objects
    async with aiosqlite_config.provide_session() as session1:
        # Create table using SQL object
        create_result = await session1.execute_script(create_sql)
        assert isinstance(create_result, SQLResult)
        assert create_result.operation_type == "SCRIPT"

        # Insert data
        insert_result = await session1.execute(insert_sql, ("pool_test_data",))
        assert isinstance(insert_result, SQLResult)
        assert insert_result.rows_affected == 1
        await session1.commit()

    # Access from different session
    async with aiosqlite_config.provide_session() as session2:
        select_result = await session2.execute(select_sql, ("pool_test_data",))
        assert isinstance(select_result, SQLResult)
        assert select_result.data is not None
        assert len(select_result.data) == 1
        assert select_result.data[0]["data"] == "pool_test_data"

        # Clean up
        await session2.execute("DROP TABLE IF EXISTS pool_core_test")
        await session2.commit()


@pytest.mark.xdist_group("aiosqlite")
async def test_pool_concurrent_access(aiosqlite_config: AiosqliteConfig) -> None:
    """Test concurrent pool access with multiple sessions."""
    import asyncio

    # Prepare test table
    async with aiosqlite_config.provide_session() as setup_session:
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
        async with aiosqlite_config.provide_session() as session:
            await session.execute("INSERT INTO concurrent_test (session_id) VALUES (?)", (session_id,))
            await session.commit()

    # Run concurrent operations
    tasks = [insert_data(f"session_{i}") for i in range(5)]
    await asyncio.gather(*tasks)

    # Verify all data was inserted
    async with aiosqlite_config.provide_session() as verify_session:
        result = await verify_session.execute("SELECT COUNT(*) as count FROM concurrent_test")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["count"] == 5

        # Clean up
        await verify_session.execute("DROP TABLE IF EXISTS concurrent_test")
        await verify_session.commit()
