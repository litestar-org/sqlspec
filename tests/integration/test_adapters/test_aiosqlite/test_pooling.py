"""Integration tests for aiosqlite connection pooling."""

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig


@pytest.mark.xdist_group("aiosqlite")
async def test_shared_memory_pooling() -> None:
    """Test that shared memory databases allow pooling."""
    # Create config with shared memory database
    config = AiosqliteConfig(
        pool_config={"database": "file::memory:?cache=shared", "uri": True, "pool_min_size": 2, "pool_max_size": 5}
    )

    # Verify pooling is not disabled
    assert config.min_pool == 2
    assert config.max_pool == 5

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
        await session1.commit()  # Commit to release locks

    # Get data from another session in the pool
    async with config.provide_session() as session2:
        result = await session2.execute("SELECT value FROM shared_test WHERE id = 1")
        data = result.get_data()
        assert len(data) == 1
        assert data[0]["value"] == "shared_data"

    # Cleanup
    async with config.provide_session() as session3:
        await session3.execute("DROP TABLE IF EXISTS shared_test")
        await session3.commit()


@pytest.mark.xdist_group("aiosqlite")
async def test_regular_memory_auto_converted_pooling() -> None:
    """Test that regular memory databases are auto-converted and pooling works."""
    # Create config with regular memory database
    config = AiosqliteConfig(pool_config={"database": ":memory:", "pool_min_size": 5, "pool_max_size": 10})

    # Verify pooling is enabled (no longer forced to 1)
    assert config.min_pool == 5
    assert config.max_pool == 10

    # Verify auto-conversion happened
    assert config.connection_config["database"] == "file::memory:?cache=shared"
    assert config.connection_config["uri"] is True

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
        data = result.get_data()
        assert len(data) == 1
        assert data[0]["value"] == "converted_data"

    # Cleanup
    async with config.provide_session() as session3:
        await session3.execute("DROP TABLE IF EXISTS converted_test")
        await session3.commit()


@pytest.mark.xdist_group("aiosqlite")
async def test_file_database_pooling_enabled() -> None:
    """Test that file-based databases allow pooling."""
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        # Create config with file database
        config = AiosqliteConfig(connection_config={"database": db_path}, min_pool=3, max_pool=8)

        # Verify pooling is enabled
        assert config.min_pool == 3
        assert config.max_pool == 8

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
            data = result.get_data()
            assert len(data) == 1
            assert data[0]["value"] == "test_data"
    finally:
        import os

        try:
            os.unlink(db_path)
        except Exception:
            pass
