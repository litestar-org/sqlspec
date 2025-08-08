"""Test AIOSQLite connection functionality."""

from __future__ import annotations

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.core.result import SQLResult


@pytest.mark.xdist_group("aiosqlite")
async def test_basic_connection(aiosqlite_config: AiosqliteConfig) -> None:
    """Test basic connection establishment."""
    async with aiosqlite_config.provide_session() as driver:
        assert isinstance(driver, AiosqliteDriver)
        assert driver.connection is not None

        # Test simple query
        result = await driver.execute("SELECT 1 as test_value")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["test_value"] == 1


@pytest.mark.xdist_group("aiosqlite")
async def test_connection_reuse(aiosqlite_config: AiosqliteConfig) -> None:
    """Test connection reuse in pool."""
    # First connection
    async with aiosqlite_config.provide_session() as driver1:
        await driver1.execute("CREATE TABLE IF NOT EXISTS reuse_test (id INTEGER, data TEXT)")
        await driver1.execute("INSERT INTO reuse_test VALUES (1, 'test_data')")
        await driver1.commit()

    # Second connection should see the data
    async with aiosqlite_config.provide_session() as driver2:
        result = await driver2.execute("SELECT data FROM reuse_test WHERE id = 1")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["data"] == "test_data"

        # Clean up
        await driver2.execute("DROP TABLE IF EXISTS reuse_test")
        await driver2.commit()


@pytest.mark.xdist_group("aiosqlite")
async def test_connection_error_handling(aiosqlite_config: AiosqliteConfig) -> None:
    """Test connection error handling."""
    async with aiosqlite_config.provide_session() as driver:
        # Test invalid SQL
        with pytest.raises(Exception):
            await driver.execute("INVALID SQL SYNTAX")

        # Connection should still be usable
        result = await driver.execute("SELECT 'still_working' as status")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["status"] == "still_working"


@pytest.mark.xdist_group("aiosqlite")
async def test_connection_with_transactions(aiosqlite_config: AiosqliteConfig) -> None:
    """Test connection behavior with transactions."""
    async with aiosqlite_config.provide_session() as driver:
        # Create test table
        await driver.execute_script("""
            CREATE TABLE IF NOT EXISTS transaction_test (
                id INTEGER PRIMARY KEY,
                value TEXT
            )
        """)

        # Test explicit transaction
        await driver.execute("BEGIN TRANSACTION")
        await driver.execute("INSERT INTO transaction_test (value) VALUES ('tx_test')")
        await driver.execute("COMMIT")

        # Verify data was committed
        result = await driver.execute("SELECT COUNT(*) as count FROM transaction_test")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["count"] == 1

        # Test rollback
        await driver.execute("BEGIN TRANSACTION")
        await driver.execute("INSERT INTO transaction_test (value) VALUES ('rollback_test')")
        await driver.execute("ROLLBACK")

        # Should still have only one record
        result = await driver.execute("SELECT COUNT(*) as count FROM transaction_test")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["count"] == 1

        # Clean up
        await driver.execute("DROP TABLE IF EXISTS transaction_test")
        await driver.commit()


@pytest.mark.xdist_group("aiosqlite")
async def test_connection_context_manager_cleanup() -> None:
    """Test proper cleanup of connection context manager."""
    from uuid import uuid4

    unique_db = f"file:memdb{uuid4().hex}?mode=memory&cache=shared"
    config = AiosqliteConfig(pool_config={"database": unique_db})

    driver_ref = None
    try:
        async with config.provide_session() as driver:
            driver_ref = driver
            await driver.execute("CREATE TABLE cleanup_test (id INTEGER)")
            await driver.execute("INSERT INTO cleanup_test VALUES (1)")
            result = await driver.execute("SELECT COUNT(*) as count FROM cleanup_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 1

        # After context exit, connection should be managed by pool
        # We can't directly test connection state, but pool should handle cleanup
        assert driver_ref is not None  # Just verify we had a valid driver

    finally:
        await config.close_pool()


@pytest.mark.xdist_group("aiosqlite")
async def test_provide_connection_direct() -> None:
    """Test direct connection provision without session wrapper."""
    from uuid import uuid4

    unique_db = f"file:memdb{uuid4().hex}?mode=memory&cache=shared"
    config = AiosqliteConfig(pool_config={"database": unique_db})

    try:
        # Test provide_connection method if available
        if hasattr(config, "provide_connection"):
            async with config.provide_connection() as conn:
                assert conn is not None
                # Direct connection operations would go here
                # For aiosqlite, this might be the raw aiosqlite connection

        # Test through session as fallback
        async with config.provide_session() as driver:
            assert driver.connection is not None
            result = await driver.execute("SELECT sqlite_version() as version")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["version"] is not None

    finally:
        await config.close_pool()
