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


@pytest.mark.xdist_group("aiosqlite")
async def test_config_with_pool_config() -> None:
    """Test that AiosqliteConfig correctly accepts pool_config parameter."""
    from uuid import uuid4

    # Define connection parameters using the expected TypedDict type
    pool_config = {
        "database": f"file:test_{uuid4().hex}.db?mode=memory&cache=shared",
        "timeout": 10.0,
        "isolation_level": None,
        "check_same_thread": False,
    }

    # Create config with pool_config
    config = AiosqliteConfig(pool_config=pool_config)

    try:
        # Verify the configuration was set correctly
        connection_config = config._get_connection_config_dict()
        assert "test_" in connection_config["database"]
        assert connection_config["timeout"] == 10.0
        assert connection_config["isolation_level"] is None

        # Test that pool-related parameters are excluded from connection config
        assert "pool_min_size" not in connection_config
        assert "pool_max_size" not in connection_config

        # Verify the connection works
        async with config.provide_session() as driver:
            result = await driver.execute("SELECT 1 as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == 1

    finally:
        await config.close_pool()


@pytest.mark.xdist_group("aiosqlite")
async def test_config_with_kwargs_override() -> None:
    """Test that kwargs properly override pool_config values."""
    from uuid import uuid4

    # Define base pool config
    pool_config = {"database": "base.db", "timeout": 5.0}

    # Create config with pool_config and kwargs override
    unique_db = f"file:override_{uuid4().hex}.db?mode=memory&cache=shared"
    config = AiosqliteConfig(
        pool_config=pool_config,
        database=unique_db,  # This should override pool_config["database"]
        timeout=15.0,  # This should override pool_config["timeout"]
    )

    try:
        # Verify kwargs overrode pool_config
        connection_config = config._get_connection_config_dict()
        assert connection_config["database"] == unique_db
        assert connection_config["timeout"] == 15.0

        # Verify the connection works with overridden config
        async with config.provide_session() as driver:
            result = await driver.execute("SELECT 'override_test' as status")
            assert isinstance(result, SQLResult)
            assert result.data[0]["status"] == "override_test"

    finally:
        await config.close_pool()


@pytest.mark.xdist_group("aiosqlite")
async def test_config_memory_database_conversion() -> None:
    """Test that :memory: databases are converted to shared memory."""

    # Test with explicit :memory:
    config = AiosqliteConfig(pool_config={"database": ":memory:"})

    try:
        # Should be converted to shared memory
        connection_config = config._get_connection_config_dict()
        assert connection_config["database"] == "file::memory:?cache=shared"
        assert connection_config.get("uri") is True

        # Verify it works
        async with config.provide_session() as driver:
            result = await driver.execute("SELECT 'memory_test' as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == "memory_test"

    finally:
        await config.close_pool()


@pytest.mark.xdist_group("aiosqlite")
async def test_config_default_database() -> None:
    """Test that default database is shared memory."""

    # Test with no database specified
    config = AiosqliteConfig()

    try:
        # Should default to shared memory
        connection_config = config._get_connection_config_dict()
        assert connection_config["database"] == "file::memory:?cache=shared"
        assert connection_config.get("uri") is True

        # Verify it works
        async with config.provide_session() as driver:
            result = await driver.execute("SELECT 'default_test' as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == "default_test"

    finally:
        await config.close_pool()


@pytest.mark.xdist_group("aiosqlite")
async def test_config_parameter_preservation() -> None:
    """Test that aiosqlite config properly preserves parameters."""

    # Test that pool_config is properly passed and used
    pool_config = {"database": "parameter_test.db", "isolation_level": None, "cached_statements": 100}

    config = AiosqliteConfig(pool_config=pool_config)

    try:
        # Verify all parameters are preserved
        connection_config = config._get_connection_config_dict()
        assert connection_config["database"] == "parameter_test.db"
        assert connection_config["isolation_level"] is None
        assert connection_config["cached_statements"] == 100

        # Verify connection works with these settings
        async with config.provide_session() as driver:
            await driver.execute("CREATE TABLE IF NOT EXISTS parameter_test (id INTEGER)")
            await driver.execute("INSERT INTO parameter_test VALUES (42)")
            result = await driver.execute("SELECT id FROM parameter_test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["id"] == 42

            # Clean up
            await driver.execute("DROP TABLE parameter_test")
            await driver.commit()

    finally:
        await config.close_pool()
