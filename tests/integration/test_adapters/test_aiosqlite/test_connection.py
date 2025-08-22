"""Test AIOSQLite connection functionality."""

from __future__ import annotations

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.core.result import SQLResult

pytestmark = pytest.mark.xdist_group("sqlite")


async def test_basic_connection(aiosqlite_config: AiosqliteConfig) -> None:
    """Test basic connection establishment."""
    async with aiosqlite_config.provide_session() as driver:
        assert isinstance(driver, AiosqliteDriver)
        assert driver.connection is not None

        result = await driver.execute("SELECT 1 as test_value")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["test_value"] == 1


async def test_connection_reuse(aiosqlite_config: AiosqliteConfig) -> None:
    """Test connection reuse in pool."""

    async with aiosqlite_config.provide_session() as driver1:
        await driver1.execute("CREATE TABLE IF NOT EXISTS reuse_test (id INTEGER, data TEXT)")
        await driver1.execute("INSERT INTO reuse_test VALUES (1, 'test_data')")
        await driver1.commit()

    async with aiosqlite_config.provide_session() as driver2:
        result = await driver2.execute("SELECT data FROM reuse_test WHERE id = 1")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["data"] == "test_data"

        await driver2.execute("DROP TABLE IF EXISTS reuse_test")
        await driver2.commit()


async def test_connection_error_handling(aiosqlite_config: AiosqliteConfig) -> None:
    """Test connection error handling."""
    async with aiosqlite_config.provide_session() as driver:
        with pytest.raises(Exception):
            await driver.execute("INVALID SQL SYNTAX")

        result = await driver.execute("SELECT 'still_working' as status")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["status"] == "still_working"


async def test_connection_with_transactions(aiosqlite_config: AiosqliteConfig) -> None:
    """Test connection behavior with transactions."""
    async with aiosqlite_config.provide_session() as driver:
        await driver.execute_script("""
            CREATE TABLE IF NOT EXISTS transaction_test (
                id INTEGER PRIMARY KEY,
                value TEXT
            )
        """)

        await driver.execute("BEGIN TRANSACTION")
        await driver.execute("INSERT INTO transaction_test (value) VALUES ('tx_test')")
        await driver.execute("COMMIT")

        result = await driver.execute("SELECT COUNT(*) as count FROM transaction_test")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["count"] == 1

        await driver.execute("BEGIN TRANSACTION")
        await driver.execute("INSERT INTO transaction_test (value) VALUES ('rollback_test')")
        await driver.execute("ROLLBACK")

        result = await driver.execute("SELECT COUNT(*) as count FROM transaction_test")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["count"] == 1

        await driver.execute("DROP TABLE IF EXISTS transaction_test")
        await driver.commit()


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

        assert driver_ref is not None

    finally:
        await config.close_pool()


async def test_provide_connection_direct() -> None:
    """Test direct connection provision without session wrapper."""
    from uuid import uuid4

    unique_db = f"file:memdb{uuid4().hex}?mode=memory&cache=shared"
    config = AiosqliteConfig(pool_config={"database": unique_db})

    try:
        if hasattr(config, "provide_connection"):
            async with config.provide_connection() as conn:
                assert conn is not None

        async with config.provide_session() as driver:
            assert driver.connection is not None
            result = await driver.execute("SELECT sqlite_version() as version")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["version"] is not None

    finally:
        await config.close_pool()


async def test_config_with_pool_config() -> None:
    """Test that AiosqliteConfig correctly accepts pool_config parameter."""
    from uuid import uuid4

    pool_config = {
        "database": f"file:test_{uuid4().hex}.db?mode=memory&cache=shared",
        "timeout": 10.0,
        "isolation_level": None,
        "check_same_thread": False,
    }

    config = AiosqliteConfig(pool_config=pool_config)

    try:
        connection_config = config._get_connection_config_dict()
        assert "test_" in connection_config["database"]
        assert connection_config["timeout"] == 10.0
        assert connection_config["isolation_level"] is None

        assert "pool_min_size" not in connection_config
        assert "pool_max_size" not in connection_config

        async with config.provide_session() as driver:
            result = await driver.execute("SELECT 1 as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == 1

    finally:
        await config.close_pool()


async def test_config_with_kwargs_override() -> None:
    """Test that kwargs properly override pool_config values."""
    from uuid import uuid4

    pool_config = {"database": "base.db", "timeout": 5.0}

    unique_db = f"file:override_{uuid4().hex}.db?mode=memory&cache=shared"
    # Override pool_config with specific test values
    test_pool_config = {**pool_config, "database": unique_db, "timeout": 15.0}
    config = AiosqliteConfig(pool_config=test_pool_config)

    try:
        connection_config = config._get_connection_config_dict()
        assert connection_config["database"] == unique_db
        assert connection_config["timeout"] == 15.0

        async with config.provide_session() as driver:
            result = await driver.execute("SELECT 'override_test' as status")
            assert isinstance(result, SQLResult)
            assert result.data[0]["status"] == "override_test"

    finally:
        await config.close_pool()


async def test_config_memory_database_conversion() -> None:
    """Test that :memory: databases are converted to shared memory."""

    config = AiosqliteConfig(pool_config={"database": ":memory:"})

    try:
        connection_config = config._get_connection_config_dict()
        assert connection_config["database"] == "file::memory:?cache=shared"
        assert connection_config.get("uri") is True

        async with config.provide_session() as driver:
            result = await driver.execute("SELECT 'memory_test' as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == "memory_test"

    finally:
        await config.close_pool()


async def test_config_default_database() -> None:
    """Test that default database is shared memory."""

    config = AiosqliteConfig()

    try:
        connection_config = config._get_connection_config_dict()
        assert connection_config["database"] == "file::memory:?cache=shared"
        assert connection_config.get("uri") is True

        async with config.provide_session() as driver:
            result = await driver.execute("SELECT 'default_test' as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == "default_test"

    finally:
        await config.close_pool()


async def test_config_parameter_preservation() -> None:
    """Test that aiosqlite config properly preserves parameters."""

    pool_config = {"database": "parameter_test.db", "isolation_level": None, "cached_statements": 100}

    config = AiosqliteConfig(pool_config=pool_config)

    try:
        connection_config = config._get_connection_config_dict()
        assert connection_config["database"] == "parameter_test.db"
        assert connection_config["isolation_level"] is None
        assert connection_config["cached_statements"] == 100

        async with config.provide_session() as driver:
            await driver.execute("CREATE TABLE IF NOT EXISTS parameter_test (id INTEGER)")
            await driver.execute("INSERT INTO parameter_test VALUES (42)")
            result = await driver.execute("SELECT id FROM parameter_test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["id"] == 42

            await driver.execute("DROP TABLE parameter_test")
            await driver.commit()

    finally:
        await config.close_pool()
