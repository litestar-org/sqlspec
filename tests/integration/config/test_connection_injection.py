"""Integration tests for connection_instance parameter injection.

Tests that pre-created pool/connection instances can be injected via the
connection_instance parameter and work correctly with database operations.

This validates that the standardized parameter naming works end-to-end
with real database connections.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.sqlite.config import SqliteConfig

pytestmark = pytest.mark.xdist_group("config")


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_connection_instance_with_pre_created_pool(asyncpg_connection_config: dict) -> None:
    """Test AsyncpgConfig with connection_instance using pre-created pool."""
    import asyncpg

    # Create pool manually
    pool = await asyncpg.create_pool(**asyncpg_connection_config, min_size=1, max_size=2)

    try:
        # Inject pool into config
        config = AsyncpgConfig(connection_config=asyncpg_connection_config, connection_instance=pool)

        # Verify pool is used
        assert config.connection_instance is pool

        # Test database operation
        async with config.provide_session() as session:
            result = await session.select_one("SELECT 1 as value")
            assert result["value"] == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_connection_instance_bypasses_pool_creation(asyncpg_connection_config: dict) -> None:
    """Test that connection_instance bypasses _create_pool logic."""
    import asyncpg

    # Create pool manually
    pool = await asyncpg.create_pool(**asyncpg_connection_config, min_size=1, max_size=2)

    try:
        # Config with connection_instance should not call _create_pool
        config = AsyncpgConfig(connection_config=asyncpg_connection_config, connection_instance=pool)

        # Get pool - should return the injected one
        retrieved_pool = await config.provide_pool()
        assert retrieved_pool is pool

        # Verify it works
        async with config.provide_session() as session:
            result = await session.select_one("SELECT 2 as value")
            assert result["value"] == 2
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_aiosqlite_connection_instance_with_pre_created_pool(tmp_path: Path) -> None:
    """Test AiosqliteConfig with connection_instance using pre-created pool."""
    from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool

    db_path = tmp_path / "test.db"

    # Create pool manually
    pool = AiosqliteConnectionPool(connection_parameters={"database": str(db_path)}, pool_size=2)

    try:
        # Inject pool into config
        config = AiosqliteConfig(connection_config={"database": str(db_path)}, connection_instance=pool)

        # Verify pool is used
        assert config.connection_instance is pool

        # Test database operation
        async with config.provide_session() as session:
            await session.execute("CREATE TABLE test (id INTEGER, value TEXT)")
            await session.execute("INSERT INTO test VALUES (1, 'test')")
            result = await session.select_one("SELECT value FROM test WHERE id = 1")
            assert result["value"] == "test"
    finally:
        await pool.close()


def test_sqlite_connection_instance_with_pre_created_pool(tmp_path: Path) -> None:
    """Test SqliteConfig with connection_instance using pre-created pool."""
    from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

    db_path = tmp_path / "test.db"

    # Create pool manually
    pool = SqliteConnectionPool(connection_parameters={"database": str(db_path)})

    try:
        # Inject pool into config
        config = SqliteConfig(connection_config={"database": str(db_path)}, connection_instance=pool)

        # Verify pool is used
        assert config.connection_instance is pool

        # Test database operation
        with config.provide_session() as session:
            session.execute("CREATE TABLE test (id INTEGER, value TEXT)")
            session.execute("INSERT INTO test VALUES (1, 'test')")
            result = session.select_one("SELECT value FROM test WHERE id = 1")
            assert result["value"] == "test"
    finally:
        pool.close()


def test_duckdb_connection_instance_with_pre_created_pool() -> None:
    """Test DuckDBConfig with connection_instance using pre-created pool."""
    from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

    # Create pool manually
    pool = DuckDBConnectionPool(connection_config={"database": ":memory:"})

    try:
        # Inject pool into config
        config = DuckDBConfig(connection_config={"database": ":memory:"}, connection_instance=pool)

        # Verify pool is used
        assert config.connection_instance is pool

        # Test database operation
        with config.provide_session() as session:
            session.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
            session.execute("INSERT INTO test VALUES (1, 'test')")
            result = session.select_one("SELECT value FROM test WHERE id = 1")
            assert result["value"] == "test"
    finally:
        pool.close()


def test_sqlite_connection_instance_none_creates_new_pool(tmp_path: Path) -> None:
    """Test that connection_instance=None causes new pool creation."""
    db_path = tmp_path / "test.db"

    config = SqliteConfig(
        connection_config={"database": str(db_path), "pool_min_size": 2, "pool_max_size": 5}, connection_instance=None
    )

    # Should create new pool
    assert config.connection_instance is None

    # Using provide_pool should create pool
    pool = config.provide_pool()
    assert pool is not None

    # Verify it works
    with config.provide_session() as session:
        session.execute("CREATE TABLE test (id INTEGER)")
        session.execute("INSERT INTO test VALUES (1)")
        result = session.select_one("SELECT COUNT(*) as count FROM test")
        assert result["count"] == 1

    config.close_pool()


@pytest.mark.asyncio
async def test_aiosqlite_connection_instance_none_creates_new_pool(tmp_path: Path) -> None:
    """Test that connection_instance=None causes new pool creation for async."""
    db_path = tmp_path / "test.db"

    config = AiosqliteConfig(
        connection_config={"database": str(db_path), "pool_min_size": 2, "pool_max_size": 5}, connection_instance=None
    )

    # Should create new pool
    assert config.connection_instance is None

    # Using provide_pool should create pool
    pool = await config.provide_pool()
    assert pool is not None

    # Verify it works
    async with config.provide_session() as session:
        await session.execute("CREATE TABLE test (id INTEGER)")
        await session.execute("INSERT INTO test VALUES (1)")
        result = await session.select_one("SELECT COUNT(*) as count FROM test")
        assert result["count"] == 1

    await config.close_pool()


def test_connection_instance_persists_across_sessions() -> None:
    """Test that connection_instance persists across multiple sessions."""
    from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

    pool = DuckDBConnectionPool(connection_config={"database": ":memory:"})

    try:
        config = DuckDBConfig(connection_config={"database": ":memory:"}, connection_instance=pool)

        # First session
        with config.provide_session() as session1:
            session1.execute("CREATE TABLE test (id INTEGER)")
            session1.execute("INSERT INTO test VALUES (1)")

        # Second session - should use same pool
        with config.provide_session() as session2:
            result = session2.select_one("SELECT COUNT(*) as count FROM test")
            assert result["count"] == 1

        # Verify connection_instance is still the same
        assert config.connection_instance is pool
    finally:
        pool.close()


def test_connection_instance_with_empty_connection_config() -> None:
    """Test that connection_instance works with empty connection_config."""
    from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

    pool = DuckDBConnectionPool(connection_config={"database": ":memory:"})

    try:
        # Empty connection_config, only connection_instance
        config = DuckDBConfig(connection_config={}, connection_instance=pool)

        assert config.connection_instance is pool
        # DuckDB adds default database parameter
        assert "database" in config.connection_config

        # Should still work
        with config.provide_session() as session:
            result = session.select_one("SELECT 1 as value")
            assert result["value"] == 1
    finally:
        pool.close()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_asyncpg_connection_instance_overrides_connection_config_pool_params(
    asyncpg_connection_config: dict,
) -> None:
    """Test that connection_instance overrides pool parameters in connection_config."""
    import asyncpg

    # Create pool with specific size
    pool = await asyncpg.create_pool(**asyncpg_connection_config, min_size=3, max_size=5)

    try:
        # Config has different pool params but connection_instance should take precedence
        merged_config = dict(asyncpg_connection_config)
        merged_config["min_size"] = 10  # This should be ignored
        merged_config["max_size"] = 20  # This should be ignored
        config = AsyncpgConfig(connection_config=merged_config, connection_instance=pool)

        # The injected pool should be used, not a new one with config params
        retrieved_pool = await config.provide_pool()
        assert retrieved_pool is pool

        # Verify pool has original size, not config size
        # (We can't directly check min/max_size, but we can verify it's the same pool object)
        async with config.provide_session() as session:
            result = await session.select_one("SELECT 1 as value")
            assert result["value"] == 1
    finally:
        await pool.close()


def test_connection_instance_manual_close() -> None:
    """Test that manually created connection_instance can be closed independently."""
    from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

    pool = DuckDBConnectionPool(connection_config={"database": ":memory:"})

    config = DuckDBConfig(connection_config={"database": ":memory:"}, connection_instance=pool)

    # Use the config
    with config.provide_session() as session:
        session.execute("CREATE TABLE test (id INTEGER)")

    # Close the pool manually (not via config.close_pool())
    pool.close()

    # Config's connection_instance is now closed
    # Attempting to use should fail or create new pool depending on implementation
    assert config.connection_instance is pool


def test_sqlite_connection_instance_after_close_pool() -> None:
    """Test that connection_instance is set to None after close_pool()."""
    from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

    pool = SqliteConnectionPool(connection_parameters={"database": ":memory:"})

    config = SqliteConfig(connection_config={"database": ":memory:"}, connection_instance=pool)

    # Close the pool via config
    config.close_pool()

    # connection_instance should be set to None
    assert config.connection_instance is None


@pytest.mark.asyncio
async def test_aiosqlite_connection_instance_after_close_pool() -> None:
    """Test that connection_instance can be closed via config."""
    from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool

    pool = AiosqliteConnectionPool(connection_parameters={"database": ":memory:"}, pool_size=2)

    config = AiosqliteConfig(connection_config={"database": ":memory:"}, connection_instance=pool)

    # Close the pool via config
    await config.close_pool()

    # Verify pool is closed
    assert pool.is_closed


def test_connection_instance_with_mock_pool() -> None:
    """Test that connection_instance accepts mock pools for testing."""
    mock_pool = MagicMock()
    mock_pool.acquire = MagicMock()

    config = DuckDBConfig(connection_config={"database": ":memory:"}, connection_instance=mock_pool)

    assert config.connection_instance is mock_pool


@pytest.mark.asyncio
async def test_connection_instance_with_async_mock_pool() -> None:
    """Test that connection_instance accepts async mock pools for testing."""
    mock_pool = MagicMock()
    mock_pool.acquire = AsyncMock()

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"}, connection_instance=mock_pool)

    assert config.connection_instance is mock_pool
