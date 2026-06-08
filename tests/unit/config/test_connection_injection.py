"""Unit tests for connection_instance parameter injection config logic.

Tests config-level behavior of the connection_instance parameter: empty
connection_config handling, manual close, post-close_pool state, and mock pool
acceptance. End-to-end pooled behavior is covered by the lifecycle contract
suite.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig, AiosqlitePoolParams
from sqlspec.adapters.asyncpg.config import AsyncpgConfig, AsyncpgPoolConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig, DuckDBPoolParams
from sqlspec.adapters.sqlite.config import SqliteConfig, SqliteConnectionParams

pytestmark = pytest.mark.xdist_group("config")


def test_connection_instance_with_empty_connection_config() -> None:
    """Test that connection_instance works with empty connection_config."""
    from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

    pool = DuckDBConnectionPool(connection_config={"database": ":memory:"})

    try:
        # Empty connection_config, only connection_instance
        config = DuckDBConfig(connection_config=DuckDBPoolParams(), connection_instance=pool)

        assert config.connection_instance is pool
        # DuckDB adds default database parameter
        assert "database" in config.connection_config

        # Should still work
        with config.provide_session() as session:
            result = session.select_one("SELECT 1 as value")
            assert result["value"] == 1
    finally:
        pool.close()


def test_connection_instance_manual_close() -> None:
    """Test that manually created connection_instance can be closed independently."""
    from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

    pool = DuckDBConnectionPool(connection_config={"database": ":memory:"})

    config = DuckDBConfig(connection_config=DuckDBPoolParams(database=":memory:"), connection_instance=pool)

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

    config = SqliteConfig(connection_config=SqliteConnectionParams(database=":memory:"), connection_instance=pool)

    # Close the pool via config
    config.close_pool()

    # connection_instance should be set to None
    assert config.connection_instance is None


async def test_aiosqlite_connection_instance_after_close_pool() -> None:
    """Test that connection_instance can be closed via config."""
    from sqlspec.adapters.aiosqlite.pool import AiosqliteConnectionPool

    pool = AiosqliteConnectionPool(connection_parameters={"database": ":memory:"}, pool_size=2)

    config = AiosqliteConfig(connection_config=AiosqlitePoolParams(database=":memory:"), connection_instance=pool)

    # Close the pool via config
    await config.close_pool()

    # Verify pool is closed
    assert pool.is_closed


def test_connection_instance_with_mock_pool() -> None:
    """Test that connection_instance accepts mock pools for testing."""
    mock_pool = MagicMock()
    mock_pool.acquire = MagicMock()

    config = DuckDBConfig(connection_config=DuckDBPoolParams(database=":memory:"), connection_instance=mock_pool)

    assert config.connection_instance is mock_pool


async def test_connection_instance_with_async_mock_pool() -> None:
    """Test that connection_instance accepts async mock pools for testing."""
    mock_pool = MagicMock()
    mock_pool.acquire = AsyncMock()

    config = AsyncpgConfig(
        connection_config=AsyncpgPoolConfig(dsn="postgresql://localhost/test"), connection_instance=mock_pool
    )

    assert config.connection_instance is mock_pool
