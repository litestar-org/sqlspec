"""Tests for Oracle async configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from oracledb import AsyncConnection, AsyncConnectionPool

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleAsyncPoolConfig
from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import Generator


class MockOracleAsync(OracleAsyncConfig):
    """Mock implementation of OracleAsync for testing."""

    async def create_connection(*args: Any, **kwargs: Any) -> AsyncConnection:
        """Mock create_connection method."""
        return MagicMock(spec=AsyncConnection)

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Mock connection_config_dict property."""
        return {}

    async def close_pool(self) -> None:
        """Mock close_pool method."""
        pass


@pytest.fixture(scope="session")
def mock_oracle_async_pool() -> Generator[MagicMock, None, None]:
    """Create a mock Oracle async pool."""
    pool = MagicMock(spec=AsyncConnectionPool)
    # Set up async context manager for connection
    connection = MagicMock(spec=AsyncConnection)
    async_cm = MagicMock()
    async_cm.__aenter__ = AsyncMock(return_value=connection)
    async_cm.__aexit__ = AsyncMock(return_value=None)
    pool.acquire.return_value = async_cm
    return pool


@pytest.fixture(scope="session")
def mock_oracle_async_connection() -> Generator[MagicMock, None, None]:
    """Create a mock Oracle async connection."""
    return MagicMock(spec=AsyncConnection)


def test_default_values() -> None:
    """Test default values for OracleAsync."""
    config = OracleAsyncConfig()
    assert config.pool_config is None
    assert config.pool_instance is None


def test_with_all_values() -> None:
    """Test OracleAsync with all values set."""
    mock_pool = MagicMock(spec=AsyncConnectionPool)
    pool_config = OracleAsyncPoolConfig(
        pool=mock_pool,
    )
    config = OracleAsyncConfig(
        pool_config=pool_config,
    )

    assert config.pool_config == pool_config
    assert config.pool_instance is None


def test_connection_config_dict() -> None:
    """Test connection_config_dict property."""
    mock_pool = MagicMock(spec=AsyncConnectionPool)
    pool_config = OracleAsyncPoolConfig(
        pool=mock_pool,
    )
    config = OracleAsyncConfig(
        pool_config=pool_config,
    )
    config_dict = config.connection_config_dict
    assert "pool" in config_dict
    assert config_dict["pool"] is mock_pool


def test_pool_config_dict_with_pool_config() -> None:
    """Test pool_config_dict with pool configuration."""
    mock_pool = MagicMock(spec=AsyncConnectionPool)
    pool_config = OracleAsyncPoolConfig(
        pool=mock_pool,
    )
    config = MockOracleAsync(pool_config=pool_config)
    pool_config_dict = config.pool_config_dict
    assert "pool" in pool_config_dict
    assert pool_config_dict["pool"] is mock_pool


def test_pool_config_dict_with_pool_instance() -> None:
    """Test pool_config_dict with pool instance."""
    pool = MagicMock(spec=AsyncConnectionPool)
    config = MockOracleAsync(pool_instance=pool)
    with pytest.raises(ImproperConfigurationError, match="'pool_config' methods can not be used"):
        config.pool_config_dict


@pytest.mark.asyncio
async def test_create_pool_with_existing_pool() -> None:
    """Test create_pool with existing pool instance."""
    pool = MagicMock(spec=AsyncConnectionPool)
    config = MockOracleAsync(pool_instance=pool)
    assert await config.create_pool() is pool


@pytest.mark.asyncio
async def test_create_pool_without_config_or_instance() -> None:
    """Test create_pool without pool config or instance."""
    config = MockOracleAsync()
    with pytest.raises(ImproperConfigurationError, match="One of 'pool_config' or 'pool_instance' must be provided"):
        await config.create_pool()


@pytest.mark.asyncio
async def test_provide_connection(mock_oracle_async_pool: MagicMock, mock_oracle_async_connection: MagicMock) -> None:
    """Test provide_connection context manager."""
    config = MockOracleAsync(pool_instance=mock_oracle_async_pool)
    # Set up async context manager for connection
    async_cm = MagicMock()
    async_cm.__aenter__ = AsyncMock(return_value=mock_oracle_async_connection)
    async_cm.__aexit__ = AsyncMock(return_value=None)
    mock_oracle_async_pool.acquire.return_value = async_cm
    async with config.provide_connection() as connection:
        assert connection is mock_oracle_async_connection
