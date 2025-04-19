"""Tests for Asyncpg configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import asyncpg
import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig
from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import Generator


class MockAsyncpg(AsyncpgConfig):
    """Mock implementation of Asyncpg for testing."""

    async def create_connection(*args: Any, **kwargs: Any) -> asyncpg.Connection[Any]:
        """Mock create_connection method."""
        return MagicMock(spec=asyncpg.Connection)

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Mock connection_config_dict property."""
        _ = super().connection_config_dict
        return {}


class MockAsyncpgPool(AsyncpgPoolConfig):
    """Mock implementation of AsyncpgPool for testing."""

    def __init__(self, dsn: str, pool_instance: Any | None = None, **kwargs: Any) -> None:
        """Initialize with dsn and optional pool_instance."""
        super().__init__(dsn=dsn, **kwargs)  # pyright: ignore
        self._pool_instance = pool_instance

    async def create_pool(self, *args: Any, **kwargs: Any) -> asyncpg.Pool[Any]:
        """Mock create_pool method."""
        if self._pool_instance is not None:
            return self._pool_instance  # type: ignore[no-any-return]
        # Check if pool_config is None or not set
        if getattr(self, "pool_config", None) is None:
            raise ImproperConfigurationError("One of 'pool_config' or 'pool_instance' must be provided.")
        return MagicMock(spec=asyncpg.Pool)

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Mock pool_config_dict property."""
        if self._pool_instance is not None:
            raise ImproperConfigurationError(
                "'pool_config' methods can not be used when a 'pool_instance' is provided."
            )
        return {}


@pytest.fixture(scope="session")
def mock_asyncpg_pool() -> Generator[MagicMock, None, None]:
    """Create a mock Asyncpg pool."""
    pool = MagicMock(spec=asyncpg.Pool)
    # Set up context manager for connection
    connection = MagicMock(spec=asyncpg.Connection)
    pool.acquire.return_value.__aenter__.return_value = connection
    return pool


@pytest.fixture(scope="session")
def mock_asyncpg_connection() -> Generator[MagicMock, None, None]:
    """Create a mock Asyncpg connection."""
    return MagicMock(spec=asyncpg.Connection)


def test_default_values() -> None:
    """Test default values for Asyncpg."""
    config = AsyncpgConfig()
    assert config.pool_config is None
    assert config.pool_instance is None


def test_with_all_values() -> None:
    """Test Asyncpg with all values set."""
    pool_config = AsyncpgPoolConfig(
        dsn="postgres://test_user:test_pass@localhost:5432/test_db",
        min_size=1,
        max_size=10,
        max_inactive_connection_lifetime=300.0,
        max_queries=50000,
    )
    config = AsyncpgConfig(pool_config=pool_config)

    assert config.pool_config == pool_config
    assert config.pool_instance is None


def test_connection_config_dict() -> None:
    """Test connection_config_dict property."""
    pool_config = AsyncpgPoolConfig(
        dsn="postgres://test_user:test_pass@localhost:5432/test_db",
    )
    config = AsyncpgConfig(pool_config=pool_config)
    config_dict = config.connection_config_dict
    assert config_dict["dsn"] == "postgres://test_user:test_pass@localhost:5432/test_db"


def test_pool_config_dict_with_pool_config() -> None:
    """Test pool_config_dict with pool configuration."""
    pool_config = AsyncpgPoolConfig(
        dsn="postgres://test_user:test_pass@localhost:5432/test_db",
        min_size=1,
        max_size=10,
        max_inactive_connection_lifetime=300.0,
        max_queries=50000,
    )
    config = MockAsyncpg(pool_config=pool_config)
    pool_config_dict = config.pool_config_dict
    assert pool_config_dict["dsn"] == "postgres://test_user:test_pass@localhost:5432/test_db"
    assert pool_config_dict["min_size"] == 1
    assert pool_config_dict["max_size"] == 10
    assert pool_config_dict["max_inactive_connection_lifetime"] == 300.0
    assert pool_config_dict["max_queries"] == 50000


def test_pool_config_dict_with_pool_instance() -> None:
    """Test pool_config_dict with pool instance."""
    pool = MagicMock(spec=asyncpg.Pool)
    config = MockAsyncpg(pool_instance=pool)
    with pytest.raises(ImproperConfigurationError, match="'pool_config' methods can not be used"):
        config.pool_config_dict


async def test_create_pool_with_existing_pool() -> None:
    """Test create_pool with existing pool instance."""
    pool = MagicMock(spec=asyncpg.Pool)
    config = MockAsyncpgPool(dsn="postgres://test", pool_instance=pool)
    assert await config.create_pool() is pool


async def test_create_pool_without_config_or_instance() -> None:
    """Test create_pool without pool config or instance."""
    config = MockAsyncpgPool(dsn="postgres://test")
    with pytest.raises(ImproperConfigurationError, match="One of 'pool_config' or 'pool_instance' must be provided"):
        await config.create_pool()


async def test_provide_connection(mock_asyncpg_pool: MagicMock, mock_asyncpg_connection: MagicMock) -> None:
    """Test provide_connection context manager."""
    config = MockAsyncpg(pool_instance=mock_asyncpg_pool)
    # Set up the mock to return our expected connection
    mock_asyncpg_pool.acquire.return_value.__aenter__.return_value = mock_asyncpg_connection
    async with config.provide_connection() as connection:
        assert connection is mock_asyncpg_connection
