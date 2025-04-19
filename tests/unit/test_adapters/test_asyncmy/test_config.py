"""Tests for asyncmy configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import asyncmy  # pyright: ignore
import pytest

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyPoolConfig
from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import Generator


class MockAsyncmy(AsyncmyConfig):
    """Mock implementation of Asyncmy for testing."""

    async def create_connection(*args: Any, **kwargs: Any) -> asyncmy.Connection:  # pyright: ignore
        """Mock create_connection method."""
        return MagicMock(spec=asyncmy.Connection)  # pyright: ignore

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Mock connection_config_dict property."""
        _ = super().connection_config_dict  # pyright: ignore
        return {}


class MockAsyncmyPool(AsyncmyPoolConfig):
    """Mock implementation of AsyncmyPool for testing."""

    def __init__(self, host: str = "localhost", pool_instance: Any | None = None, **kwargs: Any) -> None:
        """Initialize with host and optional pool_instance."""
        super().__init__(host=host, **kwargs)  # pyright: ignore
        self._pool_instance = pool_instance

    async def create_pool(self, *args: Any, **kwargs: Any) -> asyncmy.Pool:  # pyright: ignore
        """Mock create_pool method."""
        if self._pool_instance is not None:
            return self._pool_instance
        # Check if pool_config is None or not set
        if getattr(self, "pool_config", None) is None:
            raise ImproperConfigurationError("One of 'pool_config' or 'pool_instance' must be provided.")
        return MagicMock(spec=asyncmy.Pool)  # pyright: ignore

    @property
    def pool_config_dict(self) -> dict[str, Any]:
        """Mock pool_config_dict property."""
        if self._pool_instance is not None:
            raise ImproperConfigurationError(
                "'pool_config' methods can not be used when a 'pool_instance' is provided."
            )
        return {}


@pytest.fixture(scope="session")
def mock_asyncmy_pool() -> Generator[MagicMock, None, None]:
    """Create a mock asyncmy pool."""
    pool = MagicMock(spec=asyncmy.Pool)  # pyright: ignore
    # Set up context manager for connection
    connection = MagicMock(spec=asyncmy.Connection)  # pyright: ignore
    pool.acquire.return_value.__aenter__.return_value = connection
    return pool


@pytest.fixture(scope="session")
def mock_asyncmy_connection() -> Generator[MagicMock, None, None]:
    """Create a mock asyncmy connection."""
    return MagicMock(spec=asyncmy.Connection)  # pyright: ignore


def test_default_values() -> None:
    """Test default values for asyncmy."""
    config = AsyncmyConfig()
    assert config.pool_config is None
    assert config.pool_instance is None  # pyright: ignore


def test_with_all_values() -> None:
    """Test asyncmy with all values set."""
    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_pass",
        database="test_db",
        minsize=1,
        maxsize=10,
    )
    config = AsyncmyConfig(pool_config=pool_config)

    assert config.pool_config == pool_config
    assert config.pool_instance is None  # pyright: ignore
    assert config.connection_config_dict == {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_pass",
        "database": "test_db",
    }


def test_connection_config_dict() -> None:
    """Test connection_config_dict property."""
    pool_config = AsyncmyPoolConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_pass",
        database="test_db",
    )
    config = AsyncmyConfig(pool_config=pool_config)
    config_dict = config.connection_config_dict
    assert config_dict["host"] == "localhost"
    assert config_dict["port"] == 3306
    assert config_dict["user"] == "test_user"
    assert config_dict["password"] == "test_pass"
    assert config_dict["database"] == "test_db"


def test_pool_config_dict_with_pool_instance() -> None:
    """Test pool_config_dict with pool instance."""
    pool = MagicMock(spec=asyncmy.Pool)  # pyright: ignore
    config = MockAsyncmy(pool_instance=pool)  # pyright: ignore
    with pytest.raises(ImproperConfigurationError, match="'pool_config' methods can not be used"):
        config.pool_config_dict  # pyright: ignore


async def test_create_pool_with_existing_pool() -> None:
    """Test create_pool with existing pool instance."""
    pool = MagicMock(spec=asyncmy.Pool)  # pyright: ignore
    config = MockAsyncmyPool(host="mysql://test", pool_instance=pool)  # pyright: ignore
    assert await config.create_pool() is pool  # pyright: ignore


async def test_create_pool_without_config_or_instance() -> None:
    """Test create_pool without pool config or instance."""
    config = MockAsyncmyPool(host="mysql://test")  # pyright: ignore
    with pytest.raises(ImproperConfigurationError, match="One of 'pool_config' or 'pool_instance' must be provided"):
        await config.create_pool()  # pyright: ignore


async def test_provide_connection(mock_asyncmy_pool: MagicMock, mock_asyncmy_connection: MagicMock) -> None:
    """Test provide_connection context manager."""
    config = MockAsyncmy(pool_instance=mock_asyncmy_pool)  # pyright: ignore
    # Set up the mock to return our expected connection
    mock_asyncmy_pool.acquire.return_value.__aenter__.return_value = mock_asyncmy_connection
    async with config.provide_connection() as connection:  # pyright: ignore
        assert connection is mock_asyncmy_connection
