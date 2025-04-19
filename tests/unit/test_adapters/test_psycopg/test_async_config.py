"""Tests for Psycopg async configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgAsyncPoolConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator


class MockPsycopgAsync(PsycopgAsyncConfig):
    """Mock implementation of PsycopgAsync for testing."""

    async def create_connection(*args: Any, **kwargs: Any) -> AsyncConnection:
        """Mock create_connection method."""
        return MagicMock(spec=AsyncConnection)

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Mock connection_config_dict property."""
        return {}

    async def close_pool(self) -> None:
        """Mock close_pool method."""
        if self.pool_instance is not None:
            await self.pool_instance.close()
            self.pool_instance = None


@pytest.fixture(scope="session")
def mock_psycopg_async_pool() -> Generator[MagicMock, None, None]:
    """Create a mock Psycopg async pool."""
    pool = MagicMock(spec=AsyncConnectionPool)
    # Set up async context manager for connection
    connection = MagicMock(spec=AsyncConnection)
    async_cm = MagicMock()
    async_cm.__aenter__ = AsyncMock(return_value=connection)
    async_cm.__aexit__ = AsyncMock(return_value=None)
    # Set up the acquire method
    pool.acquire = AsyncMock(return_value=async_cm)
    return pool


@pytest.fixture(scope="session")
def mock_psycopg_async_connection() -> Generator[MagicMock, None, None]:
    """Create a mock Psycopg async connection."""
    return MagicMock(spec=AsyncConnection)


def test_default_values() -> None:
    """Test default values for PsycopgAsyncPool."""
    config = PsycopgAsyncPoolConfig()
    assert config.conninfo is Empty
    assert config.kwargs is Empty
    assert config.min_size is Empty
    assert config.max_size is Empty
    assert config.name is Empty
    assert config.timeout is Empty
    assert config.max_waiting is Empty
    assert config.max_lifetime is Empty
    assert config.max_idle is Empty
    assert config.reconnect_timeout is Empty
    assert config.num_workers is Empty
    assert config.configure is Empty


def test_with_all_values() -> None:
    """Test configuration with all values set."""

    def configure_connection(conn: AsyncConnection) -> None:
        """Configure connection."""
        pass

    config = PsycopgAsyncPoolConfig(
        conninfo="postgresql://user:pass@localhost:5432/db",
        kwargs={"application_name": "test"},
        min_size=1,
        max_size=10,
        name="test_pool",
        timeout=5.0,
        max_waiting=5,
        max_lifetime=3600.0,
        max_idle=300.0,
        reconnect_timeout=5.0,
        num_workers=2,
        configure=configure_connection,
    )

    assert config.conninfo == "postgresql://user:pass@localhost:5432/db"
    assert config.kwargs == {"application_name": "test"}
    assert config.min_size == 1
    assert config.max_size == 10
    assert config.name == "test_pool"
    assert config.timeout == 5.0
    assert config.max_waiting == 5
    assert config.max_lifetime == 3600.0
    assert config.max_idle == 300.0
    assert config.reconnect_timeout == 5.0
    assert config.num_workers == 2
    assert config.configure == configure_connection


def test_pool_config_dict_with_pool_config() -> None:
    """Test pool_config_dict with pool configuration."""
    pool_config = PsycopgAsyncPoolConfig(
        conninfo="postgresql://user:pass@localhost:5432/db",
        min_size=1,
        max_size=10,
    )
    config = MockPsycopgAsync(pool_config=pool_config)
    config_dict = config.pool_config_dict
    assert "conninfo" in config_dict
    assert "min_size" in config_dict
    assert "max_size" in config_dict
    assert config_dict["conninfo"] == "postgresql://user:pass@localhost:5432/db"
    assert config_dict["min_size"] == 1
    assert config_dict["max_size"] == 10


def test_pool_config_dict_with_pool_instance() -> None:
    """Test pool_config_dict raises error with pool instance."""
    config = MockPsycopgAsync(pool_instance=MagicMock(spec=AsyncConnectionPool))
    with pytest.raises(ImproperConfigurationError, match="'pool_config' methods can not be used"):
        config.pool_config_dict


@pytest.mark.asyncio
async def test_create_pool_with_existing_pool() -> None:
    """Test create_pool with existing pool instance."""
    existing_pool = MagicMock(spec=AsyncConnectionPool)
    config = MockPsycopgAsync(pool_instance=existing_pool)
    pool = await config.create_pool()
    assert pool is existing_pool


@pytest.mark.asyncio
async def test_create_pool_without_config_or_instance() -> None:
    """Test create_pool raises error without pool config or instance."""
    config = MockPsycopgAsync()
    with pytest.raises(
        ImproperConfigurationError,
        match="One of 'pool_config' or 'pool_instance' must be provided",
    ):
        await config.create_pool()


@pytest.mark.asyncio
async def test_provide_connection(mock_psycopg_async_pool: MagicMock, mock_psycopg_async_connection: MagicMock) -> None:
    """Test provide_connection context manager."""
    # Create an async context manager that returns our connection
    async_cm = MagicMock()
    async_cm.__aenter__ = AsyncMock(return_value=mock_psycopg_async_connection)
    async_cm.__aexit__ = AsyncMock(return_value=None)

    # Create a mock pool that returns our async context manager
    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=async_cm)
    mock_pool.close = AsyncMock()  # Add close method
    mock_pool._workers = []  # Ensure no workers are running  # noqa: SLF001

    config = MockPsycopgAsync(pool_instance=mock_pool)  # pyright: ignore

    # Mock the provide_pool method to return our mock pool
    config.provide_pool = AsyncMock(return_value=mock_pool)  # type: ignore[method-assign]

    try:
        async with config.provide_connection() as conn:
            assert conn is mock_psycopg_async_connection
    finally:
        await config.close_pool()  # Ensure pool is closed
