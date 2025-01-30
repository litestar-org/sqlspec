"""Tests for AsyncPG configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from asyncpg import Connection, Pool, Record
from asyncpg.pool import PoolConnectionProxy

from sqlspec.adapters.asyncpg.config import AsyncPgConfig, AsyncPgPoolConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def mock_asyncpg_pool() -> Generator[MagicMock, None, None]:
    """Create a mock AsyncPG pool."""
    with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool") as mock_create_pool:
        pool = MagicMock(spec=Pool)
        mock_create_pool.return_value = pool

        # Make create_pool awaitable
        async def async_create_pool(*args: Any, **kwargs: Any) -> Pool:  # pyright: ignore[reportUnknownParameterType,reportMissingTypeArgument]
            return pool

        mock_create_pool.side_effect = async_create_pool
        yield pool


@pytest.fixture
def mock_asyncpg_connection() -> Generator[MagicMock, None, None]:
    """Create a mock AsyncPG connection."""
    connection = MagicMock(spec=PoolConnectionProxy)
    yield connection


class TestAsyncPgPoolConfig:
    """Test AsyncPgPoolConfig class."""

    def test_default_values(self) -> None:
        """Test default values for AsyncPgPoolConfig."""
        config = AsyncPgPoolConfig(dsn="postgresql://localhost/test")
        assert config.dsn == "postgresql://localhost/test"
        assert config.connect_kwargs is Empty
        assert config.connection_class is Empty
        assert config.record_class is Empty
        assert config.min_size is Empty
        assert config.max_size is Empty
        assert config.max_queries is Empty
        assert config.max_inactive_connection_lifetime is Empty
        assert config.setup is Empty
        assert config.init is Empty
        assert config.loop is Empty

    def test_with_all_values(self) -> None:
        """Test AsyncPgPoolConfig with all values set."""
        config = AsyncPgPoolConfig(
            dsn="postgresql://localhost/test",
            connect_kwargs={"ssl": True},
            connection_class=Connection,
            record_class=Record,
            min_size=1,
            max_size=10,
            max_queries=1000,
            max_inactive_connection_lifetime=300.0,
            loop=MagicMock(),
        )
        assert config.dsn == "postgresql://localhost/test"
        assert config.connect_kwargs == {"ssl": True}
        assert config.connection_class == Connection  # pyright: ignore[reportUnknownMemberType]
        assert config.record_class == Record
        assert config.min_size == 1
        assert config.max_size == 10
        assert config.max_queries == 1000
        assert config.max_inactive_connection_lifetime == 300.0
        assert config.setup is Empty  # pyright: ignore[reportUnknownMemberType]
        assert config.init is Empty  # pyright: ignore[reportUnknownMemberType]
        assert config.loop is not Empty


class MockAsyncPgConfig(AsyncPgConfig):
    """Mock AsyncPgConfig for testing."""

    async def create_connection(self, *args: Any, **kwargs: Any) -> PoolConnectionProxy:  # pyright: ignore[reportUnknownParameterType,reportMissingTypeArgument]
        """Mock create_connection method."""
        return MagicMock(spec=PoolConnectionProxy)

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Mock connection_config_dict property."""
        return {}


class TestAsyncPgConfig:
    """Test AsyncPgConfig class."""

    def test_default_values(self) -> None:
        """Test default values for AsyncPgConfig."""
        config = MockAsyncPgConfig()
        assert config.pool_config is None
        assert config.pool_instance is None
        assert callable(config.json_deserializer)
        assert callable(config.json_serializer)

    def test_pool_config_dict_with_pool_config(self) -> None:
        """Test pool_config_dict with pool configuration."""
        pool_config = AsyncPgPoolConfig(dsn="postgresql://localhost/test", min_size=1, max_size=10)
        config = MockAsyncPgConfig(pool_config=pool_config)
        config_dict = config.pool_config_dict
        assert config_dict == {"dsn": "postgresql://localhost/test", "min_size": 1, "max_size": 10}

    def test_pool_config_dict_with_pool_instance(self) -> None:
        """Test pool_config_dict raises error with pool instance."""
        config = MockAsyncPgConfig(pool_instance=MagicMock(spec=Pool))
        with pytest.raises(ImproperConfigurationError, match="'pool_config' methods can not be used"):
            config.pool_config_dict

    @pytest.mark.asyncio
    async def test_create_pool_with_pool_config(self, mock_asyncpg_pool: MagicMock) -> None:
        """Test create_pool with pool configuration."""
        pool_config = AsyncPgPoolConfig(dsn="postgresql://localhost/test")
        config = MockAsyncPgConfig(pool_config=pool_config)
        pool = await config.create_pool()
        assert pool is mock_asyncpg_pool

    @pytest.mark.asyncio
    async def test_create_pool_with_existing_pool(self) -> None:
        """Test create_pool with existing pool instance."""
        existing_pool = MagicMock(spec=Pool)
        config = MockAsyncPgConfig(pool_instance=existing_pool)
        pool = await config.create_pool()
        assert pool is existing_pool

    @pytest.mark.asyncio
    async def test_create_pool_without_config_or_instance(self) -> None:
        """Test create_pool raises error without pool config or instance."""
        config = MockAsyncPgConfig()
        with pytest.raises(
            ImproperConfigurationError, match="One of 'pool_config' or 'pool_instance' must be provided"
        ):
            await config.create_pool()

    @pytest.mark.asyncio
    async def test_provide_connection(self, mock_asyncpg_pool: MagicMock, mock_asyncpg_connection: MagicMock) -> None:
        """Test provide_connection context manager."""
        # Make the pool's acquire method return an async context manager
        acquire_context = AsyncMock()
        acquire_context.__aenter__.return_value = mock_asyncpg_connection
        mock_asyncpg_pool.acquire.return_value = acquire_context

        config = MockAsyncPgConfig(pool_config=AsyncPgPoolConfig(dsn="postgresql://localhost/test"))

        async with config.provide_connection() as conn:
            assert conn is mock_asyncpg_connection
