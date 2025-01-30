"""Tests for Psycopg async configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from sqlspec.adapters.psycopg.config import PsycoPgAsyncDatabaseConfig, PsycoPgAsyncPoolConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator


class MockPsycoPgAsyncDatabaseConfig(PsycoPgAsyncDatabaseConfig):
    """Mock implementation of PsycoPgAsyncDatabaseConfig for testing."""

    async def create_connection(self, *args: Any, **kwargs: Any) -> AsyncConnection:
        """Mock create_connection method."""
        return MagicMock(spec=AsyncConnection)

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Mock connection_config_dict property."""
        return {}


@pytest.fixture
def mock_psycopg_pool() -> Generator[MagicMock, None, None]:
    """Create a mock Psycopg pool."""
    pool = MagicMock(spec=AsyncConnectionPool)
    # Set up async context manager for connection
    connection = MagicMock(spec=AsyncConnection)
    async_cm = MagicMock()
    async_cm.__aenter__ = AsyncMock(return_value=connection)
    async_cm.__aexit__ = AsyncMock(return_value=None)
    pool.connection.return_value = async_cm
    yield pool


@pytest.fixture
def mock_psycopg_connection() -> Generator[MagicMock, None, None]:
    """Create a mock Psycopg connection."""
    connection = MagicMock(spec=AsyncConnection)
    yield connection


class TestPsycoPgAsyncPoolConfig:
    """Test PsycoPgAsyncPoolConfig class."""

    def test_default_values(self) -> None:
        """Test default values for PsycoPgAsyncPoolConfig."""
        config = PsycoPgAsyncPoolConfig()
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

    def test_with_all_values(self) -> None:
        """Test configuration with all values set."""

        def configure_connection(conn: AsyncConnection) -> None:
            """Configure connection."""

        config = PsycoPgAsyncPoolConfig(
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


class TestPsycoPgAsyncDatabaseConfig:
    """Test PsycoPgAsyncDatabaseConfig class."""

    def test_default_values(self) -> None:
        """Test default values for PsycoPgAsyncDatabaseConfig."""
        config = MockPsycoPgAsyncDatabaseConfig()
        assert config.pool_config is None
        assert config.pool_instance is None
        assert config.__is_async__ is True
        assert config.__supports_connection_pooling__ is True

    def test_pool_config_dict_with_pool_config(self) -> None:
        """Test pool_config_dict with pool configuration."""
        pool_config = PsycoPgAsyncPoolConfig(
            conninfo="postgresql://user:pass@localhost:5432/db",
            min_size=1,
            max_size=10,
        )
        config = MockPsycoPgAsyncDatabaseConfig(pool_config=pool_config)
        config_dict = config.pool_config_dict
        assert config_dict == {
            "conninfo": "postgresql://user:pass@localhost:5432/db",
            "min_size": 1,
            "max_size": 10,
        }

    def test_pool_config_dict_with_pool_instance(self) -> None:
        """Test pool_config_dict raises error with pool instance."""
        config = MockPsycoPgAsyncDatabaseConfig(pool_instance=MagicMock(spec=AsyncConnectionPool))
        with pytest.raises(ImproperConfigurationError, match="'pool_config' methods can not be used"):
            config.pool_config_dict

    @pytest.mark.asyncio
    async def test_create_pool_with_existing_pool(self) -> None:
        """Test create_pool with existing pool instance."""
        existing_pool = MagicMock(spec=AsyncConnectionPool)
        config = MockPsycoPgAsyncDatabaseConfig(pool_instance=existing_pool)
        pool = await config.create_pool()
        assert pool is existing_pool

    @pytest.mark.asyncio
    async def test_create_pool_without_config_or_instance(self) -> None:
        """Test create_pool raises error without pool config or instance."""
        config = MockPsycoPgAsyncDatabaseConfig()
        with pytest.raises(
            ImproperConfigurationError, match="One of 'pool_config' or 'pool_instance' must be provided"
        ):
            await config.create_pool()

    @pytest.mark.asyncio
    async def test_provide_connection(self, mock_psycopg_pool: MagicMock, mock_psycopg_connection: MagicMock) -> None:
        """Test provide_connection context manager."""
        # Set up the connection context manager
        async_cm = MagicMock()
        async_cm.__aenter__ = AsyncMock(return_value=mock_psycopg_connection)
        async_cm.__aexit__ = AsyncMock(return_value=None)
        mock_psycopg_pool.connection.return_value = async_cm

        config = MockPsycoPgAsyncDatabaseConfig(pool_instance=mock_psycopg_pool)
        async with config.provide_connection() as conn:
            assert conn is mock_psycopg_connection
