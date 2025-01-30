"""Tests for Psycopg sync configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from psycopg import Connection
from psycopg_pool import ConnectionPool

from sqlspec.adapters.psycopg.config import PsycoPgSyncDatabaseConfig, PsycoPgSyncPoolConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator


class MockPsycoPgSyncDatabaseConfig(PsycoPgSyncDatabaseConfig):
    """Mock implementation of PsycoPgSyncDatabaseConfig for testing."""

    def create_connection(*args: Any, **kwargs: Any) -> Connection:
        """Mock create_connection method."""
        return MagicMock(spec=Connection)

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Mock connection_config_dict property."""
        return {}


@pytest.fixture
def mock_psycopg_pool() -> Generator[MagicMock, None, None]:
    """Create a mock Psycopg pool."""
    pool = MagicMock(spec=ConnectionPool)
    # Set up context manager for connection
    connection = MagicMock(spec=Connection)
    pool.connection.return_value.__enter__.return_value = connection
    yield pool


@pytest.fixture
def mock_psycopg_connection() -> Generator[MagicMock, None, None]:
    """Create a mock Psycopg connection."""
    connection = MagicMock(spec=Connection)
    yield connection


class TestPsycoPgSyncPoolConfig:
    """Test PsycoPgSyncPoolConfig class."""

    def test_default_values(self) -> None:
        """Test default values for PsycoPgSyncPoolConfig."""
        pool_config = PsycoPgSyncPoolConfig()
        assert pool_config.conninfo is Empty
        assert pool_config.kwargs is Empty
        assert pool_config.min_size is Empty
        assert pool_config.max_size is Empty
        assert pool_config.name is Empty
        assert pool_config.timeout is Empty
        assert pool_config.max_waiting is Empty
        assert pool_config.max_lifetime is Empty
        assert pool_config.max_idle is Empty
        assert pool_config.reconnect_timeout is Empty
        assert pool_config.num_workers is Empty
        assert pool_config.configure is Empty

        config = MockPsycoPgSyncDatabaseConfig()
        assert config.pool_config is None
        assert config.pool_instance is None
        assert config.__is_async__ is False
        assert config.__supports_connection_pooling__ is True

    def test_with_all_values(self) -> None:
        """Test configuration with all values set."""

        def configure_connection(conn: Connection) -> None:
            """Configure connection."""

        pool_config = PsycoPgSyncPoolConfig(
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

        assert pool_config.conninfo == "postgresql://user:pass@localhost:5432/db"
        assert pool_config.kwargs == {"application_name": "test"}
        assert pool_config.min_size == 1
        assert pool_config.max_size == 10
        assert pool_config.name == "test_pool"
        assert pool_config.timeout == 5.0
        assert pool_config.max_waiting == 5
        assert pool_config.max_lifetime == 3600.0
        assert pool_config.max_idle == 300.0
        assert pool_config.reconnect_timeout == 5.0
        assert pool_config.num_workers == 2
        assert pool_config.configure == configure_connection

    def test_pool_config_dict_with_pool_config(self) -> None:
        """Test pool_config_dict with pool configuration."""
        pool_config = PsycoPgSyncPoolConfig(
            conninfo="postgresql://user:pass@localhost:5432/db",
            min_size=1,
            max_size=10,
        )
        config = MockPsycoPgSyncDatabaseConfig(pool_config=pool_config)
        config_dict = config.pool_config_dict
        assert config_dict == {
            "conninfo": "postgresql://user:pass@localhost:5432/db",
            "min_size": 1,
            "max_size": 10,
        }

    def test_pool_config_dict_with_pool_instance(self) -> None:
        """Test pool_config_dict raises error with pool instance."""
        config = MockPsycoPgSyncDatabaseConfig(pool_instance=MagicMock(spec=ConnectionPool))
        with pytest.raises(ImproperConfigurationError, match="'pool_config' methods can not be used"):
            config.pool_config_dict

    def test_create_pool_with_existing_pool(self) -> None:
        """Test create_pool with existing pool instance."""
        existing_pool = MagicMock(spec=ConnectionPool)
        config = MockPsycoPgSyncDatabaseConfig(pool_instance=existing_pool)
        pool = config.create_pool()
        assert pool is existing_pool

    def test_create_pool_without_config_or_instance(self) -> None:
        """Test create_pool raises error without pool config or instance."""
        config = MockPsycoPgSyncDatabaseConfig()
        with pytest.raises(
            ImproperConfigurationError, match="One of 'pool_config' or 'pool_instance' must be provided"
        ):
            config.create_pool()

    def test_provide_connection(self, mock_psycopg_pool: MagicMock, mock_psycopg_connection: MagicMock) -> None:
        """Test provide_connection context manager."""
        # Set up the connection context manager
        mock_psycopg_pool.connection.return_value.__enter__.return_value = mock_psycopg_connection

        config = MockPsycoPgSyncDatabaseConfig(pool_instance=mock_psycopg_pool)
        with config.provide_connection() as conn:
            assert conn is mock_psycopg_connection
