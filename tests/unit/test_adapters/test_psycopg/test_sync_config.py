"""Tests for Psycopg sync configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from psycopg import Connection
from psycopg_pool import ConnectionPool

from sqlspec.adapters.psycopg.config import PsycopgSyncConfig, PsycopgSyncPoolConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator


class MockPsycopgSync(PsycopgSyncConfig):
    """Mock implementation of PsycopgSync for testing."""

    def create_connection(*args: Any, **kwargs: Any) -> Connection:
        """Mock create_connection method."""
        return MagicMock(spec=Connection)

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Mock connection_config_dict property."""
        return {}

    def close_pool(self) -> None:
        """Mock close_pool method."""
        pass


@pytest.fixture(scope="session")
def mock_psycopg_pool() -> Generator[MagicMock, None, None]:
    """Create a mock Psycopg pool."""
    pool = MagicMock(spec=ConnectionPool)
    # Set up context manager for connection
    connection = MagicMock(spec=Connection)
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=connection)
    cm.__exit__ = MagicMock(return_value=None)
    # Set up the connection method
    pool.connection = MagicMock(return_value=cm)
    return pool


@pytest.fixture(scope="session")
def mock_psycopg_connection() -> Generator[MagicMock, None, None]:
    """Create a mock Psycopg connection."""
    return MagicMock(spec=Connection)


def test_default_values() -> None:
    """Test default values for PsycopgSyncPool."""
    config = PsycopgSyncPoolConfig()
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
    """Test PsycopgSyncPool with all values set."""

    def configure_connection(conn: Connection) -> None:
        """Configure connection."""
        pass

    config = PsycopgSyncPoolConfig(
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
    pool_config = PsycopgSyncPoolConfig(
        conninfo="postgresql://user:pass@localhost:5432/db",
        min_size=1,
        max_size=10,
    )
    config = MockPsycopgSync(pool_config=pool_config)
    config_dict = config.pool_config_dict
    assert "conninfo" in config_dict
    assert "min_size" in config_dict
    assert "max_size" in config_dict
    assert config_dict["conninfo"] == "postgresql://user:pass@localhost:5432/db"
    assert config_dict["min_size"] == 1
    assert config_dict["max_size"] == 10


def test_pool_config_dict_with_pool_instance() -> None:
    """Test pool_config_dict with pool instance."""
    pool = MagicMock(spec=ConnectionPool)
    config = MockPsycopgSync(pool_instance=pool)
    with pytest.raises(ImproperConfigurationError, match="'pool_config' methods can not be used"):
        config.pool_config_dict


def test_create_pool_with_existing_pool() -> None:
    """Test create_pool with existing pool instance."""
    pool = MagicMock(spec=ConnectionPool)
    config = MockPsycopgSync(pool_instance=pool)
    assert config.create_pool() is pool


def test_create_pool_without_config_or_instance() -> None:
    """Test create_pool without pool config or instance."""
    config = MockPsycopgSync()
    with pytest.raises(ImproperConfigurationError, match="One of 'pool_config' or 'pool_instance' must be provided"):
        config.create_pool()


def test_provide_connection(mock_psycopg_pool: MagicMock, mock_psycopg_connection: MagicMock) -> None:
    """Test provide_connection context manager."""
    # Set up the mock pool to return our connection
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=mock_psycopg_connection)
    cm.__exit__ = MagicMock(return_value=None)
    mock_psycopg_pool.connection = MagicMock(return_value=cm)

    config = MockPsycopgSync(pool_instance=mock_psycopg_pool)

    with config.provide_connection() as connection:
        assert connection is mock_psycopg_connection
