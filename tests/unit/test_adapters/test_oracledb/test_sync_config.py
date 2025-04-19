"""Tests for Oracle sync configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from oracledb import Connection, ConnectionPool

from sqlspec.adapters.oracledb.config import OracleSyncConfig, OracleSyncPoolConfig
from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import Generator


class MockOracleSync(OracleSyncConfig):
    """Mock implementation of OracleSync for testing."""

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
def mock_oracle_pool() -> Generator[MagicMock, None, None]:
    """Create a mock Oracle pool."""
    pool = MagicMock(spec=ConnectionPool)
    # Set up context manager for connection
    connection = MagicMock(spec=Connection)
    pool.acquire.return_value.__enter__.return_value = connection
    return pool


@pytest.fixture(scope="session")
def mock_oracle_connection() -> Generator[MagicMock, None, None]:
    """Create a mock Oracle connection."""
    return MagicMock(spec=Connection)


def test_default_values() -> None:
    """Test default values for OracleSync."""
    config = OracleSyncConfig()
    assert config.pool_config is None
    assert config.pool_instance is None


def test_with_all_values() -> None:
    """Test OracleSync with all values set."""
    mock_pool = MagicMock(spec=ConnectionPool)
    pool_config = OracleSyncPoolConfig(
        pool=mock_pool,
    )
    config = OracleSyncConfig(
        pool_config=pool_config,
    )

    assert config.pool_config == pool_config
    assert config.pool_instance is None


def test_connection_config_dict() -> None:
    """Test connection_config_dict property."""
    mock_pool = MagicMock(spec=ConnectionPool)
    pool_config = OracleSyncPoolConfig(
        pool=mock_pool,
    )
    config = OracleSyncConfig(
        pool_config=pool_config,
    )
    config_dict = config.connection_config_dict
    assert "pool" in config_dict
    assert config_dict["pool"] is mock_pool


def test_pool_config_dict_with_pool_config() -> None:
    """Test pool_config_dict with pool configuration."""
    mock_pool = MagicMock(spec=ConnectionPool)
    pool_config = OracleSyncPoolConfig(
        pool=mock_pool,
    )
    config = MockOracleSync(pool_config=pool_config)
    pool_config_dict = config.pool_config_dict
    assert "pool" in pool_config_dict
    assert pool_config_dict["pool"] is mock_pool


def test_pool_config_dict_with_pool_instance() -> None:
    """Test pool_config_dict with pool instance."""
    pool = MagicMock(spec=ConnectionPool)
    config = MockOracleSync(pool_instance=pool)
    with pytest.raises(ImproperConfigurationError, match="'pool_config' methods can not be used"):
        config.pool_config_dict


def test_create_pool_with_existing_pool() -> None:
    """Test create_pool with existing pool instance."""
    pool = MagicMock(spec=ConnectionPool)
    config = MockOracleSync(pool_instance=pool)
    assert config.create_pool() is pool


def test_create_pool_without_config_or_instance() -> None:
    """Test create_pool without pool config or instance."""
    config = MockOracleSync()
    with pytest.raises(ImproperConfigurationError, match="One of 'pool_config' or 'pool_instance' must be provided"):
        config.create_pool()


def test_provide_connection(mock_oracle_pool: MagicMock, mock_oracle_connection: MagicMock) -> None:
    """Test provide_connection context manager."""
    config = MockOracleSync(pool_instance=mock_oracle_pool)
    # Set up context manager for connection
    cm = MagicMock()
    cm.__enter__.return_value = mock_oracle_connection
    cm.__exit__.return_value = None
    mock_oracle_pool.acquire.return_value = cm
    with config.provide_connection() as connection:
        assert connection is mock_oracle_connection
