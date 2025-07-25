"""Unit tests for Psqlpy configuration."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyConnectionParams, PsqlpyDriver, PsqlpyPoolParams
from sqlspec.statement.sql import SQLConfig


def test_psqlpy_typed_dict_structure() -> None:
    """Test Psqlpy TypedDict structure."""
    # Test that we can create valid connection params
    connection_params: PsqlpyConnectionParams = {
        "dsn": "postgresql://test_user:test_password@localhost:5432/test_db",
        "username": "test_user",
        "password": "test_password",
        "db_name": "test_db",
        "host": "localhost",
        "port": 5432,
        "ssl_mode": "require",
        "application_name": "test_app",
    }
    assert connection_params["dsn"] == "postgresql://test_user:test_password@localhost:5432/test_db"
    assert connection_params["username"] == "test_user"
    assert connection_params["password"] == "test_password"
    assert connection_params["db_name"] == "test_db"
    assert connection_params["host"] == "localhost"
    assert connection_params["port"] == 5432
    assert connection_params["ssl_mode"] == "require"
    assert connection_params["application_name"] == "test_app"

    # Test that we can create valid pool params
    pool_params: PsqlpyPoolParams = {
        "dsn": "postgresql://test_user:test_password@localhost:5432/test_db",
        "max_db_pool_size": 20,
        "conn_recycling_method": "clean",
        "hosts": ["host1", "host2"],
        "ports": [5432, 5433],
    }
    assert pool_params["dsn"] == "postgresql://test_user:test_password@localhost:5432/test_db"
    assert pool_params["max_db_pool_size"] == 20
    assert pool_params["conn_recycling_method"] == "clean"
    assert pool_params["hosts"] == ["host1", "host2"]
    assert pool_params["ports"] == [5432, 5433]


def test_psqlpy_config_basic_creation() -> None:
    """Test Psqlpy config creation with basic parameters."""
    # Test minimal config creation
    config = PsqlpyConfig(pool_config={"dsn": "postgresql://test_user:test_password@localhost:5432/test_db"})
    assert config.pool_config["dsn"] == "postgresql://test_user:test_password@localhost:5432/test_db"

    # Test with all parameters including extra
    config_full = PsqlpyConfig(
        pool_config={"dsn": "postgresql://test_user:test_password@localhost:5432/test_db", "extra": {"custom": "value"}}
    )
    assert config_full.pool_config["dsn"] == "postgresql://test_user:test_password@localhost:5432/test_db"
    assert config_full.pool_config["custom"] == "value"


def test_psqlpy_config_extras_handling() -> None:
    """Test Psqlpy config extras parameter handling."""
    # Test with extra field in pool_config
    config = PsqlpyConfig(
        pool_config={
            "dsn": "postgresql://test_user:test_password@localhost:5432/test_db",
            "extra": {"custom_param": "value", "debug": True},
        }
    )
    assert config.pool_config["custom_param"] == "value"
    assert config.pool_config["debug"] is True

    # Test with extra field in pool_config
    config2 = PsqlpyConfig(
        pool_config={
            "dsn": "postgresql://test_user:test_password@localhost:5432/test_db",
            "extra": {"unknown_param": "test", "another_param": 42},
        }
    )
    assert config2.pool_config["unknown_param"] == "test"
    assert config2.pool_config["another_param"] == 42


def test_psqlpy_config_initialization() -> None:
    """Test Psqlpy config initialization."""
    # Test with default parameters
    config = PsqlpyConfig(pool_config={"dsn": "postgresql://test_user:test_password@localhost:5432/test_db"})
    assert isinstance(config.statement_config, SQLConfig)
    # Test with custom parameters
    custom_statement_config = SQLConfig()
    config = PsqlpyConfig(
        pool_config={"dsn": "postgresql://test_user:test_password@localhost:5432/test_db"},
        statement_config=custom_statement_config,
    )
    assert config.statement_config is custom_statement_config


@pytest.mark.asyncio
async def test_psqlpy_config_provide_session() -> None:
    """Test Psqlpy config provide_session context manager."""
    config = PsqlpyConfig(pool_config={"dsn": "postgresql://test_user:test_password@localhost:5432/test_db"})

    # Mock the pool creation to avoid real database connection
    with patch.object(PsqlpyConfig, "_create_pool") as mock_create_pool:
        # Create a mock pool with acquire context manager
        mock_pool = MagicMock()
        mock_connection = AsyncMock()
        mock_connection.close = AsyncMock()

        # Set up the acquire method to return an async context manager
        mock_pool.acquire = MagicMock()
        mock_acquire_cm = AsyncMock()
        mock_acquire_cm.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_acquire_cm.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = mock_acquire_cm

        mock_create_pool.return_value = mock_pool

        # Test session context manager behavior
        async with config.provide_session() as session:
            assert isinstance(session, PsqlpyDriver)
            # Check that parameter styles were set
            assert session.config is not None
            assert session.config.allowed_parameter_styles == ("numeric",)
            assert session.config.default_parameter_style == "numeric"


def test_psqlpy_config_driver_type() -> None:
    """Test Psqlpy config driver_type property."""
    config = PsqlpyConfig(pool_config={"dsn": "postgresql://test_user:test_password@localhost:5432/test_db"})
    assert config.driver_type is PsqlpyDriver


def test_psqlpy_config_is_async() -> None:
    """Test Psqlpy config is_async attribute."""
    config = PsqlpyConfig(pool_config={"dsn": "postgresql://test_user:test_password@localhost:5432/test_db"})
    assert config.is_async is True
    assert PsqlpyConfig.is_async is True


def test_psqlpy_config_supports_connection_pooling() -> None:
    """Test Psqlpy config supports_connection_pooling attribute."""
    config = PsqlpyConfig(pool_config={"dsn": "postgresql://test_user:test_password@localhost:5432/test_db"})
    assert config.supports_connection_pooling is True
    assert PsqlpyConfig.supports_connection_pooling is True
