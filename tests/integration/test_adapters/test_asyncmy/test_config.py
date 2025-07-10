"""Unit tests for Asyncmy configuration."""

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyConnectionParams, AsyncmyDriver, AsyncmyPoolParams
from sqlspec.statement.sql import SQLConfig


def test_asyncmy_typed_dict_structure() -> None:
    """Test Asyncmy TypedDict structure."""
    # Test that we can create valid connection params
    connection_params: AsyncmyConnectionParams = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    assert connection_params["host"] == "localhost"
    assert connection_params["port"] == 3306

    # Test that pool params inherit from connection params and add pool-specific fields
    pool_params: AsyncmyPoolParams = {"host": "localhost", "port": 3306, "minsize": 5, "maxsize": 20, "echo": True}
    assert pool_params["host"] == "localhost"
    assert pool_params["minsize"] == 5


def test_asyncmy_config_basic_creation() -> None:
    """Test Asyncmy config creation with basic parameters."""
    # Test minimal config creation
    pool_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.pool_config["host"] == "localhost"
    assert config.pool_config["port"] == 3306
    assert config.pool_config["user"] == "test_user"
    assert config.pool_config["password"] == "test_password"
    assert config.pool_config["database"] == "test_db"

    # Test with additional parameters
    pool_config_full = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
        "custom": "value",  # Additional parameters
    }
    config_full = AsyncmyConfig(pool_config=pool_config_full)
    assert config_full.pool_config["host"] == "localhost"
    assert config_full.pool_config["port"] == 3306
    assert config_full.pool_config["user"] == "test_user"
    assert config_full.pool_config["password"] == "test_password"
    assert config_full.pool_config["database"] == "test_db"
    assert config_full.pool_config["custom"] == "value"


def test_asyncmy_config_with_no_pool_config() -> None:
    """Test Asyncmy config with no pool config."""
    config = AsyncmyConfig()

    # Should have empty pool_config
    assert config.pool_config == {}

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)
    assert config.default_row_type is dict


def test_asyncmy_config_initialization() -> None:
    """Test Asyncmy config initialization."""
    # Test with default parameters
    pool_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = AsyncmyConfig(pool_config=pool_config)
    assert isinstance(config.statement_config, SQLConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig()
    config = AsyncmyConfig(pool_config=pool_config, statement_config=custom_statement_config)
    assert config.statement_config is custom_statement_config


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_config_provide_session(mysql_service: MySQLService) -> None:
    """Test Asyncmy config provide_session context manager."""

    pool_config = {
        "host": mysql_service.host,
        "port": mysql_service.port,
        "user": mysql_service.user,
        "password": mysql_service.password,
        "database": mysql_service.db,
    }
    config = AsyncmyConfig(pool_config=pool_config)

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, AsyncmyDriver)
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("pyformat_positional",)
        assert session.config.default_parameter_style == "pyformat_positional"


def test_asyncmy_config_driver_type() -> None:
    """Test Asyncmy config driver_type property."""
    pool_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.driver_type is AsyncmyDriver


def test_asyncmy_config_is_async() -> None:
    """Test Asyncmy config is_async attribute."""
    pool_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.is_async is True
    assert AsyncmyConfig.is_async is True


def test_asyncmy_config_supports_connection_pooling() -> None:
    """Test Asyncmy config supports_connection_pooling attribute."""
    pool_config = {
        "host": "localhost",
        "port": 3306,
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
    }
    config = AsyncmyConfig(pool_config=pool_config)
    assert config.supports_connection_pooling is True
    assert AsyncmyConfig.supports_connection_pooling is True
