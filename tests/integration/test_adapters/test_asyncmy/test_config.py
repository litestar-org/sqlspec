"""Unit tests for Asyncmy configuration."""

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import CONNECTION_FIELDS, POOL_FIELDS, AsyncmyConfig, AsyncmyDriver
from sqlspec.statement.sql import SQLConfig


def test_asyncmy_field_constants() -> None:
    """Test Asyncmy CONNECTION_FIELDS and POOL_FIELDS constants."""
    expected_connection_fields = {
        "host",
        "user",
        "password",
        "database",
        "port",
        "unix_socket",
        "charset",
        "connect_timeout",
        "read_default_file",
        "read_default_group",
        "autocommit",
        "local_infile",
        "ssl",
        "sql_mode",
        "init_command",
        "cursor_class",
    }
    assert CONNECTION_FIELDS == expected_connection_fields

    # POOL_FIELDS should be a superset of CONNECTION_FIELDS
    assert CONNECTION_FIELDS.issubset(POOL_FIELDS)

    # Check pool-specific fields
    pool_specific = POOL_FIELDS - CONNECTION_FIELDS
    expected_pool_specific = {"minsize", "maxsize", "echo", "pool_recycle"}
    assert pool_specific == expected_pool_specific


def test_asyncmy_config_basic_creation() -> None:
    """Test Asyncmy config creation with basic parameters."""
    # Test minimal config creation
    config = AsyncmyConfig(host="localhost", port=3306, user="test_user", password="test_password", database="test_db")
    assert config.host == "localhost"
    assert config.port == 3306
    assert config.user == "test_user"
    assert config.password == "test_password"
    assert config.database == "test_db"

    # Test with all parameters
    config_full = AsyncmyConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        custom="value",  # Additional parameters are stored in extras
    )
    assert config_full.host == "localhost"
    assert config_full.port == 3306
    assert config_full.user == "test_user"
    assert config_full.password == "test_password"
    assert config_full.database == "test_db"
    assert config_full.extras["custom"] == "value"


def test_asyncmy_config_extras_handling() -> None:
    """Test Asyncmy config extras parameter handling."""
    # Test with kwargs going to extras
    config = AsyncmyConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        custom_param="value",
        debug=True,
    )
    assert config.extras["custom_param"] == "value"
    assert config.extras["debug"] is True

    # Test with kwargs going to extras
    config2 = AsyncmyConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        unknown_param="test",
        another_param=42,
    )
    assert config2.extras["unknown_param"] == "test"
    assert config2.extras["another_param"] == 42


def test_asyncmy_config_initialization() -> None:
    """Test Asyncmy config initialization."""
    # Test with default parameters
    config = AsyncmyConfig(host="localhost", port=3306, user="test_user", password="test_password", database="test_db")
    assert isinstance(config.statement_config, SQLConfig)
    # Test with custom parameters
    custom_statement_config = SQLConfig()

    config = AsyncmyConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        statement_config=custom_statement_config,
    )
    assert config.statement_config is custom_statement_config


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_config_provide_session(mysql_service: MySQLService) -> None:
    """Test Asyncmy config provide_session context manager."""

    config = AsyncmyConfig(
        host=mysql_service.host,
        port=mysql_service.port,
        user=mysql_service.user,
        password=mysql_service.password,
        database=mysql_service.db,
    )

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, AsyncmyDriver)
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("pyformat_positional",)
        assert session.config.target_parameter_style == "pyformat_positional"


def test_asyncmy_config_driver_type() -> None:
    """Test Asyncmy config driver_type property."""
    config = AsyncmyConfig(host="localhost", port=3306, user="test_user", password="test_password", database="test_db")
    assert config.driver_type is AsyncmyDriver


def test_asyncmy_config_is_async() -> None:
    """Test Asyncmy config is_async attribute."""
    config = AsyncmyConfig(host="localhost", port=3306, user="test_user", password="test_password", database="test_db")
    assert config.is_async is True
    assert AsyncmyConfig.is_async is True


def test_asyncmy_config_supports_connection_pooling() -> None:
    """Test Asyncmy config supports_connection_pooling attribute."""
    config = AsyncmyConfig(host="localhost", port=3306, user="test_user", password="test_password", database="test_db")
    assert config.supports_connection_pooling is True
    assert AsyncmyConfig.supports_connection_pooling is True
