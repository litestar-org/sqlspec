"""Unit tests for Asyncmy configuration."""

import pytest

from sqlspec.adapters.asyncmy import CONNECTION_FIELDS, POOL_FIELDS, AsyncmyConfig, AsyncmyDriver
from sqlspec.config import InstrumentationConfig
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
        extras={"custom": "value"},
    )
    assert config.host == "localhost"
    assert config.port == 3306
    assert config.user == "test_user"
    assert config.password == "test_password"
    assert config.database == "test_db"
    assert config_full.extras["custom"] == "value"


def test_asyncmy_config_extras_handling() -> None:
    """Test Asyncmy config extras parameter handling."""
    # Test with explicit extras
    config = AsyncmyConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        extras={"custom_param": "value", "debug": True},
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
    assert isinstance(config.instrumentation, InstrumentationConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig()
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = AsyncmyConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


@pytest.mark.asyncio
async def test_asyncmy_config_provide_session() -> None:
    """Test Asyncmy config provide_session context manager."""
    config = AsyncmyConfig(host="localhost", port=3306, user="test_user", password="test_password", database="test_db")

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, AsyncmyDriver)
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("pyformat_positional",)
        assert session.config.target_parameter_style == "pyformat_positional"
        assert session.instrumentation_config is config.instrumentation


def test_asyncmy_config_driver_type() -> None:
    """Test Asyncmy config driver_type property."""
    config = AsyncmyConfig(host="localhost", port=3306, user="test_user", password="test_password", database="test_db")
    assert config.driver_type is AsyncmyDriver


def test_asyncmy_config_is_async() -> None:
    """Test Asyncmy config __is_async__ attribute."""
    config = AsyncmyConfig(host="localhost", port=3306, user="test_user", password="test_password", database="test_db")
    assert config.__is_async__ is True
    assert AsyncmyConfig.__is_async__ is True


def test_asyncmy_config_supports_connection_pooling() -> None:
    """Test Asyncmy config __supports_connection_pooling__ attribute."""
    config = AsyncmyConfig(host="localhost", port=3306, user="test_user", password="test_password", database="test_db")
    assert config.__supports_connection_pooling__ is True
    assert AsyncmyConfig.__supports_connection_pooling__ is True


def test_asyncmy_config_from_pool_config() -> None:
    """Test Asyncmy config from_pool_config backward compatibility."""
    # Test basic backward compatibility
    pool_config = {
        "port": "test_port",
        "init_command": "test_init_command",
        "local_infile": "test_local_infile",
        "pool_recycle": 10,
        "minsize": 10,
    }
    config = AsyncmyConfig.from_pool_config(pool_config)
    # Add specific assertions based on fields
    assert config.extras == {}

    # Test with extra parameters
    pool_config_with_extras = {
        "port": "test_port",
        "init_command": "test_init_command",
        "unknown_param": "test_value",
        "another_param": 42,
    }
    config_extras = AsyncmyConfig.from_pool_config(pool_config_with_extras)
    assert config_extras.extras["unknown_param"] == "test_value"
    assert config_extras.extras["another_param"] == 42
