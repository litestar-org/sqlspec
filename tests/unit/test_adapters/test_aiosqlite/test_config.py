"""Unit tests for Aiosqlite configuration."""

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteConnectionParams, AiosqliteDriver
from sqlspec.statement.sql import SQLConfig


def test_aiosqlite_typed_dict_structure() -> None:
    """Test Aiosqlite TypedDict structure."""
    # Test that we can create valid connection params
    connection_params: AiosqliteConnectionParams = {
        "database": "test.db",
        "timeout": 30.0,
        "detect_types": 0,
        "isolation_level": "DEFERRED",
        "check_same_thread": True,
        "cached_statements": 128,
        "uri": False,
    }
    assert connection_params["database"] == "test.db"
    assert connection_params["timeout"] == 30.0
    assert connection_params["detect_types"] == 0
    assert connection_params["isolation_level"] == "DEFERRED"
    assert connection_params["check_same_thread"] is True
    assert connection_params["cached_statements"] == 128
    assert connection_params["uri"] is False


def test_aiosqlite_config_basic_creation() -> None:
    """Test Aiosqlite config creation with basic parameters."""
    # Test minimal config creation
    config = AiosqliteConfig()
    assert config.connection_config["database"] == ":memory:"

    # Test with all parameters including extra
    config_full = AiosqliteConfig(connection_config={"database": "test.db", "extra": {"custom": "value"}})
    assert config_full.connection_config["database"] == "test.db"
    assert config_full.connection_config["custom"] == "value"


def test_aiosqlite_config_extras_handling() -> None:
    """Test Aiosqlite config extras parameter handling."""
    # Test with extra parameter
    config = AiosqliteConfig(
        connection_config={"database": ":memory:", "extra": {"custom_param": "value", "debug": True}}
    )
    assert config.connection_config["custom_param"] == "value"
    assert config.connection_config["debug"] is True

    # Test with more extra params
    config2 = AiosqliteConfig(
        connection_config={"database": ":memory:", "extra": {"unknown_param": "test", "another_param": 42}}
    )
    assert config2.connection_config["unknown_param"] == "test"
    assert config2.connection_config["another_param"] == 42


def test_aiosqlite_config_initialization() -> None:
    """Test Aiosqlite config initialization."""
    # Test with default parameters
    config = AiosqliteConfig()
    assert isinstance(config.statement_config, SQLConfig)
    # Test with custom parameters
    custom_statement_config = SQLConfig()
    config = AiosqliteConfig(connection_config={"database": ":memory:"}, statement_config=custom_statement_config)
    assert config.statement_config is custom_statement_config


@pytest.mark.asyncio
async def test_aiosqlite_config_provide_session() -> None:
    """Test Aiosqlite config provide_session context manager."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, AiosqliteDriver)
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("qmark", "named_colon")
        assert session.config.default_parameter_style == "qmark"


def test_aiosqlite_config_driver_type() -> None:
    """Test Aiosqlite config driver_type property."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    assert config.driver_type is AiosqliteDriver


def test_aiosqlite_config_is_async() -> None:
    """Test Aiosqlite config is_async attribute."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    assert config.is_async is True
    assert AiosqliteConfig.is_async is True


def test_aiosqlite_config_supports_connection_pooling() -> None:
    """Test Aiosqlite config supports_connection_pooling attribute."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    assert config.supports_connection_pooling is False
    assert AiosqliteConfig.supports_connection_pooling is False


def test_aiosqlite_config_from_connection_config() -> None:
    """Test Aiosqlite config initialization with various parameters."""
    # Test basic initialization
    config = AiosqliteConfig(
        connection_config={"database": "test_database", "isolation_level": "IMMEDIATE", "cached_statements": 100}
    )
    assert config.connection_config["database"] == "test_database"
    assert config.connection_config["isolation_level"] == "IMMEDIATE"
    assert config.connection_config["cached_statements"] == 100

    # Test with extras (passed via extra in connection_config)
    config_extras = AiosqliteConfig(
        connection_config={
            "database": "test_database",
            "isolation_level": "IMMEDIATE",
            "extra": {"unknown_param": "test_value", "another_param": 42},
        }
    )
    assert config_extras.connection_config["unknown_param"] == "test_value"
    assert config_extras.connection_config["another_param"] == 42
