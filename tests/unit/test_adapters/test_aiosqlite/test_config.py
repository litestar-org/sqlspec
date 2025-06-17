"""Unit tests for Aiosqlite configuration."""

import pytest

from sqlspec.adapters.aiosqlite import CONNECTION_FIELDS, AiosqliteConfig, AiosqliteDriver
from sqlspec.statement.sql import SQLConfig


def test_aiosqlite_field_constants() -> None:
    """Test Aiosqlite CONNECTION_FIELDS constants."""
    expected_connection_fields = {
        "database",
        "timeout",
        "detect_types",
        "isolation_level",
        "check_same_thread",
        "cached_statements",
        "uri",
    }
    assert CONNECTION_FIELDS == expected_connection_fields


def test_aiosqlite_config_basic_creation() -> None:
    """Test Aiosqlite config creation with basic parameters."""
    # Test minimal config creation
    config = AiosqliteConfig(database=":memory:")
    assert config.database == ":memory:"

    # Test with all parameters
    config_full = AiosqliteConfig(database=":memory:", custom="value")
    assert config.database == ":memory:"
    assert config_full.extras["custom"] == "value"


def test_aiosqlite_config_extras_handling() -> None:
    """Test Aiosqlite config extras parameter handling."""
    # Test with kwargs going to extras
    config = AiosqliteConfig(database=":memory:", custom_param="value", debug=True)
    assert config.extras["custom_param"] == "value"
    assert config.extras["debug"] is True

    # Test with kwargs going to extras
    config2 = AiosqliteConfig(database=":memory:", unknown_param="test", another_param=42)
    assert config2.extras["unknown_param"] == "test"
    assert config2.extras["another_param"] == 42


def test_aiosqlite_config_initialization() -> None:
    """Test Aiosqlite config initialization."""
    # Test with default parameters
    config = AiosqliteConfig(database=":memory:")
    assert isinstance(config.statement_config, SQLConfig)
    # Test with custom parameters
    custom_statement_config = SQLConfig()
    config = AiosqliteConfig(database=":memory:", statement_config=custom_statement_config)
    assert config.statement_config is custom_statement_config


@pytest.mark.asyncio
async def test_aiosqlite_config_provide_session() -> None:
    """Test Aiosqlite config provide_session context manager."""
    config = AiosqliteConfig(database=":memory:")

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, AiosqliteDriver)
        # Check that parameter styles were set
        assert session.config.allowed_parameter_styles == ("qmark", "named_colon")
        assert session.config.target_parameter_style == "qmark"


def test_aiosqlite_config_driver_type() -> None:
    """Test Aiosqlite config driver_type property."""
    config = AiosqliteConfig(database=":memory:")
    assert config.driver_type is AiosqliteDriver


def test_aiosqlite_config_is_async() -> None:
    """Test Aiosqlite config is_async attribute."""
    config = AiosqliteConfig(database=":memory:")
    assert config.is_async is True
    assert AiosqliteConfig.is_async is True


def test_aiosqlite_config_supports_connection_pooling() -> None:
    """Test Aiosqlite config supports_connection_pooling attribute."""
    config = AiosqliteConfig(database=":memory:")
    assert config.supports_connection_pooling is False
    assert AiosqliteConfig.supports_connection_pooling is False


def test_aiosqlite_config_from_connection_config() -> None:
    """Test Aiosqlite config initialization with various parameters."""
    # Test basic initialization
    config = AiosqliteConfig(database="test_database", isolation_level="IMMEDIATE", cached_statements=100)
    assert config.database == "test_database"
    assert config.isolation_level == "IMMEDIATE"
    assert config.cached_statements == 100

    # Test with extras (passed as kwargs)
    config_extras = AiosqliteConfig(
        database="test_database", isolation_level="IMMEDIATE", unknown_param="test_value", another_param=42
    )
    assert config_extras.extras["unknown_param"] == "test_value"
    assert config_extras.extras["another_param"] == 42
