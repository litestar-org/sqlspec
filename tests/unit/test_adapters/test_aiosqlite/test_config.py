"""Unit tests for Aiosqlite configuration."""

import logging
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteConnectionParams, AiosqliteDriver
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    pass


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
        assert session.config is not None
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
    assert config.supports_connection_pooling is True
    assert AiosqliteConfig.supports_connection_pooling is True


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


# Memory Database Detection Tests
def test_is_memory_database() -> None:
    """Test memory database detection logic."""
    config = AiosqliteConfig()

    # Test standard :memory: database
    assert config._is_memory_database(":memory:") is True

    # Test empty string
    assert config._is_memory_database("") is True

    # Test None (though shouldn't happen in practice)
    assert config._is_memory_database(None) is True  # type: ignore[arg-type]

    # Test file::memory: without shared cache
    assert config._is_memory_database("file::memory:") is True
    assert config._is_memory_database("file::memory:?mode=memory") is True

    # Test shared memory (should NOT be detected as problematic)
    assert config._is_memory_database("file::memory:?cache=shared") is False
    assert config._is_memory_database("file::memory:?mode=memory&cache=shared") is False

    # Test regular file databases
    assert config._is_memory_database("test.db") is False
    assert config._is_memory_database("/path/to/database.db") is False
    assert config._is_memory_database("file:test.db") is False


@pytest.mark.parametrize(
    "database,uri,expected_min,expected_max,should_warn",
    [
        (":memory:", None, 1, 1, True),
        ("", None, 1, 1, True),
        ("file::memory:", True, 1, 1, True),
        ("file::memory:?cache=shared", True, 5, 20, False),
        ("test.db", None, 5, 20, False),
        ("/tmp/test.db", None, 3, 10, False),
    ],
    ids=["memory", "empty", "uri_memory", "shared_memory", "file", "absolute_path"],
)
def test_memory_database_pooling_override(
    database: str,
    uri: "bool | None",
    expected_min: int,
    expected_max: int,
    should_warn: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that memory databases override pool sizes."""
    connection_config = {"database": database}
    if uri is not None:
        connection_config["uri"] = uri  # type: ignore[assignment]

    # Clear any previous log records
    caplog.clear()
    caplog.set_level(logging.WARNING)

    # Create config with explicit pool sizes
    config = AiosqliteConfig(
        connection_config=connection_config,
        min_pool=expected_min if not should_warn else 5,
        max_pool=expected_max if not should_warn else 20,
    )

    # Check pool sizes
    assert config.min_pool == expected_min
    assert config.max_pool == expected_max

    # Check warning
    if should_warn:
        assert "In-memory SQLite database detected" in caplog.text
        assert "Disabling connection pooling" in caplog.text
    else:
        assert "In-memory SQLite database detected" not in caplog.text


@pytest.mark.asyncio
async def test_connection_health_check() -> None:
    """Test connection health check functionality."""
    # This test is for the connection pool class
    from sqlspec.adapters.aiosqlite.config import AiosqliteConnectionPool

    # Create a pool with file database
    pool = AiosqliteConnectionPool(connection_params={"database": "test.db"}, min_pool=1, max_pool=5)

    mock_connection = AsyncMock()

    # Test healthy connection
    mock_connection.execute = AsyncMock(return_value=None)
    assert await pool._is_connection_alive(mock_connection) is True
    mock_connection.execute.assert_called_once_with("SELECT 1")

    # Test unhealthy connection (execute fails)
    mock_connection.execute = AsyncMock(side_effect=Exception("Connection error"))
    assert await pool._is_connection_alive(mock_connection) is False
