"""Unit tests for AIOSQLite configuration."""

from typing import NoReturn
from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.aiosqlite.config import AiosqliteConnectionConfig
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_aiosqlite_connection_config_creation() -> None:
    """Test AIOSQLite connection config creation with valid parameters."""
    # Test basic config creation
    config = AiosqliteConnectionConfig(
        database=":memory:",
        timeout=20.0,
        detect_types=1,
        isolation_level="DEFERRED",
        check_same_thread=False,
        cached_statements=100,
        uri=False,
    )
    assert config.get("database") == ":memory:"
    assert config.get("timeout") == 20.0
    assert config.get("detect_types") == 1
    assert config.get("isolation_level") == "DEFERRED"
    assert config.get("check_same_thread") is False
    assert config.get("cached_statements") == 100
    assert config.get("uri") is False

    # Test with minimal parameters
    config_minimal = AiosqliteConnectionConfig(database="test.db")
    assert config_minimal.get("database") == "test.db"

    # Test with file path
    config_file = AiosqliteConnectionConfig(
        database="/tmp/test.db",
        timeout=30.0,
        isolation_level="IMMEDIATE",
    )
    assert config_file.get("database") == "/tmp/test.db"
    assert config_file.get("timeout") == 30.0
    assert config_file.get("isolation_level") == "IMMEDIATE"


def test_aiosqlite_config_initialization() -> None:
    """Test AIOSQLite config initialization."""
    # Test with basic connection config
    connection_config = AiosqliteConnectionConfig(database=":memory:")
    config = AiosqliteConfig(connection_config=connection_config)

    assert config.connection_config.get("database") == ":memory:"
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)

    # Test with custom parameters
    custom_connection_config = AiosqliteConnectionConfig(
        database="custom.db",
        timeout=45.0,
        detect_types=True,
        isolation_level="EXCLUSIVE",
        check_same_thread=False,
        cached_statements=200,
    )
    custom_statement_config = SQLConfig(strict_mode=False)
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = AiosqliteConfig(
        connection_config=custom_connection_config,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.connection_config.get("database") == "custom.db"
    assert config.connection_config.get("timeout") == 45.0
    assert config.connection_config.get("detect_types") is True
    assert config.connection_config.get("isolation_level") == "EXCLUSIVE"
    assert config.connection_config.get("check_same_thread") is False
    assert config.connection_config.get("cached_statements") == 200
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


def test_aiosqlite_config_connection_config_dict() -> None:
    """Test AIOSQLite config connection_config_dict property."""
    connection_config = AiosqliteConnectionConfig(
        database=":memory:",
        timeout=20.0,
        detect_types=True,
        isolation_level="DEFERRED",
        check_same_thread=False,
    )
    config = AiosqliteConfig(connection_config=connection_config)

    config_dict = config.connection_config_dict
    expected = {
        "database": ":memory:",
        "timeout": 20.0,
        "detect_types": True,
        "isolation_level": "DEFERRED",
        "check_same_thread": False,
    }

    # Check that all expected keys are present
    for key, value in expected.items():
        assert config_dict[key] == value


@patch("aiosqlite.connect")
@pytest.mark.asyncio
async def test_aiosqlite_config_create_connection(mock_connect: Mock) -> None:
    """Test AIOSQLite config create_connection method (mocked)."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    connection_config = AiosqliteConnectionConfig(
        database=":memory:",
        timeout=20.0,
        detect_types=True,
        isolation_level="DEFERRED",
    )
    config = AiosqliteConfig(connection_config=connection_config)

    connection = await config.create_connection()

    # Verify connect was called with correct parameters
    mock_connect.assert_called_once_with(
        database=":memory:",
        timeout=20.0,
        detect_types=True,
        isolation_level="DEFERRED",
    )
    assert connection is mock_connection


@patch("aiosqlite.connect")
@pytest.mark.asyncio
async def test_aiosqlite_config_provide_connection(mock_connect: Mock) -> None:
    """Test AIOSQLite config provide_connection context manager."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    connection_config = AiosqliteConnectionConfig(database=":memory:")
    config = AiosqliteConfig(connection_config=connection_config)

    # Test context manager behavior
    async with config.provide_connection() as conn:
        assert conn is mock_connection
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


@patch("aiosqlite.connect")
@pytest.mark.asyncio
async def test_aiosqlite_config_provide_connection_error_handling(mock_connect: Mock) -> NoReturn:
    """Test AIOSQLite config provide_connection error handling."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    connection_config = AiosqliteConnectionConfig(database=":memory:")
    config = AiosqliteConfig(connection_config=connection_config)

    # Test error handling and cleanup
    with pytest.raises(ValueError):
        async with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Verify connection was still closed despite error
    mock_connection.close.assert_called_once()


@patch("aiosqlite.connect")
@pytest.mark.asyncio
async def test_aiosqlite_config_provide_session(mock_connect: Mock) -> None:
    """Test AIOSQLite config provide_session context manager."""
    mock_connection = AsyncMock()
    mock_connect.return_value = mock_connection

    connection_config = AiosqliteConnectionConfig(database=":memory:")
    config = AiosqliteConfig(connection_config=connection_config)

    # Test session context manager behavior
    async with config.provide_session() as session:
        assert isinstance(session, AiosqliteDriver)
        assert session.connection is mock_connection
        assert session.config is config.statement_config
        assert session.instrumentation_config is config.instrumentation
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_aiosqlite_config_driver_type() -> None:
    """Test AIOSQLite config driver_type property."""
    connection_config = AiosqliteConnectionConfig(database=":memory:")
    config = AiosqliteConfig(connection_config=connection_config)
    assert config.driver_type is AiosqliteDriver


def test_aiosqlite_config_connection_type() -> None:
    """Test AIOSQLite config connection_type property."""
    import aiosqlite

    connection_config = AiosqliteConnectionConfig(database=":memory:")
    config = AiosqliteConfig(connection_config=connection_config)
    assert config.connection_type is aiosqlite.Connection


def test_aiosqlite_config_is_async() -> None:
    """Test AIOSQLite config __is_async__ attribute."""
    connection_config = AiosqliteConnectionConfig(database=":memory:")
    config = AiosqliteConfig(connection_config=connection_config)
    assert config.__is_async__ is True
    assert AiosqliteConfig.__is_async__ is True


def test_aiosqlite_config_supports_connection_pooling() -> None:
    """Test AIOSQLite config __supports_connection_pooling__ attribute."""
    connection_config = AiosqliteConnectionConfig(database=":memory:")
    config = AiosqliteConfig(connection_config=connection_config)
    assert config.__supports_connection_pooling__ is False
    assert AiosqliteConfig.__supports_connection_pooling__ is False


def test_aiosqlite_config_database_paths() -> None:
    """Test AIOSQLite config with different database path types."""
    # Test in-memory database
    config_memory = AiosqliteConnectionConfig(database=":memory:")
    config = AiosqliteConfig(connection_config=config_memory)
    assert config.connection_config.get("database") == ":memory:"

    # Test file path
    config_file = AiosqliteConnectionConfig(database="/path/to/database.db")
    config = AiosqliteConfig(connection_config=config_file)
    assert config.connection_config.get("database") == "/path/to/database.db"

    # Test relative path
    config_relative = AiosqliteConnectionConfig(database="./test.db")
    config = AiosqliteConfig(connection_config=config_relative)
    assert config.connection_config.get("database") == "./test.db"


def test_aiosqlite_config_isolation_levels() -> None:
    """Test AIOSQLite config with different isolation levels."""
    isolation_levels = ["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]

    for level in isolation_levels:
        connection_config = AiosqliteConnectionConfig(
            database=":memory:",
            isolation_level=level,  # type: ignore[typeddict-item]
        )
        config = AiosqliteConfig(connection_config=connection_config)
        assert config.connection_config.get("isolation_level") == level

    # Test None isolation level separately
    connection_config_none = AiosqliteConnectionConfig(
        database=":memory:",
        isolation_level=None,
    )
    config_none = AiosqliteConfig(connection_config=connection_config_none)
    assert config_none.connection_config.get("isolation_level") is None


def test_aiosqlite_config_timeout_settings() -> None:
    """Test AIOSQLite config with timeout settings."""
    timeout_values = [5.0, 20.0, 60.0, 120.0]

    for timeout in timeout_values:
        connection_config = AiosqliteConnectionConfig(
            database=":memory:",
            timeout=timeout,
        )
        config = AiosqliteConfig(connection_config=connection_config)
        assert config.connection_config.get("timeout") == timeout


def test_aiosqlite_config_detect_types() -> None:
    """Test AIOSQLite config with detect_types settings."""
    # Test with True
    config_true = AiosqliteConnectionConfig(
        database=":memory:",
        detect_types=True,
    )
    config = AiosqliteConfig(connection_config=config_true)
    assert config.connection_config.get("detect_types") is True

    # Test with False
    config_false = AiosqliteConnectionConfig(
        database=":memory:",
        detect_types=False,
    )
    config = AiosqliteConfig(connection_config=config_false)
    assert config.connection_config.get("detect_types") is False


def test_aiosqlite_config_check_same_thread() -> None:
    """Test AIOSQLite config with check_same_thread settings."""
    # Test with True
    config_true = AiosqliteConnectionConfig(
        database=":memory:",
        check_same_thread=True,
    )
    config = AiosqliteConfig(connection_config=config_true)
    assert config.connection_config.get("check_same_thread") is True

    # Test with False (recommended for async)
    config_false = AiosqliteConnectionConfig(
        database=":memory:",
        check_same_thread=False,
    )
    config = AiosqliteConfig(connection_config=config_false)
    assert config.connection_config.get("check_same_thread") is False


def test_aiosqlite_config_cached_statements() -> None:
    """Test AIOSQLite config with cached_statements settings."""
    cache_values = [0, 50, 100, 200, 500]

    for cache_size in cache_values:
        connection_config = AiosqliteConnectionConfig(
            database=":memory:",
            cached_statements=cache_size,
        )
        config = AiosqliteConfig(connection_config=connection_config)
        assert config.connection_config.get("cached_statements") == cache_size


def test_aiosqlite_config_uri_parameter() -> None:
    """Test AIOSQLite config with URI parameter."""
    # Test with URI=True
    config_uri = AiosqliteConnectionConfig(
        database="file:memdb1?mode=memory&cache=shared",
        uri=True,
    )
    config = AiosqliteConfig(connection_config=config_uri)
    assert config.connection_config.get("uri") is True
    assert config.connection_config.get("database") == "file:memdb1?mode=memory&cache=shared"

    # Test with URI=False
    config_no_uri = AiosqliteConnectionConfig(
        database=":memory:",
        uri=False,
    )
    config = AiosqliteConfig(connection_config=config_no_uri)
    assert config.connection_config.get("uri") is False
