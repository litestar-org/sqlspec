"""Unit tests for SQLite configuration."""

from unittest.mock import Mock, patch

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteConnectionConfig, SqliteDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_sqlite_connection_config_creation() -> None:
    """Test SQLite connection config creation with valid parameters."""
    # Test basic config creation
    config = SqliteConnectionConfig(database=":memory:")
    assert config["database"] == ":memory:"

    # Test with all parameters
    config_full = SqliteConnectionConfig(
        database="/tmp/test.db",
        timeout=30.0,
        detect_types=0,
        isolation_level="DEFERRED",
        check_same_thread=False,
        cached_statements=100,
        uri=True,
    )
    assert config_full["database"] == "/tmp/test.db"
    assert config_full.get("timeout") == 30.0
    assert config_full.get("detect_types") == 0
    assert config_full.get("isolation_level") == "DEFERRED"
    assert config_full.get("check_same_thread") is False
    assert config_full.get("cached_statements") == 100
    assert config_full.get("uri") is True


def test_sqlite_connection_config_validation() -> None:
    """Test SQLite connection config parameter validation."""
    # Test with invalid timeout type (should not raise at runtime)
    config = SqliteConnectionConfig(database=":memory:", timeout="invalid")  # type: ignore[typeddict-item]
    assert config["database"] == ":memory:"
    assert config["timeout"] == "invalid"  # type: ignore[comparison-overlap]  # pyright: ignore
    # Test with invalid detect_types
    config2 = SqliteConnectionConfig(database=":memory:", detect_types="invalid")  # type: ignore[typeddict-item]
    assert config2["detect_types"] == "invalid"  # type: ignore[comparison-overlap]  # pyright: ignore


def test_sqlite_config_initialization() -> None:
    """Test SQLite config initialization."""
    # Test with default parameters
    connection_config = SqliteConnectionConfig(database=":memory:")
    config = SqliteConfig(connection_config=connection_config)
    assert config.connection_config["database"] == ":memory:"
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)

    # Test with custom parameters
    custom_connection_config = SqliteConnectionConfig(
        database="/tmp/custom.db",
        timeout=60.0,
    )
    custom_statement_config = SQLConfig()
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = SqliteConfig(
        connection_config=custom_connection_config,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.connection_config["database"] == "/tmp/custom.db"
    assert config.connection_config.get("timeout") == 60.0
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_sqlite_config_connection_creation(mock_connect: Mock) -> None:
    """Test SQLite config connection creation (mocked)."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    connection_config = SqliteConnectionConfig(
        database="/tmp/test.db",
        timeout=30.0,
    )
    config = SqliteConfig(connection_config=connection_config)

    connection = config.create_connection()

    # Verify connection creation was called with correct parameters
    mock_connect.assert_called_once_with(
        database="/tmp/test.db",
        timeout=30.0,
    )
    assert connection is mock_connection


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_sqlite_config_provide_connection(mock_connect: Mock) -> None:
    """Test SQLite config provide_connection context manager."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    connection_config = SqliteConnectionConfig(database=":memory:")
    config = SqliteConfig(connection_config=connection_config)

    # Test context manager behavior
    with config.provide_connection() as conn:
        assert conn is mock_connection
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_sqlite_config_provide_connection_error_handling(mock_connect: Mock) -> None:
    """Test SQLite config provide_connection error handling."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    connection_config = SqliteConnectionConfig(database=":memory:")
    config = SqliteConfig(connection_config=connection_config)

    # Test error handling and cleanup
    with pytest.raises(ValueError):
        with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Verify connection was still closed despite error
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_sqlite_config_provide_session(mock_connect: Mock) -> None:
    """Test SQLite config provide_session context manager."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    connection_config = SqliteConnectionConfig(database=":memory:")
    config = SqliteConfig(connection_config=connection_config)

    # Test session context manager behavior
    with config.provide_session() as session:
        assert isinstance(session, SqliteDriver)
        assert session.connection is mock_connection
        assert session.config is config.statement_config
        assert session.instrumentation_config is config.instrumentation
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_sqlite_config_connection_config_dict() -> None:
    """Test SQLite config connection_config_dict property."""
    connection_config = SqliteConnectionConfig(
        database="/tmp/test.db",
        timeout=30.0,
        check_same_thread=False,
    )
    config = SqliteConfig(connection_config=connection_config)

    config_dict = config.connection_config_dict
    expected = {
        "database": "/tmp/test.db",
        "timeout": 30.0,
        "check_same_thread": False,
    }

    # Check that all expected keys are present
    for key, value in expected.items():
        assert config_dict[key] == value


def test_sqlite_config_driver_type() -> None:
    """Test SQLite config driver_type property."""
    connection_config = SqliteConnectionConfig(database=":memory:")
    config = SqliteConfig(connection_config=connection_config)
    assert config.driver_type is SqliteDriver


def test_sqlite_config_connection_type() -> None:
    """Test SQLite config connection_type property."""
    connection_config = SqliteConnectionConfig(database=":memory:")
    config = SqliteConfig(connection_config=connection_config)
    # SQLite uses the standard library sqlite3.Connection
    from sqlite3 import Connection

    assert config.connection_type is Connection


def test_sqlite_config_file_database_path() -> None:
    """Test SQLite config with file database path."""
    test_path = "/tmp/test_database.db"
    connection_config = SqliteConnectionConfig(database=test_path)
    config = SqliteConfig(connection_config=connection_config)
    assert config.connection_config["database"] == test_path


def test_sqlite_config_memory_database() -> None:
    """Test SQLite config with in-memory database."""
    connection_config = SqliteConnectionConfig(database=":memory:")
    config = SqliteConfig(connection_config=connection_config)
    assert config.connection_config["database"] == ":memory:"


def test_sqlite_config_uri_database() -> None:
    """Test SQLite config with URI database."""
    uri = "file:test.db?mode=memory&cache=shared"
    connection_config = SqliteConnectionConfig(database=uri, uri=True)
    config = SqliteConfig(connection_config=connection_config)
    assert config.connection_config["database"] == uri
    assert config.connection_config.get("uri") is True


def test_sqlite_config_isolation_levels() -> None:
    """Test SQLite config with different isolation levels."""
    for isolation_level in [None, "DEFERRED", "IMMEDIATE", "EXCLUSIVE"]:
        connection_config = SqliteConnectionConfig(
            database=":memory:",
            isolation_level=isolation_level,
        )
        config = SqliteConfig(connection_config=connection_config)
        assert config.connection_config.get("isolation_level") == isolation_level


def test_sqlite_config_detect_types() -> None:
    """Test SQLite config with detect_types parameter."""
    import sqlite3

    for detect_types in [0, sqlite3.PARSE_DECLTYPES, sqlite3.PARSE_COLNAMES]:
        connection_config = SqliteConnectionConfig(
            database=":memory:",
            detect_types=detect_types,
        )
        config = SqliteConfig(connection_config=connection_config)
        assert config.connection_config.get("detect_types") == detect_types
