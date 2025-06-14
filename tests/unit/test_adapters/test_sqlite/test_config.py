"""Unit tests for SQLite configuration."""

from unittest.mock import Mock, patch

import pytest

from sqlspec.adapters.sqlite import CONNECTION_FIELDS, SqliteConfig, SqliteDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQLConfig


def test_sqlite_connection_fields_constant() -> None:
    """Test SQLite CONNECTION_FIELDS constant definition."""
    expected_fields = frozenset(
        {
            "database",
            "timeout",
            "detect_types",
            "isolation_level",
            "check_same_thread",
            "factory",
            "cached_statements",
            "uri",
        }
    )
    assert CONNECTION_FIELDS == expected_fields


def test_sqlite_config_basic_creation() -> None:
    """Test SQLite config creation with basic parameters."""
    # Test minimal config creation
    config = SqliteConfig(database=":memory:")
    assert config.database == ":memory:"
    assert config.timeout is None
    assert config.extras == {}

    # Test with all parameters
    config_full = SqliteConfig(
        database="/tmp/test.db",
        timeout=30.0,
        detect_types=0,
        isolation_level="DEFERRED",
        check_same_thread=False,
        cached_statements=100,
        uri=True,
    )
    assert config_full.database == "/tmp/test.db"
    assert config_full.timeout == 30.0
    assert config_full.detect_types == 0
    assert config_full.isolation_level == "DEFERRED"
    assert config_full.check_same_thread is False
    assert config_full.cached_statements == 100
    assert config_full.uri is True


def test_sqlite_config_extras_handling() -> None:
    """Test SQLite config extras parameter handling."""
    # Test with explicit extras
    config = SqliteConfig(database=":memory:", extras={"custom_param": "value", "debug": True})
    assert config.extras["custom_param"] == "value"
    assert config.extras["debug"] is True

    # Test with kwargs going to extras
    config2 = SqliteConfig(database=":memory:", unknown_param="test", another_param=42)
    assert config2.extras["unknown_param"] == "test"
    assert config2.extras["another_param"] == 42


def test_sqlite_config_initialization() -> None:
    """Test SQLite config initialization."""
    # Test with default parameters
    config = SqliteConfig(database=":memory:")
    assert config.database == ":memory:"
    assert isinstance(config.statement_config, SQLConfig)
    assert isinstance(config.instrumentation, InstrumentationConfig)

    # Test with custom parameters
    custom_statement_config = SQLConfig()
    custom_instrumentation = InstrumentationConfig(log_queries=True)

    config = SqliteConfig(
        database="/tmp/custom.db",
        timeout=60.0,
        statement_config=custom_statement_config,
        instrumentation=custom_instrumentation,
    )
    assert config.database == "/tmp/custom.db"
    assert config.timeout == 60.0
    assert config.statement_config is custom_statement_config
    assert config.instrumentation.log_queries is True


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_sqlite_config_connection_creation(mock_connect: Mock) -> None:
    """Test SQLite config connection creation (mocked)."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    config = SqliteConfig(database="/tmp/test.db", timeout=30.0)

    connection = config.create_connection()

    # Verify connection creation was called with correct parameters
    mock_connect.assert_called_once_with(database="/tmp/test.db", timeout=30.0)
    assert connection is mock_connection


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_sqlite_config_provide_connection(mock_connect: Mock) -> None:
    """Test SQLite config provide_connection context manager."""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection

    config = SqliteConfig(database=":memory:")

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

    config = SqliteConfig(database=":memory:")

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

    config = SqliteConfig(database=":memory:")

    # Test session context manager behavior
    with config.provide_session() as session:
        assert isinstance(session, SqliteDriver)
        assert session.connection is mock_connection
        # Check that parameter style settings were injected
        assert session.config.allowed_parameter_styles == ("qmark", "named_colon")
        assert session.config.target_parameter_style == "qmark"
        assert session.instrumentation_config is config.instrumentation
        # Verify connection is not closed yet
        mock_connection.close.assert_not_called()

    # Verify connection was closed after context exit
    mock_connection.close.assert_called_once()


def test_sqlite_config_connection_config_dict() -> None:
    """Test SQLite config connection_config_dict property."""
    config = SqliteConfig(database="/tmp/test.db", timeout=30.0, check_same_thread=False)

    config_dict = config.connection_config_dict
    expected = {"database": "/tmp/test.db", "timeout": 30.0, "check_same_thread": False}

    # Check that all expected keys are present
    for key, value in expected.items():
        assert config_dict[key] == value


def test_sqlite_config_driver_type() -> None:
    """Test SQLite config driver_type property."""
    config = SqliteConfig(database=":memory:")
    assert config.driver_type is SqliteDriver


def test_sqlite_config_connection_type() -> None:
    """Test SQLite config connection_type property."""
    config = SqliteConfig(database=":memory:")
    # SQLite uses the standard library sqlite3.Connection
    from sqlite3 import Connection

    assert config.connection_type is Connection


def test_sqlite_config_file_database_path() -> None:
    """Test SQLite config with file database path."""
    test_path = "/tmp/test_database.db"
    config = SqliteConfig(database=test_path)
    assert config.database == test_path


def test_sqlite_config_memory_database() -> None:
    """Test SQLite config with in-memory database."""
    config = SqliteConfig(database=":memory:")
    assert config.database == ":memory:"


def test_sqlite_config_uri_database() -> None:
    """Test SQLite config with URI database."""
    uri = "file:test.db?mode=memory&cache=shared"
    config = SqliteConfig(database=uri, uri=True)
    assert config.database == uri
    assert config.uri is True


def test_sqlite_config_isolation_levels() -> None:
    """Test SQLite config with different isolation levels."""
    for isolation_level in [None, "DEFERRED", "IMMEDIATE", "EXCLUSIVE"]:
        config = SqliteConfig(database=":memory:", isolation_level=isolation_level)
        assert config.isolation_level == isolation_level


def test_sqlite_config_detect_types() -> None:
    """Test SQLite config with detect_types parameter."""
    import sqlite3

    for detect_types in [0, sqlite3.PARSE_DECLTYPES, sqlite3.PARSE_COLNAMES]:
        config = SqliteConfig(database=":memory:", detect_types=detect_types)
        assert config.detect_types == detect_types


def test_sqlite_config_from_connection_config() -> None:
    """Test SQLite config creation from old-style connection_config for backward compatibility."""
    # Test basic backward compatibility
    connection_config = {"database": "/tmp/test.db", "timeout": 30.0, "check_same_thread": False}
    config = SqliteConfig.from_connection_config(connection_config)
    assert config.database == "/tmp/test.db"
    assert config.timeout == 30.0
    assert config.check_same_thread is False
    assert config.extras == {}

    # Test with extra parameters
    connection_config_with_extras = {
        "database": ":memory:",
        "timeout": 60.0,
        "unknown_param": "test_value",
        "another_param": 42,
    }
    config_extras = SqliteConfig.from_connection_config(connection_config_with_extras)
    assert config_extras.database == ":memory:"
    assert config_extras.timeout == 60.0
    assert config_extras.extras["unknown_param"] == "test_value"
    assert config_extras.extras["another_param"] == 42

    # Test missing database parameter
    with pytest.raises(ValueError, match="database parameter is required"):
        SqliteConfig.from_connection_config({"timeout": 30.0})
