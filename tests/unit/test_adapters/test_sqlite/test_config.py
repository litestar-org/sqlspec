"""Unit tests for SQLite configuration.

This module tests the SqliteConfig class including:
- Basic configuration initialization
- Connection parameter handling
- Context manager behavior
- Backward compatibility
- Error handling
- Property accessors
"""

import sqlite3
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteConnectionParams, SqliteDriver
from sqlspec.statement.sql import SQLConfig

if TYPE_CHECKING:
    pass


# TypedDict Tests
def test_sqlite_connection_params_typeddict() -> None:
    """Test SqliteConnectionParams TypedDict accepts all expected fields."""
    # This test validates that all expected fields are accepted by the TypedDict
    connection_params: SqliteConnectionParams = {
        "database": ":memory:",
        "timeout": 30.0,
        "detect_types": 0,
        "isolation_level": "DEFERRED",
        "check_same_thread": False,
        "factory": None,
        "cached_statements": 100,
        "uri": True,
    }

    # Create config with the TypedDict - :memory: will be auto-converted
    config = SqliteConfig(connection_config=connection_params)
    expected_params = dict(connection_params)
    expected_params["database"] = "file::memory:?cache=shared"
    expected_params["uri"] = True
    assert config.connection_config == expected_params


# Initialization Tests
@pytest.mark.parametrize(
    "connection_config,expected_config",
    [
        ({"database": ":memory:"}, {"database": "file::memory:?cache=shared", "uri": True}),
        (
            {
                "database": "/tmp/test.db",
                "timeout": 30.0,
                "detect_types": sqlite3.PARSE_DECLTYPES,
                "isolation_level": "DEFERRED",
                "check_same_thread": False,
                "cached_statements": 100,
                "uri": True,
            },
            {
                "database": "/tmp/test.db",
                "timeout": 30.0,
                "detect_types": sqlite3.PARSE_DECLTYPES,
                "isolation_level": "DEFERRED",
                "check_same_thread": False,
                "cached_statements": 100,
                "uri": True,
            },
        ),
    ],
    ids=["minimal", "full"],
)
def test_config_initialization(connection_config: dict[str, Any], expected_config: dict[str, Any]) -> None:
    """Test config initialization with various parameters."""
    config = SqliteConfig(connection_config=connection_config)

    # Check that the connection_config is stored properly
    assert config.connection_config == expected_config

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)


def test_default_connection_config() -> None:
    """Test default connection config when none is provided."""
    config = SqliteConfig()
    assert config.connection_config == {"database": "file::memory:?cache=shared", "uri": True}


@pytest.mark.parametrize(
    "statement_config,expected_type",
    [(None, SQLConfig), (SQLConfig(), SQLConfig), (SQLConfig(parse_errors_as_warnings=False), SQLConfig)],
    ids=["default", "empty", "custom"],
)
def test_statement_config_initialization(statement_config: "SQLConfig | None", expected_type: type[SQLConfig]) -> None:
    """Test statement config initialization."""
    config = SqliteConfig(connection_config={"database": ":memory:"}, statement_config=statement_config)
    assert isinstance(config.statement_config, expected_type)

    if statement_config is not None:
        assert config.statement_config is statement_config


# Connection Creation Tests
@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_create_connection(mock_connect: MagicMock) -> None:
    """Test connection creation."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = SqliteConfig(connection_config={"database": "/tmp/test.db", "timeout": 30.0})
    connection = config.create_connection()

    # Verify connection creation (None values should be filtered out)
    mock_connect.assert_called_once_with(database="/tmp/test.db", timeout=30.0)
    assert connection is mock_connection

    # Verify row factory was set
    assert mock_connection.row_factory == sqlite3.Row


# Context Manager Tests
@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_provide_connection_success(mock_connect: MagicMock) -> None:
    """Test provide_connection context manager normal flow."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    # Both file and memory databases now use pooling
    config = SqliteConfig(connection_config={"database": "test.db"})

    with config.provide_connection() as conn:
        assert conn is mock_connection
        # With pooling, close is not called directly
        mock_connection.close.assert_not_called()

    # Pool manages connection lifecycle - connection is returned to pool, not closed
    mock_connection.close.assert_not_called()


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_provide_connection_error_handling(mock_connect: MagicMock) -> None:
    """Test provide_connection context manager error handling."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = SqliteConfig(connection_config={"database": "test.db"})

    with pytest.raises(ValueError, match="Test error"):
        with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # With pooling, connection is returned to pool even on error
    mock_connection.close.assert_not_called()


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_provide_session(mock_connect: MagicMock) -> None:
    """Test provide_session context manager."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = SqliteConfig(connection_config={"database": "test.db"})

    with config.provide_session() as session:
        assert isinstance(session, SqliteDriver)
        assert session.connection is mock_connection

        # Check parameter style injection
        assert session.config is not None
        assert session.config.allowed_parameter_styles == ("qmark", "named_colon")
        assert session.config.default_parameter_style == "qmark"

        mock_connection.close.assert_not_called()

    # With pooling, connection is returned to pool, not closed
    mock_connection.close.assert_not_called()


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_provide_session_with_custom_config(mock_connect: MagicMock) -> None:
    """Test provide_session with custom statement config."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    # Custom statement config with parameter styles already set
    custom_config = SQLConfig(allowed_parameter_styles=("qmark",), default_parameter_style="qmark")
    config = SqliteConfig(connection_config={"database": ":memory:"}, statement_config=custom_config)

    with config.provide_session() as session:
        # Should use the custom config's parameter styles
        assert session.config is not None
        assert session.config.allowed_parameter_styles == ("qmark",)
        assert session.config.default_parameter_style == "qmark"


# Property Tests
@pytest.mark.parametrize(
    "connection_config,expected_dict",
    [
        ({"database": ":memory:"}, {"database": "file::memory:?cache=shared", "uri": True}),
        (
            {"database": "/tmp/test.db", "timeout": 30.0, "check_same_thread": False, "isolation_level": "DEFERRED"},
            {"database": "/tmp/test.db", "timeout": 30.0, "isolation_level": "DEFERRED", "check_same_thread": False},
        ),
    ],
    ids=["minimal", "partial"],
)
def test_connection_config_storage(connection_config: dict[str, Any], expected_dict: dict[str, Any]) -> None:
    """Test connection_config storage."""
    config = SqliteConfig(connection_config=connection_config)
    assert config.connection_config == expected_dict


def test_driver_type() -> None:
    """Test driver_type class attribute."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    assert config.driver_type is SqliteDriver


def test_connection_type() -> None:
    """Test connection_type class attribute."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    assert config.connection_type is sqlite3.Connection


# Database Path Tests
@pytest.mark.parametrize(
    "database,uri,description",
    [
        ("/tmp/test_database.db", None, "file_path"),
        (":memory:", None, "memory"),
        ("file:test.db?mode=memory&cache=shared", True, "uri_mode"),
        ("file:///absolute/path/test.db", True, "uri_absolute"),
    ],
    ids=["file", "memory", "uri_with_params", "uri_absolute"],
)
def test_database_paths(database: str, uri: "bool | None", description: str) -> None:
    """Test various database path configurations."""
    connection_config = {"database": database}
    if uri is not None:
        connection_config["uri"] = uri  # type: ignore[assignment]

    config = SqliteConfig(connection_config=connection_config)
    
    # Memory databases get auto-converted to shared memory for pooling
    if database == ":memory:":
        assert config.connection_config["database"] == "file::memory:?cache=shared"
        assert config.connection_config["uri"] is True
    else:
        assert config.connection_config["database"] == database
        if uri is not None:
            assert config.connection_config["uri"] == uri


# SQLite-Specific Parameter Tests
@pytest.mark.parametrize(
    "isolation_level", [None, "DEFERRED", "IMMEDIATE", "EXCLUSIVE"], ids=["none", "deferred", "immediate", "exclusive"]
)
def test_isolation_levels(isolation_level: "str | None") -> None:
    """Test different isolation levels."""
    connection_config = {"database": ":memory:"}
    if isolation_level is not None:
        connection_config["isolation_level"] = isolation_level

    config = SqliteConfig(connection_config=connection_config)
    assert config.connection_config.get("isolation_level") == isolation_level
    # Verify memory database was auto-converted
    assert config.connection_config["database"] == "file::memory:?cache=shared"
    assert config.connection_config["uri"] is True


@pytest.mark.parametrize(
    "detect_types",
    [0, sqlite3.PARSE_DECLTYPES, sqlite3.PARSE_COLNAMES, sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES],
    ids=["none", "decltypes", "colnames", "both"],
)
def test_detect_types(detect_types: int) -> None:
    """Test detect_types parameter."""
    config = SqliteConfig(connection_config={"database": ":memory:", "detect_types": detect_types})
    assert config.connection_config["detect_types"] == detect_types
    # Verify memory database was auto-converted
    assert config.connection_config["database"] == "file::memory:?cache=shared"
    assert config.connection_config["uri"] is True


# Parameter Style Tests
def test_supported_parameter_styles() -> None:
    """Test supported parameter styles class attribute."""
    assert SqliteConfig.supported_parameter_styles == ("qmark", "named_colon")


def test_default_parameter_style() -> None:
    """Test preferred parameter style class attribute."""
    assert SqliteConfig.default_parameter_style == "qmark"


# Edge Cases
@pytest.mark.parametrize(
    "connection_config,expected_error",
    [
        ({"database": ""}, None),  # Empty string is allowed and will be converted
        ({"database": None}, None),  # None is allowed in TypedDict but filtered out
    ],
    ids=["empty_string", "none_database"],
)
def test_edge_cases(connection_config: dict[str, Any], expected_error: "type[Exception] | None") -> None:
    """Test edge cases for config initialization."""
    if expected_error:
        with pytest.raises(expected_error):
            SqliteConfig(connection_config=connection_config)
    else:
        config = SqliteConfig(connection_config=connection_config)
        # Empty string database is treated as memory database and converted
        if connection_config.get("database") == "":
            assert config.connection_config["database"] == "file::memory:?cache=shared"
            assert config.connection_config["uri"] is True
        else:
            # For None database, it gets filtered out and defaults to memory conversion
            assert config.connection_config["database"] == "file::memory:?cache=shared"
            assert config.connection_config["uri"] is True


# Memory Database Detection Tests
def test_is_memory_database() -> None:
    """Test memory database detection logic."""
    config = SqliteConfig()

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
    "database,uri,expected_min,expected_max,expected_database,expected_uri",
    [
        (":memory:", None, 5, 20, "file::memory:?cache=shared", True),
        ("", None, 5, 20, "file::memory:?cache=shared", True),
        ("file::memory:", True, 5, 20, "file::memory:?cache=shared", True),
        ("file::memory:?mode=memory", True, 5, 20, "file::memory:?mode=memory&cache=shared", True),
        ("file::memory:?cache=shared", True, 5, 20, "file::memory:?cache=shared", True),
        ("test.db", None, 5, 20, "test.db", None),
        ("/tmp/test.db", None, 3, 10, "/tmp/test.db", None),
    ],
    ids=["memory", "empty", "uri_memory", "uri_memory_with_params", "shared_memory", "file", "absolute_path"],
)
def test_memory_database_auto_conversion(
    database: str,
    uri: "bool | None",
    expected_min: int,
    expected_max: int,
    expected_database: str,
    expected_uri: "bool | None",
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that memory databases are automatically converted to shared memory for pooling."""
    connection_config = {"database": database}
    if uri is not None:
        connection_config["uri"] = uri  # type: ignore[assignment]

    # Clear any previous log records
    caplog.clear()

    # Create config with explicit pool sizes
    config = SqliteConfig(connection_config=connection_config, min_pool_size=expected_min, max_pool_size=expected_max)

    # Check pool sizes - should use requested sizes, not be overridden
    assert config.min_pool_size == expected_min
    assert config.max_pool_size == expected_max

    # Check database conversion
    assert config.connection_config["database"] == expected_database
    if expected_uri is not None:
        assert config.connection_config["uri"] == expected_uri
    else:
        assert config.connection_config.get("uri") == expected_uri

    # Should not have any warnings about disabling pooling
    assert "In-memory SQLite database detected" not in caplog.text
    assert "Disabling connection pooling" not in caplog.text


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_connection_health_check(mock_connect: MagicMock) -> None:
    """Test connection health check functionality."""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_connection

    config = SqliteConfig(connection_config={"database": "test.db"})
    pool = config.provide_pool()

    # Test healthy connection
    mock_connection.execute.return_value = mock_cursor
    assert pool._is_connection_alive(mock_connection) is True
    mock_connection.execute.assert_called_with("SELECT 1")
    mock_cursor.close.assert_called_once()

    # Test unhealthy connection (execute fails)
    mock_connection.execute.reset_mock()
    mock_connection.execute.side_effect = Exception("Connection error")
    assert pool._is_connection_alive(mock_connection) is False


# Auto-Conversion Tests
def test_convert_to_shared_memory_function() -> None:
    """Test the _convert_to_shared_memory method directly."""
    config = SqliteConfig()

    # Test :memory: conversion
    config.connection_config = {"database": ":memory:"}
    config._convert_to_shared_memory()
    assert config.connection_config["database"] == "file::memory:?cache=shared"
    assert config.connection_config["uri"] is True

    # Test file::memory: conversion
    config.connection_config = {"database": "file::memory:", "uri": True}
    config._convert_to_shared_memory()
    assert config.connection_config["database"] == "file::memory:?cache=shared"
    assert config.connection_config["uri"] is True

    # Test file::memory: with existing params
    config.connection_config = {"database": "file::memory:?mode=memory", "uri": True}
    config._convert_to_shared_memory()
    assert config.connection_config["database"] == "file::memory:?mode=memory&cache=shared"
    assert config.connection_config["uri"] is True

    # Test already shared (should not change)
    config.connection_config = {"database": "file::memory:?cache=shared", "uri": True}
    original_database = config.connection_config["database"]
    config._convert_to_shared_memory()
    assert config.connection_config["database"] == original_database


@pytest.mark.parametrize(
    "original_database,expected_database,expected_uri",
    [
        (":memory:", "file::memory:?cache=shared", True),
        ("file::memory:", "file::memory:?cache=shared", True),
        ("file::memory:?mode=memory", "file::memory:?mode=memory&cache=shared", True),
        ("file::memory:?cache=shared", "file::memory:?cache=shared", True),
        ("file::memory:?mode=memory&cache=shared", "file::memory:?mode=memory&cache=shared", True),
        ("test.db", "test.db", None),  # Regular file should not change
    ],
    ids=[
        "memory",
        "file_memory",
        "file_memory_with_params",
        "already_shared",
        "already_shared_with_params",
        "regular_file",
    ],
)
def test_auto_conversion_scenarios(original_database: str, expected_database: str, expected_uri: "bool | None") -> None:
    """Test various auto-conversion scenarios."""
    connection_config = {"database": original_database}
    config = SqliteConfig(connection_config=connection_config)

    assert config.connection_config["database"] == expected_database
    if expected_uri is not None:
        assert config.connection_config["uri"] == expected_uri
    else:
        # For regular files, uri should not be set or should remain as originally specified
        assert config.connection_config.get("uri") is None


def test_no_warnings_with_auto_conversion(caplog: pytest.LogCaptureFixture) -> None:
    """Test that no warnings are logged when auto-conversion happens."""
    caplog.clear()

    # Test various memory database types
    test_configs = [
        {"database": ":memory:"},
        {"database": ""},
        {"database": "file::memory:"},
        {"database": "file::memory:?mode=memory"},
    ]

    for connection_config in test_configs:
        caplog.clear()
        config = SqliteConfig(connection_config=connection_config)

        # Should have pooling enabled
        assert config.min_pool_size > 1
        assert config.max_pool_size > 1

        # Should not have warning messages
        assert "In-memory SQLite database detected" not in caplog.text
        assert "Disabling connection pooling" not in caplog.text
