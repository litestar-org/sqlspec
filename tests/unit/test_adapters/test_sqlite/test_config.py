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

    # Create config with the TypedDict
    config = SqliteConfig(connection_config=connection_params)
    assert config.connection_config == connection_params


# Initialization Tests
@pytest.mark.parametrize(
    "connection_config,expected_config",
    [
        ({"database": ":memory:"}, {"database": ":memory:"}),
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
    assert config.default_row_type is dict


def test_default_connection_config() -> None:
    """Test default connection config when none is provided."""
    config = SqliteConfig()
    assert config.connection_config == {"database": ":memory:"}


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

    config = SqliteConfig(connection_config={"database": ":memory:"})

    with config.provide_connection() as conn:
        assert conn is mock_connection
        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_provide_connection_error_handling(mock_connect: MagicMock) -> None:
    """Test provide_connection context manager error handling."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = SqliteConfig(connection_config={"database": ":memory:"})

    with pytest.raises(ValueError, match="Test error"):
        with config.provide_connection() as conn:
            assert conn is mock_connection
            raise ValueError("Test error")

    # Connection should still be closed on error
    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_provide_session(mock_connect: MagicMock) -> None:
    """Test provide_session context manager."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = SqliteConfig(connection_config={"database": ":memory:"})

    with config.provide_session() as session:
        assert isinstance(session, SqliteDriver)
        assert session.connection is mock_connection

        # Check parameter style injection
        assert session.config.allowed_parameter_styles == ("qmark", "named_colon")
        assert session.config.default_parameter_style == "qmark"

        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


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
        assert session.config.allowed_parameter_styles == ("qmark",)
        assert session.config.default_parameter_style == "qmark"


# Property Tests
@pytest.mark.parametrize(
    "connection_config,expected_dict",
    [
        ({"database": ":memory:"}, {"database": ":memory:"}),
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


@pytest.mark.parametrize(
    "detect_types",
    [0, sqlite3.PARSE_DECLTYPES, sqlite3.PARSE_COLNAMES, sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES],
    ids=["none", "decltypes", "colnames", "both"],
)
def test_detect_types(detect_types: int) -> None:
    """Test detect_types parameter."""
    config = SqliteConfig(connection_config={"database": ":memory:", "detect_types": detect_types})
    assert config.connection_config["detect_types"] == detect_types


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
        ({"database": ""}, None),  # Empty string is allowed
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
        assert config.connection_config == connection_config
