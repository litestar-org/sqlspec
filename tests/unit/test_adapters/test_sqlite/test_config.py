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

from sqlspec.adapters.sqlite import CONNECTION_FIELDS, SqliteConfig, SqliteDriver
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    pass


# Constants Tests
def test_connection_fields_constant() -> None:
    """Test CONNECTION_FIELDS constant contains all expected fields."""
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


# Initialization Tests
@pytest.mark.parametrize(
    "kwargs,expected_attrs",
    [
        (
            {"database": ":memory:"},
            {
                "database": ":memory:",
                "timeout": None,
                "detect_types": None,
                "isolation_level": None,
                "check_same_thread": None,
                "factory": None,
                "cached_statements": None,
                "uri": None,
                "extras": {},
            },
        ),
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
                "extras": {},
            },
        ),
    ],
    ids=["minimal", "full"],
)
def test_config_initialization(kwargs: dict[str, Any], expected_attrs: dict[str, Any]) -> None:
    """Test config initialization with various parameters."""
    config = SqliteConfig(**kwargs)

    for attr, expected_value in expected_attrs.items():
        assert getattr(config, attr) == expected_value

    # Check base class attributes
    assert isinstance(config.statement_config, SQLConfig)
    assert config.default_row_type is DictRow


@pytest.mark.parametrize(
    "init_kwargs,expected_extras",
    [
        ({"database": ":memory:", "custom_param": "value", "debug": True}, {"custom_param": "value", "debug": True}),
        (
            {"database": ":memory:", "unknown_param": "test", "another_param": 42},
            {"unknown_param": "test", "another_param": 42},
        ),
        ({"database": "/tmp/test.db"}, {}),
    ],
    ids=["with_custom_params", "with_unknown_params", "no_extras"],
)
def test_extras_handling(init_kwargs: dict[str, Any], expected_extras: dict[str, Any]) -> None:
    """Test handling of extra parameters."""
    config = SqliteConfig(**init_kwargs)
    assert config.extras == expected_extras


@pytest.mark.parametrize(
    "statement_config,expected_type",
    [(None, SQLConfig), (SQLConfig(), SQLConfig), (SQLConfig(strict_mode=True), SQLConfig)],
    ids=["default", "empty", "custom"],
)
def test_statement_config_initialization(statement_config: "SQLConfig | None", expected_type: type[SQLConfig]) -> None:
    """Test statement config initialization."""
    config = SqliteConfig(database=":memory:", statement_config=statement_config)
    assert isinstance(config.statement_config, expected_type)

    if statement_config is not None:
        assert config.statement_config is statement_config


# Connection Creation Tests
@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_create_connection(mock_connect: MagicMock) -> None:
    """Test connection creation."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = SqliteConfig(database="/tmp/test.db", timeout=30.0)
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

    config = SqliteConfig(database=":memory:")

    with config.provide_connection() as conn:
        assert conn is mock_connection
        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_provide_connection_error_handling(mock_connect: MagicMock) -> None:
    """Test provide_connection context manager error handling."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    config = SqliteConfig(database=":memory:")

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

    config = SqliteConfig(database=":memory:")

    with config.provide_session() as session:
        assert isinstance(session, SqliteDriver)
        assert session.connection is mock_connection

        # Check parameter style injection
        assert session.config.allowed_parameter_styles == ("qmark", "named_colon")
        assert session.config.target_parameter_style == "qmark"

        mock_connection.close.assert_not_called()

    mock_connection.close.assert_called_once()


@patch("sqlspec.adapters.sqlite.config.sqlite3.connect")
def test_provide_session_with_custom_config(mock_connect: MagicMock) -> None:
    """Test provide_session with custom statement config."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    # Custom statement config with parameter styles already set
    custom_config = SQLConfig(allowed_parameter_styles=("qmark",), target_parameter_style="qmark")
    config = SqliteConfig(database=":memory:", statement_config=custom_config)

    with config.provide_session() as session:
        # Should use the custom config's parameter styles
        assert session.config.allowed_parameter_styles == ("qmark",)
        assert session.config.target_parameter_style == "qmark"


# Property Tests
@pytest.mark.parametrize(
    "init_kwargs,expected_dict",
    [
        ({"database": ":memory:"}, {"database": ":memory:"}),
        (
            {"database": "/tmp/test.db", "timeout": 30.0, "check_same_thread": False, "isolation_level": "DEFERRED"},
            {"database": "/tmp/test.db", "timeout": 30.0, "isolation_level": "DEFERRED", "check_same_thread": False},
        ),
    ],
    ids=["minimal", "partial"],
)
def test_connection_config_dict(init_kwargs: dict[str, Any], expected_dict: dict[str, Any]) -> None:
    """Test connection_config_dict property."""
    config = SqliteConfig(**init_kwargs)
    assert config.connection_config_dict == expected_dict


def test_driver_type() -> None:
    """Test driver_type class attribute."""
    config = SqliteConfig(database=":memory:")
    assert config.driver_type is SqliteDriver


def test_connection_type() -> None:
    """Test connection_type class attribute."""
    config = SqliteConfig(database=":memory:")
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
    kwargs = {"database": database}
    if uri is not None:
        kwargs["uri"] = uri  # pyright: ignore

    config = SqliteConfig(**kwargs)  # type: ignore[arg-type]
    assert config.database == database
    if uri is not None:
        assert config.uri == uri


# SQLite-Specific Parameter Tests
@pytest.mark.parametrize(
    "isolation_level", [None, "DEFERRED", "IMMEDIATE", "EXCLUSIVE"], ids=["none", "deferred", "immediate", "exclusive"]
)
def test_isolation_levels(isolation_level: "str | None") -> None:
    """Test different isolation levels."""
    config = SqliteConfig(database=":memory:", isolation_level=isolation_level)
    assert config.isolation_level == isolation_level


@pytest.mark.parametrize(
    "detect_types",
    [0, sqlite3.PARSE_DECLTYPES, sqlite3.PARSE_COLNAMES, sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES],
    ids=["none", "decltypes", "colnames", "both"],
)
def test_detect_types(detect_types: int) -> None:
    """Test detect_types parameter."""
    config = SqliteConfig(database=":memory:", detect_types=detect_types)
    assert config.detect_types == detect_types


# Parameter Style Tests
def test_supported_parameter_styles() -> None:
    """Test supported parameter styles class attribute."""
    assert SqliteConfig.supported_parameter_styles == ("qmark", "named_colon")


def test_preferred_parameter_style() -> None:
    """Test preferred parameter style class attribute."""
    assert SqliteConfig.preferred_parameter_style == "qmark"


# Slots Test
def test_slots_defined() -> None:
    """Test that __slots__ is properly defined."""
    assert hasattr(SqliteConfig, "__slots__")
    expected_slots = {
        "_dialect",
        "pool_instance",
        "cached_statements",
        "check_same_thread",
        "database",
        "default_row_type",
        "detect_types",
        "extras",
        "factory",
        "isolation_level",
        "statement_config",
        "timeout",
        "uri",
    }
    assert set(SqliteConfig.__slots__) == expected_slots


# Edge Cases
@pytest.mark.parametrize(
    "kwargs,expected_error",
    [
        ({"database": ""}, None),  # Empty string is allowed
        ({"database": None}, TypeError),  # None should raise TypeError
    ],
    ids=["empty_string", "none_database"],
)
def test_edge_cases(kwargs: dict[str, Any], expected_error: "type[Exception] | None") -> None:
    """Test edge cases for config initialization."""
    if expected_error:
        with pytest.raises(expected_error):
            SqliteConfig(**kwargs)
    else:
        config = SqliteConfig(**kwargs)
        assert config.database == kwargs["database"]
