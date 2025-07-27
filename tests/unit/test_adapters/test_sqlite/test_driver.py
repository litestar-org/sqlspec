"""Unit tests for SQLite driver.

This module tests the SqliteDriver class including:
- Driver initialization and configuration
- Statement execution (single, many, script)
- Result wrapping and formatting
- Parameter style handling
- Type coercion overrides
- Bulk loading functionality
- Error handling
"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig


# Test Fixtures
@pytest.fixture
def mock_connection() -> MagicMock:
    """Create a mock SQLite connection."""
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_cursor = MagicMock()

    # Set up cursor context manager
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=None)

    # Mock cursor methods
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.executemany.return_value = mock_cursor
    mock_cursor.executescript.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    mock_cursor.close.return_value = None
    mock_cursor.rowcount = 0
    mock_cursor.description = None

    # Connection returns cursor
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    return mock_conn


@pytest.fixture
def driver(mock_connection: MagicMock) -> SqliteDriver:
    """Create a SQLite driver with mocked connection."""
    config = SQLConfig()
    return SqliteDriver(connection=mock_connection, config=config)


# Initialization Tests
def test_driver_initialization() -> None:
    """Test driver initialization with various parameters."""
    mock_conn = MagicMock()
    config = SQLConfig()

    driver = SqliteDriver(connection=mock_conn, config=config)

    assert driver.connection is mock_conn
    assert driver.config is config
    assert driver.dialect == "sqlite"
    assert driver.parameter_config.paramstyle == ParameterStyle.QMARK
    assert driver.parameter_config.has_native_list_expansion is False


# Cursor Context Manager Tests
def test_acquire_cursor_success(driver: SqliteDriver) -> None:
    """Test with_cursor context manager normal flow."""
    mock_conn = driver.connection
    mock_cursor = mock_conn.cursor.return_value

    with driver.with_cursor(mock_conn) as cursor:
        assert cursor is mock_cursor
        mock_cursor.close.assert_not_called()

    mock_cursor.close.assert_called_once()


def test_acquire_cursor_error_handling(driver: SqliteDriver) -> None:
    """Test with_cursor context manager error handling."""
    mock_conn = driver.connection
    mock_cursor = mock_conn.cursor.return_value

    with pytest.raises(ValueError, match="Test error"):
        with driver.with_cursor(mock_conn) as cursor:
            assert cursor is mock_cursor
            raise ValueError("Test error")

    # Cursor should still be closed
    mock_cursor.close.assert_called_once()


# Execution Tests
def test_perform_execute_single(driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test _perform_execute for a single statement."""
    mock_cursor = mock_connection.cursor.return_value
    statement = SQL("SELECT * FROM users WHERE id = ?", 1)

    with patch.object(driver, "_prepare_driver_parameters", return_value=(1,)) as mock_prepare:
        driver._perform_execute(mock_cursor, statement)

    # Verify parameters were prepared
    mock_prepare.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.executemany.assert_not_called()


def test_perform_execute_many(driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test _perform_execute for an executemany statement."""
    mock_cursor = mock_connection.cursor.return_value
    statement = SQL("INSERT INTO users (name) VALUES (?)").as_many([["Alice"], ["Bob"]])

    with patch.object(driver, "_prepare_driver_parameters", return_value=[["Alice"], ["Bob"]]) as mock_prepare:
        driver._perform_execute(mock_cursor, statement)

    # Verify parameters were prepared
    mock_prepare.assert_called_once()
    mock_cursor.executemany.assert_called_once()
    mock_cursor.execute.assert_not_called()


# Result Building Tests
def test_build_result_select(driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test _build_result for a SELECT statement."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = [(1, "Alice")]
    statement = SQL("SELECT * FROM users")

    with patch.object(driver, "returns_rows", return_value=True):
        result = driver._build_result(mock_cursor, statement)

    assert result.data == [(1, "Alice")]
    assert result.column_names == ["id", "name"]


def test_build_result_dml(driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test _build_result for a DML statement."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.rowcount = 1
    statement = SQL("INSERT INTO users (name) VALUES ('Alice')")

    with patch.object(driver, "returns_rows", return_value=False):
        result = driver._build_result(mock_cursor, statement)

    assert result.rows_affected == 1


# Integration Test for the Dispatcher


def test_dispatch_execution_integration(driver: SqliteDriver) -> None:
    """Test that the public execute method correctly uses the dispatcher."""
    statement = SQL("SELECT * FROM users")

    with patch.object(driver, "_dispatch_execution") as mock_dispatch:
        driver.execute(statement)
        mock_dispatch.assert_called_once()
