"""Unit tests for DuckDB driver."""

from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.duckdb import DuckDBDriver
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig


# Test Fixtures
@pytest.fixture
def mock_connection() -> MagicMock:
    """Create a mock DuckDB connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def driver(mock_connection: MagicMock) -> DuckDBDriver:
    """Create a DuckDB driver with mocked connection."""
    return DuckDBDriver(connection=mock_connection, config=SQLConfig())


# Initialization Tests
def test_driver_initialization(driver: DuckDBDriver) -> None:
    """Test driver initialization with various parameters."""
    assert driver.dialect == "duckdb"
    assert driver.parameter_config.paramstyle == ParameterStyle.QMARK
    assert driver.parameter_config.has_native_list_expansion is True


# Cursor Management Tests
def test_with_cursor(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test with_cursor context manager."""
    mock_cursor = mock_connection.cursor.return_value
    with driver.with_cursor(mock_connection) as cursor:
        assert cursor is mock_cursor
    mock_cursor.close.assert_called_once()


# Execution Logic Tests
def test_perform_execute_single(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test _perform_execute for a single statement."""
    mock_cursor = mock_connection.cursor.return_value
    statement = SQL("SELECT * FROM users WHERE id = ?", 1)

    with patch.object(driver, "_prepare_driver_parameters", return_value=[1]):
        driver._perform_execute(mock_cursor, statement)

    mock_cursor.execute.assert_called_once()
    mock_cursor.executemany.assert_not_called()


def test_perform_execute_many(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test _perform_execute for an executemany statement."""
    mock_cursor = mock_connection.cursor.return_value
    statement = SQL("INSERT INTO users (name) VALUES (?)").as_many([["Alice"], ["Bob"]])

    with patch.object(driver, "_prepare_driver_parameters", return_value=[["Alice"], ["Bob"]]):
        driver._perform_execute(mock_cursor, statement)

    mock_cursor.executemany.assert_called_once()
    mock_cursor.execute.assert_not_called()


# Result Building Tests
def test_build_result_select(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test _build_result for a SELECT statement."""
    mock_cursor = mock_connection.cursor.return_value
    statement = SQL("SELECT * FROM users")

    with patch.object(driver, "returns_rows", return_value=True) as mock_returns_rows:
        with patch.object(driver, "_build_select_result") as mock_build_select:
            driver._build_result(mock_cursor, statement)
            mock_returns_rows.assert_called_once_with(statement.expression)
            mock_build_select.assert_called_once_with(mock_cursor, statement)


def test_build_result_dml(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test _build_result for a DML statement."""
    mock_cursor = mock_connection.cursor.return_value
    statement = SQL("INSERT INTO users (name) VALUES ('Alice')")

    with patch.object(driver, "returns_rows", return_value=False) as mock_returns_rows:
        with patch.object(driver, "_build_modify_result") as mock_build_modify:
            driver._build_result(mock_cursor, statement)
            mock_returns_rows.assert_called_once_with(statement.expression)
            mock_build_modify.assert_called_once_with(mock_cursor, statement)


# Dispatcher Integration Tests
def test_execute_uses_dispatcher(driver: DuckDBDriver) -> None:
    """Test that the public execute method correctly uses the dispatcher."""
    statement = SQL("SELECT * FROM users")
    with patch.object(driver, "_dispatch_execution") as mock_dispatch:
        driver.execute(statement)
        mock_dispatch.assert_called_once()
