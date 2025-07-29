"""Unit tests for PSQLPy driver."""

from unittest.mock import AsyncMock, patch

import pytest

from sqlspec.adapters.psqlpy import PsqlpyDriver
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_psqlpy_connection() -> AsyncMock:
    """Create a mock PSQLPy connection."""
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def driver(mock_psqlpy_connection: AsyncMock) -> PsqlpyDriver:
    """Create a PSQLPy driver with mocked connection."""
    return PsqlpyDriver(connection=mock_psqlpy_connection, config=SQLConfig())


# Initialization Tests
@pytest.mark.asyncio
async def test_driver_initialization(driver: PsqlpyDriver) -> None:
    """Test driver initialization."""
    assert driver.dialect == "postgres"
    assert driver.parameter_config.default_parameter_style == ParameterStyle.NUMERIC


# Cursor Management Tests
@pytest.mark.asyncio
async def test_acquire_cursor(driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock) -> None:
    """Test with_cursor context manager."""
    async with driver.with_cursor(mock_psqlpy_connection) as cursor:
        assert cursor is mock_psqlpy_connection


# Execution Logic Tests
@pytest.mark.asyncio
async def test_perform_execute_single(driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock) -> None:
    """Test _perform_execute for a single statement."""
    statement = SQL("SELECT 1")
    await driver._perform_execute(mock_psqlpy_connection, statement)
    # Driver compiles internally, so check execute was called
    mock_psqlpy_connection.execute.assert_called_once()


@pytest.mark.asyncio
async def test_perform_execute_many(driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock) -> None:
    """Test _perform_execute for an executemany statement."""
    statement = SQL("INSERT INTO t (id) VALUES ($1)").as_many([[1], [2]])
    await driver._perform_execute(mock_psqlpy_connection, statement)
    # Driver compiles internally, so check execute_many was called
    mock_psqlpy_connection.execute_many.assert_called_once()


# Result Building Tests
@pytest.mark.asyncio
async def test_build_result_select(driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock) -> None:
    """Test _build_result for a SELECT statement."""
    statement = SQL("SELECT * FROM users")
    with patch.object(driver, "returns_rows", return_value=True):
        with patch.object(driver, "_extract_select_data") as mock_extract_select:
            mock_extract_select.return_value = ([], [], 0)
            await driver._build_result(mock_psqlpy_connection, statement)
            mock_extract_select.assert_called_once_with(mock_psqlpy_connection)


@pytest.mark.asyncio
async def test_build_result_dml(driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock) -> None:
    """Test _build_result for a DML statement."""
    statement = SQL("INSERT INTO users (name) VALUES ('Alice')")
    with patch.object(driver, "returns_rows", return_value=False):
        with patch.object(driver, "_extract_execute_rowcount") as mock_extract_rowcount:
            mock_extract_rowcount.return_value = 1
            await driver._build_result(mock_psqlpy_connection, statement)
            mock_extract_rowcount.assert_called_once_with(mock_psqlpy_connection)


# Dispatcher Integration Tests
@pytest.mark.asyncio
async def test_execute_uses_dispatcher(driver: PsqlpyDriver) -> None:
    """Test that the public execute method correctly uses the dispatcher."""
    statement = SQL("SELECT * FROM users")
    with patch.object(driver, "_dispatch_execution", new_callable=AsyncMock) as mock_dispatch:
        await driver.execute(statement)
        mock_dispatch.assert_called_once()
