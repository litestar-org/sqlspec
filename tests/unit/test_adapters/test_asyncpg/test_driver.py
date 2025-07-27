"""Unit tests for AsyncPG driver."""

from unittest.mock import AsyncMock, patch

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig


# Test Fixtures
@pytest.fixture
def mock_connection() -> AsyncMock:
    """Create a mock AsyncPG connection."""
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def driver(mock_connection: AsyncMock) -> AsyncpgDriver:
    """Create an AsyncPG driver with mocked connection."""
    return AsyncpgDriver(connection=mock_connection, config=SQLConfig())


# Initialization Tests
@pytest.mark.asyncio
async def test_driver_initialization(driver: AsyncpgDriver) -> None:
    """Test driver initialization."""
    assert driver.dialect == "postgres"
    assert driver.parameter_config.default_parameter_style == ParameterStyle.NUMERIC


# Cursor Management Tests
@pytest.mark.asyncio
async def test_acquire_cursor(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test with_cursor context manager."""
    async with driver.with_cursor(mock_connection) as cursor:
        assert cursor is mock_connection


# Execution Logic Tests
@pytest.mark.asyncio
async def test_perform_execute_single(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test _perform_execute for a single statement."""
    statement = SQL("SELECT 1")
    await driver._perform_execute(mock_connection, statement)
    # Driver compiles internally, so check execute was called
    mock_connection.execute.assert_called_once()


@pytest.mark.asyncio
async def test_perform_execute_many(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test _perform_execute for an executemany statement."""
    statement = SQL("INSERT INTO t (id) VALUES ($1)").as_many([[1], [2]])
    await driver._perform_execute(mock_connection, statement)
    # Driver compiles internally, so check executemany was called
    mock_connection.executemany.assert_called_once()


# Result Building Tests
@pytest.mark.asyncio
async def test_build_result_select(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test _build_result for a SELECT statement."""
    statement = SQL("SELECT * FROM users")
    with patch.object(driver, "returns_rows", return_value=True):
        with patch.object(driver, "_build_asyncpg_select_result") as mock_build_select:
            await driver._build_result(mock_connection, statement)
            mock_build_select.assert_called_once()


@pytest.mark.asyncio
async def test_build_result_dml(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test _build_result for a DML statement."""
    statement = SQL("INSERT INTO users (name) VALUES ('Alice')")
    with patch.object(driver, "returns_rows", return_value=False):
        with patch.object(driver, "_build_modify_result_async") as mock_build_modify:
            await driver._build_result(mock_connection, statement)
            mock_build_modify.assert_called_once()


# Dispatcher Integration Tests
@pytest.mark.asyncio
async def test_execute_uses_dispatcher(driver: AsyncpgDriver) -> None:
    """Test that the public execute method correctly uses the dispatcher."""
    statement = SQL("SELECT * FROM users")
    with patch.object(driver, "_dispatch_execution", new_callable=AsyncMock) as mock_dispatch:
        await driver.execute(statement)
        mock_dispatch.assert_called_once()
