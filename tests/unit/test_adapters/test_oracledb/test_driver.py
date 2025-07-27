"""Unit tests for OracleDB drivers."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_sync_connection() -> MagicMock:
    """Create a mock Oracle sync connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def sync_driver(mock_sync_connection: MagicMock) -> OracleSyncDriver:
    """Create an Oracle sync driver with mocked connection."""
    return OracleSyncDriver(connection=mock_sync_connection, config=SQLConfig())


@pytest.fixture
def mock_async_connection() -> AsyncMock:
    """Create a mock Oracle async connection."""
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    # cursor() method is synchronous even on async connections
    mock_conn.cursor = MagicMock(return_value=mock_cursor)
    return mock_conn


@pytest.fixture
def async_driver(mock_async_connection: AsyncMock) -> OracleAsyncDriver:
    """Create an Oracle async driver with mocked connection."""
    return OracleAsyncDriver(connection=mock_async_connection, config=SQLConfig())


# Sync Driver Tests
def test_sync_driver_initialization(sync_driver: OracleSyncDriver) -> None:
    """Test sync driver initialization."""
    assert sync_driver.dialect == "oracle"
    assert sync_driver.parameter_config.default_parameter_style == ParameterStyle.NAMED_COLON


def test_sync_with_cursor(sync_driver: OracleSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync with_cursor."""
    mock_cursor = mock_sync_connection.cursor.return_value
    with sync_driver.with_cursor(mock_sync_connection) as cursor:
        assert cursor is mock_cursor
    mock_cursor.close.assert_called_once()


def test_sync_perform_execute_single(sync_driver: OracleSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync _perform_execute for a single statement."""
    mock_cursor = mock_sync_connection.cursor.return_value
    statement = SQL("SELECT 1 FROM DUAL")

    with patch.object(sync_driver, "_prepare_driver_parameters", return_value={}):
        sync_driver._perform_execute(mock_cursor, statement)

    mock_cursor.execute.assert_called_once()
    mock_cursor.executemany.assert_not_called()


def test_sync_perform_execute_many(sync_driver: OracleSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync _perform_execute for an executemany statement."""
    mock_cursor = mock_sync_connection.cursor.return_value
    statement = SQL("INSERT INTO t (id) VALUES (:id)").as_many([{"id": 1}, {"id": 2}])

    with patch.object(sync_driver, "_prepare_driver_parameters", return_value=[{"id": 1}, {"id": 2}]):
        sync_driver._perform_execute(mock_cursor, statement)

    mock_cursor.executemany.assert_called_once()
    mock_cursor.execute.assert_not_called()


# Async Driver Tests
@pytest.mark.asyncio
async def test_async_driver_initialization(async_driver: OracleAsyncDriver) -> None:
    """Test async driver initialization."""
    assert async_driver.dialect == "oracle"
    assert async_driver.parameter_config.default_parameter_style == ParameterStyle.NAMED_COLON


@pytest.mark.asyncio
async def test_async_with_cursor(async_driver: OracleAsyncDriver, mock_async_connection: AsyncMock) -> None:
    """Test async with_cursor."""
    # The cursor is already set up in the fixture
    mock_cursor = mock_async_connection.cursor.return_value
    async with async_driver.with_cursor(mock_async_connection) as cursor:
        assert cursor is mock_cursor
    mock_cursor.close.assert_called_once()


@pytest.mark.asyncio
async def test_async_perform_execute_single(async_driver: OracleAsyncDriver, mock_async_connection: AsyncMock) -> None:
    """Test async _perform_execute for a single statement."""
    mock_cursor = mock_async_connection.cursor.return_value
    statement = SQL("SELECT 1 FROM DUAL")

    with patch.object(async_driver, "_prepare_driver_parameters", return_value={}):
        await async_driver._perform_execute(mock_cursor, statement)

    mock_cursor.execute.assert_called_once()
    mock_cursor.executemany.assert_not_called()


@pytest.mark.asyncio
async def test_async_perform_execute_many(async_driver: OracleAsyncDriver, mock_async_connection: AsyncMock) -> None:
    """Test async _perform_execute for an executemany statement."""
    mock_cursor = mock_async_connection.cursor.return_value
    statement = SQL("INSERT INTO t (id) VALUES (:id)").as_many([{"id": 1}, {"id": 2}])

    with patch.object(async_driver, "_prepare_driver_parameters", return_value=[{"id": 1}, {"id": 2}]):
        await async_driver._perform_execute(mock_cursor, statement)

    mock_cursor.executemany.assert_called_once()
    mock_cursor.execute.assert_not_called()
