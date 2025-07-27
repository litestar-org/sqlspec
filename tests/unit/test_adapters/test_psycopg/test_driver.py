"""Unit tests for Psycopg drivers."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.psycopg import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig


# Test Fixtures
@pytest.fixture
def mock_sync_connection() -> MagicMock:
    """Create a mock Psycopg sync connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def sync_driver(mock_sync_connection: MagicMock) -> PsycopgSyncDriver:
    """Create a Psycopg sync driver with mocked connection."""
    return PsycopgSyncDriver(connection=mock_sync_connection, config=SQLConfig())


@pytest.fixture
def mock_async_connection() -> AsyncMock:
    """Create a mock Psycopg async connection."""
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def async_driver(mock_async_connection: AsyncMock) -> PsycopgAsyncDriver:
    """Create a Psycopg async driver with mocked connection."""
    return PsycopgAsyncDriver(connection=mock_async_connection, config=SQLConfig())


# Sync Driver Tests
def test_sync_driver_initialization(sync_driver: PsycopgSyncDriver) -> None:
    """Test sync driver initialization."""
    assert sync_driver.dialect == "postgres"
    assert sync_driver.parameter_config.default_parameter_style == ParameterStyle.POSITIONAL_PYFORMAT


def test_syncwith_cursor(sync_driver: PsycopgSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync with_cursor."""
    with patch.object(mock_sync_connection, "cursor") as mock_cursor_method:
        with sync_driver.with_cursor(mock_sync_connection):
            pass
        mock_cursor_method.assert_called_once()


def test_sync_perform_execute_single(sync_driver: PsycopgSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync _perform_execute for a single statement."""
    mock_cursor = mock_sync_connection.cursor.return_value
    statement = SQL("SELECT 1")
    sync_driver._perform_execute(mock_cursor, statement)
    # Driver compiles internally, so check execute was called
    mock_cursor.execute.assert_called_once()


def test_sync_perform_execute_many(sync_driver: PsycopgSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync _perform_execute for an executemany statement."""
    mock_cursor = mock_sync_connection.cursor.return_value
    statement = SQL("INSERT INTO t (id) VALUES (%s)").as_many([[1], [2]])
    sync_driver._perform_execute(mock_cursor, statement)
    # Driver compiles internally, so check executemany was called
    mock_cursor.executemany.assert_called_once()


# Async Driver Tests
@pytest.mark.asyncio
async def test_async_driver_initialization(async_driver: PsycopgAsyncDriver) -> None:
    """Test async driver initialization."""
    assert async_driver.dialect == "postgres"
    assert async_driver.parameter_config.default_parameter_style == ParameterStyle.POSITIONAL_PYFORMAT


@pytest.mark.asyncio
async def test_asyncwith_cursor(async_driver: PsycopgAsyncDriver, mock_async_connection: AsyncMock) -> None:
    """Test async with_cursor."""
    mock_cursor = AsyncMock()
    mock_async_connection.cursor.return_value = mock_cursor
    async with async_driver.with_cursor(mock_async_connection):
        pass
    mock_async_connection.cursor.assert_called_once()


@pytest.mark.asyncio
async def test_async_perform_execute_single(async_driver: PsycopgAsyncDriver, mock_async_connection: AsyncMock) -> None:
    """Test async _perform_execute for a single statement."""
    mock_cursor = mock_async_connection.cursor.return_value
    statement = SQL("SELECT 1")
    await async_driver._perform_execute(mock_cursor, statement)
    # Driver compiles internally, so check execute was called
    mock_cursor.execute.assert_called_once()


@pytest.mark.asyncio
async def test_async_perform_execute_many(async_driver: PsycopgAsyncDriver, mock_async_connection: AsyncMock) -> None:
    """Test async _perform_execute for an executemany statement."""
    mock_cursor = mock_async_connection.cursor.return_value
    statement = SQL("INSERT INTO t (id) VALUES (%s)").as_many([[1], [2]])
    await async_driver._perform_execute(mock_cursor, statement)
    # Driver compiles internally, so check executemany was called
    mock_cursor.executemany.assert_called_once()
