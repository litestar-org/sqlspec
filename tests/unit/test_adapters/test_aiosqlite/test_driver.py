"""Unit tests for AIOSQLite driver."""

from typing import Any
from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConnection, AiosqliteDriver
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import is_dict_with_field


@pytest.fixture
def mock_aiosqlite_connection() -> AsyncMock:
    """Create a mock AIOSQLite connection with async context manager support."""
    mock_connection = AsyncMock(spec=AiosqliteConnection)
    mock_connection.__aenter__.return_value = mock_connection
    mock_connection.__aexit__.return_value = None
    mock_cursor = AsyncMock()
    mock_cursor.__aenter__.return_value = mock_cursor
    mock_cursor.__aexit__.return_value = None

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_connection.cursor.side_effect = _cursor
    mock_connection.execute.return_value = mock_cursor
    mock_connection.executemany.return_value = mock_cursor
    mock_connection.executescript.return_value = mock_cursor
    mock_cursor.close.return_value = None
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None
    mock_cursor.fetchall.return_value = [(1, "test")]
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]
    mock_cursor.rowcount = 0
    return mock_connection


@pytest.fixture
def aiosqlite_driver(mock_aiosqlite_connection: AsyncMock) -> AiosqliteDriver:
    """Create an AIOSQLite driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    return AiosqliteDriver(connection=mock_aiosqlite_connection, config=config)


def test_aiosqlite_driver_initialization(mock_aiosqlite_connection: AsyncMock) -> None:
    """Test AIOSQLite driver initialization."""
    config = SQLConfig()
    driver = AiosqliteDriver(connection=mock_aiosqlite_connection, config=config)

    # Test driver attributes are set correctly
    assert driver.connection is mock_aiosqlite_connection
    assert driver.config is config
    assert driver.dialect == "sqlite"
    # AIOSQLite doesn't support native arrow operations
    assert driver.supports_native_arrow_export is False
    assert driver.supports_native_arrow_import is False


def test_aiosqlite_driver_dialect_property(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver dialect property."""
    assert aiosqlite_driver.dialect == "sqlite"


def test_aiosqlite_driver_supports_arrow(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver Arrow support."""
    # AIOSQLite doesn't support native arrow operations
    assert aiosqlite_driver.supports_native_arrow_export is False
    assert aiosqlite_driver.supports_native_arrow_import is False


def test_aiosqlite_driver_placeholder_style(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver placeholder style detection."""
    placeholder_style = aiosqlite_driver.default_parameter_style
    assert placeholder_style == ParameterStyle.QMARK


@pytest.mark.asyncio
async def test_aiosqlite_driver_execute_statement_select(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver _execute_statement for SELECT statements."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [(1, "test")]
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = ?", parameters=[1])

    # Call execute_statement which will handle the mock setup
    result = await aiosqlite_driver._execute_statement(statement)

    # Verify connection operations
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()

    # The result should be a dict with expected structure
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_aiosqlite_driver_fetch_arrow_table_with_parameters(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver fetch_arrow_table method with parameters."""
    # Setup mock cursor and result data
    mock_cursor = AsyncMock()
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]
    mock_cursor.fetchall.return_value = [{"id": 42, "name": "Test User"}]

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    # Create SQL statement with parameters
    statement = SQL("SELECT id, name FROM users WHERE id = ?", parameters=[42])

    # Call execute_statement which will handle the mock setup
    result = await aiosqlite_driver._execute_statement(statement)

    # Verify connection operations with parameters
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()

    # The result should be a dict with expected structure
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_aiosqlite_driver_non_query_statement(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver with non-query statement."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.rowcount = 1

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None

    # Create non-query statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')")
    result = await aiosqlite_driver._execute_statement(statement)

    # Verify cursor operations
    mock_cursor.execute.assert_called_once()

    # The result should be a DMLResultDict for non-query statements
    assert isinstance(result, dict)
    assert is_dict_with_field(result, "rows_affected")
    assert result["rows_affected"] == 1  # pyright: ignore


@pytest.mark.asyncio
async def test_aiosqlite_driver_execute_with_connection_override(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver execute with connection override."""
    # Create override connection
    override_connection = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]
    mock_cursor.fetchall.return_value = [{"id": 1}]

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    override_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None

    # Create SQL statement
    statement = SQL("SELECT id FROM users")
    result = await aiosqlite_driver._execute_statement(statement, connection=override_connection)

    # Verify cursor operations
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()

    # The result should be a dict with expected structure
    assert isinstance(result, dict)
