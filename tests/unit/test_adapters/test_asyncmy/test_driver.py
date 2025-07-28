"""Unit tests for Asyncmy driver."""

from unittest.mock import AsyncMock, Mock

import pytest

from sqlspec.adapters.asyncmy import AsyncmyDriver
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_asyncmy_connection() -> AsyncMock:
    """Create a mock Asyncmy connection."""
    mock_connection = AsyncMock()
    mock_cursor = AsyncMock()

    # cursor() in asyncmy returns cursor directly (sync), not a coroutine
    mock_connection.cursor = Mock(return_value=mock_cursor)  # Use regular Mock for sync method

    # But cursor methods are async
    mock_cursor.close = AsyncMock(return_value=None)
    mock_cursor.execute = AsyncMock(return_value=None)
    mock_cursor.executemany = AsyncMock(return_value=None)
    mock_cursor.fetchall = AsyncMock(return_value=[])
    mock_cursor.description = None
    mock_cursor.rowcount = 0
    return mock_connection


@pytest.fixture
def asyncmy_driver(mock_asyncmy_connection: AsyncMock) -> AsyncmyDriver:
    """Create an Asyncmy driver with mocked connection."""
    config = SQLConfig()  # Disable strict mode for unit tests
    return AsyncmyDriver(connection=mock_asyncmy_connection, config=config)


def test_asyncmy_driver_initialization(mock_asyncmy_connection: AsyncMock) -> None:
    """Test Asyncmy driver initialization."""
    config = SQLConfig()
    driver = AsyncmyDriver(connection=mock_asyncmy_connection, config=config)

    # Test driver attributes are set correctly
    assert driver.connection is mock_asyncmy_connection
    assert driver.config is config
    assert driver.dialect == "mysql"


def test_asyncmy_driver_dialect_property(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver dialect property."""
    assert asyncmy_driver.dialect == "mysql"


def test_asyncmy_driver_placeholder_style(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver placeholder style detection."""
    placeholder_style = asyncmy_driver.parameter_config.default_parameter_style
    assert placeholder_style == ParameterStyle.POSITIONAL_PYFORMAT


@pytest.mark.asyncio
async def test_asyncmy_config_dialect_property() -> None:
    """Test AsyncMy config dialect property."""
    from sqlspec.adapters.asyncmy import AsyncmyConfig

    config = AsyncmyConfig(
        pool_config={"host": "localhost", "port": 3306, "database": "test", "user": "test", "password": "test"}
    )
    assert config.dialect == "mysql"


@pytest.mark.asyncio
async def test_asyncmy_driver_get_cursor(asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock) -> None:
    """Test Asyncmy driver with_cursor context manager."""
    # Get the mock cursor that the fixture set up
    mock_cursor = mock_asyncmy_connection.cursor()

    async with asyncmy_driver.with_cursor(mock_asyncmy_connection) as cursor:
        assert cursor is mock_cursor
        mock_cursor.close.assert_not_called()

    # Verify cursor close was called after context exit
    mock_cursor.close.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_execute_statement_select(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver execute for SELECT statements."""
    # Get the mock cursor from the fixture and configure it
    mock_cursor = mock_asyncmy_connection.cursor()
    mock_cursor.fetchall.return_value = [(1, "test", "test@example.com")]  # Match the 3 columns
    mock_cursor.description = ["id", "name", "email"]

    # Reset call count after setup
    mock_asyncmy_connection.cursor.reset_mock()

    # Create SQL statement with parameters - use qmark style for unit test
    statement = SQL("SELECT * FROM users WHERE id = ?", [1])
    result = await asyncmy_driver.execute(statement)

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"

    # Verify cursor operations
    mock_asyncmy_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_execute_with_parameters(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver execute method with parameters."""
    # Get the mock cursor from the fixture and configure it
    mock_cursor = mock_asyncmy_connection.cursor()
    mock_cursor.description = ["id", "name"]  # Match the SELECT query
    mock_cursor.fetchall.return_value = [(42, "Test User")]

    # Reset call count after setup
    mock_asyncmy_connection.cursor.reset_mock()

    # Create SQL statement with parameters
    statement = SQL("SELECT id, name FROM users WHERE id = ?", 42)
    result = await asyncmy_driver.execute(statement)

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"

    # Verify cursor operations with parameters
    mock_asyncmy_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_non_query_statement(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver with non-query statement."""
    # Setup mock cursor
    mock_cursor = mock_asyncmy_connection.cursor()
    mock_cursor.rowcount = 1

    # Create non-query statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')")
    result = await asyncmy_driver.execute(statement)

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1
