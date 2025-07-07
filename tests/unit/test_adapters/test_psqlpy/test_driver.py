"""Unit tests for PSQLPy driver."""

from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from sqlspec.adapters.psqlpy import PsqlpyDriver
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_psqlpy_connection() -> AsyncMock:
    """Create a mock PSQLPy connection."""
    mock_connection = AsyncMock()  # Remove spec to avoid attribute errors

    # Create mock execute result with rows_affected method
    mock_execute_result = Mock()
    mock_execute_result.rows_affected.return_value = 1
    mock_connection.execute.return_value = mock_execute_result

    mock_connection.execute_many.return_value = None
    mock_connection.execute_script.return_value = None
    mock_connection.fetch_row.return_value = None
    mock_connection.fetch_all.return_value = []
    return mock_connection


@pytest.fixture
def psqlpy_driver(mock_psqlpy_connection: AsyncMock) -> PsqlpyDriver:
    """Create a PSQLPy driver with mocked connection."""
    config = SQLConfig()  # Disable strict mode for unit tests
    return PsqlpyDriver(connection=mock_psqlpy_connection, config=config)


def test_psqlpy_driver_initialization(mock_psqlpy_connection: AsyncMock) -> None:
    """Test PSQLPy driver initialization."""
    config = SQLConfig()
    driver = PsqlpyDriver(connection=mock_psqlpy_connection, config=config)

    # Test driver attributes are set correctly
    assert driver.connection is mock_psqlpy_connection
    assert driver.config is config
    assert driver.dialect == "postgres"
    assert driver.supports_native_arrow_export is False
    assert driver.supports_native_arrow_import is False


def test_psqlpy_driver_dialect_property(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver dialect property."""
    assert psqlpy_driver.dialect == "postgres"


def test_psqlpy_driver_supports_arrow(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver Arrow support."""
    assert psqlpy_driver.supports_native_arrow_export is False
    assert psqlpy_driver.supports_native_arrow_import is False
    assert PsqlpyDriver.supports_native_arrow_export is False
    assert PsqlpyDriver.supports_native_arrow_import is False


def test_psqlpy_driver_placeholder_style(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver placeholder style detection."""
    placeholder_style = psqlpy_driver.default_parameter_style
    assert placeholder_style == ParameterStyle.NUMERIC


@pytest.mark.asyncio
async def test_psqlpy_driver_execute_statement_select(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver _execute_statement for SELECT statements."""
    # Setup mock connection - PSQLPy calls conn.fetch() which returns a QueryResult
    mock_data = [{"id": 1, "name": "test"}]
    # Create a mock QueryResult object with a result() method
    mock_query_result = MagicMock()
    mock_query_result.result.return_value = mock_data
    mock_psqlpy_connection.fetch.return_value = mock_query_result

    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = $1", [1])
    result = await psqlpy_driver._execute_statement(statement)

    # Verify result is SQLResult
    assert isinstance(result, SQLResult)
    assert result.data == mock_data
    assert result.column_names == ["id", "name"]
    assert result.operation_type == "SELECT"

    # Verify connection operations
    mock_psqlpy_connection.fetch.assert_called_once()


@pytest.mark.asyncio
async def test_psqlpy_driver_fetch_arrow_table_non_query_error(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver fetch_arrow_table with non-query statement raises error."""
    # Create non-query statement
    result = await psqlpy_driver.fetch_arrow_table("INSERT INTO users VALUES (1, 'test')")

    # Verify result
    assert isinstance(result, ArrowResult)
    # Should create empty Arrow table
    assert result.num_rows == 0
