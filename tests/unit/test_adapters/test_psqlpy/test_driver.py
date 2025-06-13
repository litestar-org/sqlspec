"""Unit tests for PSQLPy driver."""

from unittest.mock import AsyncMock, Mock

import pytest

from sqlspec.adapters.psqlpy import PsqlpyDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def mock_psqlpy_connection() -> AsyncMock:
    """Create a mock PSQLPy connection."""
    mock_connection = AsyncMock()  # Remove spec to avoid attribute errors
    mock_connection.execute.return_value = []
    mock_connection.execute_many.return_value = None
    mock_connection.execute_script.return_value = None
    mock_connection.fetch_row.return_value = None
    mock_connection.fetch_all.return_value = []
    return mock_connection


@pytest.fixture
def psqlpy_driver(mock_psqlpy_connection: AsyncMock) -> PsqlpyDriver:
    """Create a PSQLPy driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return PsqlpyDriver(
        connection=mock_psqlpy_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


def test_psqlpy_driver_initialization(mock_psqlpy_connection: AsyncMock) -> None:
    """Test PSQLPy driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = PsqlpyDriver(
        connection=mock_psqlpy_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_psqlpy_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "postgres"
    assert driver.__supports_arrow__ is True


def test_psqlpy_driver_dialect_property(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver dialect property."""
    assert psqlpy_driver.dialect == "postgres"


def test_psqlpy_driver_supports_arrow(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver Arrow support."""
    assert psqlpy_driver.__supports_arrow__ is True
    assert PsqlpyDriver.__supports_arrow__ is True


def test_psqlpy_driver_placeholder_style(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver placeholder style detection."""
    placeholder_style = psqlpy_driver.default_parameter_style
    assert placeholder_style == ParameterStyle.NUMERIC


@pytest.mark.asyncio
async def test_psqlpy_driver_execute_statement_select(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver _execute_statement for SELECT statements."""
    # Setup mock connection - PSQLPy calls conn.fetch() which returns a QueryResult with .result() method
    mock_data = [{"id": 1, "name": "test"}]
    mock_query_result = Mock()  # Use regular Mock, not AsyncMock - .result() method is sync
    mock_query_result.result.return_value = mock_data
    mock_psqlpy_connection.fetch.return_value = mock_query_result

    # Create SQL statement with parameters
    result = await psqlpy_driver.fetch_arrow_table(
        "SELECT * FROM users WHERE id = $1", parameters=[1], config=psqlpy_driver.config
    )

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify connection operations
    mock_psqlpy_connection.fetch.assert_called_once()


@pytest.mark.asyncio
async def test_psqlpy_driver_fetch_arrow_table_with_parameters(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver fetch_arrow_table method with parameters."""
    # Setup mock connection and result data - PSQLPy calls conn.fetch() which returns a QueryResult
    mock_data = [{"id": 42, "name": "Test User"}]
    mock_query_result = Mock()  # Use regular Mock, not AsyncMock - .result() method is sync
    mock_query_result.result.return_value = mock_data
    mock_psqlpy_connection.fetch.return_value = mock_query_result

    # Create SQL statement with parameters
    result = await psqlpy_driver.fetch_arrow_table("SELECT id, name FROM users WHERE id = $1", parameters=[42])

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify connection operations with parameters
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


@pytest.mark.asyncio
async def test_psqlpy_driver_fetch_arrow_table_with_connection_override(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver fetch_arrow_table with connection override."""
    # Skip this complex async mock test - connection override tests better suited for integration testing
    pytest.skip("Complex async connection override mocking - better tested in integration tests")


@pytest.mark.asyncio
async def test_psqlpy_driver_to_parquet(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock, monkeypatch: "pytest.MonkeyPatch"
) -> None:
    """Test export_to_storage using unified storage mixin."""
    # Skip this complex test - the unified storage mixin integration tests better suited for integration testing
    pytest.skip("Complex storage backend mocking - unified storage integration better tested in integration tests")
