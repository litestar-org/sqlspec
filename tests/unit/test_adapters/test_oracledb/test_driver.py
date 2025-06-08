"""Unit tests for OracleDB drivers."""

import tempfile
from typing import Any
from unittest.mock import AsyncMock, Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.oracledb import (
    OracleAsyncConnection,
    OracleAsyncDriver,
    OracleSyncConnection,
    OracleSyncDriver,
)
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_oracle_sync_connection() -> Mock:
    """Create a mock Oracle sync connection."""
    return Mock(spec=OracleSyncConnection)


@pytest.fixture
def mock_oracle_async_connection() -> AsyncMock:
    """Create a mock Oracle async connection."""
    return AsyncMock(spec=OracleAsyncConnection)


@pytest.fixture
def oracle_sync_driver(mock_oracle_sync_connection: Mock) -> OracleSyncDriver:
    """Create an Oracle sync driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return OracleSyncDriver(
        connection=mock_oracle_sync_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


@pytest.fixture
def oracle_async_driver(mock_oracle_async_connection: Mock) -> OracleAsyncDriver:
    """Create an Oracle async driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return OracleAsyncDriver(
        connection=mock_oracle_async_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


def test_oracle_sync_driver_initialization(mock_oracle_sync_connection: Mock) -> None:
    """Test Oracle sync driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = OracleSyncDriver(
        connection=mock_oracle_sync_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_oracle_sync_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "oracle"
    assert driver.__supports_arrow__ is True


def test_oracle_async_driver_initialization(mock_oracle_async_connection: AsyncMock) -> None:
    """Test Oracle async driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = OracleAsyncDriver(
        connection=mock_oracle_async_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_oracle_async_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "oracle"
    assert driver.__supports_arrow__ is True


def test_oracle_sync_driver_dialect_property(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver dialect property."""
    assert oracle_sync_driver.dialect == "oracle"


def test_oracle_async_driver_dialect_property(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver dialect property."""
    assert oracle_async_driver.dialect == "oracle"


def test_oracle_sync_driver_supports_arrow(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver Arrow support."""
    assert oracle_sync_driver.__supports_arrow__ is True
    assert OracleSyncDriver.__supports_arrow__ is True


def test_oracle_async_driver_supports_arrow(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver Arrow support."""
    assert oracle_async_driver.__supports_arrow__ is True
    assert OracleAsyncDriver.__supports_arrow__ is True


def test_oracle_sync_driver_placeholder_style(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver placeholder style detection."""
    placeholder_style = oracle_sync_driver._get_placeholder_style()
    assert placeholder_style.value == "named_colon"


def test_oracle_async_driver_placeholder_style(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver placeholder style detection."""
    placeholder_style = oracle_async_driver._get_placeholder_style()
    assert placeholder_style.value == "named_colon"


def test_oracle_sync_driver_get_cursor(oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock) -> None:
    """Test Oracle sync driver _get_cursor context manager."""
    mock_cursor = Mock()
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    with oracle_sync_driver._get_cursor(mock_oracle_sync_connection) as cursor:
        assert cursor is mock_cursor

    # Verify cursor was created and closed
    mock_oracle_sync_connection.cursor.assert_called_once()
    mock_cursor.close.assert_called_once()


@pytest.mark.asyncio
async def test_oracle_async_driver_get_cursor(
    oracle_async_driver: OracleAsyncDriver, mock_oracle_async_connection: AsyncMock
) -> None:
    """Test Oracle async driver _get_cursor context manager."""
    mock_cursor = AsyncMock()
    mock_oracle_async_connection.cursor.return_value = mock_cursor

    async with oracle_async_driver._get_cursor(mock_oracle_async_connection) as cursor:
        assert cursor is mock_cursor

    # Verify cursor was created and closed
    mock_oracle_async_connection.cursor.assert_called_once()
    mock_cursor.close.assert_called_once()


def test_oracle_sync_driver_execute_statement_select(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver _execute_statement for SELECT statements."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("SELECT * FROM users WHERE id = 1")

    # Call execute_statement which will handle the mock setup
    result = oracle_sync_driver._execute_statement(statement)

    # Verify the mock was called correctly
    mock_oracle_sync_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.close.assert_called_once()

    # The result should be a dict with expected structure
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_oracle_async_driver_fetch_arrow_table_with_parameters(
    oracle_async_driver: OracleAsyncDriver, mock_oracle_async_connection: AsyncMock
) -> None:
    """Test Oracle async driver fetch_arrow_table method with parameters."""
    # Setup mock cursor and result data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("ID",), ("NAME",)]
    mock_cursor.fetchall.return_value = [(42, "Test User")]
    mock_oracle_async_connection.cursor.return_value = mock_cursor

    # Create SQL statement with parameters
    statement = SQL("SELECT id, name FROM users WHERE id = :user_id", parameters={"user_id": 42})

    # Call execute_statement which will handle the mock setup
    result = await oracle_async_driver._execute_statement(statement)

    # Verify the mock was called correctly
    mock_cursor.execute.assert_called_once()
    mock_cursor.close.assert_called_once()

    # The result should be a dict with expected structure
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_oracle_async_driver_non_query_statement(
    oracle_async_driver: OracleAsyncDriver, mock_oracle_async_connection: AsyncMock
) -> None:
    """Test Oracle async driver with non-query statement."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.rowcount = 1
    mock_cursor.statusmessage = "INSERT 0 1"
    mock_oracle_async_connection.cursor.return_value = mock_cursor

    # Create non-query statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')")
    result = await oracle_async_driver._execute_statement(statement)

    # Verify cursor operations
    mock_cursor.execute.assert_called_once()
    mock_cursor.close.assert_called_once()

    # The result should be a dict with expected structure
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_oracle_async_driver_to_parquet(
    oracle_async_driver: OracleAsyncDriver,
    mock_oracle_async_connection: AsyncMock,
    monkeypatch: "pytest.MonkeyPatch",
) -> None:
    """Test to_parquet writes correct data to a Parquet file (async)."""
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
    mock_oracle_async_connection.cursor.return_value = mock_cursor
    statement = SQL("SELECT id, name FROM users")
    called = {}

    def patched_write_table(table: Any, path: Any, **kwargs: Any) -> None:
        called["table"] = table
        called["path"] = path

    monkeypatch.setattr(pq, "write_table", patched_write_table)
    with tempfile.NamedTemporaryFile() as tmp:
        await oracle_async_driver.export_to_storage(statement.to_sql(), tmp.name)
        assert "table" in called
        assert called["path"] == tmp.name
        assert isinstance(called["table"], pa.Table)
    mock_cursor.close.assert_called_once()
