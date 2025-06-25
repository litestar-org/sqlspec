"""Unit tests for OracleDB drivers."""

from unittest.mock import AsyncMock, Mock

import pytest

from sqlspec.adapters.oracledb import OracleAsyncConnection, OracleAsyncDriver, OracleSyncConnection, OracleSyncDriver
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.sql import SQLConfig


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
    return OracleSyncDriver(connection=mock_oracle_sync_connection, config=config)


@pytest.fixture
def oracle_async_driver(mock_oracle_async_connection: Mock) -> OracleAsyncDriver:
    """Create an Oracle async driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    return OracleAsyncDriver(connection=mock_oracle_async_connection, config=config)


def test_oracle_sync_driver_initialization(mock_oracle_sync_connection: Mock) -> None:
    """Test Oracle sync driver initialization."""
    config = SQLConfig()
    driver = OracleSyncDriver(connection=mock_oracle_sync_connection, config=config)

    # Test driver attributes are set correctly
    assert driver.connection is mock_oracle_sync_connection
    assert driver.config is config
    assert driver.dialect == "oracle"
    assert driver.supports_native_arrow_export is False
    assert driver.supports_native_arrow_import is False


def test_oracle_async_driver_initialization(mock_oracle_async_connection: AsyncMock) -> None:
    """Test Oracle async driver initialization."""
    config = SQLConfig()
    driver = OracleAsyncDriver(connection=mock_oracle_async_connection, config=config)

    # Test driver attributes are set correctly
    assert driver.connection is mock_oracle_async_connection
    assert driver.config is config
    assert driver.dialect == "oracle"
    assert driver.supports_native_arrow_export is False
    assert driver.supports_native_arrow_import is False


def test_oracle_sync_driver_dialect_property(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver dialect property."""
    assert oracle_sync_driver.dialect == "oracle"


def test_oracle_async_driver_dialect_property(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver dialect property."""
    assert oracle_async_driver.dialect == "oracle"


def test_oracle_sync_driver_supports_arrow(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver Arrow support."""
    assert oracle_sync_driver.supports_native_arrow_export is False
    assert oracle_sync_driver.supports_native_arrow_import is False
    assert OracleSyncDriver.supports_native_arrow_export is False
    assert OracleSyncDriver.supports_native_arrow_import is False


def test_oracle_async_driver_supports_arrow(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver Arrow support."""
    assert oracle_async_driver.supports_native_arrow_export is False
    assert oracle_async_driver.supports_native_arrow_import is False
    assert OracleAsyncDriver.supports_native_arrow_export is False
    assert OracleAsyncDriver.supports_native_arrow_import is False


def test_oracle_sync_driver_placeholder_style(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver placeholder style detection."""
    placeholder_style = oracle_sync_driver.default_parameter_style
    assert placeholder_style == ParameterStyle.NAMED_COLON


def test_oracle_async_driver_placeholder_style(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver placeholder style detection."""
    placeholder_style = oracle_async_driver.default_parameter_style
    assert placeholder_style == ParameterStyle.NAMED_COLON


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
