"""Unit tests for Psycopg drivers."""

import tempfile
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.psycopg import (
    PsycopgAsyncConnection,
    PsycopgAsyncDriver,
    PsycopgSyncConnection,
    PsycopgSyncDriver,
)
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_psycopg_sync_connection() -> Mock:
    """Create a mock Psycopg sync connection."""
    mock_connection = Mock(spec=PsycopgSyncConnection)
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]
    mock_cursor.rowcount = 0
    mock_cursor.statusmessage = "EXECUTE"
    return mock_connection


@pytest.fixture
def mock_psycopg_async_connection() -> AsyncMock:
    """Create a mock Psycopg async connection."""
    mock_connection = AsyncMock(spec=PsycopgAsyncConnection)
    mock_cursor = AsyncMock()
    mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_connection.cursor.return_value.__aexit__.return_value = None
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]
    mock_cursor.rowcount = 0
    mock_cursor.statusmessage = "EXECUTE"
    return mock_connection


@pytest.fixture
def psycopg_sync_driver(mock_psycopg_sync_connection: PsycopgSyncConnection) -> PsycopgSyncDriver:
    """Create a Psycopg sync driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return PsycopgSyncDriver(
        connection=mock_psycopg_sync_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


@pytest.fixture
def psycopg_async_driver(mock_psycopg_async_connection: AsyncMock) -> PsycopgAsyncDriver:
    """Create a Psycopg async driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return PsycopgAsyncDriver(
        connection=mock_psycopg_async_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


def test_psycopg_sync_driver_initialization(mock_psycopg_sync_connection: PsycopgSyncConnection) -> None:
    """Test Psycopg sync driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = PsycopgSyncDriver(
        connection=mock_psycopg_sync_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_psycopg_sync_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "postgres"
    assert driver.__supports_arrow__ is False


def test_psycopg_async_driver_initialization(mock_psycopg_async_connection: AsyncMock) -> None:
    """Test Psycopg async driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = PsycopgAsyncDriver(
        connection=mock_psycopg_async_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_psycopg_async_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "postgres"
    assert driver.__supports_arrow__ is False


def test_psycopg_sync_driver_dialect_property(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver dialect property."""
    assert psycopg_sync_driver.dialect == "postgres"


def test_psycopg_config_dialect_property() -> None:
    """Test Psycopg config dialect property."""
    from sqlspec.adapters.psycopg import PsycopgConfig

    config = PsycopgConfig(connection_string="postgresql://test:test@localhost/test")
    assert config.dialect == "postgres"


def test_psycopg_async_driver_dialect_property(psycopg_async_driver: PsycopgAsyncDriver) -> None:
    """Test Psycopg async driver dialect property."""
    assert psycopg_async_driver.dialect == "postgres"


def test_psycopg_sync_driver_supports_arrow(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver Arrow support."""
    assert psycopg_sync_driver.__supports_arrow__ is False
    assert PsycopgSyncDriver.__supports_arrow__ is False


def test_psycopg_async_driver_supports_arrow(psycopg_async_driver: PsycopgAsyncDriver) -> None:
    """Test Psycopg async driver Arrow support."""
    assert psycopg_async_driver.__supports_arrow__ is False
    assert PsycopgAsyncDriver.__supports_arrow__ is False


def test_psycopg_sync_driver_placeholder_style(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver placeholder style detection."""
    placeholder_style = psycopg_sync_driver._get_placeholder_style()
    assert placeholder_style.value == "pyformat_named"


def test_psycopg_async_driver_placeholder_style(psycopg_async_driver: PsycopgAsyncDriver) -> None:
    """Test Psycopg async driver placeholder style detection."""
    placeholder_style = psycopg_async_driver._get_placeholder_style()
    assert placeholder_style.value == "pyformat_named"


def test_psycopg_sync_driver_get_cursor(
    psycopg_sync_driver: PsycopgSyncDriver, mock_psycopg_sync_connection: PsycopgSyncConnection
) -> None:
    """Test Psycopg sync driver _get_cursor context manager."""
    mock_cursor = MagicMock()
    mock_psycopg_sync_connection.cursor = MagicMock(return_value=mock_cursor)
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    with psycopg_sync_driver._get_cursor(mock_psycopg_sync_connection) as cursor:
        assert cursor is mock_cursor
    mock_psycopg_sync_connection.cursor.assert_called_once()


@pytest.mark.asyncio
async def test_psycopg_async_driver_get_cursor(
    psycopg_async_driver: PsycopgAsyncDriver, mock_psycopg_async_connection: AsyncMock
) -> None:
    """Test Psycopg async driver _get_cursor context manager."""
    mock_cursor = AsyncMock()
    mock_psycopg_async_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_psycopg_async_connection.cursor.return_value.__aexit__.return_value = None

    async with psycopg_async_driver._get_cursor(mock_psycopg_async_connection) as cursor:
        assert cursor is mock_cursor

    # Verify cursor context manager was used
    mock_psycopg_async_connection.cursor.assert_called_once()


def test_psycopg_sync_driver_execute_statement_select(
    psycopg_sync_driver: PsycopgSyncDriver, mock_psycopg_sync_connection: PsycopgSyncConnection
) -> None:
    """Test Psycopg sync driver _execute_statement for SELECT statements."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [{"id": 1, "name": "test"}]
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]
    mock_psycopg_sync_connection.cursor = MagicMock(return_value=mock_cursor)
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    # Create SQL statement
    statement = SQL("SELECT * FROM users WHERE id = 1")

    # Call execute_statement which will handle the mock setup
    result = psycopg_sync_driver._execute_statement(statement)

    # Verify the mock was called correctly
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()

    # The result should be a dict with expected structure
    assert isinstance(result, dict)
    assert "column_names" in result
    assert "data" in result


@pytest.mark.asyncio
async def test_psycopg_async_driver_to_parquet(
    psycopg_async_driver: PsycopgAsyncDriver,
    mock_psycopg_async_connection: AsyncMock,
    monkeypatch: "pytest.MonkeyPatch",
) -> None:
    """Test to_parquet writes correct data to a Parquet file (async)."""
    mock_cursor = mock_psycopg_async_connection.cursor.return_value.__aenter__.return_value
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]
    mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
    statement = SQL("SELECT id, name FROM users")
    called = {}

    def patched_write_table(table: Any, path: Any, **kwargs: Any) -> None:
        called["table"] = table
        called["path"] = path

    monkeypatch.setattr(pq, "write_table", patched_write_table)

    with tempfile.NamedTemporaryFile() as tmp:
        await psycopg_async_driver.export_to_storage(statement.to_sql(), tmp.name)
        assert "table" in called
        assert called["path"] == tmp.name
        assert isinstance(called["table"], pa.Table)
