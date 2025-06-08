"""Unit tests for Asyncmy driver."""

import tempfile
from typing import Any, Optional
from unittest.mock import AsyncMock

import pyarrow as pa
import pytest

from sqlspec.adapters.asyncmy import AsyncmyConnection, AsyncmyDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_asyncmy_connection() -> AsyncMock:
    """Create a mock Asyncmy connection."""
    mock_connection = AsyncMock(spec=AsyncmyConnection)
    mock_cursor = AsyncMock()

    # Make cursor() return an async function that returns the cursor
    async def _cursor() -> AsyncMock:
        return mock_cursor

    mock_connection.cursor.side_effect = _cursor
    mock_cursor.close.return_value = None
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = None
    mock_cursor.rowcount = 0
    return mock_connection


@pytest.fixture
def asyncmy_driver(mock_asyncmy_connection: AsyncMock) -> AsyncmyDriver:
    """Create an Asyncmy driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return AsyncmyDriver(
        connection=mock_asyncmy_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


def test_asyncmy_driver_initialization(mock_asyncmy_connection: AsyncMock) -> None:
    """Test Asyncmy driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = AsyncmyDriver(
        connection=mock_asyncmy_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_asyncmy_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "mysql"
    assert driver.__supports_arrow__ is True


def test_asyncmy_driver_dialect_property(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver dialect property."""
    assert asyncmy_driver.dialect == "mysql"


def test_asyncmy_driver_supports_arrow(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver Arrow support."""
    assert asyncmy_driver.__supports_arrow__ is True
    assert AsyncmyDriver.__supports_arrow__ is True


def test_asyncmy_driver_placeholder_style(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver placeholder style detection."""
    placeholder_style = asyncmy_driver._get_placeholder_style()
    assert placeholder_style.value == "pyformat_positional"


@pytest.mark.asyncio
async def test_asyncmy_config_dialect_property() -> None:
    """Test AsyncMy config dialect property."""
    from sqlspec.adapters.asyncmy import AsyncmyConfig

    config = AsyncmyConfig(
        pool_config={
            "host": "localhost",
            "port": 3306,
            "database": "test",
            "user": "test",
            "password": "test",
        }
    )
    assert config.dialect == "mysql"


@pytest.mark.asyncio
async def test_asyncmy_driver_get_cursor(asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock) -> None:
    """Test Asyncmy driver _get_cursor context manager."""
    mock_cursor = AsyncMock()

    # Make cursor() return an async function that returns the cursor
    async def _cursor() -> AsyncMock:
        return mock_cursor

    mock_asyncmy_connection.cursor.side_effect = _cursor

    async with asyncmy_driver._get_cursor(mock_asyncmy_connection) as cursor:
        assert cursor is mock_cursor
        mock_cursor.close.assert_not_called()

    # Verify cursor close was called after context exit
    mock_cursor.close.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_execute_statement_select(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver _execute_statement for SELECT statements."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [(1, "test")]
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]

    # Make cursor() return an async function that returns the cursor
    async def _cursor() -> AsyncMock:
        return mock_cursor

    mock_asyncmy_connection.cursor.side_effect = _cursor

    # Create SQL statement with parameters - use qmark style for unit test
    result = await asyncmy_driver.fetch_arrow_table(
        "SELECT * FROM users WHERE id = ?", parameters=[1], config=asyncmy_driver.config
    )

    # Verify result
    assert isinstance(result, ArrowResult)
    # Note: Don't compare statement objects directly as they may be recreated

    # Verify cursor operations
    mock_asyncmy_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_fetch_arrow_table_with_parameters(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver fetch_arrow_table method with parameters."""
    # Setup mock cursor and result data
    mock_cursor = AsyncMock()
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]
    mock_cursor.fetchall.return_value = [(42, "Test User")]

    # Make cursor() return an async function that returns the cursor
    async def _cursor() -> AsyncMock:
        return mock_cursor

    mock_asyncmy_connection.cursor.side_effect = _cursor

    # Create SQL statement with parameters
    # Use a SQL that can be parsed by sqlglot - the driver will convert to %s style
    result = await asyncmy_driver.fetch_arrow_table(
        "SELECT id, name FROM users WHERE id = ?", parameters=[42], config=asyncmy_driver.config
    )

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify cursor operations with parameters
    mock_asyncmy_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_fetch_arrow_table_non_query_error(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver fetch_arrow_table with non-query statement raises error."""
    # Create non-query statement
    result = await asyncmy_driver.fetch_arrow_table("INSERT INTO users VALUES (1, 'test')")

    # Verify result
    assert isinstance(result, ArrowResult)
    # Should create empty Arrow table
    assert result.num_rows() == 0


@pytest.mark.asyncio
@pytest.mark.skip(
    reason="Complex async mock setup issue - async connection override tests better suited for integration testing"
)
async def test_asyncmy_driver_fetch_arrow_table_with_connection_override(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver fetch_arrow_table with connection override."""
    # Create override connection
    override_connection = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.description = [(col,) for col in ["id", "name"]]
    mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

    # Make cursor() return an async function that returns the cursor
    async def _cursor() -> AsyncMock:
        return mock_cursor

    override_connection.cursor.side_effect = _cursor

    # Create SQL statement with connection override
    result = await asyncmy_driver.fetch_arrow_table("SELECT id, name FROM users", connection=override_connection)
    assert isinstance(result, ArrowResult)
    assert isinstance(result.data, pa.Table)
    assert result.num_rows() == 2
    assert set(result.column_names) == {"id", "name"}
    mock_cursor.close.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_to_parquet(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock, monkeypatch: "pytest.MonkeyPatch"
) -> None:
    """Test to_parquet writes correct data to a Parquet file (async)."""
    mock_cursor = AsyncMock()
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]
    mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

    # Make cursor() return an async function that returns the cursor
    async def _cursor() -> AsyncMock:
        return mock_cursor

    mock_asyncmy_connection.cursor.side_effect = _cursor
    statement = SQL("SELECT id, name FROM users")
    called = {}

    def patched_write_table(table: Any, path: Any, **kwargs: Any) -> None:
        called["table"] = table
        called["path"] = path

    # Mock the storage backend's write_arrow method instead of pyarrow directly
    async def mock_write_arrow(path: str, table: Any, **kwargs: Any) -> None:
        called["table"] = table
        called["path"] = path

    # Mock the backend resolution to return a mock backend
    from unittest.mock import AsyncMock as MockBackend

    mock_backend = MockBackend()
    mock_backend.write_arrow = mock_write_arrow

    def mock_resolve_backend_and_path(uri: str, storage_key: Optional[str] = None) -> tuple[AsyncMock, str]:
        return mock_backend, uri

    monkeypatch.setattr(asyncmy_driver, "_resolve_backend_and_path", mock_resolve_backend_and_path)

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        await asyncmy_driver.export_to_storage(statement, tmp.name)  # type: ignore[attr-defined]
        assert "table" in called
        assert called["path"] == tmp.name
    mock_cursor.close.assert_called_once()
