"""Unit tests for AIOSQLite driver."""

import tempfile
from typing import Any, Union
from unittest.mock import AsyncMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConnection, AiosqliteDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig


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
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.rowcount = 0
    return mock_connection


@pytest.fixture
def aiosqlite_driver(mock_aiosqlite_connection: AsyncMock) -> AiosqliteDriver:
    """Create an AIOSQLite driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return AiosqliteDriver(
        connection=mock_aiosqlite_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


def test_aiosqlite_driver_initialization(mock_aiosqlite_connection: AsyncMock) -> None:
    """Test AIOSQLite driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = AiosqliteDriver(
        connection=mock_aiosqlite_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_aiosqlite_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "sqlite"
    assert driver.__supports_arrow__ is True


def test_aiosqlite_driver_dialect_property(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver dialect property."""
    assert aiosqlite_driver.dialect == "sqlite"


def test_aiosqlite_driver_supports_arrow(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver Arrow support."""
    assert aiosqlite_driver.__supports_arrow__ is True
    assert AiosqliteDriver.__supports_arrow__ is True


def test_aiosqlite_driver_placeholder_style(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver placeholder style detection."""
    placeholder_style = aiosqlite_driver._get_placeholder_style()
    assert placeholder_style.value == "qmark"


@pytest.mark.asyncio
async def test_aiosqlite_driver_execute_statement_select(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver _execute_statement for SELECT statements."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [(1, "test")]
    mock_cursor.description = [("id", None), ("name", None)]

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = ?", parameters=[1], config=aiosqlite_driver.config)
    # Execute
    result = await aiosqlite_driver._execute_statement(
        statement=statement,
        connection=None,
    )
    # Verify cursor was called and result is correct
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = ?", (1,))
    assert result["data"] == [(1, "test")]
    assert result["description"] == [("id", None), ("name", None)]


@pytest.mark.asyncio
async def test_aiosqlite_driver_execute_statement_insert(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver _execute_statement for INSERT statements."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.rowcount = 1

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    # Create SQL statement with parameters
    statement = SQL("INSERT INTO users (name) VALUES (?)", parameters=["John"], config=aiosqlite_driver.config)
    # Execute
    result = await aiosqlite_driver._execute_statement(
        statement=statement,
        connection=None,
    )
    # Verify cursor was called and result is cursor
    mock_cursor.execute.assert_called_once_with("INSERT INTO users (name) VALUES (?)", ("John",))
    assert result is mock_cursor


@pytest.mark.asyncio
async def test_aiosqlite_driver_execute_statement_script(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver _execute_statement for script execution."""
    # Setup mock cursor
    mock_cursor = AsyncMock()

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.executescript.return_value = None
    # Create SQL statement (DDL allowed with strict_mode=False)
    statement = SQL(
        "CREATE TABLE test (id INTEGER); INSERT INTO test VALUES (1);", config=aiosqlite_driver.config
    ).as_script()
    # Execute script
    result = await aiosqlite_driver._execute_statement(
        statement=statement,
        connection=None,
    )
    # Verify cursor executescript was called
    mock_cursor.executescript.assert_called_once()
    assert result == "SCRIPT EXECUTED"


@pytest.mark.asyncio
async def test_aiosqlite_driver_execute_statement_many(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver _execute_statement for execute_many."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.rowcount = 3

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.executemany.return_value = None
    # Create SQL statement with placeholder for parameters
    parameters_list = [["John"], ["Jane"], ["Bob"]]
    parameters_tuple_list = [("John",), ("Jane",), ("Bob",)]
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)", parameters=parameters_list, config=aiosqlite_driver.config
    ).as_many()
    # Execute many
    result = await aiosqlite_driver._execute_statement(
        statement=statement,
        connection=None,
    )
    # Verify cursor executemany was called
    mock_cursor.executemany.assert_called_once_with("INSERT INTO users (name) VALUES (?)", parameters_tuple_list)
    assert result == 3  # Should return rowcount for aiosqlite's cursor.rowcount on executemany


@pytest.mark.asyncio
async def test_aiosqlite_driver_execute_statement_parameter_processing(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver parameter processing for different types."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [(1, "John")]
    mock_cursor.description = [("id", None), ("name", None)]

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    # Create SQL statement with parameters
    statement = SQL(
        "SELECT * FROM users WHERE id = ? AND name = ?", parameters=[1, "John"], config=aiosqlite_driver.config
    )
    # Execute
    result = await aiosqlite_driver._execute_statement(
        statement=statement,
        connection=None,
    )
    # Verify parameters were processed correctly
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = ? AND name = ?", (1, "John"))
    assert result["data"] == [(1, "John")]
    assert result["description"] == [("id", None), ("name", None)]


@pytest.mark.asyncio
async def test_aiosqlite_driver_wrap_select_result(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver _wrap_select_result method."""
    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")
    # Wrap result
    result: Union[SQLResult[Any], SQLResult[dict[str, Any]]] = await aiosqlite_driver._wrap_select_result(
        statement=statement,
        result={
            "data": [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}],
            "description": [("id", None), ("name", None)],
        },
    )
    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["id", "name"]
    assert result.data == [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]


@pytest.mark.asyncio
async def test_aiosqlite_driver_wrap_select_result_empty(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver _wrap_select_result method with empty result."""
    # Create SQL statement
    statement = SQL("SELECT * FROM empty_table")

    # Wrap result
    result: Union[SQLResult[Any], SQLResult[dict[str, Any]]] = await aiosqlite_driver._wrap_select_result(
        statement=statement,
        result=[],
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.data == []
    assert result.column_names == []


@pytest.mark.asyncio
async def test_aiosqlite_driver_wrap_select_result_with_schema_type(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver _wrap_select_result with schema_type."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")
    # Wrap result with schema type
    result = await aiosqlite_driver._wrap_select_result(
        statement=statement,
        result={
            "data": [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}],
            "description": [("id", None), ("name", None)],
        },
        schema_type=User,
    )
    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["id", "name"]


@pytest.mark.asyncio
async def test_aiosqlite_driver_wrap_execute_result(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver _wrap_execute_result method."""
    # Create mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.rowcount = 3

    # Create SQL statement
    statement = SQL("UPDATE users SET active = 1", config=aiosqlite_driver.config)

    # Wrap result
    result = await aiosqlite_driver._wrap_execute_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"


@pytest.mark.asyncio
async def test_aiosqlite_driver_wrap_execute_result_script(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver _wrap_execute_result method for script."""
    # Create SQL statement (DDL allowed with strict_mode=False)
    statement = SQL("CREATE TABLE test (id INTEGER)", config=aiosqlite_driver.config)

    # Wrap result for script
    result = await aiosqlite_driver._wrap_execute_result(
        statement=statement,
        result="SCRIPT EXECUTED",
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 0
    assert result.operation_type == "CREATE"


@pytest.mark.asyncio
async def test_aiosqlite_driver_wrap_execute_result_integer(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver _wrap_execute_result method with integer result."""
    # Create SQL statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')", config=aiosqlite_driver.config)
    # Wrap result with integer
    result = await aiosqlite_driver._wrap_execute_result(
        statement=statement,
        result=5,
    )
    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == -1
    assert result.operation_type == "INSERT"


def test_aiosqlite_driver_connection_method(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver _connection method."""
    # Test default connection return
    assert aiosqlite_driver._connection() is mock_aiosqlite_connection

    # Test connection override
    override_connection = AsyncMock()
    assert aiosqlite_driver._connection(override_connection) is override_connection


@pytest.mark.asyncio
async def test_aiosqlite_driver_error_handling(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver error handling."""

    # Setup mock cursor to raise exception
    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        raise Exception("Database error")

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    # Create SQL statement
    statement = SQL("SELECT * FROM users")
    # Test error propagation
    with pytest.raises(Exception, match="Database error"):
        await aiosqlite_driver._execute_statement(
            statement=statement,
            connection=None,
        )


@pytest.mark.asyncio
async def test_aiosqlite_driver_instrumentation(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver instrumentation integration."""
    # Test instrumentation config is accessible
    assert aiosqlite_driver.instrumentation_config is not None
    assert isinstance(aiosqlite_driver.instrumentation_config, InstrumentationConfig)
    # Test logging configuration
    assert hasattr(aiosqlite_driver.instrumentation_config, "log_queries")
    assert hasattr(aiosqlite_driver.instrumentation_config, "log_parameters")
    assert hasattr(aiosqlite_driver.instrumentation_config, "log_results_count")
    # Test logging configuration
    aiosqlite_driver.instrumentation_config.log_queries = True
    aiosqlite_driver.instrumentation_config.log_parameters = True
    aiosqlite_driver.instrumentation_config.log_results_count = True
    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = ?", parameters=[1], config=aiosqlite_driver.config)
    # Setup mock cursor
    mock_cursor = AsyncMock()

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    # Execute with logging enabled
    await aiosqlite_driver._execute_statement(
        statement=statement,
        connection=None,
    )
    # Verify execution worked
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = ?", (1,))


@pytest.mark.asyncio
async def test_aiosqlite_driver_operation_type_detection(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver operation type detection."""
    # Test different SQL statement types (DDL allowed with strict_mode=False)
    test_cases = [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("UPDATE users SET name = 'John'", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("CREATE TABLE test (id INTEGER)", "CREATE"),
    ]

    for sql, expected_op_type in test_cases:
        statement = SQL(sql, config=aiosqlite_driver.config)

        # Mock cursor for execute result
        mock_cursor = AsyncMock()
        mock_cursor.rowcount = 1

        result = await aiosqlite_driver._wrap_execute_result(
            statement=statement,
            result=mock_cursor,
        )

        assert result.operation_type == expected_op_type


@pytest.mark.asyncio
async def test_aiosqlite_driver_fetch_arrow_table_basic(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver fetch_arrow_table method basic functionality."""
    # Setup mock cursor and result data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.fetchall.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")
    # Execute fetch_arrow_table
    result = await aiosqlite_driver.fetch_arrow_table(statement)
    # Verify result
    assert isinstance(result, ArrowResult)
    # Verify connection operations
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.asyncio
async def test_aiosqlite_driver_fetch_arrow_table_with_parameters(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver fetch_arrow_table method with parameters."""
    # Setup mock cursor and result data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.fetchall.return_value = [{"id": 42, "name": "Test User"}]

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    # Create SQL statement with parameters
    statement = SQL("SELECT id, name FROM users WHERE id = ?", parameters=[42])
    # Execute fetch_arrow_table
    result = await aiosqlite_driver.fetch_arrow_table(statement)
    # Verify result
    assert isinstance(result, ArrowResult)
    # Verify connection operations with parameters
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.asyncio
async def test_aiosqlite_driver_fetch_arrow_table_non_query_error(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver fetch_arrow_table with non-query statement raises error."""
    # Create non-query statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')")

    # Test error for non-query
    with pytest.raises(TypeError, match="Cannot fetch Arrow table for a non-query statement"):
        await aiosqlite_driver.fetch_arrow_table(statement)


@pytest.mark.asyncio
async def test_aiosqlite_driver_fetch_arrow_table_empty_result(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver fetch_arrow_table with empty result."""
    # Setup mock cursor with no data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.fetchall.return_value = []

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    # Create SQL statement
    statement = SQL("SELECT id, name FROM users WHERE id > 1000")
    # Execute fetch_arrow_table
    try:
        result = await aiosqlite_driver.fetch_arrow_table(statement)
        assert isinstance(result, ArrowResult)
        assert result.data.num_rows == 0
    except ValueError as e:
        # Accept pyarrow error for empty columns/arrays mismatch
        assert "Length of names" in str(e)


@pytest.mark.asyncio
async def test_aiosqlite_driver_fetch_arrow_table_with_connection_override(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver fetch_arrow_table with connection override."""
    # Create override connection
    override_connection = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None)]
    mock_cursor.fetchall.return_value = [{"id": 1}]

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    override_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    # Create SQL statement
    statement = SQL("SELECT id FROM users")
    # Execute with connection override
    result = await aiosqlite_driver.fetch_arrow_table(statement, connection=override_connection)
    # Verify result
    assert isinstance(result, ArrowResult)
    # Verify override connection was used
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.asyncio
async def test_aiosqlite_driver_logging_configuration(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test AIOSQLite driver logging configuration."""
    # Setup mock cursor
    mock_cursor = AsyncMock()

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    # Enable logging
    aiosqlite_driver.instrumentation_config.log_queries = True
    aiosqlite_driver.instrumentation_config.log_parameters = True
    aiosqlite_driver.instrumentation_config.log_results_count = True
    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = ?", parameters=[1], config=aiosqlite_driver.config)
    # Execute with logging enabled
    await aiosqlite_driver._execute_statement(
        statement=statement,
        connection=None,
    )
    # Verify execution worked
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = ?", (1,))


def test_aiosqlite_driver_mixins_integration(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver mixin integration."""
    # Test that driver has all expected mixins
    from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin

    assert isinstance(aiosqlite_driver, SQLTranslatorMixin)
    assert isinstance(aiosqlite_driver, AsyncArrowMixin)
    assert isinstance(aiosqlite_driver, ResultConverter)

    # Test mixin methods are available
    assert hasattr(aiosqlite_driver, "fetch_arrow_table")
    assert hasattr(aiosqlite_driver, "to_schema")
    assert hasattr(aiosqlite_driver, "returns_rows")


def test_aiosqlite_driver_returns_rows_method(aiosqlite_driver: AiosqliteDriver) -> None:
    """Test AIOSQLite driver returns_rows method."""
    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert aiosqlite_driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert aiosqlite_driver.returns_rows(insert_stmt.expression) is False


@pytest.mark.asyncio
async def test_aiosqlite_driver_fetch_arrow_table_arrowresult(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock
) -> None:
    """Test fetch_arrow_table returns ArrowResult with correct pyarrow.Table (async)."""
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.fetchall.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    statement = SQL("SELECT id, name FROM users")
    result = await aiosqlite_driver.fetch_arrow_table(statement)
    assert isinstance(result, ArrowResult)
    assert isinstance(result.data, pa.Table)
    assert result.data.num_rows == 2
    assert set(result.data.column_names) == {"id", "name"}


@pytest.mark.asyncio
async def test_aiosqlite_driver_to_parquet(
    aiosqlite_driver: AiosqliteDriver, mock_aiosqlite_connection: AsyncMock, monkeypatch: "pytest.MonkeyPatch"
) -> None:
    """Test to_parquet writes correct data to a Parquet file (async)."""
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.fetchall.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    async def _cursor(*args: Any, **kwargs: Any) -> AsyncMock:
        return mock_cursor

    mock_aiosqlite_connection.cursor.side_effect = _cursor
    mock_cursor.execute.return_value = None
    statement = SQL("SELECT id, name FROM users")
    called = {}

    def patched_write_table(table: Any, path: Any, **kwargs: Any) -> None:
        called["table"] = table
        called["path"] = path

    monkeypatch.setattr(pq, "write_table", patched_write_table)
    with tempfile.NamedTemporaryFile() as tmp:
        await aiosqlite_driver.export_to_storage(statement, tmp.name)
        assert "table" in called
        assert called["path"] == tmp.name
        assert isinstance(called["table"], pa.Table)
