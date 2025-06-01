"""Unit tests for Asyncmy driver."""

from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.asyncmy import AsyncmyConnection, AsyncmyDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_asyncmy_connection() -> AsyncMock:
    """Create a mock Asyncmy connection."""
    mock_connection = AsyncMock(spec=AsyncmyConnection)
    mock_cursor = AsyncMock()
    mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_connection.cursor.return_value.__aexit__.return_value = None
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
async def test_asyncmy_driver_get_cursor(asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock) -> None:
    """Test Asyncmy driver _get_cursor context manager."""
    mock_cursor = AsyncMock()
    mock_asyncmy_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_asyncmy_connection.cursor.return_value.__aexit__.return_value = None

    async with asyncmy_driver._get_cursor(mock_asyncmy_connection) as cursor:
        assert cursor is mock_cursor
        mock_cursor.close.assert_not_called()

    # Verify cursor close was called after context exit
    mock_cursor.close.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_execute_impl_select(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver _execute_impl for SELECT statements."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [(1, "test")]
    mock_cursor.description = [("id", None), ("name", None)]
    mock_asyncmy_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_asyncmy_connection.cursor.return_value.__aexit__.return_value = None

    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = %s", parameters=[1], config=asyncmy_driver.config)

    # Execute
    result = await asyncmy_driver._execute_impl(
        statement=statement,
        parameters=None,
        connection=None,
        config=None,
        is_many=False,
        is_script=False,
    )

    # Verify cursor was created and execute was called
    mock_asyncmy_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = %s", [1])
    assert result == ([(1, "test")], [("id", None), ("name", None)])


@pytest.mark.asyncio
async def test_asyncmy_driver_execute_impl_insert(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver _execute_impl for INSERT statements."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.rowcount = 1
    mock_asyncmy_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_asyncmy_connection.cursor.return_value.__aexit__.return_value = None

    # Create SQL statement with parameters
    statement = SQL("INSERT INTO users (name) VALUES (%s)", parameters=["John"], config=asyncmy_driver.config)

    # Execute
    result = await asyncmy_driver._execute_impl(
        statement=statement,
        parameters=None,
        connection=None,
        config=None,
        is_many=False,
        is_script=False,
    )

    # Verify cursor was created and execute was called
    mock_asyncmy_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("INSERT INTO users (name) VALUES (%s)", ["John"])
    assert result is mock_cursor


@pytest.mark.asyncio
async def test_asyncmy_driver_execute_impl_script(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver _execute_impl for script execution."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_asyncmy_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_asyncmy_connection.cursor.return_value.__aexit__.return_value = None

    # Create SQL statement (DDL allowed with strict_mode=False)
    statement = SQL("CREATE TABLE test (id INT); INSERT INTO test VALUES (1);", config=asyncmy_driver.config)

    # Execute script
    result = await asyncmy_driver._execute_impl(
        statement=statement,
        parameters=None,
        connection=None,
        config=None,
        is_many=False,
        is_script=True,
    )

    # Verify cursor was created and execute was called with multi=True
    mock_asyncmy_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    # Check that multi=True was passed to execute
    execute_call_args = mock_cursor.execute.call_args
    assert execute_call_args.kwargs.get("multi") is True
    assert result == "SCRIPT EXECUTED"


@pytest.mark.asyncio
async def test_asyncmy_driver_execute_impl_many(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver _execute_impl for execute_many."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.rowcount = 3
    mock_asyncmy_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_asyncmy_connection.cursor.return_value.__aexit__.return_value = None

    # Create SQL statement with placeholder for parameters
    statement = SQL("INSERT INTO users (name) VALUES (%s)", parameters=["dummy"], config=asyncmy_driver.config)
    parameters = [["John"], ["Jane"], ["Bob"]]

    # Execute many
    result = await asyncmy_driver._execute_impl(
        statement=statement,
        parameters=parameters,
        connection=None,
        config=None,
        is_many=True,
        is_script=False,
    )

    # Verify cursor was created and executemany was called
    mock_asyncmy_connection.cursor.assert_called_once()
    mock_cursor.executemany.assert_called_once_with(
        "INSERT INTO users (name) VALUES (%s)", [["John"], ["Jane"], ["Bob"]]
    )
    assert result == 3  # Should return rowcount


@pytest.mark.asyncio
async def test_asyncmy_driver_execute_impl_parameter_processing(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver parameter processing for different types."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [(1, "John")]
    mock_cursor.description = [("id", None), ("name", None)]
    mock_asyncmy_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_asyncmy_connection.cursor.return_value.__aexit__.return_value = None

    # Create SQL statement with parameters
    statement = SQL(
        "SELECT * FROM users WHERE id = %s AND name = %s", parameters=[1, "John"], config=asyncmy_driver.config
    )

    # Execute
    result = await asyncmy_driver._execute_impl(
        statement=statement,
        parameters=None,
        connection=None,
        config=None,
        is_many=False,
        is_script=False,
    )

    # Verify parameters were processed correctly
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = %s AND name = %s", [1, "John"])
    assert result == ([(1, "John")], [("id", None), ("name", None)])


@pytest.mark.asyncio
async def test_asyncmy_driver_wrap_select_result(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver _wrap_select_result method."""
    # Create mock cursor with data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.fetchall.return_value = [
        (1, "John"),
        (2, "Jane"),
    ]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result
    result = await asyncmy_driver._wrap_select_result(
        statement=statement,
        raw_driver_result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SelectResult)
    assert result.statement is statement
    assert result.column_names == ["id", "name"]
    # Note: Asyncmy driver has specific data handling logic
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_wrap_select_result_empty(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver _wrap_select_result method with empty result."""
    # Create mock cursor with no data
    mock_cursor = AsyncMock()
    mock_cursor.description = None
    mock_cursor.fetchall.return_value = []

    # Create SQL statement
    statement = SQL("SELECT * FROM empty_table")

    # Wrap result
    result = await asyncmy_driver._wrap_select_result(
        statement=statement,
        raw_driver_result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SelectResult)
    assert result.statement is statement
    assert result.data == []
    assert result.column_names == []


@pytest.mark.asyncio
async def test_asyncmy_driver_wrap_select_result_with_schema_type(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver _wrap_select_result with schema_type."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    # Create mock cursor with data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.fetchall.return_value = [
        (1, "John"),
        (2, "Jane"),
    ]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result with schema type
    result = await asyncmy_driver._wrap_select_result(
        statement=statement,
        raw_driver_result=mock_cursor,
        schema_type=User,
    )

    # Verify result
    assert isinstance(result, SelectResult)
    assert result.statement is statement
    assert result.column_names == ["id", "name"]


@pytest.mark.asyncio
async def test_asyncmy_driver_wrap_execute_result(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver _wrap_execute_result method."""
    # Create mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.rowcount = 3

    # Create SQL statement
    statement = SQL("UPDATE users SET active = 1", config=asyncmy_driver.config)

    # Wrap result
    result = await asyncmy_driver._wrap_execute_result(
        statement=statement,
        raw_driver_result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, ExecuteResult)
    assert result.statement is statement
    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"


@pytest.mark.asyncio
async def test_asyncmy_driver_wrap_execute_result_script(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver _wrap_execute_result method for script."""
    # Create SQL statement (DDL allowed with strict_mode=False)
    statement = SQL("CREATE TABLE test (id INT)", config=asyncmy_driver.config)

    # Wrap result for script
    result = await asyncmy_driver._wrap_execute_result(
        statement=statement,
        raw_driver_result="SCRIPT EXECUTED",
    )

    # Verify result
    assert isinstance(result, ExecuteResult)
    assert result.statement is statement
    assert result.rows_affected == 0
    assert result.operation_type == "CREATE"


def test_asyncmy_driver_connection_method(asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock) -> None:
    """Test Asyncmy driver _connection method."""
    # Test default connection return
    assert asyncmy_driver._connection() is mock_asyncmy_connection

    # Test connection override
    override_connection = AsyncMock()
    assert asyncmy_driver._connection(override_connection) is override_connection


@pytest.mark.asyncio
async def test_asyncmy_driver_error_handling(asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock) -> None:
    """Test Asyncmy driver error handling."""
    # Setup mock to raise exception
    mock_asyncmy_connection.cursor.side_effect = Exception("Database error")

    # Create SQL statement
    statement = SQL("SELECT * FROM users")

    # Test error propagation
    with pytest.raises(Exception, match="Database error"):
        await asyncmy_driver._execute_impl(
            statement=statement,
            parameters=None,
            connection=None,
            config=None,
            is_many=False,
            is_script=False,
        )


def test_asyncmy_driver_instrumentation(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver instrumentation integration."""
    # Test instrumentation config is accessible
    assert asyncmy_driver.instrumentation_config is not None
    assert isinstance(asyncmy_driver.instrumentation_config, InstrumentationConfig)

    # Test logging configuration
    assert hasattr(asyncmy_driver.instrumentation_config, "log_queries")
    assert hasattr(asyncmy_driver.instrumentation_config, "log_parameters")
    assert hasattr(asyncmy_driver.instrumentation_config, "log_results_count")


@pytest.mark.asyncio
async def test_asyncmy_driver_operation_type_detection(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver operation type detection."""
    # Test different SQL statement types (DDL allowed with strict_mode=False)
    test_cases = [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("UPDATE users SET name = 'John'", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("CREATE TABLE test (id INT)", "CREATE"),
    ]

    for sql, expected_op_type in test_cases:
        statement = SQL(sql, config=asyncmy_driver.config)

        # Mock cursor for execute result
        mock_cursor = AsyncMock()
        mock_cursor.rowcount = 1

        result = await asyncmy_driver._wrap_execute_result(
            statement=statement,
            raw_driver_result=mock_cursor,
        )

        assert result.operation_type == expected_op_type


@pytest.mark.asyncio
async def test_asyncmy_driver_select_to_arrow_basic(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver select_to_arrow method basic functionality."""
    # Setup mock cursor and result data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.fetchall.return_value = [
        (1, "Alice"),
        (2, "Bob"),
    ]
    mock_asyncmy_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Execute select_to_arrow
    result = await asyncmy_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)
    # Note: Don't compare statement objects directly as they may be recreated

    # Verify cursor operations
    mock_asyncmy_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_select_to_arrow_with_parameters(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver select_to_arrow method with parameters."""
    # Setup mock cursor and result data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.fetchall.return_value = [(42, "Test User")]
    mock_asyncmy_connection.cursor.return_value = mock_cursor

    # Create SQL statement with parameters
    statement = SQL("SELECT id, name FROM users WHERE id = %s", parameters=[42])

    # Execute select_to_arrow
    result = await asyncmy_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify cursor operations with parameters
    mock_asyncmy_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_select_to_arrow_non_query_error(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver select_to_arrow with non-query statement raises error."""
    # Create non-query statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')")

    # Test error for non-query
    with pytest.raises(TypeError, match="Cannot fetch Arrow table for a non-query statement"):
        await asyncmy_driver.select_to_arrow(statement)


@pytest.mark.asyncio
async def test_asyncmy_driver_select_to_arrow_empty_result(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver select_to_arrow with empty result."""
    # Setup mock cursor with no data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.fetchall.return_value = []
    mock_asyncmy_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users WHERE id > 1000")

    # Execute select_to_arrow
    result = await asyncmy_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)
    # Should create empty Arrow table
    assert result.data.num_rows == 0


@pytest.mark.asyncio
async def test_asyncmy_driver_select_to_arrow_with_connection_override(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver select_to_arrow with connection override."""
    # Create override connection
    override_connection = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id", None)]
    mock_cursor.fetchall.return_value = [(1,)]
    override_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("SELECT id FROM users")

    # Execute with connection override
    result = await asyncmy_driver.select_to_arrow(statement, connection=override_connection)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify override connection was used
    override_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.asyncio
async def test_asyncmy_driver_logging_configuration(
    asyncmy_driver: AsyncmyDriver, mock_asyncmy_connection: AsyncMock
) -> None:
    """Test Asyncmy driver logging configuration."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_asyncmy_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_asyncmy_connection.cursor.return_value.__aexit__.return_value = None

    # Enable logging
    asyncmy_driver.instrumentation_config.log_queries = True
    asyncmy_driver.instrumentation_config.log_parameters = True
    asyncmy_driver.instrumentation_config.log_results_count = True

    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = %s", parameters=[1], config=asyncmy_driver.config)

    # Execute with logging enabled
    await asyncmy_driver._execute_impl(
        statement=statement,
        parameters=None,
        connection=None,
        config=None,
        is_many=False,
        is_script=False,
    )

    # Verify execution worked
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = %s", [1])


def test_asyncmy_driver_mixins_integration(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver mixin integration."""
    # Test that driver has all expected mixins
    from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin

    assert isinstance(asyncmy_driver, SQLTranslatorMixin)
    assert isinstance(asyncmy_driver, AsyncArrowMixin)
    assert isinstance(asyncmy_driver, ResultConverter)

    # Test mixin methods are available
    assert hasattr(asyncmy_driver, "select_to_arrow")
    assert hasattr(asyncmy_driver, "to_schema")
    assert hasattr(asyncmy_driver, "returns_rows")


def test_asyncmy_driver_returns_rows_method(asyncmy_driver: AsyncmyDriver) -> None:
    """Test Asyncmy driver returns_rows method."""
    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert asyncmy_driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert asyncmy_driver.returns_rows(insert_stmt.expression) is False
