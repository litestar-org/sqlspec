"""Unit tests for OracleDB drivers."""

from typing import Any, Union
from unittest.mock import AsyncMock, Mock

import pytest

from sqlspec.adapters.oracledb import (
    OracleAsyncConnection,
    OracleAsyncDriver,
    OracleSyncConnection,
    OracleSyncDriver,
)
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult, SQLResult
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


def test_oracle_sync_driver_execute_impl_select(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver _execute_impl for SELECT statements."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Create SQL statement with parameters
    statement = SQL(
        "SELECT * FROM users WHERE id = :user_id", parameters={"user_id": 1}, config=oracle_sync_driver.config
    )

    # Execute
    result = oracle_sync_driver._execute_statement(
        statement=statement,
    )

    # Verify cursor was created and execute was called
    mock_oracle_sync_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = :user_id", {"user_id": 1})
    mock_cursor.close.assert_called_once()
    assert result is mock_cursor


@pytest.mark.asyncio
async def test_oracle_async_driver_execute_impl_select(
    oracle_async_driver: OracleAsyncDriver, mock_oracle_async_connection: AsyncMock
) -> None:
    """Test Oracle async driver _execute_impl for SELECT statements."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_oracle_async_connection.cursor.return_value = mock_cursor

    # Create SQL statement with parameters
    statement = SQL(
        "SELECT * FROM users WHERE id = :user_id", parameters={"user_id": 1}, config=oracle_async_driver.config
    )

    # Execute
    result = await oracle_async_driver._execute_statement(
        statement=statement,
    )

    # Verify cursor was created and execute was called
    mock_oracle_async_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = :user_id", {"user_id": 1})
    mock_cursor.close.assert_called_once()
    assert result is mock_cursor


def test_oracle_sync_driver_execute_impl_insert(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver _execute_impl for INSERT statements."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_cursor.rowcount = 1
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Create SQL statement with parameters
    statement = SQL(
        "INSERT INTO users (name) VALUES (:name)", parameters={"name": "John"}, config=oracle_sync_driver.config
    )

    # Execute
    result = oracle_sync_driver._execute_statement(
        statement=statement,
    )

    # Verify cursor was created and execute was called
    mock_oracle_sync_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("INSERT INTO users (name) VALUES (:name)", {"name": "John"})
    mock_cursor.close.assert_called_once()
    assert result is mock_cursor


def test_oracle_sync_driver_execute_impl_script(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver _execute_impl for script execution."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Test script execution
    script_statement = SQL(
        "CREATE TABLE test (id NUMBER); INSERT INTO test VALUES (1);", config=oracle_sync_driver.config
    ).as_script()
    script_result = oracle_sync_driver._execute_statement(script_statement)

    # Verify cursor was created and execute was called
    mock_oracle_sync_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.close.assert_called_once()
    assert script_result == "SCRIPT EXECUTED"


def test_oracle_sync_driver_execute_impl_many(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver _execute_impl for execute_many."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_cursor.rowcount = [1, 1, 1]  # Oracle can return list for executemany
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Create SQL statement with placeholder for parameters
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)",
        parameters=[["John"], ["Jane"], ["Bob"]],
        config=oracle_sync_driver.config,
    ).as_many()

    # Execute many
    result = oracle_sync_driver._execute_statement(
        statement=statement,
    )

    # Verify cursor was created and executemany was called
    mock_oracle_sync_connection.cursor.assert_called_once()
    mock_cursor.executemany.assert_called_once_with(
        "INSERT INTO users (name) VALUES (?)", [["John"], ["Jane"], ["Bob"]]
    )
    mock_cursor.close.assert_called_once()
    assert result is mock_cursor


def test_oracle_sync_driver_execute_impl_parameter_processing(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver parameter processing for different types."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Create SQL statement with parameters
    statement = SQL(
        "SELECT * FROM users WHERE id = :user_id AND name = :name",
        parameters={"user_id": 1, "name": "John"},
        config=oracle_sync_driver.config,
    )

    # Execute
    oracle_sync_driver._execute_statement(
        statement=statement,
    )

    # Verify parameters were processed correctly
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM users WHERE id = :user_id AND name = :name", {"user_id": 1, "name": "John"}
    )


def test_oracle_sync_driver_execute_impl_positional_parameters(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver with positional parameters converted to named."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Create SQL statement with named parameters (Oracle style)
    statement = SQL(
        "SELECT * FROM users WHERE id = :user_id AND name = :user_name",
        parameters={"user_id": 1, "user_name": "John"},
        config=oracle_sync_driver.config,
    )

    # Execute
    oracle_sync_driver._execute_statement(
        statement=statement,
    )

    # Verify named parameters were processed correctly
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM users WHERE id = :user_id AND name = :user_name", {"user_id": 1, "user_name": "John"}
    )


def test_oracle_sync_driver_wrap_select_result(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver _wrap_select_result method."""
    # Create mock cursor with data
    mock_cursor = Mock()
    mock_cursor.description = [("ID",), ("NAME",)]
    mock_cursor.fetchall.return_value = [(1, "John"), (2, "Jane")]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result
    result = oracle_sync_driver._wrap_select_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["ID", "NAME"]
    assert result.data == [{"ID": 1, "NAME": "John"}, {"ID": 2, "NAME": "Jane"}]


def test_oracle_sync_driver_wrap_select_result_empty(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver _wrap_select_result method with empty result."""
    # Create mock cursor with no data
    mock_cursor = Mock()
    mock_cursor.description = None
    mock_cursor.fetchall.return_value = []

    # Create SQL statement
    statement = SQL("SELECT * FROM empty_table")

    # Wrap result
    result = oracle_sync_driver._wrap_select_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.data == []
    assert result.column_names == []


def test_oracle_sync_driver_wrap_execute_result(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver _wrap_execute_result method."""
    # Create mock cursor
    mock_cursor = Mock()
    mock_cursor.rowcount = 3

    # Create SQL statement
    statement = SQL("UPDATE users SET active = 1", config=oracle_sync_driver.config)

    # Wrap result
    result = oracle_sync_driver._wrap_execute_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"


def test_oracle_sync_driver_wrap_execute_result_script(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver _wrap_execute_result method for script."""
    # Create SQL statement (DDL allowed with strict_mode=False)
    statement = SQL("CREATE TABLE test (id NUMBER)", config=oracle_sync_driver.config)

    # Wrap result for script
    result = oracle_sync_driver._wrap_execute_result(
        statement=statement,
        result="SCRIPT EXECUTED",
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 0
    assert result.operation_type == "CREATE"


def test_oracle_sync_driver_wrap_execute_result_list_rowcount(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver _wrap_execute_result method with list rowcount."""
    # Create mock cursor with list rowcount (from executemany)
    mock_cursor = Mock()
    mock_cursor.rowcount = [1, 1, 1]

    # Create SQL statement
    statement = SQL("INSERT INTO users VALUES (:name)", config=oracle_sync_driver.config)

    # Wrap result
    result = oracle_sync_driver._wrap_execute_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 3  # Sum of list
    assert result.operation_type == "INSERT"


def test_oracle_sync_driver_connection_method(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver _connection method."""
    # Test default connection return
    assert oracle_sync_driver._connection() is mock_oracle_sync_connection

    # Test connection override
    override_connection = Mock()
    assert oracle_sync_driver._connection(override_connection) is override_connection


@pytest.mark.asyncio
async def test_oracle_async_driver_connection_method(
    oracle_async_driver: OracleAsyncDriver, mock_oracle_async_connection: AsyncMock
) -> None:
    """Test Oracle async driver _connection method."""
    # Test default connection return
    assert oracle_async_driver._connection() is mock_oracle_async_connection

    # Test connection override
    override_connection = AsyncMock()
    assert oracle_async_driver._connection(override_connection) is override_connection


def test_oracle_sync_driver_error_handling(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver error handling."""
    # Setup mock to raise exception
    mock_oracle_sync_connection.cursor.side_effect = Exception("Database error")

    # Create SQL statement
    statement = SQL("SELECT * FROM users")

    # Test error propagation
    with pytest.raises(Exception, match="Database error"):
        oracle_sync_driver._execute_statement(statement=statement)


@pytest.mark.asyncio
async def test_oracle_async_driver_error_handling(
    oracle_async_driver: OracleAsyncDriver, mock_oracle_async_connection: AsyncMock
) -> None:
    """Test Oracle async driver error handling."""
    # Setup mock to raise exception
    mock_oracle_async_connection.cursor.side_effect = Exception("Database error")

    # Create SQL statement
    statement = SQL("SELECT * FROM users")

    # Test error propagation
    with pytest.raises(Exception, match="Database error"):
        await oracle_async_driver._execute_statement(statement=statement)


def test_oracle_sync_driver_instrumentation(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver instrumentation integration."""
    # Test instrumentation config is accessible
    assert oracle_sync_driver.instrumentation_config is not None
    assert isinstance(oracle_sync_driver.instrumentation_config, InstrumentationConfig)

    # Test logging configuration
    assert hasattr(oracle_sync_driver.instrumentation_config, "log_queries")
    assert hasattr(oracle_sync_driver.instrumentation_config, "log_parameters")
    assert hasattr(oracle_sync_driver.instrumentation_config, "log_results_count")

    # Test execution with logging enabled
    statement = SQL("SELECT * FROM users")
    oracle_sync_driver._execute_statement(
        statement=statement,
    )

    # Verify execution worked
    mock_cursor = Mock()
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users")


@pytest.mark.asyncio
async def test_oracle_async_driver_instrumentation(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver instrumentation integration."""
    # Test instrumentation config is accessible
    assert oracle_async_driver.instrumentation_config is not None
    assert isinstance(oracle_async_driver.instrumentation_config, InstrumentationConfig)

    # Test logging configuration
    assert hasattr(oracle_async_driver.instrumentation_config, "log_queries")
    assert hasattr(oracle_async_driver.instrumentation_config, "log_parameters")
    assert hasattr(oracle_async_driver.instrumentation_config, "log_results_count")

    # Test execution with logging enabled
    statement = SQL("SELECT * FROM users")
    await oracle_async_driver._execute_statement(
        statement=statement,
    )

    # Verify execution worked
    mock_cursor = AsyncMock()
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users")


def test_oracle_sync_driver_operation_type_detection(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver operation type detection."""
    # Test different SQL statement types (DDL allowed with strict_mode=False)
    test_cases = [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("UPDATE users SET name = 'John'", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("CREATE TABLE test (id NUMBER)", "CREATE"),
    ]

    for sql, expected_op_type in test_cases:
        statement = SQL(sql, config=oracle_sync_driver.config)

        # Mock cursor for execute result
        mock_cursor = Mock()
        mock_cursor.rowcount = 1

        result = oracle_sync_driver._wrap_execute_result(
            statement=statement,
            result=mock_cursor,
        )

        assert result.operation_type == expected_op_type


def test_oracle_sync_driver_logging_configuration(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver logging configuration."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Enable logging
    oracle_sync_driver.instrumentation_config.log_queries = True
    oracle_sync_driver.instrumentation_config.log_parameters = True
    oracle_sync_driver.instrumentation_config.log_results_count = True

    # Create SQL statement with parameters
    statement = SQL(
        "SELECT * FROM users WHERE id = :user_id", parameters={"user_id": 1}, config=oracle_sync_driver.config
    )

    # Execute with logging enabled
    oracle_sync_driver._execute_statement(
        statement=statement,
    )

    # Verify execution worked
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = :user_id", {"user_id": 1})


def test_oracle_sync_driver_mixins_integration(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver mixin integration."""
    # Test that driver has all expected mixins
    from sqlspec.statement.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowMixin

    assert isinstance(oracle_sync_driver, SQLTranslatorMixin)
    assert isinstance(oracle_sync_driver, SyncArrowMixin)
    assert isinstance(oracle_sync_driver, ResultConverter)

    # Test mixin methods are available
    assert hasattr(oracle_sync_driver, "select_to_arrow")
    assert hasattr(oracle_sync_driver, "to_schema")
    assert hasattr(oracle_sync_driver, "returns_rows")


def test_oracle_async_driver_mixins_integration(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver mixin integration."""
    # Test that driver has all expected mixins
    from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin

    assert isinstance(oracle_async_driver, SQLTranslatorMixin)
    assert isinstance(oracle_async_driver, AsyncArrowMixin)
    assert isinstance(oracle_async_driver, ResultConverter)

    # Test mixin methods are available
    assert hasattr(oracle_async_driver, "select_to_arrow")
    assert hasattr(oracle_async_driver, "to_schema")
    assert hasattr(oracle_async_driver, "returns_rows")


def test_oracle_sync_driver_returns_rows_method(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver returns_rows method."""
    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert oracle_sync_driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert oracle_sync_driver.returns_rows(insert_stmt.expression) is False


def test_oracle_async_driver_returns_rows_method(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver returns_rows method."""
    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert oracle_async_driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert oracle_async_driver.returns_rows(insert_stmt.expression) is False


def test_oracle_sync_driver_select_to_arrow_basic(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver select_to_arrow method basic functionality."""
    # Setup mock cursor and result data
    mock_cursor = Mock()
    mock_cursor.description = [("ID",), ("NAME",)]
    mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Execute select_to_arrow
    result = oracle_sync_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify cursor operations
    mock_oracle_sync_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.close.assert_called_once()


def test_oracle_sync_driver_select_to_arrow_with_parameters(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver select_to_arrow method with parameters."""
    # Setup mock cursor and result data
    mock_cursor = Mock()
    mock_cursor.description = [("ID",), ("NAME",)]
    mock_cursor.fetchall.return_value = [(42, "Test User")]
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Create SQL statement with parameters
    statement = SQL("SELECT id, name FROM users WHERE id = :user_id", parameters={"user_id": 42})

    # Execute select_to_arrow
    result = oracle_sync_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify cursor operations with parameters
    mock_cursor.execute.assert_called_once_with("SELECT id, name FROM users WHERE id = :user_id", {"user_id": 42})


def test_oracle_sync_driver_select_to_arrow_non_query_error(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver select_to_arrow with non-query statement raises error."""
    # Create non-query statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')")

    # Test error for non-query
    with pytest.raises(TypeError, match="Cannot fetch Arrow table for a non-query statement"):
        oracle_sync_driver.select_to_arrow(statement)


def test_oracle_sync_driver_select_to_arrow_empty_result(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver select_to_arrow with empty result."""
    # Setup mock cursor with no data
    mock_cursor = Mock()
    mock_cursor.description = [("ID",), ("NAME",)]
    mock_cursor.fetchall.return_value = []
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users WHERE id > 1000")

    # Execute select_to_arrow
    result = oracle_sync_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)
    # Should create empty Arrow table
    assert result.data.num_rows == 0


@pytest.mark.asyncio
async def test_oracle_async_driver_select_to_arrow_basic(
    oracle_async_driver: OracleAsyncDriver, mock_oracle_async_connection: AsyncMock
) -> None:
    """Test Oracle async driver select_to_arrow method basic functionality."""
    # Setup mock cursor and result data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("ID",), ("NAME",)]
    mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
    mock_oracle_async_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Execute select_to_arrow
    result = await oracle_async_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify cursor operations
    mock_oracle_async_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.close.assert_called_once()


@pytest.mark.asyncio
async def test_oracle_async_driver_select_to_arrow_with_parameters(
    oracle_async_driver: OracleAsyncDriver, mock_oracle_async_connection: AsyncMock
) -> None:
    """Test Oracle async driver select_to_arrow method with parameters."""
    # Setup mock cursor and result data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("ID",), ("NAME",)]
    mock_cursor.fetchall.return_value = [(42, "Test User")]
    mock_oracle_async_connection.cursor.return_value = mock_cursor

    # Create SQL statement with parameters
    statement = SQL("SELECT id, name FROM users WHERE id = :user_id", parameters={"user_id": 42})

    # Execute select_to_arrow
    result = await oracle_async_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify cursor operations with parameters
    mock_cursor.execute.assert_called_once_with("SELECT id, name FROM users WHERE id = :user_id", {"user_id": 42})


@pytest.mark.asyncio
async def test_oracle_async_driver_select_to_arrow_non_query_error(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver select_to_arrow with non-query statement raises error."""
    # Create non-query statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')")

    # Test error for non-query
    with pytest.raises(TypeError, match="Cannot fetch Arrow table for a non-query statement"):
        await oracle_async_driver.select_to_arrow(statement)


def test_oracle_sync_driver_select_to_arrow_with_connection_override(
    oracle_sync_driver: OracleSyncDriver,
) -> None:
    """Test Oracle sync driver select_to_arrow with connection override."""
    # Create override connection
    override_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.description = [("ID",)]
    mock_cursor.fetchall.return_value = [(1,)]
    override_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("SELECT id FROM users")

    # Execute with connection override
    result = oracle_sync_driver.select_to_arrow(statement, connection=override_connection)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify override connection was used
    override_connection.cursor.assert_called_once()


def test_oracle_sync_driver_named_parameters(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver named parameter handling."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Create SQL statement with named parameters
    statement = SQL(
        "SELECT * FROM users WHERE name = :name AND age = :age",
        parameters={"name": "John", "age": 30},
        config=oracle_sync_driver.config,
    )

    # Execute
    oracle_sync_driver._execute_statement(
        statement=statement,
    )

    # Verify named parameters were processed correctly
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM users WHERE name = :name AND age = :age", {"name": "John", "age": 30}
    )


@pytest.mark.asyncio
async def test_oracle_async_driver_named_parameters(
    oracle_async_driver: OracleAsyncDriver, mock_oracle_async_connection: AsyncMock
) -> None:
    """Test Oracle async driver named parameter handling."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_oracle_async_connection.cursor.return_value = mock_cursor

    # Create SQL statement with named parameters
    statement = SQL(
        "SELECT * FROM users WHERE name = :name AND age = :age",
        parameters={"name": "John", "age": 30},
        config=oracle_async_driver.config,
    )

    # Execute
    await oracle_async_driver._execute_statement(
        statement=statement,
    )

    # Verify named parameters were processed correctly
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM users WHERE name = :name AND age = :age", {"name": "John", "age": 30}
    )


def test_oracle_sync_driver_single_parameter_conversion(
    oracle_sync_driver: OracleSyncDriver, mock_oracle_sync_connection: Mock
) -> None:
    """Test Oracle sync driver single parameter conversion."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_oracle_sync_connection.cursor.return_value = mock_cursor

    # Create SQL statement with single named parameter
    statement = SQL(
        "SELECT * FROM users WHERE id = :user_id",
        parameters={"user_id": "test_value"},
        config=oracle_sync_driver.config,
    )

    # Execute
    oracle_sync_driver._execute_statement(
        statement=statement,
    )

    # Verify single parameter was processed correctly
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = :user_id", {"user_id": "test_value"})


def test_oracle_sync_driver_wrap_select_result_with_schema_type(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle sync driver _wrap_select_result with schema_type."""
    from dataclasses import dataclass

    @dataclass
    class User:
        ID: int
        NAME: str

    # Create mock cursor with data
    mock_cursor = Mock()
    mock_cursor.description = [("ID",), ("NAME",)]
    mock_cursor.fetchall.return_value = [(1, "John"), (2, "Jane")]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result with schema type
    result: Union[SQLResult[User], SQLResult[dict[str, Any]]] = oracle_sync_driver._wrap_select_result(
        statement=statement,
        result=mock_cursor,
        schema_type=User,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["ID", "NAME"]


@pytest.mark.asyncio
async def test_oracle_async_driver_wrap_select_result(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver _wrap_select_result method."""
    # Create mock cursor with data
    mock_cursor = AsyncMock()
    mock_cursor.description = [("ID",), ("NAME",)]
    mock_cursor.fetchall.return_value = [(1, "John"), (2, "Jane")]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result
    result: Union[SQLResult[Any], SQLResult[dict[str, Any]]] = await oracle_async_driver._wrap_select_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["ID", "NAME"]
    assert result.data == [{"ID": 1, "NAME": "John"}, {"ID": 2, "NAME": "Jane"}]


@pytest.mark.asyncio
async def test_oracle_async_driver_wrap_execute_result(oracle_async_driver: OracleAsyncDriver) -> None:
    """Test Oracle async driver _wrap_execute_result method."""
    # Create mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.rowcount = 5

    # Create SQL statement
    statement = SQL("UPDATE users SET active = 1", config=oracle_async_driver.config)

    # Wrap result
    result = await oracle_async_driver._wrap_execute_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 5
    assert result.operation_type == "UPDATE"


def test_oracle_driver_uppercase_column_names(oracle_sync_driver: OracleSyncDriver) -> None:
    """Test Oracle driver handling of uppercase column names (Oracle convention)."""
    # Create mock cursor with uppercase column names (Oracle default)
    mock_cursor = Mock()
    mock_cursor.description = [("USER_ID",), ("USER_NAME",), ("CREATED_DATE",)]
    mock_cursor.fetchall.return_value = [(1, "John Doe", "2023-01-01"), (2, "Jane Smith", "2023-01-02")]

    # Create SQL statement
    statement = SQL("SELECT user_id, user_name, created_date FROM users")

    # Wrap result
    result = oracle_sync_driver._wrap_select_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result with uppercase column names
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["USER_ID", "USER_NAME", "CREATED_DATE"]
    assert result.data == [
        {"USER_ID": 1, "USER_NAME": "John Doe", "CREATED_DATE": "2023-01-01"},
        {"USER_ID": 2, "USER_NAME": "Jane Smith", "CREATED_DATE": "2023-01-02"},
    ]
