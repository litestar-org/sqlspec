"""Unit tests for DuckDB driver."""

from unittest.mock import Mock

import pytest

from sqlspec.adapters.duckdb import DuckDBConnection, DuckDBDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_duckdb_connection() -> Mock:
    """Create a mock DuckDB connection."""
    mock_connection = Mock(spec=DuckDBConnection)
    mock_cursor = Mock()
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.close.return_value = None
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.executemany.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = None
    mock_cursor.rowcount = 0
    return mock_connection


@pytest.fixture
def duckdb_driver(mock_duckdb_connection: Mock) -> DuckDBDriver:
    """Create a DuckDB driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return DuckDBDriver(
        connection=mock_duckdb_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


def test_duckdb_driver_initialization(mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = DuckDBDriver(
        connection=mock_duckdb_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_duckdb_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "duckdb"
    assert driver.__supports_arrow__ is True


def test_duckdb_driver_dialect_property(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver dialect property."""
    assert duckdb_driver.dialect == "duckdb"


def test_duckdb_driver_supports_arrow(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver Arrow support."""
    assert duckdb_driver.__supports_arrow__ is True
    assert DuckDBDriver.__supports_arrow__ is True


def test_duckdb_driver_placeholder_style(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver placeholder style detection."""
    placeholder_style = duckdb_driver._get_placeholder_style()
    assert placeholder_style == "qmark"


def test_duckdb_driver_get_cursor(duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver _get_cursor context manager."""
    mock_cursor = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor

    with duckdb_driver._get_cursor(mock_duckdb_connection) as cursor:
        assert cursor is mock_cursor
        mock_cursor.close.assert_not_called()

    # Verify cursor was closed after context exit
    mock_cursor.close.assert_called_once()


def test_duckdb_driver_execute_impl_select(duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver _execute_impl for SELECT statements."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor

    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = ?", parameters=[1])

    # Execute
    result = duckdb_driver._execute_statement(
        statement=statement,
    )

    # Verify cursor was created and execute was called
    mock_duckdb_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = ?", [1])
    assert result is mock_cursor


def test_duckdb_driver_execute_impl_insert(duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver _execute_impl for INSERT statements."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor

    # Create SQL statement with parameters
    statement = SQL("INSERT INTO users (name) VALUES (?)", parameters=["John"])

    # Execute
    result = duckdb_driver._execute_statement(
        statement=statement,
    )

    # Verify cursor was created and execute was called
    mock_duckdb_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("INSERT INTO users (name) VALUES (?)", ["John"])
    assert result is mock_cursor


def test_duckdb_driver_execute_impl_script(duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver _execute_impl for script execution."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor

    # Test script execution
    script_statement = SQL(
        "CREATE TABLE test (id INTEGER); INSERT INTO test VALUES (1);", config=duckdb_driver.config
    ).as_script()
    script_result = duckdb_driver._execute_statement(script_statement)

    # Verify cursor was created and execute was called with static SQL
    # Note: SQL may be transformed by the pipeline (INTEGER -> INT)
    mock_duckdb_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    assert script_result == "SCRIPT EXECUTED"


def test_duckdb_driver_execute_impl_many(duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver _execute_impl for execute_many."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor

    # Test the new as_many() API that accepts parameters directly
    parameters = [["John"], ["Jane"], ["Bob"]]
    statement = SQL("INSERT INTO users (name) VALUES (?)").as_many(parameters)

    result = duckdb_driver._execute_statement(statement=statement)

    # The statement should have is_many=True and the correct parameters
    assert statement.is_many is True
    assert statement.parameters == parameters

    # Verify cursor was created and executemany was called
    mock_duckdb_connection.cursor.assert_called_once()
    mock_cursor.executemany.assert_called_once_with(
        "INSERT INTO users (name) VALUES (?)", [["John"], ["Jane"], ["Bob"]]
    )
    assert result is mock_cursor


def test_duckdb_driver_execute_impl_parameter_processing(
    duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock
) -> None:
    """Test DuckDB driver parameter processing for different types."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor

    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = ? AND name = ?", parameters=[1, "John"])

    # Execute
    result = duckdb_driver._execute_statement(
        statement=statement,
    )

    # Verify parameters were processed correctly
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = ? AND name = ?", [1, "John"])
    assert result is mock_cursor


def test_duckdb_driver_execute_impl_single_parameter(duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver with single parameter value."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor

    # Create SQL statement with single parameter
    statement = SQL("SELECT * FROM users WHERE id = ?", parameters=[42])

    # Execute
    result = duckdb_driver._execute_statement(
        statement=statement,
    )

    # Verify single parameter was processed correctly
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = ?", [42])
    assert result is mock_cursor


def test_duckdb_driver_wrap_select_result(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver _wrap_select_result method."""
    # Create mock cursor with data
    mock_cursor = Mock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = [
        (1, "John"),
        (2, "Jane"),
    ]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result
    result = duckdb_driver._wrap_select_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.data == [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
    assert result.column_names == ["id", "name"]


def test_duckdb_driver_wrap_select_result_empty(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver _wrap_select_result method with empty result."""
    # Create mock cursor with no data
    mock_cursor = Mock()
    mock_cursor.description = None
    mock_cursor.fetchall.return_value = []

    # Create SQL statement
    statement = SQL("SELECT * FROM empty_table")

    # Wrap result
    result = duckdb_driver._wrap_select_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.data == []
    assert result.column_names == []


def test_duckdb_driver_wrap_select_result_with_schema_type(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver _wrap_select_result with schema_type."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    # Create mock cursor with data
    mock_cursor = Mock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = [
        (1, "John"),
        (2, "Jane"),
    ]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result with schema type
    result = duckdb_driver._wrap_select_result(
        statement=statement,
        result=mock_cursor,
        schema_type=User,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    # Data should be converted to schema type by the ResultConverter mixin
    assert result.column_names == ["id", "name"]


def test_duckdb_driver_wrap_execute_result(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver _wrap_execute_result method."""
    # Create mock cursor
    mock_cursor = Mock()
    mock_cursor.rowcount = 3

    # Create SQL statement
    statement = SQL("UPDATE users SET active = 1")

    # Wrap result
    result = duckdb_driver._wrap_execute_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"


def test_duckdb_driver_wrap_execute_result_script(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver _wrap_execute_result method for script."""
    # Create SQL statement (DDL allowed with strict_mode=False)
    statement = SQL("CREATE TABLE test (id INTEGER)", config=duckdb_driver.config)

    # Wrap result for script
    result = duckdb_driver._wrap_execute_result(
        statement=statement,
        result="SCRIPT EXECUTED",
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 0
    # Operation type is determined from the SQL statement, not the result type
    assert result.operation_type == "CREATE"


def test_duckdb_driver_wrap_execute_result_no_rowcount(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver _wrap_execute_result method when cursor has no rowcount."""
    # Create mock cursor without rowcount
    mock_cursor = Mock()
    del mock_cursor.rowcount  # Remove rowcount attribute

    # Create SQL statement
    statement = SQL("INSERT INTO users (name) VALUES ('test')")

    # Wrap result
    result = duckdb_driver._wrap_execute_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == -1  # Default when rowcount not available
    assert result.operation_type == "INSERT"


def test_duckdb_driver_connection_method(duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver _connection method."""
    # Test default connection return
    assert duckdb_driver._connection() is mock_duckdb_connection

    # Test connection override
    override_connection = Mock()
    assert duckdb_driver._connection(override_connection) is override_connection


def test_duckdb_driver_error_handling(duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver error handling."""
    # Setup mock to raise exception
    mock_duckdb_connection.cursor.side_effect = Exception("Database error")

    # Create SQL statement
    statement = SQL("SELECT * FROM users")

    # Test error propagation
    with pytest.raises(Exception, match="Database error"):
        duckdb_driver._execute_statement(statement=statement)


def test_duckdb_driver_instrumentation(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver instrumentation integration."""
    # Test instrumentation config is accessible
    assert duckdb_driver.instrumentation_config is not None
    assert isinstance(duckdb_driver.instrumentation_config, InstrumentationConfig)

    # Test logging configuration
    assert hasattr(duckdb_driver.instrumentation_config, "log_queries")
    assert hasattr(duckdb_driver.instrumentation_config, "log_parameters")
    assert hasattr(duckdb_driver.instrumentation_config, "log_results_count")

    # Test logging enabled
    statement = SQL("SELECT * FROM users WHERE id = ?", parameters=[1])
    result = duckdb_driver._execute_statement(
        statement=statement,
    )

    # Verify execution worked
    assert result is not None
    assert result.statement is statement
    assert result.data == [{"id": 1}]
    assert result.column_names == ["id"]
    assert result.operation_type == "SELECT"
    assert result.rows_affected == -1
    assert result.metadata == {}


def test_duckdb_driver_operation_type_detection(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver operation type detection."""
    # Test different SQL statement types (DDL allowed with strict_mode=False)
    test_cases = [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("UPDATE users SET name = 'John'", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("CREATE TABLE test (id INTEGER)", "CREATE"),
    ]

    for sql, expected_op_type in test_cases:
        statement = SQL(sql, config=duckdb_driver.config)

        # Mock cursor for execute result
        mock_cursor = Mock()
        mock_cursor.rowcount = 1

        result = duckdb_driver._wrap_execute_result(
            statement=statement,
            result=mock_cursor,
        )

        assert result.operation_type == expected_op_type


def test_duckdb_driver_select_to_arrow_basic(duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver select_to_arrow method basic functionality."""
    # Setup mock cursor and arrow table
    mock_cursor = Mock()
    mock_arrow_table = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Execute select_to_arrow
    result = duckdb_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)
    assert result.statement is statement
    assert result.data is mock_arrow_table

    # Verify cursor operations
    mock_duckdb_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT id, name FROM users", [])
    mock_cursor.fetch_arrow_table.assert_called_once()


def test_duckdb_driver_select_to_arrow_with_parameters(
    duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock
) -> None:
    """Test DuckDB driver select_to_arrow method with parameters."""
    # Setup mock cursor and arrow table
    mock_cursor = Mock()
    mock_arrow_table = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    # Create SQL statement with parameters
    statement = SQL("SELECT id, name FROM users WHERE id = ?", parameters=[42])
    parameters = [42]

    # Execute select_to_arrow
    result = duckdb_driver.select_to_arrow(statement, parameters=parameters)

    # Verify result
    assert isinstance(result, ArrowResult)
    assert result.data is mock_arrow_table

    # Verify cursor operations with parameters
    mock_duckdb_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetch_arrow_table.assert_called_once()


def test_duckdb_driver_select_to_arrow_non_query_error(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver select_to_arrow with non-query statement raises error."""
    # Create non-query statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')")

    # Test error for non-query
    with pytest.raises(TypeError, match="Cannot fetch Arrow table for a non-query statement"):
        duckdb_driver.select_to_arrow(statement)


def test_duckdb_driver_select_to_arrow_execute_returns_none(
    duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock
) -> None:
    """Test DuckDB driver select_to_arrow when execute returns None."""
    # Setup mock cursor that returns None
    mock_cursor = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = None

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Test error when execute returns None
    with pytest.raises(Exception, match="DuckDB execute returned None"):
        duckdb_driver.select_to_arrow(statement)


def test_duckdb_driver_select_to_arrow_fetch_error(duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver select_to_arrow when fetch_arrow_table fails."""
    # Setup mock cursor that raises error on fetch_arrow_table
    mock_cursor = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.fetch_arrow_table.side_effect = Exception("Arrow conversion failed")

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Test error handling
    with pytest.raises(Exception, match="Failed to convert DuckDB result to Arrow table"):
        duckdb_driver.select_to_arrow(statement)


def test_duckdb_driver_select_to_arrow_with_sql_object(
    duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock
) -> None:
    """Test DuckDB driver select_to_arrow with SQL object directly."""
    # Setup mock cursor and arrow table
    mock_cursor = Mock()
    mock_arrow_table = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    # Create SQL statement object
    sql_statement = SQL("SELECT id, name FROM users WHERE active = ?", parameters=[True])

    # Execute select_to_arrow
    result = duckdb_driver.select_to_arrow(sql_statement)

    # Verify result
    assert isinstance(result, ArrowResult)
    assert result.statement is sql_statement
    assert result.data is mock_arrow_table

    # Verify cursor operations
    mock_duckdb_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetch_arrow_table.assert_called_once()


def test_duckdb_driver_select_to_arrow_with_connection_override(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver select_to_arrow with connection override."""
    # Create override connection
    override_connection = Mock()
    mock_cursor = Mock()
    mock_arrow_table = Mock()
    override_connection.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    # Create SQL statement
    statement = SQL("SELECT id FROM users")

    # Execute with connection override
    result = duckdb_driver.select_to_arrow(statement, connection=override_connection)

    # Verify result
    assert isinstance(result, ArrowResult)
    assert result.data is mock_arrow_table

    # Verify override connection was used
    override_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetch_arrow_table.assert_called_once()


def test_duckdb_driver_logging_configuration(duckdb_driver: DuckDBDriver, mock_duckdb_connection: Mock) -> None:
    """Test DuckDB driver logging configuration."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_duckdb_connection.cursor.return_value = mock_cursor

    # Enable logging
    duckdb_driver.instrumentation_config.log_queries = True
    duckdb_driver.instrumentation_config.log_parameters = True
    duckdb_driver.instrumentation_config.log_results_count = True

    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = ?", parameters=[1])

    # Execute with logging enabled
    result = duckdb_driver._execute_statement(
        statement=statement,
    )

    # Verify execution worked
    assert result is mock_cursor
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = ?", [1])


def test_duckdb_driver_mixins_integration(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver mixin integration."""
    # Test that driver has all expected mixins
    from sqlspec.statement.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowMixin

    assert isinstance(duckdb_driver, SQLTranslatorMixin)
    assert isinstance(duckdb_driver, SyncArrowMixin)
    assert isinstance(duckdb_driver, ResultConverter)

    # Test mixin methods are available
    assert hasattr(duckdb_driver, "select_to_arrow")
    assert hasattr(duckdb_driver, "to_schema")
    assert hasattr(duckdb_driver, "returns_rows")


def test_duckdb_driver_returns_rows_method(duckdb_driver: DuckDBDriver) -> None:
    """Test DuckDB driver returns_rows method."""
    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert duckdb_driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert duckdb_driver.returns_rows(insert_stmt.expression) is False
