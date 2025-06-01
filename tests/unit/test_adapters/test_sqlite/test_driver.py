"""Unit tests for SQLite driver."""

from unittest.mock import Mock

import pytest

from sqlspec.adapters.sqlite import SqliteConnection, SqliteDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_sqlite_connection() -> Mock:
    """Create a mock SQLite connection."""
    mock_connection = Mock(spec=SqliteConnection)
    mock_cursor = Mock()
    mock_connection.cursor.return_value = mock_cursor
    mock_connection.execute.return_value = mock_cursor
    mock_connection.executemany.return_value = mock_cursor
    mock_connection.executescript.return_value = mock_cursor
    return mock_connection


@pytest.fixture
def sqlite_driver(mock_sqlite_connection: Mock) -> SqliteDriver:
    """Create a SQLite driver with mocked connection."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig()
    return SqliteDriver(
        connection=mock_sqlite_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


def test_sqlite_driver_initialization(mock_sqlite_connection: Mock) -> None:
    """Test SQLite driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = SqliteDriver(
        connection=mock_sqlite_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_sqlite_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "sqlite"
    assert driver.parameter_style == "qmark"


def test_sqlite_driver_dialect_property(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver dialect property."""
    assert sqlite_driver.dialect == "sqlite"


def test_sqlite_driver_parameter_style(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver parameter style."""
    assert sqlite_driver.parameter_style == "qmark"


def test_sqlite_driver_placeholder_style(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver placeholder style detection."""
    placeholder_style = sqlite_driver._get_placeholder_style()
    assert placeholder_style == "qmark"


def test_sqlite_driver_execute_impl_select(sqlite_driver: SqliteDriver, mock_sqlite_connection: Mock) -> None:
    """Test SQLite driver _execute_impl for SELECT statements."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_sqlite_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("SELECT * FROM users WHERE id = ?")
    parameters = (1,)

    # Execute
    result = sqlite_driver._execute_impl(
        statement=statement,
        parameters=parameters,
        connection=None,
        config=None,
        is_many=False,
        is_script=False,
    )

    # Verify cursor was created and execute was called
    mock_sqlite_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = ?", (1,))
    assert result is mock_cursor


def test_sqlite_driver_execute_impl_insert(sqlite_driver: SqliteDriver, mock_sqlite_connection: Mock) -> None:
    """Test SQLite driver _execute_impl for INSERT statements."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_sqlite_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("INSERT INTO users (name) VALUES (?)")
    parameters = ("John",)

    # Execute
    result = sqlite_driver._execute_impl(
        statement=statement,
        parameters=parameters,
        connection=None,
        config=None,
        is_many=False,
        is_script=False,
    )

    # Verify cursor was created and execute was called
    mock_sqlite_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("INSERT INTO users (name) VALUES (?)", ("John",))
    assert result is mock_cursor


def test_sqlite_driver_execute_impl_script(sqlite_driver: SqliteDriver, mock_sqlite_connection: Mock) -> None:
    """Test SQLite driver _execute_impl for script execution."""
    # Create SQL statement
    statement = SQL("CREATE TABLE test (id INTEGER); INSERT INTO test VALUES (1);")

    # Execute script
    result = sqlite_driver._execute_impl(
        statement=statement,
        parameters=None,
        connection=None,
        config=None,
        is_many=False,
        is_script=True,
    )

    # Verify executescript was called
    mock_sqlite_connection.executescript.assert_called_once_with(
        "CREATE TABLE test (id INTEGER); INSERT INTO test VALUES (1);"
    )
    assert result == "SCRIPT EXECUTED"


def test_sqlite_driver_execute_impl_many(sqlite_driver: SqliteDriver, mock_sqlite_connection: Mock) -> None:
    """Test SQLite driver _execute_impl for execute_many."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_sqlite_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("INSERT INTO users (name) VALUES (?)")
    parameters = [("John",), ("Jane",), ("Bob",)]

    # Execute many
    result = sqlite_driver._execute_impl(
        statement=statement,
        parameters=parameters,
        connection=None,
        config=None,
        is_many=True,
        is_script=False,
    )

    # Verify cursor was created and executemany was called
    mock_sqlite_connection.cursor.assert_called_once()
    mock_cursor.executemany.assert_called_once_with(
        "INSERT INTO users (name) VALUES (?)", [("John",), ("Jane",), ("Bob",)]
    )
    assert result is mock_cursor


def test_sqlite_driver_wrap_select_result(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver _wrap_select_result method."""
    # Create mock cursor with data
    mock_cursor = Mock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = [
        {"id": 1, "name": "John"},
        {"id": 2, "name": "Jane"},
    ]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result
    result = sqlite_driver._wrap_select_result(
        statement=statement,
        raw_driver_result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SelectResult)
    assert result.statement is statement
    assert result.data == [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
    assert result.column_names == ["id", "name"]


def test_sqlite_driver_wrap_select_result_empty(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver _wrap_select_result method with empty result."""
    # Create mock cursor with no data
    mock_cursor = Mock()
    mock_cursor.description = None
    mock_cursor.fetchall.return_value = []

    # Create SQL statement
    statement = SQL("SELECT * FROM empty_table")

    # Wrap result
    result = sqlite_driver._wrap_select_result(
        statement=statement,
        raw_driver_result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SelectResult)
    assert result.statement is statement
    assert result.data == []
    assert result.column_names == []


def test_sqlite_driver_wrap_execute_result(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver _wrap_execute_result method."""
    # Create mock cursor
    mock_cursor = Mock()
    mock_cursor.rowcount = 3

    # Create SQL statement
    statement = SQL("UPDATE users SET active = 1")

    # Wrap result
    result = sqlite_driver._wrap_execute_result(
        statement=statement,
        raw_driver_result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, ExecuteResult)
    assert result.statement is statement
    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"


def test_sqlite_driver_wrap_execute_result_script(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver _wrap_execute_result method for script."""
    # Create SQL statement
    statement = SQL("CREATE TABLE test (id INTEGER)")

    # Wrap result for script
    result = sqlite_driver._wrap_execute_result(
        statement=statement,
        raw_driver_result="SCRIPT EXECUTED",
    )

    # Verify result
    assert isinstance(result, ExecuteResult)
    assert result.statement is statement
    assert result.rows_affected == 0
    assert result.operation_type == "SCRIPT"


def test_sqlite_driver_connection_method(sqlite_driver: SqliteDriver, mock_sqlite_connection: Mock) -> None:
    """Test SQLite driver _connection method."""
    # Test default connection return
    assert sqlite_driver._connection() is mock_sqlite_connection

    # Test connection override
    override_connection = Mock()
    assert sqlite_driver._connection(override_connection) is override_connection


def test_sqlite_driver_error_handling(sqlite_driver: SqliteDriver, mock_sqlite_connection: Mock) -> None:
    """Test SQLite driver error handling."""
    # Setup mock to raise exception
    mock_sqlite_connection.cursor.side_effect = Exception("Database error")

    # Create SQL statement
    statement = SQL("SELECT * FROM users")

    # Test error propagation
    with pytest.raises(Exception, match="Database error"):
        sqlite_driver._execute_impl(
            statement=statement,
            parameters=None,
            connection=None,
            config=None,
            is_many=False,
            is_script=False,
        )


def test_sqlite_driver_instrumentation(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver instrumentation integration."""
    # Test instrumentation config is accessible
    assert sqlite_driver.instrumentation_config is not None
    assert isinstance(sqlite_driver.instrumentation_config, InstrumentationConfig)

    # Test logging configuration
    assert hasattr(sqlite_driver.instrumentation_config, "log_queries")
    assert hasattr(sqlite_driver.instrumentation_config, "log_parameters")
    assert hasattr(sqlite_driver.instrumentation_config, "log_results_count")


def test_sqlite_driver_parameter_processing(sqlite_driver: SqliteDriver, mock_sqlite_connection: Mock) -> None:
    """Test SQLite driver parameter processing."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_sqlite_connection.cursor.return_value = mock_cursor

    # Test with different parameter types
    statement = SQL("SELECT * FROM users WHERE id = ? AND name = ?")

    # Test with tuple parameters
    tuple_params = (1, "John")
    sqlite_driver._execute_impl(
        statement=statement,
        parameters=tuple_params,
        connection=None,
        config=None,
        is_many=False,
        is_script=False,
    )

    mock_cursor.execute.assert_called_with("SELECT * FROM users WHERE id = ? AND name = ?", (1, "John"))


def test_sqlite_driver_cursor_management(sqlite_driver: SqliteDriver, mock_sqlite_connection: Mock) -> None:
    """Test SQLite driver cursor management."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_sqlite_connection.cursor.return_value = mock_cursor

    # Create SQL statement
    statement = SQL("SELECT * FROM users")

    # Execute
    result = sqlite_driver._execute_impl(
        statement=statement,
        parameters=None,
        connection=None,
        config=None,
        is_many=False,
        is_script=False,
    )

    # Verify cursor was created and returned
    mock_sqlite_connection.cursor.assert_called_once()
    assert result is mock_cursor


def test_sqlite_driver_supports_arrow_attribute(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver __supports_arrow__ attribute."""
    # SQLite driver should not support Arrow by default
    assert sqlite_driver.__supports_arrow__ is False
    assert SqliteDriver.__supports_arrow__ is False


def test_sqlite_driver_with_schema_type(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver _wrap_select_result with schema_type."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    # Create mock cursor with data
    mock_cursor = Mock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = [
        {"id": 1, "name": "John"},
        {"id": 2, "name": "Jane"},
    ]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result with schema type
    result = sqlite_driver._wrap_select_result(
        statement=statement,
        raw_driver_result=mock_cursor,
        schema_type=User,
    )

    # Verify result
    assert isinstance(result, SelectResult)
    assert result.statement is statement
    # Data should be converted to schema type by the ResultConverter mixin
    assert result.column_names == ["id", "name"]


def test_sqlite_driver_operation_type_detection(sqlite_driver: SqliteDriver) -> None:
    """Test SQLite driver operation type detection."""
    # Test different SQL statement types
    test_cases = [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("UPDATE users SET name = 'John'", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("CREATE TABLE test (id INTEGER)", "CREATE"),
    ]

    for sql, expected_op_type in test_cases:
        statement = SQL(sql)

        # Mock cursor for execute result
        mock_cursor = Mock()
        mock_cursor.rowcount = 1

        result = sqlite_driver._wrap_execute_result(
            statement=statement,
            raw_driver_result=mock_cursor,
        )

        assert result.operation_type == expected_op_type
