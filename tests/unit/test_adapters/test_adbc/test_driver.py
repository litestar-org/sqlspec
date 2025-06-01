"""Unit tests for ADBC driver."""

from unittest.mock import Mock, patch

import pytest
from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.adapters.adbc.driver import AdbcDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.exceptions import SQLConversionError
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow


@pytest.fixture
def mock_adbc_connection() -> Mock:
    """Create a mock ADBC connection."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {
        "vendor_name": "PostgreSQL",
        "driver_name": "adbc_driver_postgresql",
    }
    return mock_conn


@pytest.fixture
def mock_cursor() -> Mock:
    """Create a mock ADBC cursor."""
    mock_cursor = Mock(spec=Cursor)
    mock_cursor.description = [["id"], ["name"], ["email"]]
    mock_cursor.rowcount = 1
    mock_cursor.fetchall.return_value = [
        (1, "John Doe", "john@example.com"),
        (2, "Jane Smith", "jane@example.com"),
    ]
    return mock_cursor


@pytest.fixture
def adbc_driver(mock_adbc_connection: Mock) -> AdbcDriver:
    """Create an ADBC driver with mock connection."""
    return AdbcDriver(
        connection=mock_adbc_connection,
        config=SQLConfig(strict_mode=False),
        instrumentation_config=InstrumentationConfig(),
    )


def test_adbc_driver_initialization(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver initialization with default parameters."""
    driver = AdbcDriver(connection=mock_adbc_connection)

    assert driver.connection == mock_adbc_connection
    assert driver.dialect == "postgres"  # Based on mock connection info
    assert driver.__supports_arrow__ is True
    assert driver.default_row_type == DictRow
    assert isinstance(driver.config, SQLConfig)
    assert isinstance(driver.instrumentation_config, InstrumentationConfig)


def test_adbc_driver_initialization_with_config(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver initialization with custom configuration."""
    config = SQLConfig(strict_mode=False)
    instrumentation = InstrumentationConfig()

    driver = AdbcDriver(
        connection=mock_adbc_connection,
        config=config,
        instrumentation_config=instrumentation,
    )

    assert driver.config == config
    assert driver.instrumentation_config == instrumentation


def test_adbc_driver_get_dialect_postgresql() -> None:
    """Test AdbcDriver._get_dialect detects PostgreSQL."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {
        "vendor_name": "PostgreSQL",
        "driver_name": "adbc_driver_postgresql",
    }

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "postgres"


def test_adbc_driver_get_dialect_bigquery() -> None:
    """Test AdbcDriver._get_dialect detects BigQuery."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {
        "vendor_name": "BigQuery",
        "driver_name": "adbc_driver_bigquery",
    }

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "bigquery"


def test_adbc_driver_get_dialect_sqlite() -> None:
    """Test AdbcDriver._get_dialect detects SQLite."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {
        "vendor_name": "SQLite",
        "driver_name": "adbc_driver_sqlite",
    }

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "sqlite"


def test_adbc_driver_get_dialect_duckdb() -> None:
    """Test AdbcDriver._get_dialect detects DuckDB."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {
        "vendor_name": "DuckDB",
        "driver_name": "adbc_driver_duckdb",
    }

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "duckdb"


def test_adbc_driver_get_dialect_mysql() -> None:
    """Test AdbcDriver._get_dialect detects MySQL."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {
        "vendor_name": "MySQL",
        "driver_name": "mysql_driver",
    }

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "mysql"


def test_adbc_driver_get_dialect_snowflake() -> None:
    """Test AdbcDriver._get_dialect detects Snowflake."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {
        "vendor_name": "Snowflake",
        "driver_name": "adbc_driver_snowflake",
    }

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "snowflake"


def test_adbc_driver_get_dialect_flightsql() -> None:
    """Test AdbcDriver._get_dialect detects Flight SQL."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {
        "vendor_name": "Apache Arrow",
        "driver_name": "adbc_driver_flightsql",
    }

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "sqlite"  # FlightSQL defaults to sqlite


def test_adbc_driver_get_dialect_unknown() -> None:
    """Test AdbcDriver._get_dialect defaults to postgres for unknown drivers."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {
        "vendor_name": "Unknown DB",
        "driver_name": "unknown_driver",
    }

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "postgres"


def test_adbc_driver_get_dialect_exception() -> None:
    """Test AdbcDriver._get_dialect handles exceptions gracefully."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.side_effect = Exception("Connection error")

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "postgres"  # Default fallback


def test_adbc_driver_get_placeholder_style_postgresql(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver._get_placeholder_style for PostgreSQL."""
    mock_adbc_connection.adbc_get_info.return_value = {
        "vendor_name": "PostgreSQL",
        "driver_name": "adbc_driver_postgresql",
    }

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver._get_placeholder_style()
    assert style == ParameterStyle.NUMERIC


def test_adbc_driver_get_placeholder_style_sqlite(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver._get_placeholder_style for SQLite."""
    mock_adbc_connection.adbc_get_info.return_value = {
        "vendor_name": "SQLite",
        "driver_name": "adbc_driver_sqlite",
    }

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver._get_placeholder_style()
    assert style == ParameterStyle.QMARK


def test_adbc_driver_get_placeholder_style_bigquery(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver._get_placeholder_style for BigQuery."""
    mock_adbc_connection.adbc_get_info.return_value = {
        "vendor_name": "BigQuery",
        "driver_name": "adbc_driver_bigquery",
    }

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver._get_placeholder_style()
    assert style == ParameterStyle.NAMED_AT


def test_adbc_driver_get_placeholder_style_duckdb(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver._get_placeholder_style for DuckDB."""
    mock_adbc_connection.adbc_get_info.return_value = {
        "vendor_name": "DuckDB",
        "driver_name": "adbc_driver_duckdb",
    }

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver._get_placeholder_style()
    assert style == ParameterStyle.QMARK


def test_adbc_driver_get_placeholder_style_mysql(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver._get_placeholder_style for MySQL."""
    mock_adbc_connection.adbc_get_info.return_value = {
        "vendor_name": "MySQL",
        "driver_name": "mysql_driver",
    }

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver._get_placeholder_style()
    assert style == ParameterStyle.QMARK


def test_adbc_driver_get_placeholder_style_snowflake(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver._get_placeholder_style for Snowflake."""
    mock_adbc_connection.adbc_get_info.return_value = {
        "vendor_name": "Snowflake",
        "driver_name": "adbc_driver_snowflake",
    }

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver._get_placeholder_style()
    assert style == ParameterStyle.QMARK


def test_adbc_driver_get_cursor_context_manager(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._get_cursor context manager."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    with AdbcDriver._get_cursor(mock_connection) as cursor:
        assert cursor == mock_cursor

    # Cursor should be closed after context exit
    mock_cursor.close.assert_called_once()


def test_adbc_driver_get_cursor_exception_handling(adbc_driver: AdbcDriver) -> None:
    """Test AdbcDriver._get_cursor handles cursor close exceptions."""
    mock_connection = adbc_driver.connection
    mock_cursor = Mock(spec=Cursor)
    mock_cursor.close.side_effect = Exception("Close error")
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    # Should not raise exception even if cursor.close() fails
    with AdbcDriver._get_cursor(mock_connection) as cursor:
        assert cursor == mock_cursor


def test_adbc_driver_execute_impl_select(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._execute_impl for SELECT statements."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    statement = SQL("SELECT * FROM users WHERE id = $1")
    parameters = [123]

    result = adbc_driver._execute_impl(statement, parameters)

    assert result == mock_cursor
    mock_cursor.execute.assert_called_once()

    # Check the SQL and parameters
    call_args = mock_cursor.execute.call_args
    assert "SELECT * FROM users WHERE id = $1" in call_args[0][0]
    assert call_args[0][1] == [123]


def test_adbc_driver_execute_impl_script(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._execute_impl for script execution."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore
    mock_cursor.statusmessage = "CREATE TABLE"

    statement = SQL("CREATE TABLE test AS SELECT 1 as id")

    result = adbc_driver._execute_impl(statement, is_script=True)

    assert result == "CREATE TABLE"
    mock_cursor.execute.assert_called_once()


def test_adbc_driver_execute_impl_script_no_status(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._execute_impl for script execution without status message."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore
    # No statusmessage attribute

    statement = SQL("CREATE TABLE test AS SELECT 1 as id")

    result = adbc_driver._execute_impl(statement, is_script=True)

    assert result == "SCRIPT EXECUTED"
    mock_cursor.execute.assert_called_once()


def test_adbc_driver_execute_impl_many(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._execute_impl for execute many operations."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    statement = SQL("INSERT INTO users (name) VALUES (?)")
    parameters = [["John"], ["Jane"], ["Bob"]]

    result = adbc_driver._execute_impl(statement, parameters, is_many=True)

    assert result == mock_cursor
    mock_cursor.executemany.assert_called_once()


def test_adbc_driver_execute_impl_with_connection_override(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._execute_impl with connection override."""
    override_connection = Mock(spec=Connection)
    override_connection.cursor.return_value = mock_cursor

    statement = SQL("SELECT 1")

    result = adbc_driver._execute_impl(statement, connection=override_connection)

    assert result == mock_cursor
    override_connection.cursor.assert_called_once()
    # Original connection should not be used
    adbc_driver.connection.cursor.assert_not_called()  # pyright: ignore


def test_adbc_driver_execute_impl_no_parameters(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._execute_impl with no parameters."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    statement = SQL("SELECT * FROM users")

    result = adbc_driver._execute_impl(statement)

    assert result == mock_cursor
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM users",
        [],
    )


def test_adbc_driver_execute_impl_list_parameters(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._execute_impl with list parameters."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    statement = SQL("SELECT * FROM users WHERE id IN ($1, $2)")
    parameters = [1, 2]

    result = adbc_driver._execute_impl(statement, parameters)

    assert result == mock_cursor
    mock_cursor.execute.assert_called_once()


def test_adbc_driver_execute_impl_single_parameter(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._execute_impl with single parameter."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    statement = SQL("SELECT * FROM users WHERE id = $1")
    parameters = 123

    result = adbc_driver._execute_impl(statement, parameters)

    assert result == mock_cursor


def test_adbc_driver_wrap_select_result(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._wrap_select_result wraps cursor results."""
    mock_cursor.description = [["id"], ["name"], ["email"]]
    mock_cursor.fetchall.return_value = [
        (1, "John", "john@example.com"),
        (2, "Jane", "jane@example.com"),
    ]

    statement = SQL("SELECT id, name, email FROM users")

    result = adbc_driver._wrap_select_result(statement, mock_cursor)

    assert isinstance(result, SelectResult)
    assert result.statement == statement
    assert result.column_names == ["id", "name", "email"]
    assert len(result.data) == 2
    assert result.data[0] == {"id": 1, "name": "John", "email": "john@example.com"}
    assert result.data[1] == {"id": 2, "name": "Jane", "email": "jane@example.com"}


def test_adbc_driver_wrap_select_result_empty(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._wrap_select_result with empty results."""
    mock_cursor.description = [["id"], ["name"]]
    mock_cursor.fetchall.return_value = []

    statement = SQL("SELECT id, name FROM users WHERE id = -1")

    result = adbc_driver._wrap_select_result(statement, mock_cursor)

    assert isinstance(result, SelectResult)
    assert result.statement == statement
    assert result.column_names == ["id", "name"]
    assert result.data == []


def test_adbc_driver_wrap_select_result_no_description(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._wrap_select_result with no cursor description."""
    mock_cursor.description = None
    mock_cursor.fetchall.return_value = []

    statement = SQL("SELECT 1")

    result = adbc_driver._wrap_select_result(statement, mock_cursor)

    assert isinstance(result, SelectResult)
    assert result.statement == statement
    assert result.column_names == []
    assert result.data == []


def test_adbc_driver_wrap_select_result_with_schema_type(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._wrap_select_result with schema type conversion."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    mock_cursor.description = [["id"], ["name"]]
    mock_cursor.fetchall.return_value = [(1, "John")]

    statement = SQL("SELECT id, name FROM users")

    with patch.object(adbc_driver, "to_schema") as mock_to_schema:
        mock_to_schema.return_value = [User(id=1, name="John")]

        result = adbc_driver._wrap_select_result(statement, mock_cursor, schema_type=User)

        assert isinstance(result, SelectResult)
        assert result.data == [User(id=1, name="John")]
        mock_to_schema.assert_called_once()


def test_adbc_driver_wrap_execute_result_cursor(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._wrap_execute_result for cursor results."""
    mock_cursor.rowcount = 3

    statement = SQL("INSERT INTO users (name) VALUES ('John')")

    result = adbc_driver._wrap_execute_result(statement, mock_cursor)

    assert isinstance(result, ExecuteResult)
    assert result.statement == statement
    assert result.rows_affected == 3
    assert result.data["rows_affected"] == 3
    assert result.data["operation_type"] == "UNKNOWN"


def test_adbc_driver_wrap_execute_result_string(adbc_driver: AdbcDriver) -> None:
    """Test AdbcDriver._wrap_execute_result for string results (script execution)."""
    statement = SQL("CREATE TABLE test AS SELECT 1 as id")

    result = adbc_driver._wrap_execute_result(statement, "CREATE TABLE")

    assert isinstance(result, ExecuteResult)
    assert result.statement == statement
    assert result.rows_affected == 0
    assert result.operation_type in ("SCRIPT", "UNKNOWN")


def test_adbc_driver_wrap_execute_result_no_rowcount(adbc_driver: AdbcDriver) -> None:
    """Test AdbcDriver._wrap_execute_result when cursor has no rowcount."""
    mock_cursor = Mock(spec=Cursor)
    # No rowcount attribute

    statement = SQL("UPDATE users SET active = true")

    result = adbc_driver._wrap_execute_result(statement, mock_cursor)

    assert isinstance(result, ExecuteResult)
    assert result.statement == statement
    assert result.rows_affected == -1


def test_adbc_driver_select_to_arrow_success(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.select_to_arrow for successful Arrow table creation."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    mock_arrow_table = Mock()
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    statement = SQL("SELECT * FROM users")

    result = adbc_driver.select_to_arrow(statement)

    assert isinstance(result, ArrowResult)
    assert result.statement.sql == "SELECT * FROM users"  # pyright: ignore
    assert result.data == mock_arrow_table
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetch_arrow_table.assert_called_once()


def test_adbc_driver_select_to_arrow_with_parameters(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.select_to_arrow with query parameters."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    mock_arrow_table = Mock()
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    statement = SQL("SELECT * FROM users WHERE id = $1")
    parameters = [123]

    result = adbc_driver.select_to_arrow(statement, parameters)

    assert isinstance(result, ArrowResult)
    assert result.data == mock_arrow_table

    # Check parameters were passed correctly
    call_args = mock_cursor.execute.call_args
    assert call_args[0][1] == [123]


def test_adbc_driver_select_to_arrow_non_query_statement(adbc_driver: AdbcDriver) -> None:
    """Test AdbcDriver.select_to_arrow raises TypeError for non-query statements."""
    statement = SQL("INSERT INTO users (name) VALUES ('John')")

    with pytest.raises(TypeError, match="Cannot fetch Arrow table for a non-query statement"):
        adbc_driver.select_to_arrow(statement)


def test_adbc_driver_select_to_arrow_fetch_error(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.select_to_arrow handles fetch_arrow_table errors."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    mock_cursor.fetch_arrow_table.side_effect = Exception("Arrow fetch failed")

    statement = SQL("SELECT * FROM users")

    with pytest.raises(SQLConversionError, match="Failed to convert ADBC result to Arrow table"):
        adbc_driver.select_to_arrow(statement)


def test_adbc_driver_select_to_arrow_list_parameters(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.select_to_arrow with list parameters."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    mock_arrow_table = Mock()
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    statement = SQL("SELECT * FROM users WHERE id IN ($1, $2)")
    parameters = [1, 2]

    result = adbc_driver.select_to_arrow(statement, parameters)

    assert isinstance(result, ArrowResult)
    assert result.data == mock_arrow_table


def test_adbc_driver_select_to_arrow_single_parameter(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.select_to_arrow with single parameter."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    mock_arrow_table = Mock()
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    statement = SQL("SELECT * FROM users WHERE id = $1")
    parameters = 123

    result = adbc_driver.select_to_arrow(statement, parameters)

    assert isinstance(result, ArrowResult)
    assert result.data == mock_arrow_table


def test_adbc_driver_select_to_arrow_with_connection_override(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.select_to_arrow with connection override."""
    override_connection = Mock(spec=Connection)
    override_connection.cursor.return_value = mock_cursor

    mock_arrow_table = Mock()
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    statement = SQL("SELECT * FROM users")

    result = adbc_driver.select_to_arrow(statement, connection=override_connection)

    assert isinstance(result, ArrowResult)
    assert result.data == mock_arrow_table
    override_connection.cursor.assert_called_once()
    # Original connection should not be used
    adbc_driver.connection.cursor.assert_not_called()  # pyright: ignore


def test_adbc_driver_instrumentation_logging(mock_adbc_connection: Mock, mock_cursor: Mock) -> None:
    """Test AdbcDriver with instrumentation logging enabled."""
    instrumentation = InstrumentationConfig(
        log_queries=True,
        log_parameters=True,
        log_results_count=True,
    )

    driver = AdbcDriver(
        connection=mock_adbc_connection,
        instrumentation_config=instrumentation,
    )

    mock_adbc_connection.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [(1, "John")]
    mock_cursor.description = [["id"], ["name"]]

    statement = SQL("SELECT * FROM users WHERE id = $1")
    parameters = [123]

    # Execute and wrap select result to trigger logging
    cursor_result = driver._execute_impl(statement, parameters)
    select_result = driver._wrap_select_result(statement, cursor_result)

    assert isinstance(select_result, SelectResult)
    # Logging calls are verified through the instrumentation config


def test_adbc_driver_connection_method(adbc_driver: AdbcDriver) -> None:
    """Test AdbcDriver._connection method returns correct connection."""
    # Test with no override
    conn = adbc_driver._connection(None)
    assert conn == adbc_driver.connection

    # Test with override
    override_conn = Mock(spec=Connection)
    conn = adbc_driver._connection(override_conn)
    assert conn == override_conn


def test_adbc_driver_returns_rows_check(adbc_driver: AdbcDriver) -> None:
    """Test AdbcDriver.returns_rows method for different statement types."""
    # This should be implemented in the base class
    select_stmt = SQL("SELECT * FROM users")
    assert adbc_driver.returns_rows(select_stmt.expression) is True

    insert_stmt = SQL("INSERT INTO users VALUES (1, 'John')")
    assert adbc_driver.returns_rows(insert_stmt.expression) is False


def test_adbc_driver_build_statement_method(adbc_driver: AdbcDriver) -> None:
    """Test AdbcDriver._build_statement method."""
    from sqlspec.statement.builder import QueryBuilder

    # Test with SQL statement
    sql_stmt = SQL("SELECT * FROM users")
    result = adbc_driver._build_statement(sql_stmt, SQLConfig())
    assert result == sql_stmt

    # Test with QueryBuilder (mock)
    mock_builder = Mock(spec=QueryBuilder)
    mock_builder.to_statement.return_value = sql_stmt
    result = adbc_driver._build_statement(mock_builder, SQLConfig())
    assert result == sql_stmt
