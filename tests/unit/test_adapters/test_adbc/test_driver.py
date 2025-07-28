"""Unit tests for ADBC driver."""

from unittest.mock import Mock

import pytest
from adbc_driver_manager.dbapi import Connection, Cursor
from sqlglot import exp

from sqlspec.adapters.adbc.driver import AdbcDriver
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.builder import QueryBuilder
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_adbc_connection() -> Mock:
    """Create a mock ADBC connection."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {"vendor_name": "PostgreSQL", "driver_name": "adbc_driver_postgresql"}
    return mock_conn


@pytest.fixture
def mock_cursor() -> Mock:
    """Create a mock ADBC cursor."""
    mock_cursor = Mock(spec=Cursor)
    mock_cursor.description = ["id", "name", "email"]
    mock_cursor.rowcount = 1
    mock_cursor.fetchall.return_value = [(1, "John Doe", "john@example.com"), (2, "Jane Smith", "jane@example.com")]
    return mock_cursor


@pytest.fixture
def adbc_driver(mock_adbc_connection: Mock) -> AdbcDriver:
    """Create an ADBC driver with mock connection."""
    return AdbcDriver(connection=mock_adbc_connection, config=SQLConfig())


def test_adbc_driver_initialization(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver initialization with default parameters."""
    driver = AdbcDriver(connection=mock_adbc_connection)

    assert driver.connection == mock_adbc_connection
    assert driver.dialect == "postgres"  # Based on mock connection info
    assert isinstance(driver.config, SQLConfig)


def test_adbc_driver_initialization_with_config(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver initialization with custom configuration."""
    config = SQLConfig()

    driver = AdbcDriver(connection=mock_adbc_connection, config=config)

    # The driver updates the config to include the dialect
    assert driver.config is not None
    assert driver.config.parse_errors_as_warnings == config.parse_errors_as_warnings
    assert driver.config.dialect == "postgres"  # Added by driver


def test_adbc_driver_get_dialect_postgresql() -> None:
    """Test AdbcDriver._get_dialect detects PostgreSQL."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {"vendor_name": "PostgreSQL", "driver_name": "adbc_driver_postgresql"}

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "postgres"


def test_adbc_driver_get_dialect_bigquery() -> None:
    """Test AdbcDriver._get_dialect detects BigQuery."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {"vendor_name": "BigQuery", "driver_name": "adbc_driver_bigquery"}

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "bigquery"


def test_adbc_driver_get_dialect_sqlite() -> None:
    """Test AdbcDriver._get_dialect detects SQLite."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {"vendor_name": "SQLite", "driver_name": "adbc_driver_sqlite"}

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "sqlite"


def test_adbc_driver_get_dialect_duckdb() -> None:
    """Test AdbcDriver._get_dialect detects DuckDB."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {"vendor_name": "DuckDB", "driver_name": "adbc_driver_duckdb"}

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "duckdb"


def test_adbc_driver_get_dialect_mysql() -> None:
    """Test AdbcDriver._get_dialect detects MySQL."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {"vendor_name": "MySQL", "driver_name": "mysql_driver"}

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "mysql"


def test_adbc_driver_get_dialect_snowflake() -> None:
    """Test AdbcDriver._get_dialect detects Snowflake."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {"vendor_name": "Snowflake", "driver_name": "adbc_driver_snowflake"}

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "snowflake"


def test_adbc_driver_get_dialect_flightsql() -> None:
    """Test AdbcDriver._get_dialect detects Flight SQL."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {"vendor_name": "Apache Arrow", "driver_name": "adbc_driver_flightsql"}

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "sqlite"  # FlightSQL defaults to sqlite


def test_adbc_driver_get_dialect_unknown() -> None:
    """Test AdbcDriver._get_dialect defaults to postgres for unknown drivers."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.return_value = {"vendor_name": "Unknown DB", "driver_name": "unknown_driver"}

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "postgres"


def test_adbc_driver_get_dialect_exception() -> None:
    """Test AdbcDriver._get_dialect handles exceptions gracefully."""
    mock_conn = Mock(spec=Connection)
    mock_conn.adbc_get_info.side_effect = Exception("Connection error")

    dialect = AdbcDriver._get_dialect(mock_conn)
    assert dialect == "postgres"  # Default fallback


def test_adbc_driver_get_placeholder_style_postgresql(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver.default_parameter_style for PostgreSQL."""
    mock_adbc_connection.adbc_get_info.return_value = {
        "vendor_name": "PostgreSQL",
        "driver_name": "adbc_driver_postgresql",
    }

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver.parameter_config.default_parameter_style
    assert style == ParameterStyle.NUMERIC


def test_adbc_driver_get_placeholder_style_sqlite(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver.default_parameter_style for SQLite."""
    mock_adbc_connection.adbc_get_info.return_value = {"vendor_name": "SQLite", "driver_name": "adbc_driver_sqlite"}

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver.parameter_config.default_parameter_style
    assert style == ParameterStyle.QMARK


def test_adbc_driver_get_placeholder_style_bigquery(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver.default_parameter_style for BigQuery."""
    mock_adbc_connection.adbc_get_info.return_value = {"vendor_name": "BigQuery", "driver_name": "adbc_driver_bigquery"}

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver.parameter_config.default_parameter_style
    assert style == ParameterStyle.NAMED_AT


def test_adbc_driver_get_placeholder_style_duckdb(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver.default_parameter_style for DuckDB."""
    mock_adbc_connection.adbc_get_info.return_value = {"vendor_name": "DuckDB", "driver_name": "adbc_driver_duckdb"}

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver.parameter_config.default_parameter_style
    assert style == ParameterStyle.QMARK


def test_adbc_driver_get_placeholder_style_mysql(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver.default_parameter_style for MySQL."""
    mock_adbc_connection.adbc_get_info.return_value = {"vendor_name": "MySQL", "driver_name": "mysql_driver"}

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver.parameter_config.default_parameter_style
    assert style == ParameterStyle.POSITIONAL_PYFORMAT


def test_adbc_driver_get_placeholder_style_snowflake(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver.default_parameter_style for Snowflake."""
    mock_adbc_connection.adbc_get_info.return_value = {
        "vendor_name": "Snowflake",
        "driver_name": "adbc_driver_snowflake",
    }

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver.parameter_config.default_parameter_style
    assert style == ParameterStyle.QMARK


def test_adbc_driver_get_cursor_context_manager(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.with_cursor context manager."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    with adbc_driver.with_cursor(mock_connection) as cursor:
        assert cursor == mock_cursor

    # Cursor should be closed after context exit
    mock_cursor.close.assert_called_once()


def test_adbc_driver_get_cursor_exception_handling(adbc_driver: AdbcDriver) -> None:
    """Test AdbcDriver.with_cursor handles cursor close exceptions."""
    mock_connection = adbc_driver.connection
    mock_cursor = Mock(spec=Cursor)
    mock_cursor.close.side_effect = Exception("Close error")
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    # Should not raise exception even if cursor.close() fails
    with adbc_driver.with_cursor(mock_connection) as cursor:
        assert cursor == mock_cursor


def test_adbc_driver_execute_statement_select(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._execute_statement for SELECT statements."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # type: ignore[assignment]

    # Setup mock cursor for fetchall
    mock_cursor.fetchall.return_value = [(1, "John Doe", "john@example.com")]
    mock_cursor.description = [("id",), ("name",), ("email",)]

    # Use PostgreSQL-style placeholders since the mock connection is PostgreSQL
    statement = SQL("SELECT * FROM users WHERE id = $1", parameters=[123])
    result = adbc_driver.execute(statement)

    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.column_names == ["id", "name", "email"]
    assert result.rows_affected == 1
    assert result.operation_type == "SELECT"

    # Verify execute and fetchall were called
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = $1", parameters=[123])
    mock_cursor.fetchall.assert_called_once()


def test_adbc_driver_returns_rows_check(adbc_driver: AdbcDriver) -> None:
    """Test AdbcDriver.returns_rows method for different statement types."""
    # This should be implemented in the base class
    select_stmt = SQL("SELECT * FROM users")
    assert adbc_driver.returns_rows(select_stmt.expression) is True

    insert_stmt = SQL("INSERT INTO users VALUES (1, 'John')")
    assert adbc_driver.returns_rows(insert_stmt.expression) is False


def test_adbc_driver_build_statement_method(adbc_driver: AdbcDriver) -> None:
    """Test AdbcDriver._build_statement method."""

    # Create a simple test QueryBuilder subclass
    class MockQueryBuilder(QueryBuilder):
        def _create_base_expression(self) -> exp.Expression:
            return exp.Select()

        @property
        def _expected_result_type(self) -> type[SQLResult]:
            return SQLResult

    sql_config = SQLConfig()
    # Test with SQL statement
    sql_stmt = SQL("SELECT * FROM users", config=sql_config)
    result = adbc_driver._prepare_sql(sql_stmt, config=sql_config)
    assert isinstance(result, SQL)
    assert result.sql == sql_stmt.sql

    # Test with QueryBuilder - use a real QueryBuilder subclass
    test_builder = MockQueryBuilder()
    result = adbc_driver._prepare_sql(test_builder, config=sql_config)
    assert isinstance(result, SQL)
    # The result should be a SQL statement created from the builder
    assert "SELECT" in result.sql

    # Test with plain string SQL input
    string_sql = "SELECT id FROM another_table"
    built_stmt_from_string = adbc_driver._prepare_sql(string_sql, config=sql_config)
    assert isinstance(built_stmt_from_string, SQL)
    assert built_stmt_from_string.sql == string_sql
    assert built_stmt_from_string.parameters == {}

    # Test with plain string SQL and parameters
    string_sql_with_params = "SELECT id FROM yet_another_table WHERE id = ?"
    params_for_string = 1  # Pass as individual parameter, not tuple
    built_stmt_with_params = adbc_driver._prepare_sql(string_sql_with_params, params_for_string, config=sql_config)
    assert isinstance(built_stmt_with_params, SQL)
    assert built_stmt_with_params.sql == string_sql_with_params
    assert built_stmt_with_params.parameters == (1,)  # Parameters wrapped as tuple by SQL constructor
