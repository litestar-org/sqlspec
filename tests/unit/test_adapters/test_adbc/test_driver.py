"""Unit tests for ADBC driver."""

import tempfile
from typing import Any, cast
from unittest.mock import Mock

import pyarrow as pa
import pytest
from adbc_driver_manager.dbapi import Connection, Cursor
from sqlglot import exp

from sqlspec.adapters.adbc.driver import AdbcDriver
from sqlspec.exceptions import RepositoryError
from sqlspec.statement.builder import QueryBuilder
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow


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
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]
    mock_cursor.rowcount = 1
    mock_cursor.fetchall.return_value = [(1, "John Doe", "john@example.com"), (2, "Jane Smith", "jane@example.com")]
    return mock_cursor


@pytest.fixture
def adbc_driver(mock_adbc_connection: Mock) -> AdbcDriver:
    """Create an ADBC driver with mock connection."""
    return AdbcDriver(connection=mock_adbc_connection, config=SQLConfig(strict_mode=False))


def test_adbc_driver_initialization(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver initialization with default parameters."""
    driver = AdbcDriver(connection=mock_adbc_connection)

    assert driver.connection == mock_adbc_connection
    assert driver.dialect == "postgres"  # Based on mock connection info
    assert driver.supports_native_arrow_export is True
    assert driver.supports_native_arrow_import is True
    assert driver.default_row_type == DictRow
    assert isinstance(driver.config, SQLConfig)


def test_adbc_driver_initialization_with_config(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver initialization with custom configuration."""
    config = SQLConfig(strict_mode=False)

    driver = AdbcDriver(connection=mock_adbc_connection, config=config)

    assert driver.config == config


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
    style = driver.default_parameter_style
    assert style == ParameterStyle.NUMERIC


def test_adbc_driver_get_placeholder_style_sqlite(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver.default_parameter_style for SQLite."""
    mock_adbc_connection.adbc_get_info.return_value = {"vendor_name": "SQLite", "driver_name": "adbc_driver_sqlite"}

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver.default_parameter_style
    assert style == ParameterStyle.QMARK


def test_adbc_driver_get_placeholder_style_bigquery(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver.default_parameter_style for BigQuery."""
    mock_adbc_connection.adbc_get_info.return_value = {"vendor_name": "BigQuery", "driver_name": "adbc_driver_bigquery"}

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver.default_parameter_style
    assert style == ParameterStyle.NAMED_AT


def test_adbc_driver_get_placeholder_style_duckdb(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver.default_parameter_style for DuckDB."""
    mock_adbc_connection.adbc_get_info.return_value = {"vendor_name": "DuckDB", "driver_name": "adbc_driver_duckdb"}

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver.default_parameter_style
    assert style == ParameterStyle.QMARK


def test_adbc_driver_get_placeholder_style_mysql(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver.default_parameter_style for MySQL."""
    mock_adbc_connection.adbc_get_info.return_value = {"vendor_name": "MySQL", "driver_name": "mysql_driver"}

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver.default_parameter_style
    assert style == ParameterStyle.POSITIONAL_PYFORMAT


def test_adbc_driver_get_placeholder_style_snowflake(mock_adbc_connection: Mock) -> None:
    """Test AdbcDriver.default_parameter_style for Snowflake."""
    mock_adbc_connection.adbc_get_info.return_value = {
        "vendor_name": "Snowflake",
        "driver_name": "adbc_driver_snowflake",
    }

    driver = AdbcDriver(connection=mock_adbc_connection)
    style = driver.default_parameter_style
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


def test_adbc_driver_execute_statement_select(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._execute_statement for SELECT statements."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # type: ignore[assignment]

    # Setup mock cursor for fetchall
    mock_cursor.fetchall.return_value = [(1, "John Doe", "john@example.com")]
    mock_cursor.description = [("id",), ("name",), ("email",)]

    statement = SQL("SELECT * FROM users WHERE id = ?", parameters=[123])
    result = cast("SelectResultDict", adbc_driver._execute_statement(statement))

    assert isinstance(result, dict)
    assert "data" in result
    assert "column_names" in result
    assert "rows_affected" in result

    assert len(result["data"]) == 1
    assert result["column_names"] == ["id", "name", "email"]
    assert result["rows_affected"] == 1

    # Verify execute and fetchall were called
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = $1", [123])
    mock_cursor.fetchall.assert_called_once()


def test_adbc_driver_fetch_arrow_table_with_parameters(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.fetch_arrow_table with query parameters."""
    import pyarrow as pa

    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    # Setup mock cursor for ADBC native Arrow support
    mock_arrow_table = pa.table({"id": [123], "name": ["Test User"], "email": ["test@example.com"]})
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    # Create SQL statement with parameters included
    result = adbc_driver.fetch_arrow_table("SELECT * FROM users WHERE id = $1", 123)

    assert isinstance(result, ArrowResult)
    assert isinstance(result.data, pa.Table)

    # Check parameters were passed correctly
    call_args = mock_cursor.execute.call_args
    # The driver should convert single parameters to a list for ADBC
    params = call_args[0][1]
    assert isinstance(params, list)
    assert len(params) == 1
    # The first parameter should be 123 (either directly or as TypedParameter)
    first_param = params[0]
    if hasattr(first_param, "value"):
        assert first_param.value == 123
    else:
        assert first_param == 123


def test_adbc_driver_fetch_arrow_table_non_query_statement(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.fetch_arrow_table works with non-query statements (returns empty table)."""
    import pyarrow as pa

    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    # Setup mock cursor for INSERT statement - ADBC should return empty Arrow table
    empty_table = pa.table({})  # Empty table with no columns
    mock_cursor.fetch_arrow_table.return_value = empty_table

    statement = SQL("INSERT INTO users (name) VALUES ('John')")
    result = adbc_driver.fetch_arrow_table(statement)

    assert isinstance(result, ArrowResult)
    assert isinstance(result.data, pa.Table)
    assert result.data.num_rows == 0


def test_adbc_driver_fetch_arrow_table_fetch_error(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.fetch_arrow_table handles execution errors."""

    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    # Make execute fail to trigger error handling
    mock_cursor.execute.side_effect = Exception("Execute failed")

    statement = SQL("SELECT * FROM users")

    # The unified storage mixin uses wrap_exceptions, so the error will be wrapped in RepositoryError
    with pytest.raises(RepositoryError, match="An error occurred during the operation"):
        adbc_driver.fetch_arrow_table(statement)


def test_adbc_driver_fetch_arrow_table_list_parameters(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.fetch_arrow_table with list parameters."""
    import pyarrow as pa

    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    # Setup mock cursor for ADBC native Arrow support
    mock_arrow_table = pa.table(
        {"id": [1, 2], "name": ["User 1", "User 2"], "email": ["user1@example.com", "user2@example.com"]}
    )
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    # Pass parameters directly as string SQL, since that's the more common pattern
    result = adbc_driver.fetch_arrow_table("SELECT * FROM users WHERE id IN ($1, $2)", parameters=[1, 2])

    assert isinstance(result, ArrowResult)
    assert isinstance(result.data, pa.Table)
    assert result.data.num_rows == 2


def test_adbc_driver_fetch_arrow_table_single_parameter(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.fetch_arrow_table with single parameter."""
    import pyarrow as pa

    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    # Setup mock cursor for ADBC native Arrow support
    mock_arrow_table = pa.table({"id": [123], "name": ["Test User"], "email": ["test@example.com"]})
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    # Pass parameters directly as string SQL
    result = adbc_driver.fetch_arrow_table("SELECT * FROM users WHERE id = $1", parameters=123)

    assert isinstance(result, ArrowResult)
    assert isinstance(result.data, pa.Table)
    assert result.data.num_rows == 1


def test_adbc_driver_fetch_arrow_table_with_connection_override(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver.fetch_arrow_table with connection override."""
    import pyarrow as pa

    # Create a separate mock cursor for the override connection
    override_cursor = Mock(spec=Cursor)
    # Setup mock cursor for ADBC native Arrow support
    mock_arrow_table = pa.table({"id": [1], "name": ["Test User"], "email": ["test@example.com"]})
    override_cursor.fetch_arrow_table.return_value = mock_arrow_table

    override_connection = Mock(spec=Connection)
    override_connection.cursor.return_value = override_cursor

    result = adbc_driver.fetch_arrow_table("SELECT * FROM users", _connection=override_connection)

    assert isinstance(result, ArrowResult)
    assert isinstance(result.data, pa.Table)
    assert result.data.num_rows == 1
    override_connection.cursor.assert_called_once()
    # Original connection should not be used
    adbc_driver.connection.cursor.assert_not_called()  # pyright: ignore


def test_adbc_driver_instrumentation_logging(mock_adbc_connection: Mock, mock_cursor: Mock) -> None:
    """Test AdbcDriver with instrumentation logging enabled."""

    driver = AdbcDriver(connection=mock_adbc_connection)

    mock_adbc_connection.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [(1, "John")]
    mock_cursor.description = [(col,) for col in ["id", "name", "email"]]

    statement = SQL("SELECT * FROM users WHERE id = $1", parameters=[123])
    # Parameters argument removed from _execute_statement call
    cursor_result = driver._execute_statement(statement)
    assert isinstance(cursor_result, dict) and "data" in cursor_result  # Type narrowing
    select_result = driver._wrap_select_result(statement, cast("SelectResultDict", cursor_result))

    assert isinstance(select_result, SQLResult)
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

    # Create a simple test QueryBuilder subclass
    class MockQueryBuilder(QueryBuilder[SQLResult[DictRow]]):
        def _create_base_expression(self) -> exp.Expression:
            return exp.Select()

        @property
        def _expected_result_type(self) -> type[SQLResult[SQLResult[dict[str, Any]]]]:
            return SQLResult[SQLResult[dict[str, Any]]]  # type: ignore[misc]

    sql_config = SQLConfig()
    # Test with SQL statement
    sql_stmt = SQL("SELECT * FROM users", _config=sql_config)
    result = adbc_driver._build_statement(sql_stmt, _config=sql_config)
    assert isinstance(result, SQL)
    assert result.sql == sql_stmt.sql

    # Test with QueryBuilder - use a real QueryBuilder subclass
    test_builder = MockQueryBuilder()
    result = adbc_driver._build_statement(test_builder, _config=sql_config)
    assert isinstance(result, SQL)
    # The result should be a SQL statement created from the builder
    assert "SELECT" in result.sql

    # Test with plain string SQL input
    string_sql = "SELECT id FROM another_table"
    built_stmt_from_string = adbc_driver._build_statement(string_sql, _config=sql_config)
    assert isinstance(built_stmt_from_string, SQL)
    assert built_stmt_from_string.sql == string_sql
    assert built_stmt_from_string.parameters is None

    # Test with plain string SQL and parameters
    string_sql_with_params = "SELECT id FROM yet_another_table WHERE id = ?"
    params_for_string = (1,)
    built_stmt_with_params = adbc_driver._build_statement(string_sql_with_params, params_for_string, _config=sql_config)
    assert isinstance(built_stmt_with_params, SQL)
    assert built_stmt_with_params.sql == string_sql_with_params
    assert built_stmt_with_params.parameters == [1]  # Tuple params are unpacked into list


def test_adbc_driver_fetch_arrow_table_native(adbc_driver: AdbcDriver, mock_cursor: Mock) -> None:
    """Test AdbcDriver._fetch_arrow_table uses native ADBC cursor.fetch_arrow_table()."""
    mock_connection = adbc_driver.connection
    mock_connection.cursor.return_value = mock_cursor  # pyright: ignore

    # Setup mock arrow table for native fetch
    mock_arrow_table = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    mock_cursor.fetch_arrow_table.return_value = mock_arrow_table

    statement = SQL("SELECT * FROM users")
    result = adbc_driver.fetch_arrow_table(statement)

    assert isinstance(result, ArrowResult)
    assert result.data is mock_arrow_table  # Should be the exact same table
    assert result.data.num_rows == 3  # pyright: ignore
    assert result.data.column_names == ["id", "name"]  # pyright: ignore

    # Verify native fetch_arrow_table was called
    mock_cursor.fetch_arrow_table.assert_called_once()
    # Regular fetchall should NOT be called when using native Arrow
    mock_cursor.fetchall.assert_not_called()


def test_adbc_driver_to_parquet(adbc_driver: AdbcDriver, mock_cursor: Mock, monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test to_parquet writes correct data to a Parquet file using Arrow Table and pyarrow."""
    # Set up the connection mock to return our mock cursor
    adbc_driver.connection.cursor.return_value = mock_cursor  # pyright: ignore

    # Patch fetch_arrow_table to return a mock ArrowResult with a pyarrow.Table
    mock_table = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
    # Ensure the table has the expected num_rows
    assert mock_table.num_rows == 2
    # Mock at the class level since instance has __slots__
    monkeypatch.setattr(
        AdbcDriver, "_fetch_arrow_table", lambda self, stmt, **kwargs: ArrowResult(statement=stmt, data=mock_table)
    )

    # Patch the storage backend to avoid file system operations
    called = {}

    def fake_write_arrow(path: str, table: pa.Table, **kwargs: Any) -> None:
        called["table"] = table
        called["path"] = path

    # Mock the storage backend
    mock_backend = Mock()
    mock_backend.write_arrow = fake_write_arrow
    # Mock at the class level since instance has __slots__
    monkeypatch.setattr(AdbcDriver, "_get_storage_backend", lambda self, uri: mock_backend)

    # Make the driver think it doesn't have native parquet export capability
    monkeypatch.setattr(adbc_driver.__class__, "supports_native_parquet_export", False)

    statement = SQL("SELECT id, name FROM users")
    with tempfile.NamedTemporaryFile() as tmp:
        # This should use the Arrow table from fetch_arrow_table
        result = adbc_driver.export_to_storage(statement, destination_uri=tmp.name, format="parquet")  # type: ignore[attr-defined]
        assert isinstance(result, int)  # Should return number of rows
        assert result == 2  # mock_table has 2 rows
        assert called.get("table") is mock_table
        assert tmp.name in called.get("path", "")  # type: ignore[operator]
