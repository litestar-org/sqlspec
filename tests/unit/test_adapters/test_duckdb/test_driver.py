"""Unit tests for DuckDB driver.

This module tests the DuckDBDriver class including:
- Driver initialization and configuration
- Statement execution (single, many, script)
- Result wrapping and formatting
- Parameter style handling
- Type coercion overrides
- Storage functionality
- Error handling
- DuckDB-specific features (Arrow integration, native export)
"""

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.duckdb import DuckDBDriver
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, DMLResultDict, SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    pass


# Test Fixtures
@pytest.fixture
def mock_connection() -> MagicMock:
    """Create a mock DuckDB connection."""
    mock_conn = MagicMock()

    # Set up cursor methods
    mock_cursor = MagicMock()
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.executemany.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_cursor.description = []
    mock_cursor.rowcount = 0
    mock_cursor.close.return_value = None

    mock_conn.cursor.return_value = mock_cursor

    # Set up execute method
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_result.fetchone.return_value = None
    mock_result.description = []
    mock_result.arrow.return_value = MagicMock()
    mock_result.fetch_record_batch.return_value = iter([])

    mock_conn.execute.return_value = mock_result

    return mock_conn


@pytest.fixture
def driver(mock_connection: MagicMock) -> DuckDBDriver:
    """Create a DuckDB driver with mocked connection."""
    config = SQLConfig()
    return DuckDBDriver(connection=mock_connection, config=config)


# Initialization Tests
def test_driver_initialization() -> None:
    """Test driver initialization with various parameters."""
    mock_conn = MagicMock()
    config = SQLConfig()

    driver = DuckDBDriver(connection=mock_conn, config=config)

    assert driver.connection is mock_conn
    assert driver.config is config
    assert driver.dialect == "duckdb"
    assert driver.default_parameter_style == ParameterStyle.QMARK
    assert driver.supported_parameter_styles == (ParameterStyle.QMARK, ParameterStyle.NUMERIC)


def test_driver_default_row_type() -> None:
    """Test driver default row type."""
    mock_conn = MagicMock()

    # Default row type - DuckDB uses a string type hint
    driver = DuckDBDriver(connection=mock_conn)
    # DuckDB driver has a string representation for default row type
    assert str(driver.default_row_type) == "dict[str, Any]" or driver.default_row_type == DictRow

    # Custom row type
    custom_type: type[DictRow] = dict
    driver = DuckDBDriver(connection=mock_conn, default_row_type=custom_type)
    assert driver.default_row_type is custom_type


# Arrow Support Tests
def test_arrow_support_flags() -> None:
    """Test driver Arrow support flags."""
    mock_conn = MagicMock()
    driver = DuckDBDriver(connection=mock_conn)

    assert driver.supports_native_arrow_export is True
    assert driver.supports_native_arrow_import is True
    assert DuckDBDriver.supports_native_arrow_export is True
    assert DuckDBDriver.supports_native_arrow_import is True


def test_parquet_support_flags() -> None:
    """Test driver Parquet support flags."""
    mock_conn = MagicMock()
    driver = DuckDBDriver(connection=mock_conn)

    assert driver.supports_native_parquet_export is True
    assert driver.supports_native_parquet_import is True
    assert DuckDBDriver.supports_native_parquet_export is True
    assert DuckDBDriver.supports_native_parquet_import is True


# Execute Statement Tests
@pytest.mark.parametrize(
    "sql_text,is_script,is_many,expected_method",
    [
        ("SELECT * FROM users", False, False, "_execute"),
        ("INSERT INTO users VALUES (?)", False, True, "_execute_many"),
        ("CREATE TABLE test; INSERT INTO test;", True, False, "_execute_script"),
    ],
    ids=["select", "execute_many", "script"],
)
def test_execute_statement_routing(
    driver: DuckDBDriver,
    mock_connection: MagicMock,
    sql_text: str,
    is_script: bool,
    is_many: bool,
    expected_method: str,
) -> None:
    """Test that _execute_statement routes to correct method."""
    from sqlspec.statement.sql import SQLConfig

    # Create config that allows DDL if needed
    config = SQLConfig(enable_validation=False) if "CREATE" in sql_text else SQLConfig()
    statement = SQL(sql_text, _config=config)
    statement._is_script = is_script
    statement._is_many = is_many

    with patch.object(DuckDBDriver, expected_method, return_value={"rows_affected": 0}) as mock_method:
        driver._execute_statement(statement)
        mock_method.assert_called_once()


def test_execute_select_statement(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test executing a SELECT statement."""
    # Set up mock result
    mock_result = mock_connection.execute.return_value
    mock_result.fetchall.return_value = [(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com")]
    mock_result.description = [("id",), ("name",), ("email",)]

    statement = SQL("SELECT * FROM users")
    result = driver._execute_statement(statement)

    assert result == {
        "data": [(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com")],
        "column_names": ["id", "name", "email"],
        "rows_affected": 2,
    }

    mock_connection.execute.assert_called_once_with("SELECT * FROM users", [])


def test_execute_dml_statement(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test executing a DML statement (INSERT/UPDATE/DELETE)."""
    # Set up the cursor mock to return rowcount = 1 for DML operations
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.rowcount = 1

    statement = SQL("INSERT INTO users (name, email) VALUES (?, ?)", ["Alice", "alice@example.com"])
    result = driver._execute_statement(statement)

    assert result == {"rows_affected": 1}

    # DML statements should use cursor.execute, not connection.execute
    mock_cursor.execute.assert_called_once_with(
        "INSERT INTO users (name, email) VALUES (?, ?)", ["Alice", "alice@example.com"]
    )


# Parameter Style Handling Tests
@pytest.mark.parametrize(
    "sql_text,detected_style,expected_style",
    [
        ("SELECT * FROM users WHERE id = ?", ParameterStyle.QMARK, ParameterStyle.QMARK),
        ("SELECT * FROM users WHERE id = $1", ParameterStyle.NUMERIC, ParameterStyle.QMARK),  # Converted
        ("SELECT * FROM users WHERE id = :id", ParameterStyle.NAMED_COLON, ParameterStyle.QMARK),  # Converted
    ],
    ids=["qmark", "numeric_converted", "named_colon_converted"],
)
def test_parameter_style_handling(
    driver: DuckDBDriver,
    mock_connection: MagicMock,
    sql_text: str,
    detected_style: ParameterStyle,
    expected_style: ParameterStyle,
) -> None:
    """Test parameter style detection and conversion."""
    statement = SQL(sql_text, [123])  # Add a parameter

    # Mock execute to avoid actual execution
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_result.description = [("id",)]
    mock_connection.execute.return_value = mock_result

    driver._execute_statement(statement)

    # Check that execute was called (parameter style conversion happens in compile())
    mock_connection.execute.assert_called_once()

    # The SQL should have been converted to the expected style
    # DuckDB's default is QMARK, so $1 and :id should be converted to ?
    if expected_style == ParameterStyle.QMARK and detected_style != ParameterStyle.QMARK:
        assert "?" in mock_connection.execute.call_args[0][0]


# Execute Many Tests
def test_execute_many(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test executing a statement multiple times."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.rowcount = 3

    sql = "INSERT INTO users (name, email) VALUES (?, ?)"
    params = [["Alice", "alice@example.com"], ["Bob", "bob@example.com"], ["Charlie", "charlie@example.com"]]

    result = driver._execute_many(sql, params)

    assert result == {"rows_affected": 3}

    mock_cursor.executemany.assert_called_once_with(sql, params)


# Execute Script Tests
def test_execute_script(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test executing a SQL script."""
    mock_cursor = mock_connection.cursor.return_value

    script = """
    CREATE TABLE test (id INTEGER PRIMARY KEY);
    INSERT INTO test VALUES (1);
    INSERT INTO test VALUES (2);
    """

    result = driver._execute_script(script)

    assert result == {
        "statements_executed": -1,
        "status_message": "Script executed successfully.",
        "description": "The script was sent to the database.",
    }

    mock_cursor.execute.assert_called_once_with(script)


# Result Wrapping Tests
def test_wrap_select_result(driver: DuckDBDriver) -> None:
    """Test wrapping SELECT results."""
    statement = SQL("SELECT * FROM users")
    result: SelectResultDict = {"data": [(1, "Alice"), (2, "Bob")], "column_names": ["id", "name"], "rows_affected": 2}

    wrapped = driver._wrap_select_result(statement, result)

    assert isinstance(wrapped, SQLResult)
    assert wrapped.statement is statement
    assert len(wrapped.data) == 2
    assert wrapped.data == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    assert wrapped.column_names == ["id", "name"]
    assert wrapped.rows_affected == 2
    assert wrapped.operation_type == "SELECT"


def test_wrap_select_result_with_schema(driver: DuckDBDriver) -> None:
    """Test wrapping SELECT results with schema type."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    statement = SQL("SELECT * FROM users")
    result: SelectResultDict = {"data": [(1, "Alice"), (2, "Bob")], "column_names": ["id", "name"], "rows_affected": 2}

    wrapped = driver._wrap_select_result(statement, result, schema_type=User)

    assert isinstance(wrapped, SQLResult)
    assert all(isinstance(item, User) for item in wrapped.data)
    assert wrapped.data[0].id == 1
    assert wrapped.data[0].name == "Alice"


def test_wrap_execute_result_dml(driver: DuckDBDriver) -> None:
    """Test wrapping DML results."""
    statement = SQL("INSERT INTO users VALUES (?)")
    # No need to mock _expression - it's computed from the SQL

    result: DMLResultDict = {"rows_affected": 1}

    wrapped = driver._wrap_execute_result(statement, result)  # pyright: ignore

    assert isinstance(wrapped, SQLResult)
    assert wrapped.data == []
    assert wrapped.rows_affected == 1
    assert wrapped.operation_type == "INSERT"


def test_wrap_execute_result_script(driver: DuckDBDriver) -> None:
    """Test wrapping script results."""
    from sqlspec.statement.sql import SQLConfig

    config = SQLConfig(enable_validation=False)  # Allow DDL
    statement = SQL("CREATE TABLE test; INSERT INTO test;", _config=config)
    # No need to set _expression

    from sqlspec.statement.result import ScriptResultDict

    result: ScriptResultDict = {
        "statements_executed": 2,
        "status_message": "Script executed successfully.",
        "description": "The script was sent to the database.",
    }

    wrapped = driver._wrap_execute_result(statement, result)  # pyright: ignore

    assert isinstance(wrapped, SQLResult)
    assert wrapped.data == []
    assert wrapped.rows_affected == 0
    # For scripts, the operation_type is based on the first statement (CREATE)
    assert wrapped.operation_type == "CREATE"
    assert wrapped.metadata["status_message"] == "Script executed successfully."


# Connection Tests
def test_connection_method(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test _connection method."""
    # Test default connection return
    assert driver._connection() is mock_connection

    # Test connection override
    override_connection = MagicMock()
    assert driver._connection(override_connection) is override_connection


# Cursor Context Manager Tests
def test_get_cursor_context_manager(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test _get_cursor context manager."""
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor

    with driver._get_cursor(mock_connection) as cursor:
        assert cursor is mock_cursor
        mock_cursor.close.assert_not_called()

    # Verify cursor was closed after context exit
    mock_cursor.close.assert_called_once()


# Storage Mixin Tests
def test_storage_methods_available(driver: DuckDBDriver) -> None:
    """Test that driver has all storage methods from SyncStorageMixin."""
    storage_methods = ["fetch_arrow_table", "ingest_arrow_table", "export_to_storage", "import_from_storage"]

    for method in storage_methods:
        assert hasattr(driver, method)
        assert callable(getattr(driver, method))


def test_translator_mixin_integration(driver: DuckDBDriver) -> None:
    """Test SQLTranslatorMixin integration."""
    assert hasattr(driver, "returns_rows")

    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert driver.returns_rows(insert_stmt.expression) is False


def test_to_schema_mixin_integration(driver: DuckDBDriver) -> None:
    """Test ToSchemaMixin integration."""
    assert hasattr(driver, "to_schema")
    assert callable(driver.to_schema)


# DuckDB-Specific Arrow Tests
def test_fetch_arrow_table_native(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test DuckDB native Arrow table fetch."""
    import pyarrow as pa

    # Setup mock arrow table
    mock_arrow_table = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    mock_result = mock_connection.execute.return_value
    mock_result.arrow.return_value = mock_arrow_table

    statement = SQL("SELECT * FROM users")
    result = driver.fetch_arrow_table(statement)

    assert isinstance(result, ArrowResult)
    assert result.data is mock_arrow_table
    # The statement is a copy, not the same object
    assert result.statement.to_sql() == statement.to_sql()

    # Verify DuckDB native method was called
    # SQL with no parameters should pass an empty list
    mock_connection.execute.assert_called_once_with("SELECT * FROM users", [])
    mock_result.arrow.assert_called_once()


def test_fetch_arrow_table_with_parameters(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test DuckDB Arrow table fetch with parameters."""
    import pyarrow as pa

    # Setup mock arrow table
    mock_arrow_table = pa.table({"id": [1], "name": ["Alice"]})  # pyright: ignore
    mock_result = mock_connection.execute.return_value
    mock_result.arrow.return_value = mock_arrow_table

    statement = SQL("SELECT * FROM users WHERE id = ?", [42])
    result = driver.fetch_arrow_table(statement)

    assert isinstance(result, ArrowResult)
    assert result.data is mock_arrow_table

    # Verify DuckDB native method was called with parameters
    mock_connection.execute.assert_called_once_with("SELECT * FROM users WHERE id = ?", [42])
    mock_result.arrow.assert_called_once()


def test_fetch_arrow_table_streaming(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test DuckDB Arrow table fetch with streaming (batch_size)."""
    import pyarrow as pa

    # Setup mock for streaming
    mock_batch = pa.record_batch({"id": [1, 2], "name": ["Alice", "Bob"]})
    mock_result = mock_connection.execute.return_value
    mock_result.fetch_record_batch.return_value = iter([mock_batch])

    statement = SQL("SELECT * FROM users")
    result = driver.fetch_arrow_table(statement, batch_size=1000)

    assert isinstance(result, ArrowResult)
    # The statement is a copy, not the same object
    assert result.statement.to_sql() == statement.to_sql()

    # Verify DuckDB streaming method was called
    # batch_size is passed as a kwarg which SQL treats as a parameter
    mock_connection.execute.assert_called_once_with("SELECT * FROM users", {"batch_size": 1000})
    mock_result.fetch_record_batch.assert_called_once_with(1000)


def test_fetch_arrow_table_with_connection_override(driver: DuckDBDriver) -> None:
    """Test DuckDB Arrow table fetch with connection override."""
    import pyarrow as pa

    # Create override connection
    override_connection = MagicMock()
    mock_arrow_table = pa.table({"id": [1], "name": ["Alice"]})
    mock_result = MagicMock()
    mock_result.arrow.return_value = mock_arrow_table
    override_connection.execute.return_value = mock_result

    statement = SQL("SELECT * FROM users")
    result = driver.fetch_arrow_table(statement, _connection=override_connection)

    assert isinstance(result, ArrowResult)
    assert result.data is mock_arrow_table

    # Verify override connection was used
    override_connection.execute.assert_called_once_with("SELECT * FROM users", [])
    mock_result.arrow.assert_called_once()


# Native Storage Capability Tests
@pytest.mark.parametrize(
    "operation,format,expected",
    [
        ("export", "parquet", True),
        ("export", "csv", True),
        ("export", "json", True),
        ("export", "xlsx", False),
        ("import", "parquet", True),
        ("import", "csv", True),
        ("import", "json", True),
        ("import", "xlsx", False),
        ("read", "parquet", True),
        ("read", "csv", False),
        ("unknown", "parquet", False),
    ],
    ids=[
        "export_parquet",
        "export_csv",
        "export_json",
        "export_xlsx_unsupported",
        "import_parquet",
        "import_csv",
        "import_json",
        "import_xlsx_unsupported",
        "read_parquet",
        "read_csv_unsupported",
        "unknown_operation",
    ],
)
def test_has_native_capability(driver: DuckDBDriver, operation: str, format: str, expected: bool) -> None:
    """Test DuckDB native capability detection."""
    result = driver._has_native_capability(operation, format=format)
    assert result == expected


def test_export_native_parquet(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test DuckDB native Parquet export."""
    query = "SELECT * FROM users"
    destination_uri = "/path/to/output.parquet"

    result = driver._export_native(query, destination_uri, "parquet", compression="snappy", row_group_size=10000)

    # Should return 0 for successful export (mocked)
    assert result == 0

    # Verify DuckDB COPY command was executed
    mock_connection.execute.assert_called_once()
    call_args = mock_connection.execute.call_args[0][0]
    assert "COPY (" in call_args
    assert query in call_args
    assert destination_uri in call_args
    assert "FORMAT PARQUET" in call_args
    assert "COMPRESSION 'SNAPPY'" in call_args
    assert "ROW_GROUP_SIZE 10000" in call_args


def test_export_native_csv(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test DuckDB native CSV export."""
    query = "SELECT * FROM users"
    destination_uri = "/path/to/output.csv"

    result = driver._export_native(query, destination_uri, "csv", delimiter=";", quote='"')

    # Should return 0 for successful export (mocked)
    assert result == 0

    # Verify DuckDB COPY command was executed
    mock_connection.execute.assert_called_once()
    call_args = mock_connection.execute.call_args[0][0]
    assert "COPY (" in call_args
    assert query in call_args
    assert destination_uri in call_args
    assert "FORMAT CSV" in call_args
    assert "HEADER" in call_args
    assert "DELIMITER ';'" in call_args


def test_export_native_json(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test DuckDB native JSON export."""
    query = "SELECT * FROM users"
    destination_uri = "/path/to/output.json"

    result = driver._export_native(query, destination_uri, "json", compression="gzip")

    # Should return 0 for successful export (mocked)
    assert result == 0

    # Verify DuckDB COPY command was executed
    mock_connection.execute.assert_called_once()
    call_args = mock_connection.execute.call_args[0][0]
    assert "COPY (" in call_args
    assert query in call_args
    assert destination_uri in call_args
    assert "FORMAT JSON" in call_args
    assert "COMPRESSION 'GZIP'" in call_args


# Edge Cases
def test_execute_with_no_parameters(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test executing statement with no parameters."""
    mock_result = mock_connection.execute.return_value
    mock_result.fetchone.return_value = (0,)

    # Disable validation to allow DDL
    from sqlspec.statement.sql import SQLConfig

    config = SQLConfig(enable_validation=False)
    statement = SQL("CREATE TABLE test (id INTEGER)", _config=config)
    driver._execute_statement(statement)

    # Note: SQLGlot normalizes INTEGER to INT
    # DDL statements use cursor.execute, not connection.execute
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.execute.assert_called_once_with("CREATE TABLE test (id INT)", [])


def test_execute_select_with_empty_result(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test SELECT with empty result set."""
    mock_result = mock_connection.execute.return_value
    mock_result.fetchall.return_value = []
    mock_result.description = [("id",), ("name",)]

    statement = SQL("SELECT * FROM users WHERE 1=0")
    result = driver._execute_statement(statement)

    assert result == {"data": [], "column_names": ["id", "name"], "rows_affected": 0}


def test_execute_many_with_empty_parameters(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test execute_many with empty parameter list."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.rowcount = 0

    sql = "INSERT INTO users (name) VALUES (?)"
    params: list[list[str]] = []

    result = driver._execute_many(sql, params)

    assert result == {"rows_affected": 0}

    # DuckDB driver optimizes by not calling executemany with empty parameter list
    mock_cursor.executemany.assert_not_called()


def test_connection_override_in_execute(driver: DuckDBDriver) -> None:
    """Test DuckDB driver with connection override in execute methods."""
    override_connection = MagicMock()

    # Set up cursor mock for the override connection
    override_cursor = MagicMock()
    override_cursor.rowcount = 1
    override_connection.cursor.return_value = override_cursor

    statement = SQL("INSERT INTO test VALUES (1)")
    driver._execute_statement(statement, connection=override_connection)

    # INSERT statements use cursor.execute, not connection.execute
    override_cursor.execute.assert_called_once()
    # Original connection should not be called
    driver.connection.cursor.assert_not_called()  # pyright: ignore


def test_fetch_arrow_table_empty_batch_list(driver: DuckDBDriver, mock_connection: MagicMock) -> None:
    """Test DuckDB Arrow table fetch with empty batch list in streaming mode."""
    import pyarrow as pa

    # Setup mock for empty streaming
    mock_result = mock_connection.execute.return_value
    mock_result.fetch_record_batch.return_value = iter([])  # Empty iterator

    statement = SQL("SELECT * FROM empty_table")
    result = driver.fetch_arrow_table(statement, batch_size=1000)

    assert isinstance(result, ArrowResult)
    # The statement is a copy, not the same object
    assert result.statement.to_sql() == statement.to_sql()
    # Should create empty table when no batches
    assert isinstance(result.data, pa.Table)

    # batch_size is passed as a kwarg which SQL treats as a parameter
    mock_connection.execute.assert_called_once_with("SELECT * FROM empty_table", {"batch_size": 1000})
    mock_result.fetch_record_batch.assert_called_once_with(1000)
