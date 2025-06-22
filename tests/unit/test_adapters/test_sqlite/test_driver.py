"""Unit tests for SQLite driver.

This module tests the SqliteDriver class including:
- Driver initialization and configuration
- Statement execution (single, many, script)
- Result wrapping and formatting
- Parameter style handling
- Type coercion overrides
- Bulk loading functionality
- Error handling
"""

import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, PropertyMock, mock_open, patch

import pytest

from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.statement.parameters import ParameterInfo, ParameterStyle
from sqlspec.statement.result import SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow


# Test Fixtures
@pytest.fixture
def mock_connection() -> MagicMock:
    """Create a mock SQLite connection."""
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_cursor = MagicMock()

    # Set up cursor context manager
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=None)

    # Mock cursor methods
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.executemany.return_value = mock_cursor
    mock_cursor.executescript.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    mock_cursor.close.return_value = None
    mock_cursor.rowcount = 0
    mock_cursor.description = None

    # Connection returns cursor
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    return mock_conn


@pytest.fixture
def driver(mock_connection: MagicMock) -> SqliteDriver:
    """Create a SQLite driver with mocked connection."""
    config = SQLConfig()
    return SqliteDriver(connection=mock_connection, config=config)


# Initialization Tests
def test_driver_initialization() -> None:
    """Test driver initialization with various parameters."""
    mock_conn = MagicMock()
    config = SQLConfig()

    driver = SqliteDriver(connection=mock_conn, config=config)

    assert driver.connection is mock_conn
    assert driver.config is config
    assert driver.dialect == "sqlite"
    assert driver.default_parameter_style == ParameterStyle.QMARK
    assert driver.supported_parameter_styles == (ParameterStyle.QMARK, ParameterStyle.NAMED_COLON)


def test_driver_default_row_type() -> None:
    """Test driver default row type."""
    mock_conn = MagicMock()

    # Default row type
    driver = SqliteDriver(connection=mock_conn)
    assert driver.default_row_type == dict[str, Any]

    # Custom row type
    custom_type: type[DictRow] = dict
    driver = SqliteDriver(connection=mock_conn, default_row_type=custom_type)
    assert driver.default_row_type is custom_type


# Type Coercion Tests
@pytest.mark.parametrize(
    "value,expected",
    [
        (True, 1),
        (False, 0),
        (1, 1),
        (0, 0),
        ("true", "true"),  # String unchanged
        (None, None),
    ],
    ids=["true", "false", "int_1", "int_0", "string", "none"],
)
def test_coerce_boolean(driver: SqliteDriver, value: Any, expected: Any) -> None:
    """Test boolean coercion for SQLite (stores as 0/1)."""
    result = driver._coerce_boolean(value)
    assert result == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (Decimal("123.45"), "123.45"),
        (Decimal("0.00001"), "0.00001"),
        ("123.45", "123.45"),  # Already string
        (123.45, 123.45),  # Float unchanged
        (123, 123),  # Int unchanged
    ],
    ids=["decimal", "small_decimal", "string", "float", "int"],
)
def test_coerce_decimal(driver: SqliteDriver, value: Any, expected: Any) -> None:
    """Test decimal coercion for SQLite (stores as string)."""
    result = driver._coerce_decimal(value)
    assert result == expected


@pytest.mark.parametrize(
    "value,expected_type",
    [
        ({"key": "value"}, str),
        ([1, 2, 3], str),
        ({"nested": {"data": 123}}, str),
        ("already_json", str),
        (None, type(None)),
    ],
    ids=["dict", "list", "nested_dict", "string", "none"],
)
def test_coerce_json(driver: SqliteDriver, value: Any, expected_type: type) -> None:
    """Test JSON coercion for SQLite (stores as string)."""
    result = driver._coerce_json(value)
    assert isinstance(result, expected_type)

    # For dict/list, ensure it's valid JSON string
    if isinstance(value, (dict, list)):
        import json

        assert isinstance(result, str)  # Type guard for mypy
        assert json.loads(result) == value


@pytest.mark.parametrize(
    "value,expected_type",
    [([1, 2, 3], str), ((1, 2, 3), str), ([], str), ("not_array", str), (None, type(None))],
    ids=["list", "tuple", "empty_list", "string", "none"],
)
def test_coerce_array(driver: SqliteDriver, value: Any, expected_type: type) -> None:
    """Test array coercion for SQLite (stores as JSON string)."""
    result = driver._coerce_array(value)
    assert isinstance(result, expected_type)

    # For list/tuple, ensure it's valid JSON string
    if isinstance(value, (list, tuple)):
        import json

        assert isinstance(result, str)  # Type guard for mypy
        assert json.loads(result) == list(value)


# Cursor Context Manager Tests
def test_get_cursor_success() -> None:
    """Test _get_cursor context manager normal flow."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with SqliteDriver._get_cursor(mock_conn) as cursor:
        assert cursor is mock_cursor
        mock_cursor.close.assert_not_called()

    mock_cursor.close.assert_called_once()


def test_get_cursor_error_handling() -> None:
    """Test _get_cursor context manager error handling."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with pytest.raises(ValueError, match="Test error"):
        with SqliteDriver._get_cursor(mock_conn) as cursor:
            assert cursor is mock_cursor
            raise ValueError("Test error")

    # Cursor should still be closed
    mock_cursor.close.assert_called_once()


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
    driver: SqliteDriver,
    mock_connection: MagicMock,
    sql_text: str,
    is_script: bool,
    is_many: bool,
    expected_method: str,
) -> None:
    """Test that _execute_statement routes to correct method."""
    from sqlspec.statement.sql import SQLConfig

    # Create config that allows DDL
    config = SQLConfig(
        enable_validation=False  # Disable validation to allow DDL
    )
    statement = SQL(sql_text, _config=config)

    # Set the internal flags
    statement._is_script = is_script
    statement._is_many = is_many

    with patch.object(SqliteDriver, expected_method, return_value={"rows_affected": 0}) as mock_method:
        driver._execute_statement(statement)
        mock_method.assert_called_once()


def test_execute_select_statement(driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test executing a SELECT statement."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.description = [("id",), ("name",), ("email",)]
    mock_cursor.fetchall.return_value = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]
    mock_cursor.rowcount = 2

    statement = SQL("SELECT * FROM users")
    result = driver._execute_statement(statement)

    assert result == {
        "data": mock_cursor.fetchall.return_value,
        "column_names": ["id", "name", "email"],
        "rows_affected": 2,
    }

    mock_cursor.execute.assert_called_once_with("SELECT * FROM users", ())


def test_execute_dml_statement(driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test executing a DML statement (INSERT/UPDATE/DELETE)."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.rowcount = 1

    statement = SQL("INSERT INTO users (name, email) VALUES (?, ?)", ["Alice", "alice@example.com"])
    result = driver._execute_statement(statement)

    assert result == {"rows_affected": 1, "status_message": "OK"}

    mock_cursor.execute.assert_called_once_with(
        "INSERT INTO users (name, email) VALUES (?, ?)", ("Alice", "alice@example.com")
    )


# Parameter Style Handling Tests
@pytest.mark.parametrize(
    "sql_text,detected_style,expected_style",
    [
        ("SELECT * FROM users WHERE id = ?", ParameterStyle.QMARK, ParameterStyle.QMARK),
        ("SELECT * FROM users WHERE id = :id", ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_COLON),
        ("SELECT * FROM users WHERE id = $1", ParameterStyle.NUMERIC, ParameterStyle.QMARK),  # Unsupported
    ],
    ids=["qmark", "named_colon", "numeric_unsupported"],
)
def test_parameter_style_handling(
    driver: SqliteDriver,
    mock_connection: MagicMock,
    sql_text: str,
    detected_style: ParameterStyle,
    expected_style: ParameterStyle,
) -> None:
    """Test parameter style detection and conversion."""
    statement = SQL(sql_text)

    # Mock the parameter_info property to return the expected style
    mock_param_info = [ParameterInfo(name="p1", position=0, style=detected_style, ordinal=0, placeholder_text="?")]
    with (
        patch.object(type(statement), "parameter_info", new_callable=PropertyMock, return_value=mock_param_info),
        patch.object(type(statement), "compile") as mock_compile,
    ):
        mock_compile.return_value = (sql_text, None)
        driver._execute_statement(statement)

        mock_compile.assert_called_with(placeholder_style=expected_style)


# Execute Many Tests
def test_execute_many(driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test executing a statement multiple times."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.rowcount = 3

    sql = "INSERT INTO users (name, email) VALUES (?, ?)"
    params = [["Alice", "alice@example.com"], ["Bob", "bob@example.com"], ["Charlie", "charlie@example.com"]]

    result = driver._execute_many(sql, params)

    assert result == {"rows_affected": 3, "status_message": "OK"}

    expected_params = [("Alice", "alice@example.com"), ("Bob", "bob@example.com"), ("Charlie", "charlie@example.com")]
    mock_cursor.executemany.assert_called_once_with(sql, expected_params)


@pytest.mark.parametrize(
    "params,expected_formatted",
    [
        ([[1, "a"], [2, "b"]], [(1, "a"), (2, "b")]),
        ([(1, "a"), (2, "b")], [(1, "a"), (2, "b")]),
        ([1, 2, 3], [(1,), (2,), (3,)]),
        ([None, None], [(), ()]),
    ],
    ids=["list_of_lists", "list_of_tuples", "single_values", "none_values"],
)
def test_execute_many_parameter_formatting(
    driver: SqliteDriver, mock_connection: MagicMock, params: list[Any], expected_formatted: list[tuple[Any, ...]]
) -> None:
    """Test parameter formatting for executemany."""
    mock_cursor = mock_connection.cursor.return_value

    driver._execute_many("INSERT INTO test VALUES (?)", params)

    mock_cursor.executemany.assert_called_once_with("INSERT INTO test VALUES (?)", expected_formatted)


# Execute Script Tests
def test_execute_script(driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test executing a SQL script."""
    mock_cursor = mock_connection.cursor.return_value

    script = """
    CREATE TABLE test (id INTEGER PRIMARY KEY);
    INSERT INTO test VALUES (1);
    INSERT INTO test VALUES (2);
    """

    result = driver._execute_script(script)

    assert result == {"statements_executed": -1, "status_message": "SCRIPT EXECUTED"}

    mock_cursor.executescript.assert_called_once_with(script)
    mock_connection.commit.assert_called_once()


# Bulk Load Tests
@patch("pathlib.Path.open", new_callable=mock_open, read_data="id,name\n1,Alice\n2,Bob\n")
def test_bulk_load_csv(mock_file: Mock, driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test bulk loading from CSV file."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.rowcount = 2

    file_path = Path("/tmp/test.csv")
    rows = driver._bulk_load_file(file_path, "users", "csv", "append")

    assert rows == 2

    mock_cursor.executemany.assert_called_once_with("INSERT INTO users VALUES (?, ?)", [["1", "Alice"], ["2", "Bob"]])


@patch("pathlib.Path.open", new_callable=mock_open, read_data="id,name\n1,Alice\n")
def test_bulk_load_csv_replace_mode(mock_file: Mock, driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test bulk loading with replace mode."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.rowcount = 1

    file_path = Path("/tmp/test.csv")
    rows = driver._bulk_load_file(file_path, "users", "csv", "replace")

    assert rows == 1

    # Should delete existing data first
    assert mock_cursor.execute.call_args_list[0][0][0] == "DELETE FROM users"

    mock_cursor.executemany.assert_called_once()


def test_bulk_load_unsupported_format(driver: SqliteDriver) -> None:
    """Test bulk loading with unsupported format."""
    with pytest.raises(NotImplementedError, match="SQLite driver only supports CSV"):
        driver._bulk_load_file(Path("/tmp/test.parquet"), "users", "parquet", "append")


# Result Wrapping Tests
def test_wrap_select_result(driver: SqliteDriver) -> None:
    """Test wrapping SELECT results."""
    statement = SQL("SELECT * FROM users")
    result: SelectResultDict = {
        "data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        "column_names": ["id", "name"],
        "rows_affected": 2,
    }

    wrapped = driver._wrap_select_result(statement, result)  # pyright: ignore

    assert isinstance(wrapped, SQLResult)
    assert wrapped.statement is statement
    assert wrapped.data == result["data"]
    assert wrapped.column_names == ["id", "name"]
    assert wrapped.rows_affected == 2
    assert wrapped.operation_type == "SELECT"


def test_wrap_select_result_with_schema(driver: SqliteDriver) -> None:
    """Test wrapping SELECT results with schema type."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    statement = SQL("SELECT * FROM users")
    result: SelectResultDict = {
        "data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        "column_names": ["id", "name"],
        "rows_affected": 2,
    }

    wrapped = driver._wrap_select_result(statement, result, schema_type=User)

    assert isinstance(wrapped, SQLResult)
    assert all(isinstance(item, User) for item in wrapped.data)
    assert wrapped.data[0].id == 1
    assert wrapped.data[0].name == "Alice"


def test_wrap_execute_result_dml(driver: SqliteDriver) -> None:
    """Test wrapping DML results."""
    statement = SQL("INSERT INTO users VALUES (?)")
    mock_expression = MagicMock()
    mock_expression.key = "insert"

    from sqlspec.statement.result import DMLResultDict

    result: DMLResultDict = {"rows_affected": 1, "status_message": "OK"}

    with patch.object(type(statement), "expression", new_callable=PropertyMock, return_value=mock_expression):
        wrapped = driver._wrap_execute_result(statement, result)  # pyright: ignore

    assert isinstance(wrapped, SQLResult)
    assert wrapped.data == []
    assert wrapped.rows_affected == 1
    assert wrapped.operation_type == "INSERT"
    assert wrapped.metadata["status_message"] == "OK"


def test_wrap_execute_result_script(driver: SqliteDriver) -> None:
    """Test wrapping script results."""
    statement = SQL("CREATE TABLE test; INSERT INTO test;")

    from sqlspec.statement.result import ScriptResultDict

    result: ScriptResultDict = {"statements_executed": 2, "status_message": "SCRIPT EXECUTED"}

    with patch.object(type(statement), "expression", new_callable=PropertyMock, return_value=None):
        wrapped = driver._wrap_execute_result(statement, result)  # pyright: ignore

    assert isinstance(wrapped, SQLResult)
    assert wrapped.data == []
    assert wrapped.rows_affected == 0
    assert wrapped.operation_type == "SCRIPT"
    assert wrapped.metadata["status_message"] == "SCRIPT EXECUTED"
    assert wrapped.metadata["statements_executed"] == 2


# Error Handling Tests
def test_execute_with_connection_error(driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test handling connection errors during execution."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.execute.side_effect = sqlite3.OperationalError("database is locked")

    statement = SQL("SELECT * FROM users")

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        driver._execute_statement(statement)


# Edge Cases
def test_execute_with_no_parameters(driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test executing statement with no parameters."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.rowcount = 0

    from sqlspec.statement.sql import SQLConfig

    config = SQLConfig(enable_validation=False)  # Allow DDL
    statement = SQL("CREATE TABLE test (id INTEGER)", _config=config)
    driver._execute_statement(statement)

    # sqlglot normalizes INTEGER to INT
    mock_cursor.execute.assert_called_once_with("CREATE TABLE test (id INT)", ())


def test_execute_select_with_empty_result(driver: SqliteDriver, mock_connection: MagicMock) -> None:
    """Test SELECT with empty result set."""
    mock_cursor = mock_connection.cursor.return_value
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = []
    mock_cursor.rowcount = 0

    statement = SQL("SELECT * FROM users WHERE 1=0")
    result = driver._execute_statement(statement)

    assert result == {"data": [], "column_names": ["id", "name"], "rows_affected": 0}
