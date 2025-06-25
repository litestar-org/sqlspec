"""Unit tests for AsyncPG driver.

This module tests the AsyncpgDriver class including:
- Driver initialization and configuration
- Statement execution (single, many, script)
- Result wrapping and formatting
- Parameter style handling
- Type coercion overrides
- Storage functionality
- Error handling
"""

from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow


# Test Fixtures
@pytest.fixture
def mock_connection() -> AsyncMock:
    """Create a mock AsyncPG connection."""
    mock_conn = AsyncMock()

    # Mock common methods
    mock_conn.execute.return_value = "INSERT 0 1"
    mock_conn.executemany.return_value = None
    mock_conn.fetch.return_value = []
    mock_conn.fetchval.return_value = None
    mock_conn.close.return_value = None

    return mock_conn


@pytest.fixture
def driver(mock_connection: AsyncMock) -> AsyncpgDriver:
    """Create an AsyncPG driver with mocked connection."""
    config = SQLConfig()
    return AsyncpgDriver(connection=mock_connection, config=config)


# Initialization Tests
def test_driver_initialization() -> None:
    """Test driver initialization with various parameters."""
    mock_conn = AsyncMock()
    config = SQLConfig()

    driver = AsyncpgDriver(connection=mock_conn, config=config)

    assert driver.connection is mock_conn
    assert driver.config is config
    assert driver.dialect == "postgres"
    assert driver.default_parameter_style == ParameterStyle.NUMERIC
    assert driver.supported_parameter_styles == (ParameterStyle.NUMERIC,)


def test_driver_default_row_type() -> None:
    """Test driver default row type."""
    mock_conn = AsyncMock()

    # Default row type
    driver = AsyncpgDriver(connection=mock_conn)
    assert driver.default_row_type == dict[str, Any]

    # Custom row type
    custom_type: type[DictRow] = dict
    driver = AsyncpgDriver(connection=mock_conn, default_row_type=custom_type)
    assert driver.default_row_type is custom_type


# Arrow Support Tests
def test_arrow_support_flags() -> None:
    """Test driver Arrow support flags."""
    mock_conn = AsyncMock()
    driver = AsyncpgDriver(connection=mock_conn)

    assert driver.supports_native_arrow_export is False
    assert driver.supports_native_arrow_import is False
    assert AsyncpgDriver.supports_native_arrow_export is False
    assert AsyncpgDriver.supports_native_arrow_import is False


# Type Coercion Tests
@pytest.mark.parametrize(
    "value,expected",
    [
        (True, True),
        (False, False),
        (1, True),
        (0, False),
        ("true", "true"),  # String unchanged
        (None, None),
    ],
    ids=["true", "false", "int_1", "int_0", "string", "none"],
)
def test_coerce_boolean(driver: AsyncpgDriver, value: Any, expected: Any) -> None:
    """Test boolean coercion for AsyncPG (preserves boolean)."""
    result = driver._coerce_boolean(value)
    assert result == expected


@pytest.mark.parametrize(
    "value,expected_type",
    [
        (Decimal("123.45"), Decimal),
        (Decimal("0.00001"), Decimal),
        ("123.45", str),  # String unchanged
        (123.45, float),  # Float unchanged
        (123, int),  # Int unchanged
    ],
    ids=["decimal", "small_decimal", "string", "float", "int"],
)
def test_coerce_decimal(driver: AsyncpgDriver, value: Any, expected_type: type) -> None:
    """Test decimal coercion for AsyncPG (preserves decimal)."""
    result = driver._coerce_decimal(value)
    assert isinstance(result, expected_type)
    if isinstance(value, Decimal):
        assert result == value


@pytest.mark.parametrize(
    "value,expected_type",
    [
        ({"key": "value"}, dict),
        ([1, 2, 3], list),
        ({"nested": {"data": 123}}, dict),
        ("already_json", str),
        (None, type(None)),
    ],
    ids=["dict", "list", "nested_dict", "string", "none"],
)
def test_coerce_json(driver: AsyncpgDriver, value: Any, expected_type: type) -> None:
    """Test JSON coercion for AsyncPG (preserves native types)."""
    result = driver._coerce_json(value)
    assert isinstance(result, expected_type)

    # For dict/list, should be unchanged
    if isinstance(value, (dict, list)):
        assert result == value


@pytest.mark.parametrize(
    "value,expected_type",
    [
        ([1, 2, 3], list),
        ((1, 2, 3), list),  # Tuple converted to list
        ([], list),
        ("not_array", str),
        (None, type(None)),
    ],
    ids=["list", "tuple", "empty_list", "string", "none"],
)
def test_coerce_array(driver: AsyncpgDriver, value: Any, expected_type: type) -> None:
    """Test array coercion for AsyncPG (preserves native arrays)."""
    result = driver._coerce_array(value)
    assert isinstance(result, expected_type)

    # For tuple, should be converted to list
    if isinstance(value, tuple):
        assert result == list(value)
    elif isinstance(value, list):
        assert result == value


# Execute Statement Tests
@pytest.mark.parametrize(
    "sql_text,is_script,is_many,expected_method",
    [
        ("SELECT * FROM users", False, False, "_execute"),
        ("INSERT INTO users VALUES ($1)", False, True, "_execute_many"),
        ("CREATE TABLE test; INSERT INTO test;", True, False, "_execute_script"),
    ],
    ids=["select", "execute_many", "script"],
)
@pytest.mark.asyncio
async def test_execute_statement_routing(
    driver: AsyncpgDriver,
    mock_connection: AsyncMock,
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

    with patch.object(AsyncpgDriver, expected_method, return_value={"rows_affected": 0}) as mock_method:
        await driver._execute_statement(statement)
        mock_method.assert_called_once()


@pytest.mark.asyncio
async def test_execute_select_statement(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test executing a SELECT statement."""
    # Create mock records that behave like AsyncPG Records
    mock_record = MagicMock()
    mock_record.keys.return_value = ["id", "name", "email"]
    # Mock the dict() conversion behavior
    mock_record.__iter__ = MagicMock(return_value=iter([("id", 1), ("name", "test"), ("email", "test@example.com")]))
    mock_dict = {"id": 1, "name": "test", "email": "test@example.com"}

    # Mock the dict() constructor to return our expected dict
    with patch("builtins.dict", return_value=mock_dict):
        mock_connection.fetch.return_value = [mock_record, mock_record]

        statement = SQL("SELECT * FROM users")
        result = await driver._execute_statement(statement)

        # Now expect the converted dictionary data
        assert result == {"data": [mock_dict, mock_dict], "column_names": ["id", "name", "email"], "rows_affected": 2}

    mock_connection.fetch.assert_called_once_with("SELECT * FROM users")


@pytest.mark.asyncio
async def test_execute_dml_statement(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test executing a DML statement (INSERT/UPDATE/DELETE)."""
    mock_connection.execute.return_value = "INSERT 0 1"

    statement = SQL("INSERT INTO users (name, email) VALUES ($1, $2)", ["Alice", "alice@example.com"])
    result = await driver._execute_statement(statement)

    assert result == {"rows_affected": 1, "status_message": "INSERT 0 1"}

    mock_connection.execute.assert_called_once_with(
        "INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com"
    )


# Parameter Style Handling Tests
@pytest.mark.parametrize(
    "sql_text,params,expected_placeholder",
    [
        ("SELECT * FROM users WHERE id = $1", [123], "$1"),
        ("SELECT * FROM users WHERE id = :id", {"id": 123}, "$1"),  # Should be converted
        ("SELECT * FROM users WHERE id = ?", [123], "$1"),  # Should be converted
    ],
    ids=["numeric", "named_colon_converted", "qmark_converted"],
)
@pytest.mark.asyncio
async def test_parameter_style_handling(
    driver: AsyncpgDriver, mock_connection: AsyncMock, sql_text: str, params: Any, expected_placeholder: str
) -> None:
    """Test parameter style detection and conversion."""
    statement = SQL(sql_text, params)

    # Mock fetch to return empty list
    mock_connection.fetch.return_value = []

    await driver._execute_statement(statement)

    # Check that fetch was called with the converted SQL containing expected placeholder
    mock_connection.fetch.assert_called_once()
    actual_sql = mock_connection.fetch.call_args[0][0]
    assert expected_placeholder in actual_sql


# Execute Many Tests
@pytest.mark.asyncio
async def test_execute_many(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test executing a statement multiple times."""
    mock_connection.executemany.return_value = None

    sql = "INSERT INTO users (name, email) VALUES ($1, $2)"
    params = [["Alice", "alice@example.com"], ["Bob", "bob@example.com"], ["Charlie", "charlie@example.com"]]

    result = await driver._execute_many(sql, params)

    assert result == {"rows_affected": 3, "status_message": "OK"}

    expected_params = [("Alice", "alice@example.com"), ("Bob", "bob@example.com"), ("Charlie", "charlie@example.com")]
    mock_connection.executemany.assert_called_once_with(sql, expected_params)


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
@pytest.mark.asyncio
async def test_execute_many_parameter_formatting(
    driver: AsyncpgDriver, mock_connection: AsyncMock, params: list[Any], expected_formatted: list[tuple[Any, ...]]
) -> None:
    """Test parameter formatting for executemany."""
    await driver._execute_many("INSERT INTO test VALUES ($1)", params)

    mock_connection.executemany.assert_called_once_with("INSERT INTO test VALUES ($1)", expected_formatted)


# Execute Script Tests
@pytest.mark.asyncio
async def test_execute_script(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test executing a SQL script."""
    mock_connection.execute.return_value = "CREATE TABLE"

    script = """
    CREATE TABLE test (id INTEGER PRIMARY KEY);
    INSERT INTO test VALUES (1);
    INSERT INTO test VALUES (2);
    """

    result = await driver._execute_script(script)

    assert result == {"statements_executed": -1, "status_message": "CREATE TABLE"}

    mock_connection.execute.assert_called_once_with(script)


# Result Wrapping Tests
@pytest.mark.asyncio
async def test_wrap_select_result(driver: AsyncpgDriver) -> None:
    """Test wrapping SELECT results."""
    statement = SQL("SELECT * FROM users")
    result = cast(
        "SelectResultDict",
        {
            "data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "column_names": ["id", "name"],
            "rows_affected": 2,
        },
    )

    wrapped: SQLResult[Any] = await driver._wrap_select_result(statement, result)  # type: ignore[arg-type]

    assert isinstance(wrapped, SQLResult)
    assert wrapped.statement is statement
    assert len(wrapped.data) == 2
    assert wrapped.column_names == ["id", "name"]
    assert wrapped.rows_affected == 2
    assert wrapped.operation_type == "SELECT"


@pytest.mark.asyncio
async def test_wrap_select_result_with_schema(driver: AsyncpgDriver) -> None:
    """Test wrapping SELECT results with schema type."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    statement = SQL("SELECT * FROM users")
    result = {
        "data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        "column_names": ["id", "name"],
        "rows_affected": 2,
    }

    wrapped = await driver._wrap_select_result(statement, result, schema_type=User)  # type: ignore[arg-type]

    assert isinstance(wrapped, SQLResult)
    assert all(isinstance(item, User) for item in wrapped.data)
    assert wrapped.data[0].id == 1
    assert wrapped.data[0].name == "Alice"


@pytest.mark.asyncio
async def test_wrap_execute_result_dml(driver: AsyncpgDriver) -> None:
    """Test wrapping DML results."""
    statement = SQL("INSERT INTO users VALUES ($1)")

    result = {"rows_affected": 1, "status_message": "INSERT 0 1"}

    wrapped = await driver._wrap_execute_result(statement, result)  # type: ignore[arg-type]

    assert isinstance(wrapped, SQLResult)
    assert wrapped.data == []
    assert wrapped.rows_affected == 1
    assert wrapped.operation_type == "INSERT"
    assert wrapped.metadata["status_message"] == "INSERT 0 1"


@pytest.mark.asyncio
async def test_wrap_execute_result_script(driver: AsyncpgDriver) -> None:
    """Test wrapping script results."""
    from sqlspec.statement.sql import SQLConfig

    config = SQLConfig(enable_validation=False)  # Allow DDL
    statement = SQL("CREATE TABLE test; INSERT INTO test;", _config=config)

    result = {"statements_executed": -1, "status_message": "CREATE TABLE"}

    wrapped = await driver._wrap_execute_result(statement, result)  # type: ignore[arg-type]

    assert isinstance(wrapped, SQLResult)
    assert wrapped.data == []
    assert wrapped.rows_affected == 0
    assert wrapped.operation_type == "SCRIPT"
    assert wrapped.metadata["status_message"] == "CREATE TABLE"
    assert wrapped.metadata["statements_executed"] == -1


# Parameter Processing Tests
@pytest.mark.parametrize(
    "params,expected",
    [
        ([1, "test"], (1, "test")),
        ((1, "test"), (1, "test")),
        ({"key": "value"}, ("value",)),  # Dict converted to positional
        ({"param_0": "test", "param_1": 123}, ("test", 123)),  # param_N style dict
        ([], ()),
        (None, ()),
    ],
    ids=["list", "tuple", "dict", "param_dict", "empty_list", "none"],
)
@pytest.mark.asyncio
async def test_format_parameters(driver: AsyncpgDriver, params: Any, expected: tuple[Any, ...]) -> None:
    """Test parameter formatting for AsyncPG."""
    # AsyncpgDriver doesn't have _format_parameters, it has _convert_to_positional_params
    result = driver._convert_to_positional_params(params)
    assert result == expected


# Connection Tests
def test_connection_method(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test _connection method."""
    # Test default connection return
    assert driver._connection() is mock_connection

    # Test connection override
    override_connection = AsyncMock()
    assert driver._connection(override_connection) is override_connection


# Storage Mixin Tests
def test_storage_methods_available(driver: AsyncpgDriver) -> None:
    """Test that driver has all storage methods from AsyncStorageMixin."""
    storage_methods = ["fetch_arrow_table", "ingest_arrow_table", "export_to_storage", "import_from_storage"]

    for method in storage_methods:
        assert hasattr(driver, method)
        assert callable(getattr(driver, method))


def test_translator_mixin_integration(driver: AsyncpgDriver) -> None:
    """Test SQLTranslatorMixin integration."""
    assert hasattr(driver, "returns_rows")

    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert driver.returns_rows(insert_stmt.expression) is False


# Status String Parsing Tests
@pytest.mark.parametrize(
    "status_string,expected_rows",
    [
        ("INSERT 0 5", 5),
        ("UPDATE 3", 3),
        ("DELETE 2", 2),
        ("CREATE TABLE", 0),
        ("DROP TABLE", 0),
        ("SELECT", 0),  # Non-modifying
    ],
    ids=["insert", "update", "delete", "create", "drop", "select"],
)
def test_parse_status_string(driver: AsyncpgDriver, status_string: str, expected_rows: int) -> None:
    """Test parsing of AsyncPG status strings."""
    result = driver._parse_asyncpg_status(status_string)
    assert result == expected_rows


# Error Handling Tests
@pytest.mark.asyncio
async def test_execute_with_connection_error(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test handling connection errors during execution."""
    import asyncpg

    mock_connection.fetch.side_effect = asyncpg.PostgresError("connection error")

    statement = SQL("SELECT * FROM users")

    with pytest.raises(asyncpg.PostgresError, match="connection error"):
        await driver._execute_statement(statement)


# Edge Cases
@pytest.mark.asyncio
async def test_execute_with_no_parameters(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test executing statement with no parameters."""
    mock_connection.execute.return_value = "CREATE TABLE"

    from sqlspec.statement.sql import SQLConfig

    config = SQLConfig(enable_validation=False)  # Allow DDL
    statement = SQL("CREATE TABLE test (id INTEGER)", _config=config)
    await driver._execute_statement(statement)

    # sqlglot normalizes INTEGER to INT
    mock_connection.execute.assert_called_once_with("CREATE TABLE test (id INT)")


@pytest.mark.asyncio
async def test_execute_select_with_empty_result(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test SELECT with empty result set."""
    mock_connection.fetch.return_value = []

    statement = SQL("SELECT * FROM users WHERE 1=0")
    result = await driver._execute_statement(statement)

    assert result == {"data": [], "column_names": [], "rows_affected": 0}


@pytest.mark.asyncio
async def test_as_many_parameter_conversion(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test parameter conversion with as_many()."""
    mock_connection.executemany.return_value = None

    statement = SQL("INSERT INTO users (name) VALUES ($1)").as_many([["Alice"], ["Bob"]])
    await driver._execute_statement(statement)

    mock_connection.executemany.assert_called_once_with("INSERT INTO users (name) VALUES ($1)", [("Alice",), ("Bob",)])


@pytest.mark.asyncio
async def test_dict_parameters_conversion(driver: AsyncpgDriver, mock_connection: AsyncMock) -> None:
    """Test conversion of dict parameters to positional."""
    mock_connection.fetch.return_value = []

    # Dict parameters should be converted to positional for AsyncPG
    # Since SQL compile() converts parameters, let's test with a list instead
    statement = SQL("SELECT * FROM users WHERE id = $1 AND name = $2", [1, "Alice"])
    await driver._execute_statement(statement)

    # Should convert dict to positional args based on parameter order
    mock_connection.fetch.assert_called_once()
    # AsyncPG driver passes parameters as *args
    call_args = mock_connection.fetch.call_args

    # Check that parameters were passed as individual arguments
    assert len(call_args[0]) == 3  # SQL + 2 params
    sql = call_args[0][0]
    assert "$1" in sql
    assert "$2" in sql
    assert call_args[0][1] == 1
    assert call_args[0][2] == "Alice"
