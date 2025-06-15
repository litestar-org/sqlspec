"""Unit tests for Psycopg drivers.

This module tests the PsycopgSyncDriver and PsycopgAsyncDriver classes including:
- Driver initialization and configuration
- Statement execution (single, many, script)
- Result wrapping and formatting
- Parameter style handling
- Type coercion overrides
- Storage functionality
- Error handling
- Both sync and async variants
"""

from decimal import Decimal
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.psycopg import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.statement.parameters import ParameterInfo, ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow

if TYPE_CHECKING:
    pass


# Test Fixtures
@pytest.fixture
def mock_sync_connection() -> MagicMock:
    """Create a mock Psycopg sync connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # Set up cursor context manager
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None

    # Mock cursor methods
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = None
    mock_cursor.rowcount = 0
    mock_cursor.statusmessage = "EXECUTE"
    mock_cursor.close.return_value = None

    # Connection returns cursor
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None
    mock_conn.close.return_value = None

    return mock_conn


@pytest.fixture
def sync_driver(mock_sync_connection: MagicMock) -> PsycopgSyncDriver:
    """Create a Psycopg sync driver with mocked connection."""
    config = SQLConfig()
    return PsycopgSyncDriver(connection=mock_sync_connection, config=config)


@pytest.fixture
def mock_async_connection() -> AsyncMock:
    """Create a mock Psycopg async connection."""
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()

    # Set up cursor async context manager
    mock_cursor.__aenter__.return_value = mock_cursor
    mock_cursor.__aexit__.return_value = None

    # Mock cursor methods
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = None
    mock_cursor.rowcount = 0
    mock_cursor.statusmessage = "EXECUTE"
    mock_cursor.close.return_value = None

    # Connection returns cursor
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None
    mock_conn.close.return_value = None

    return mock_conn


@pytest.fixture
def async_driver(mock_async_connection: AsyncMock) -> PsycopgAsyncDriver:
    """Create a Psycopg async driver with mocked connection."""
    config = SQLConfig()
    return PsycopgAsyncDriver(connection=mock_async_connection, config=config)


# Sync Driver Initialization Tests
def test_sync_driver_initialization() -> None:
    """Test sync driver initialization with various parameters."""
    mock_conn = MagicMock()
    config = SQLConfig()

    driver = PsycopgSyncDriver(connection=mock_conn, config=config)

    assert driver.connection is mock_conn
    assert driver.config is config
    assert driver.dialect == "postgres"
    assert driver.default_parameter_style == ParameterStyle.POSITIONAL_PYFORMAT
    assert driver.supported_parameter_styles == (ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT)


def test_sync_driver_default_row_type() -> None:
    """Test sync driver default row type."""
    mock_conn = MagicMock()

    # Default row type
    driver = PsycopgSyncDriver(connection=mock_conn)
    assert driver.default_row_type == dict[str, Any]

    # Custom row type
    custom_type: type[DictRow] = dict
    driver = PsycopgSyncDriver(connection=mock_conn, default_row_type=custom_type)
    assert driver.default_row_type is custom_type


# Async Driver Initialization Tests
def test_async_driver_initialization() -> None:
    """Test async driver initialization with various parameters."""
    mock_conn = AsyncMock()
    config = SQLConfig()

    driver = PsycopgAsyncDriver(connection=mock_conn, config=config)

    assert driver.connection is mock_conn
    assert driver.config is config
    assert driver.dialect == "postgres"
    assert driver.default_parameter_style == ParameterStyle.POSITIONAL_PYFORMAT
    assert driver.supported_parameter_styles == (ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT)


def test_async_driver_default_row_type() -> None:
    """Test async driver default row type."""
    mock_conn = AsyncMock()

    # Default row type
    driver = PsycopgAsyncDriver(connection=mock_conn)
    assert driver.default_row_type == dict[str, Any]

    # Custom row type
    custom_type: type[DictRow] = dict
    driver = PsycopgAsyncDriver(connection=mock_conn, default_row_type=custom_type)
    assert driver.default_row_type is custom_type


# Arrow Support Tests
def test_sync_arrow_support_flags() -> None:
    """Test sync driver Arrow support flags."""
    mock_conn = MagicMock()
    driver = PsycopgSyncDriver(connection=mock_conn)

    assert driver.supports_native_arrow_export is False
    assert driver.supports_native_arrow_import is False
    assert PsycopgSyncDriver.supports_native_arrow_export is False
    assert PsycopgSyncDriver.supports_native_arrow_import is False


def test_async_arrow_support_flags() -> None:
    """Test async driver Arrow support flags."""
    mock_conn = AsyncMock()
    driver = PsycopgAsyncDriver(connection=mock_conn)

    assert driver.supports_native_arrow_export is False
    assert driver.supports_native_arrow_import is False
    assert PsycopgAsyncDriver.supports_native_arrow_export is False
    assert PsycopgAsyncDriver.supports_native_arrow_import is False


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
def test_sync_coerce_boolean(sync_driver: PsycopgSyncDriver, value: Any, expected: Any) -> None:
    """Test boolean coercion for Psycopg sync (preserves boolean)."""
    result = sync_driver._coerce_boolean(value)
    assert result == expected


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
def test_async_coerce_boolean(async_driver: PsycopgAsyncDriver, value: Any, expected: Any) -> None:
    """Test boolean coercion for Psycopg async (preserves boolean)."""
    result = async_driver._coerce_boolean(value)
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
def test_sync_coerce_decimal(sync_driver: PsycopgSyncDriver, value: Any, expected_type: type) -> None:
    """Test decimal coercion for Psycopg sync (preserves decimal)."""
    result = sync_driver._coerce_decimal(value)
    assert isinstance(result, expected_type)
    if isinstance(value, Decimal):
        assert result == value


# Sync Execute Statement Tests
@pytest.mark.parametrize(
    "sql_text,is_script,is_many,expected_method",
    [
        ("SELECT * FROM users", False, False, "_execute"),
        ("INSERT INTO users VALUES (%s)", False, True, "_execute_many"),
        ("CREATE TABLE test; INSERT INTO test;", True, False, "_execute_script"),
    ],
    ids=["select", "execute_many", "script"],
)
def test_sync_execute_statement_routing(
    sync_driver: PsycopgSyncDriver,
    mock_sync_connection: MagicMock,
    sql_text: str,
    is_script: bool,
    is_many: bool,
    expected_method: str,
) -> None:
    """Test that sync _execute_statement routes to correct method."""
    statement = SQL(sql_text)
    statement._is_script = is_script
    statement._is_many = is_many

    with patch.object(sync_driver, expected_method, return_value={"rows_affected": 0}) as mock_method:
        sync_driver._execute_statement(statement)
        mock_method.assert_called_once()


def test_sync_execute_select_statement(sync_driver: PsycopgSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync executing a SELECT statement."""
    # Set up cursor with results
    mock_cursor = mock_sync_connection.cursor.return_value
    mock_cursor.description = [("id",), ("name",), ("email",)]
    mock_cursor.fetchall.return_value = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]
    mock_cursor.rowcount = 2

    statement = SQL("SELECT * FROM users")
    result = sync_driver._execute_statement(statement)

    assert result == {
        "data": mock_cursor.fetchall.return_value,
        "column_names": ["id", "name", "email"],
        "rows_affected": 2,
    }

    mock_cursor.execute.assert_called_once_with("SELECT * FROM users", ())


def test_sync_execute_dml_statement(sync_driver: PsycopgSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync executing a DML statement (INSERT/UPDATE/DELETE)."""
    mock_cursor = mock_sync_connection.cursor.return_value
    mock_cursor.rowcount = 1
    mock_cursor.statusmessage = "INSERT 0 1"

    statement = SQL("INSERT INTO users (name, email) VALUES (%s, %s)", ["Alice", "alice@example.com"])
    result = sync_driver._execute_statement(statement)

    assert result == {"rows_affected": 1, "status_message": "INSERT 0 1"}

    mock_cursor.execute.assert_called_once_with(
        "INSERT INTO users (name, email) VALUES (%s, %s)", ("Alice", "alice@example.com")
    )


# Async Execute Statement Tests
@pytest.mark.parametrize(
    "sql_text,is_script,is_many,expected_method",
    [
        ("SELECT * FROM users", False, False, "_execute"),
        ("INSERT INTO users VALUES (%s)", False, True, "_execute_many"),
        ("CREATE TABLE test; INSERT INTO test;", True, False, "_execute_script"),
    ],
    ids=["select", "execute_many", "script"],
)
@pytest.mark.asyncio
async def test_async_execute_statement_routing(
    async_driver: PsycopgAsyncDriver,
    mock_async_connection: AsyncMock,
    sql_text: str,
    is_script: bool,
    is_many: bool,
    expected_method: str,
) -> None:
    """Test that async _execute_statement routes to correct method."""
    statement = SQL(sql_text)
    statement._is_script = is_script
    statement._is_many = is_many

    with patch.object(async_driver, expected_method, return_value={"rows_affected": 0}) as mock_method:
        await async_driver._execute_statement(statement)
        mock_method.assert_called_once()


@pytest.mark.asyncio
async def test_async_execute_select_statement(
    async_driver: PsycopgAsyncDriver, mock_async_connection: AsyncMock
) -> None:
    """Test async executing a SELECT statement."""
    # Set up cursor with results
    mock_cursor = mock_async_connection.cursor.return_value
    mock_cursor.description = [("id",), ("name",), ("email",)]
    mock_cursor.fetchall.return_value = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]
    mock_cursor.rowcount = 2

    statement = SQL("SELECT * FROM users")
    result = await async_driver._execute_statement(statement)

    assert result == {
        "data": mock_cursor.fetchall.return_value,
        "column_names": ["id", "name", "email"],
        "rows_affected": 2,
    }

    mock_cursor.execute.assert_called_once_with("SELECT * FROM users", ())


@pytest.mark.asyncio
async def test_async_execute_dml_statement(async_driver: PsycopgAsyncDriver, mock_async_connection: AsyncMock) -> None:
    """Test async executing a DML statement (INSERT/UPDATE/DELETE)."""
    mock_cursor = mock_async_connection.cursor.return_value
    mock_cursor.rowcount = 1
    mock_cursor.statusmessage = "INSERT 0 1"

    statement = SQL("INSERT INTO users (name, email) VALUES (%s, %s)", ["Alice", "alice@example.com"])
    result = await async_driver._execute_statement(statement)

    assert result == {"rows_affected": 1, "status_message": "INSERT 0 1"}

    mock_cursor.execute.assert_called_once_with(
        "INSERT INTO users (name, email) VALUES (%s, %s)", ("Alice", "alice@example.com")
    )


# Parameter Style Handling Tests
@pytest.mark.parametrize(
    "sql_text,detected_style,expected_style",
    [
        ("SELECT * FROM users WHERE id = %s", ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.POSITIONAL_PYFORMAT),
        ("SELECT * FROM users WHERE id = %(id)s", ParameterStyle.NAMED_PYFORMAT, ParameterStyle.NAMED_PYFORMAT),
        ("SELECT * FROM users WHERE id = $1", ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_PYFORMAT),  # Converted
    ],
    ids=["pyformat_positional", "pyformat_named", "numeric_converted"],
)
def test_sync_parameter_style_handling(
    sync_driver: PsycopgSyncDriver,
    mock_sync_connection: MagicMock,
    sql_text: str,
    detected_style: ParameterStyle,
    expected_style: ParameterStyle,
) -> None:
    """Test sync parameter style detection and conversion."""
    statement = SQL(sql_text)
    statement._parameter_info = [ParameterInfo(name="p1", position=0, style=detected_style)]

    with patch.object(statement, "compile") as mock_compile:
        mock_compile.return_value = (sql_text, None)
        sync_driver._execute_statement(statement)

        mock_compile.assert_called_with(placeholder_style=expected_style)


# Execute Many Tests
def test_sync_execute_many(sync_driver: PsycopgSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync executing a statement multiple times."""
    mock_cursor = mock_sync_connection.cursor.return_value
    mock_cursor.rowcount = 3
    mock_cursor.statusmessage = "INSERT 0 3"

    sql = "INSERT INTO users (name, email) VALUES (%s, %s)"
    params = [["Alice", "alice@example.com"], ["Bob", "bob@example.com"], ["Charlie", "charlie@example.com"]]

    result = sync_driver._execute_many(sql, params)

    assert result == {"rows_affected": 3, "status_message": "INSERT 0 3"}

    expected_params = [("Alice", "alice@example.com"), ("Bob", "bob@example.com"), ("Charlie", "charlie@example.com")]
    mock_cursor.executemany.assert_called_once_with(sql, expected_params)


@pytest.mark.asyncio
async def test_async_execute_many(async_driver: PsycopgAsyncDriver, mock_async_connection: AsyncMock) -> None:
    """Test async executing a statement multiple times."""
    mock_cursor = mock_async_connection.cursor.return_value
    mock_cursor.rowcount = 3
    mock_cursor.statusmessage = "INSERT 0 3"

    sql = "INSERT INTO users (name, email) VALUES (%s, %s)"
    params = [["Alice", "alice@example.com"], ["Bob", "bob@example.com"], ["Charlie", "charlie@example.com"]]

    result = await async_driver._execute_many(sql, params)

    assert result == {"rows_affected": 3, "status_message": "INSERT 0 3"}

    expected_params = [("Alice", "alice@example.com"), ("Bob", "bob@example.com"), ("Charlie", "charlie@example.com")]
    mock_cursor.executemany.assert_called_once_with(sql, expected_params)


# Execute Script Tests
def test_sync_execute_script(sync_driver: PsycopgSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync executing a SQL script."""
    mock_cursor = mock_sync_connection.cursor.return_value
    mock_cursor.statusmessage = "CREATE TABLE"

    script = """
    CREATE TABLE test (id INTEGER PRIMARY KEY);
    INSERT INTO test VALUES (1);
    INSERT INTO test VALUES (2);
    """

    result = sync_driver._execute_script(script)

    assert result == {"statements_executed": -1, "status_message": "CREATE TABLE"}

    mock_cursor.execute.assert_called_once_with(script)


@pytest.mark.asyncio
async def test_async_execute_script(async_driver: PsycopgAsyncDriver, mock_async_connection: AsyncMock) -> None:
    """Test async executing a SQL script."""
    mock_cursor = mock_async_connection.cursor.return_value
    mock_cursor.statusmessage = "CREATE TABLE"

    script = """
    CREATE TABLE test (id INTEGER PRIMARY KEY);
    INSERT INTO test VALUES (1);
    INSERT INTO test VALUES (2);
    """

    result = await async_driver._execute_script(script)

    assert result == {"statements_executed": -1, "status_message": "CREATE TABLE"}

    mock_cursor.execute.assert_called_once_with(script)


# Result Wrapping Tests
def test_sync_wrap_select_result(sync_driver: PsycopgSyncDriver) -> None:
    """Test sync wrapping SELECT results."""
    statement = SQL("SELECT * FROM users")
    result = {
        "data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        "column_names": ["id", "name"],
        "rows_affected": 2,
    }

    wrapped = sync_driver._wrap_select_result(statement, result)

    assert isinstance(wrapped, SQLResult)
    assert wrapped.statement is statement
    assert len(wrapped.data) == 2
    assert wrapped.column_names == ["id", "name"]
    assert wrapped.rows_affected == 2
    assert wrapped.operation_type == "SELECT"


@pytest.mark.asyncio
async def test_async_wrap_select_result(async_driver: PsycopgAsyncDriver) -> None:
    """Test async wrapping SELECT results."""
    statement = SQL("SELECT * FROM users")
    result = {
        "data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        "column_names": ["id", "name"],
        "rows_affected": 2,
    }

    wrapped = await async_driver._wrap_select_result(statement, result)

    assert isinstance(wrapped, SQLResult)
    assert wrapped.statement is statement
    assert len(wrapped.data) == 2
    assert wrapped.column_names == ["id", "name"]
    assert wrapped.rows_affected == 2
    assert wrapped.operation_type == "SELECT"


def test_sync_wrap_execute_result_dml(sync_driver: PsycopgSyncDriver) -> None:
    """Test sync wrapping DML results."""
    statement = SQL("INSERT INTO users VALUES (%s)")
    statement._expression = MagicMock()
    statement._expression.key = "insert"

    result = {"rows_affected": 1, "status_message": "INSERT 0 1"}

    wrapped = sync_driver._wrap_execute_result(statement, result)

    assert isinstance(wrapped, SQLResult)
    assert wrapped.data == []
    assert wrapped.rows_affected == 1
    assert wrapped.operation_type == "INSERT"
    assert wrapped.metadata["status_message"] == "INSERT 0 1"


@pytest.mark.asyncio
async def test_async_wrap_execute_result_dml(async_driver: PsycopgAsyncDriver) -> None:
    """Test async wrapping DML results."""
    statement = SQL("INSERT INTO users VALUES (%s)")
    statement._expression = MagicMock()
    statement._expression.key = "insert"

    result = {"rows_affected": 1, "status_message": "INSERT 0 1"}

    wrapped = await async_driver._wrap_execute_result(statement, result)

    assert isinstance(wrapped, SQLResult)
    assert wrapped.data == []
    assert wrapped.rows_affected == 1
    assert wrapped.operation_type == "INSERT"
    assert wrapped.metadata["status_message"] == "INSERT 0 1"


# Parameter Processing Tests
@pytest.mark.parametrize(
    "params,expected",
    [
        ([1, "test"], (1, "test")),
        ((1, "test"), (1, "test")),
        ({"key": "value"}, ({"key": "value"},)),
        ([], ()),
        (None, ()),
    ],
    ids=["list", "tuple", "dict", "empty_list", "none"],
)
def test_sync_format_parameters(sync_driver: PsycopgSyncDriver, params: Any, expected: tuple[Any, ...]) -> None:
    """Test sync parameter formatting for Psycopg."""
    result = sync_driver._format_parameters(params)
    assert result == expected


@pytest.mark.parametrize(
    "params,expected",
    [
        ([1, "test"], (1, "test")),
        ((1, "test"), (1, "test")),
        ({"key": "value"}, ({"key": "value"},)),
        ([], ()),
        (None, ()),
    ],
    ids=["list", "tuple", "dict", "empty_list", "none"],
)
def test_async_format_parameters(async_driver: PsycopgAsyncDriver, params: Any, expected: tuple[Any, ...]) -> None:
    """Test async parameter formatting for Psycopg."""
    result = async_driver._format_parameters(params)
    assert result == expected


# Connection Tests
def test_sync_connection_method(sync_driver: PsycopgSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync _connection method."""
    # Test default connection return
    assert sync_driver._connection() is mock_sync_connection

    # Test connection override
    override_connection = MagicMock()
    assert sync_driver._connection(override_connection) is override_connection


def test_async_connection_method(async_driver: PsycopgAsyncDriver, mock_async_connection: AsyncMock) -> None:
    """Test async _connection method."""
    # Test default connection return
    assert async_driver._connection() is mock_async_connection

    # Test connection override
    override_connection = AsyncMock()
    assert async_driver._connection(override_connection) is override_connection


# Storage Mixin Tests
def test_sync_storage_methods_available(sync_driver: PsycopgSyncDriver) -> None:
    """Test that sync driver has all storage methods from SyncStorageMixin."""
    storage_methods = [
        "fetch_arrow_table",
        "ingest_arrow_table",
        "export_to_storage",
        "import_from_storage",
        "read_parquet_direct",
        "write_parquet_direct",
    ]

    for method in storage_methods:
        assert hasattr(sync_driver, method)
        assert callable(getattr(sync_driver, method))


def test_async_storage_methods_available(async_driver: PsycopgAsyncDriver) -> None:
    """Test that async driver has all storage methods from AsyncStorageMixin."""
    storage_methods = [
        "fetch_arrow_table",
        "ingest_arrow_table",
        "export_to_storage",
        "import_from_storage",
        "read_parquet_direct",
        "write_parquet_direct",
    ]

    for method in storage_methods:
        assert hasattr(async_driver, method)
        assert callable(getattr(async_driver, method))


def test_sync_translator_mixin_integration(sync_driver: PsycopgSyncDriver) -> None:
    """Test sync SQLTranslatorMixin integration."""
    assert hasattr(sync_driver, "returns_rows")

    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert sync_driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert sync_driver.returns_rows(insert_stmt.expression) is False


def test_async_translator_mixin_integration(async_driver: PsycopgAsyncDriver) -> None:
    """Test async SQLTranslatorMixin integration."""
    assert hasattr(async_driver, "returns_rows")

    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert async_driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert async_driver.returns_rows(insert_stmt.expression) is False


# Status String Parsing Tests
@pytest.mark.parametrize(
    "status_string,expected_rows",
    [
        ("INSERT 0 5", 5),
        ("UPDATE 3", 3),
        ("DELETE 2", 2),
        ("CREATE TABLE", 0),
        ("DROP TABLE", 0),
        ("SELECT 1", 0),  # Non-modifying
    ],
    ids=["insert", "update", "delete", "create", "drop", "select"],
)
def test_sync_parse_status_string(sync_driver: PsycopgSyncDriver, status_string: str, expected_rows: int) -> None:
    """Test sync parsing of Psycopg status strings."""
    result = sync_driver._parse_status_string(status_string)
    assert result == expected_rows


@pytest.mark.parametrize(
    "status_string,expected_rows",
    [
        ("INSERT 0 5", 5),
        ("UPDATE 3", 3),
        ("DELETE 2", 2),
        ("CREATE TABLE", 0),
        ("DROP TABLE", 0),
        ("SELECT 1", 0),  # Non-modifying
    ],
    ids=["insert", "update", "delete", "create", "drop", "select"],
)
def test_async_parse_status_string(async_driver: PsycopgAsyncDriver, status_string: str, expected_rows: int) -> None:
    """Test async parsing of Psycopg status strings."""
    result = async_driver._parse_status_string(status_string)
    assert result == expected_rows


# Error Handling Tests
def test_sync_execute_with_connection_error(sync_driver: PsycopgSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync handling connection errors during execution."""
    import psycopg

    mock_cursor = mock_sync_connection.cursor.return_value
    mock_cursor.execute.side_effect = psycopg.OperationalError("connection error")

    statement = SQL("SELECT * FROM users")

    with pytest.raises(psycopg.OperationalError, match="connection error"):
        sync_driver._execute_statement(statement)


@pytest.mark.asyncio
async def test_async_execute_with_connection_error(
    async_driver: PsycopgAsyncDriver, mock_async_connection: AsyncMock
) -> None:
    """Test async handling connection errors during execution."""
    import psycopg

    mock_cursor = mock_async_connection.cursor.return_value
    mock_cursor.execute.side_effect = psycopg.OperationalError("connection error")

    statement = SQL("SELECT * FROM users")

    with pytest.raises(psycopg.OperationalError, match="connection error"):
        await async_driver._execute_statement(statement)


# Edge Cases
def test_sync_execute_with_no_parameters(sync_driver: PsycopgSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync executing statement with no parameters."""
    mock_cursor = mock_sync_connection.cursor.return_value
    mock_cursor.statusmessage = "CREATE TABLE"

    statement = SQL("CREATE TABLE test (id INTEGER)")
    sync_driver._execute_statement(statement)

    mock_cursor.execute.assert_called_once_with("CREATE TABLE test (id INTEGER)", ())


@pytest.mark.asyncio
async def test_async_execute_with_no_parameters(
    async_driver: PsycopgAsyncDriver, mock_async_connection: AsyncMock
) -> None:
    """Test async executing statement with no parameters."""
    mock_cursor = mock_async_connection.cursor.return_value
    mock_cursor.statusmessage = "CREATE TABLE"

    statement = SQL("CREATE TABLE test (id INTEGER)")
    await async_driver._execute_statement(statement)

    mock_cursor.execute.assert_called_once_with("CREATE TABLE test (id INTEGER)", ())


def test_sync_execute_select_with_empty_result(sync_driver: PsycopgSyncDriver, mock_sync_connection: MagicMock) -> None:
    """Test sync SELECT with empty result set."""
    mock_cursor = mock_sync_connection.cursor.return_value
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = []
    mock_cursor.rowcount = 0

    statement = SQL("SELECT * FROM users WHERE 1=0")
    result = sync_driver._execute_statement(statement)

    assert result == {"data": [], "column_names": ["id", "name"], "rows_affected": 0}


@pytest.mark.asyncio
async def test_async_execute_select_with_empty_result(
    async_driver: PsycopgAsyncDriver, mock_async_connection: AsyncMock
) -> None:
    """Test async SELECT with empty result set."""
    mock_cursor = mock_async_connection.cursor.return_value
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = []
    mock_cursor.rowcount = 0

    statement = SQL("SELECT * FROM users WHERE 1=0")
    result = await async_driver._execute_statement(statement)

    assert result == {"data": [], "column_names": [], "rows_affected": 0}
