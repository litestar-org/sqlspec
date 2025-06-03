"""Unit tests for Psycopg drivers."""

from unittest.mock import AsyncMock, Mock

import pytest

from sqlspec.adapters.psycopg import (
    PsycopgAsyncConnection,
    PsycopgAsyncDriver,
    PsycopgSyncConnection,
    PsycopgSyncDriver,
)
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_psycopg_sync_connection() -> Mock:
    """Create a mock Psycopg sync connection."""
    mock_connection = Mock(spec=PsycopgSyncConnection)
    mock_cursor = Mock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connection.cursor.return_value.__exit__.return_value = None
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = []
    mock_cursor.rowcount = 0
    mock_cursor.statusmessage = "EXECUTE"
    return mock_connection


@pytest.fixture
def mock_psycopg_async_connection() -> AsyncMock:
    """Create a mock Psycopg async connection."""
    mock_connection = AsyncMock(spec=PsycopgAsyncConnection)
    mock_cursor = AsyncMock()
    mock_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_connection.cursor.return_value.__aexit__.return_value = None
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = []
    mock_cursor.rowcount = 0
    mock_cursor.statusmessage = "EXECUTE"
    return mock_connection


@pytest.fixture
def psycopg_sync_driver(mock_psycopg_sync_connection: PsycopgSyncConnection) -> PsycopgSyncDriver:
    """Create a Psycopg sync driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return PsycopgSyncDriver(
        connection=mock_psycopg_sync_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


@pytest.fixture
def psycopg_async_driver(mock_psycopg_async_connection: AsyncMock) -> PsycopgAsyncDriver:
    """Create a Psycopg async driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return PsycopgAsyncDriver(
        connection=mock_psycopg_async_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


def test_psycopg_sync_driver_initialization(mock_psycopg_sync_connection: PsycopgSyncConnection) -> None:
    """Test Psycopg sync driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = PsycopgSyncDriver(
        connection=mock_psycopg_sync_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_psycopg_sync_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "postgres"
    assert driver.__supports_arrow__ is False


def test_psycopg_async_driver_initialization(mock_psycopg_async_connection: AsyncMock) -> None:
    """Test Psycopg async driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = PsycopgAsyncDriver(
        connection=mock_psycopg_async_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_psycopg_async_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "postgres"
    assert driver.__supports_arrow__ is False


def test_psycopg_sync_driver_dialect_property(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver dialect property."""
    assert psycopg_sync_driver.dialect == "postgres"


def test_psycopg_async_driver_dialect_property(psycopg_async_driver: PsycopgAsyncDriver) -> None:
    """Test Psycopg async driver dialect property."""
    assert psycopg_async_driver.dialect == "postgres"


def test_psycopg_sync_driver_supports_arrow(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver Arrow support."""
    assert psycopg_sync_driver.__supports_arrow__ is False
    assert PsycopgSyncDriver.__supports_arrow__ is False


def test_psycopg_async_driver_supports_arrow(psycopg_async_driver: PsycopgAsyncDriver) -> None:
    """Test Psycopg async driver Arrow support."""
    assert psycopg_async_driver.__supports_arrow__ is False
    assert PsycopgAsyncDriver.__supports_arrow__ is False


def test_psycopg_sync_driver_placeholder_style(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver placeholder style detection."""
    placeholder_style = psycopg_sync_driver._get_placeholder_style()
    assert placeholder_style.value == "pyformat_named"


def test_psycopg_async_driver_placeholder_style(psycopg_async_driver: PsycopgAsyncDriver) -> None:
    """Test Psycopg async driver placeholder style detection."""
    placeholder_style = psycopg_async_driver._get_placeholder_style()
    assert placeholder_style.value == "pyformat_named"


def test_psycopg_sync_driver_get_cursor(
    psycopg_sync_driver: PsycopgSyncDriver, mock_psycopg_sync_connection: PsycopgSyncConnection
) -> None:
    """Test Psycopg sync driver _get_cursor context manager."""
    mock_cursor = Mock()
    mock_psycopg_sync_connection.cursor.return_value.__enter__.return_value = mock_cursor  # pyright: ignore
    mock_psycopg_sync_connection.cursor.return_value.__exit__.return_value = None  # pyright: ignore

    with psycopg_sync_driver._get_cursor(mock_psycopg_sync_connection) as cursor:
        assert cursor is mock_cursor

    # Verify cursor context manager was used
    mock_psycopg_sync_connection.cursor.assert_called_once()  # pyright: ignore


@pytest.mark.asyncio
async def test_psycopg_async_driver_get_cursor(
    psycopg_async_driver: PsycopgAsyncDriver, mock_psycopg_async_connection: AsyncMock
) -> None:
    """Test Psycopg async driver _get_cursor context manager."""
    mock_cursor = AsyncMock()
    mock_psycopg_async_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_psycopg_async_connection.cursor.return_value.__aexit__.return_value = None

    async with psycopg_async_driver._get_cursor(mock_psycopg_async_connection) as cursor:
        assert cursor is mock_cursor

    # Verify cursor context manager was used
    mock_psycopg_async_connection.cursor.assert_called_once()


def test_psycopg_sync_driver_execute_impl_select(
    psycopg_sync_driver: PsycopgSyncDriver, mock_psycopg_sync_connection: PsycopgSyncConnection
) -> None:
    """Test Psycopg sync driver _execute_impl for SELECT statements."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [{"id": 1, "name": "test"}]
    mock_cursor.description = [Mock(name="id"), Mock(name="name")]
    mock_psycopg_sync_connection.cursor.return_value.__enter__.return_value = mock_cursor  # pyright: ignore
    mock_psycopg_sync_connection.cursor.return_value.__exit__.return_value = None  # pyright: ignore

    # Create SQL statement with parameters
    statement = SQL(
        "SELECT * FROM users WHERE id = %(user_id)s", parameters={"user_id": 1}, config=psycopg_sync_driver.config
    )

    # Execute
    result = psycopg_sync_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify cursor was created and execute was called
    mock_psycopg_sync_connection.cursor.assert_called_once()  # pyright: ignore
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = %(user_id)s", {"user_id": 1})
    assert result is mock_cursor


@pytest.mark.asyncio
async def test_psycopg_async_driver_execute_impl_select(
    psycopg_async_driver: PsycopgAsyncDriver, mock_psycopg_async_connection: AsyncMock
) -> None:
    """Test Psycopg async driver _execute_impl for SELECT statements."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = [{"id": 1, "name": "test"}]
    mock_cursor.description = [Mock(name="id"), Mock(name="name")]
    mock_psycopg_async_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_psycopg_async_connection.cursor.return_value.__aexit__.return_value = None

    # Create SQL statement with parameters
    statement = SQL(
        "SELECT * FROM users WHERE id = %(user_id)s", parameters={"user_id": 1}, config=psycopg_async_driver.config
    )

    # Execute
    result = await psycopg_async_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify cursor was created and execute was called
    mock_psycopg_async_connection.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = %(user_id)s", {"user_id": 1})
    assert result is mock_cursor


def test_psycopg_sync_driver_execute_impl_insert(
    psycopg_sync_driver: PsycopgSyncDriver, mock_psycopg_sync_connection: PsycopgSyncConnection
) -> None:
    """Test Psycopg sync driver _execute_impl for INSERT statements."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_cursor.rowcount = 1
    mock_psycopg_sync_connection.cursor.return_value.__enter__.return_value = mock_cursor  # pyright: ignore
    mock_psycopg_sync_connection.cursor.return_value.__exit__.return_value = None  # pyright: ignore

    # Create SQL statement with parameters
    statement = SQL(
        "INSERT INTO users (name) VALUES (%(name)s)", parameters={"name": "John"}, config=psycopg_sync_driver.config
    )

    # Execute
    result = psycopg_sync_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify cursor was created and execute was called
    mock_psycopg_sync_connection.cursor.assert_called_once()  # pyright: ignore
    mock_cursor.execute.assert_called_once_with("INSERT INTO users (name) VALUES (%(name)s)", {"name": "John"})
    assert result is mock_cursor


def test_psycopg_sync_driver_execute_impl_script(
    psycopg_sync_driver: PsycopgSyncDriver, mock_psycopg_sync_connection: PsycopgSyncConnection
) -> None:
    """Test Psycopg sync driver _execute_impl for script execution."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_cursor.statusmessage = "CREATE TABLE"
    mock_psycopg_sync_connection.cursor.return_value.__enter__.return_value = mock_cursor  # pyright: ignore
    mock_psycopg_sync_connection.cursor.return_value.__exit__.return_value = None  # pyright: ignore

    # Create SQL statement (DDL allowed with strict_mode=False)
    statement = SQL(
        "CREATE TABLE test (id INTEGER); INSERT INTO test VALUES (1);", config=psycopg_sync_driver.config
    ).as_script()

    # Execute script
    result = psycopg_sync_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify cursor was created and execute was called
    mock_psycopg_sync_connection.cursor.assert_called_once()  # pyright: ignore
    mock_cursor.execute.assert_called_once()
    assert result == "CREATE TABLE"


def test_psycopg_sync_driver_execute_impl_many(
    psycopg_sync_driver: PsycopgSyncDriver, mock_psycopg_sync_connection: PsycopgSyncConnection
) -> None:
    """Test Psycopg sync driver _execute_impl for execute_many."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_cursor.rowcount = 3
    mock_psycopg_sync_connection.cursor.return_value.__enter__.return_value = mock_cursor  # pyright: ignore
    mock_psycopg_sync_connection.cursor.return_value.__exit__.return_value = None  # pyright: ignore

    # Create SQL statement with placeholder for parameters
    parameters_list = [{"name": "John"}, {"name": "Jane"}, {"name": "Bob"}]
    statement = SQL(
        "INSERT INTO users (name) VALUES (%(name)s)", parameters=parameters_list, config=psycopg_sync_driver.config
    ).as_many()

    # Execute many
    result = psycopg_sync_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify cursor was created and executemany was called
    mock_psycopg_sync_connection.cursor.assert_called_once()  # pyright: ignore
    mock_cursor.executemany.assert_called_once_with(
        "INSERT INTO users (name) VALUES (%(name)s)", [{"name": "John"}, {"name": "Jane"}, {"name": "Bob"}]
    )
    assert result is mock_cursor


def test_psycopg_sync_driver_wrap_select_result(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver _wrap_select_result method."""
    # Create mock cursor with data
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [
        {"id": 1, "name": "John"},
        {"id": 2, "name": "Jane"},
    ]
    mock_cursor.description = [Mock(name="id"), Mock(name="name")]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result
    result = psycopg_sync_driver._wrap_select_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["id", "name"]
    assert result.data == [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]


def test_psycopg_sync_driver_wrap_select_result_empty(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver _wrap_select_result method with empty result."""
    # Create mock cursor with no data
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = []

    # Create SQL statement
    statement = SQL("SELECT * FROM empty_table")

    # Wrap result
    result = psycopg_sync_driver._wrap_select_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.data == []
    assert result.column_names == []


def test_psycopg_sync_driver_wrap_execute_result(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver _wrap_execute_result method."""
    # Create mock cursor
    mock_cursor = Mock()
    mock_cursor.rowcount = 3

    # Create SQL statement
    statement = SQL("UPDATE users SET active = 1", config=psycopg_sync_driver.config)

    # Wrap result
    result = psycopg_sync_driver._wrap_execute_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"


def test_psycopg_sync_driver_wrap_execute_result_script(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver _wrap_execute_result method for script."""
    # Create SQL statement (DDL allowed with strict_mode=False)
    statement = SQL("CREATE TABLE test (id INTEGER)", config=psycopg_sync_driver.config)

    # Wrap result for script
    result = psycopg_sync_driver._wrap_execute_result(
        statement=statement,
        result="CREATE TABLE",
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 0
    assert result.operation_type == "CREATE"


def test_psycopg_sync_driver_connection_method(
    psycopg_sync_driver: PsycopgSyncDriver, mock_psycopg_sync_connection: PsycopgSyncConnection
) -> None:
    """Test Psycopg sync driver _connection method."""
    # Test default connection return
    assert psycopg_sync_driver._connection() is mock_psycopg_sync_connection

    # Test connection override
    override_connection = Mock()
    assert psycopg_sync_driver._connection(override_connection) is override_connection


@pytest.mark.asyncio
async def test_psycopg_async_driver_connection_method(
    psycopg_async_driver: PsycopgAsyncDriver, mock_psycopg_async_connection: AsyncMock
) -> None:
    """Test Psycopg async driver _connection method."""
    # Test default connection return
    assert psycopg_async_driver._connection() is mock_psycopg_async_connection

    # Test connection override
    override_connection = AsyncMock()
    assert psycopg_async_driver._connection(override_connection) is override_connection


def test_psycopg_sync_driver_error_handling(
    psycopg_sync_driver: PsycopgSyncDriver, mock_psycopg_sync_connection: PsycopgSyncConnection
) -> None:
    """Test Psycopg sync driver error handling."""
    # Setup mock to raise exception
    mock_psycopg_sync_connection.cursor.side_effect = Exception("Database error")  # pyright: ignore

    # Create SQL statement
    statement = SQL("SELECT * FROM users")

    # Test error propagation
    with pytest.raises(Exception, match="Database error"):
        psycopg_sync_driver._execute_statement(
            statement=statement,
            connection=None,
        )


@pytest.mark.asyncio
async def test_psycopg_async_driver_error_handling(
    psycopg_async_driver: PsycopgAsyncDriver, mock_psycopg_async_connection: AsyncMock
) -> None:
    """Test Psycopg async driver error handling."""
    # Setup mock to raise exception
    mock_psycopg_async_connection.cursor.side_effect = Exception("Database error")

    # Create SQL statement
    statement = SQL("SELECT * FROM users")

    # Test error propagation
    with pytest.raises(Exception, match="Database error"):
        await psycopg_async_driver._execute_statement(
            statement=statement,
            connection=None,
        )


def test_psycopg_sync_driver_instrumentation(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver instrumentation integration."""
    # Test instrumentation config is accessible
    assert psycopg_sync_driver.instrumentation_config is not None
    assert isinstance(psycopg_sync_driver.instrumentation_config, InstrumentationConfig)

    # Test logging configuration
    assert hasattr(psycopg_sync_driver.instrumentation_config, "log_queries")
    assert hasattr(psycopg_sync_driver.instrumentation_config, "log_parameters")
    assert hasattr(psycopg_sync_driver.instrumentation_config, "log_results_count")

    # Setup mock cursor and statement for the test
    mock_cursor = Mock()
    # Mock the connection's cursor() method to return a context manager
    mock_cursor_cm = Mock()
    mock_cursor_cm.__enter__.return_value = mock_cursor
    mock_cursor_cm.__exit__.return_value = None
    psycopg_sync_driver.connection.cursor = Mock(return_value=mock_cursor_cm)

    statement = SQL(
        "SELECT * FROM users WHERE id = %(user_id)s", parameters={"user_id": 1}, config=psycopg_sync_driver.config
    )

    # Execute with logging enabled
    psycopg_sync_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify execution worked
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = %(user_id)s", {"user_id": 1})


@pytest.mark.asyncio
async def test_psycopg_async_driver_instrumentation(psycopg_async_driver: PsycopgAsyncDriver) -> None:
    """Test Psycopg async driver instrumentation integration."""
    # Test instrumentation config is accessible
    assert psycopg_async_driver.instrumentation_config is not None
    assert isinstance(psycopg_async_driver.instrumentation_config, InstrumentationConfig)

    # Test logging configuration
    assert hasattr(psycopg_async_driver.instrumentation_config, "log_queries")
    assert hasattr(psycopg_async_driver.instrumentation_config, "log_parameters")
    assert hasattr(psycopg_async_driver.instrumentation_config, "log_results_count")

    # Setup mock cursor and statement for the test
    mock_cursor = AsyncMock()
    # Mock the async connection's cursor() method to return an async context manager
    mock_async_cursor_cm = AsyncMock()
    mock_async_cursor_cm.__aenter__.return_value = mock_cursor
    mock_async_cursor_cm.__aexit__.return_value = None
    psycopg_async_driver.connection.cursor = AsyncMock(return_value=mock_async_cursor_cm)

    statement = SQL(
        "SELECT * FROM users WHERE id = %(user_id)s", parameters={"user_id": 1}, config=psycopg_async_driver.config
    )

    # Execute with logging enabled
    await psycopg_async_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify execution worked
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = %(user_id)s", {"user_id": 1})


def test_psycopg_sync_driver_operation_type_detection(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver operation type detection."""
    # Test different SQL statement types (DDL allowed with strict_mode=False)
    test_cases = [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("UPDATE users SET name = 'John'", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("CREATE TABLE test (id INTEGER)", "CREATE"),
    ]

    for sql, expected_op_type in test_cases:
        statement = SQL(sql, config=psycopg_sync_driver.config)

        # Mock cursor for execute result
        mock_cursor = Mock()
        mock_cursor.rowcount = 1

        result = psycopg_sync_driver._wrap_execute_result(
            statement=statement,
            result=mock_cursor,
        )

        assert result.operation_type == expected_op_type


def test_psycopg_sync_driver_logging_configuration(
    psycopg_sync_driver: PsycopgSyncDriver, mock_psycopg_sync_connection: PsycopgSyncConnection
) -> None:
    """Test Psycopg sync driver logging configuration."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_psycopg_sync_connection.cursor.return_value.__enter__.return_value = mock_cursor  # pyright: ignore
    mock_psycopg_sync_connection.cursor.return_value.__exit__.return_value = None  # pyright: ignore

    # Enable logging
    psycopg_sync_driver.instrumentation_config.log_queries = True
    psycopg_sync_driver.instrumentation_config.log_parameters = True
    psycopg_sync_driver.instrumentation_config.log_results_count = True

    # Create SQL statement with parameters
    statement = SQL(
        "SELECT * FROM users WHERE id = %(user_id)s", parameters={"user_id": 1}, config=psycopg_sync_driver.config
    )

    # Execute with logging enabled
    psycopg_sync_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify execution worked
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = %(user_id)s", {"user_id": 1})


def test_psycopg_sync_driver_mixins_integration(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver mixin integration."""
    # Test that driver has all expected mixins
    from sqlspec.statement.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowMixin

    assert isinstance(psycopg_sync_driver, SQLTranslatorMixin)
    assert isinstance(psycopg_sync_driver, SyncArrowMixin)
    assert isinstance(psycopg_sync_driver, ResultConverter)

    # Test mixin methods are available
    assert hasattr(psycopg_sync_driver, "to_schema")
    assert hasattr(psycopg_sync_driver, "returns_rows")


def test_psycopg_async_driver_mixins_integration(psycopg_async_driver: PsycopgAsyncDriver) -> None:
    """Test Psycopg async driver mixin integration."""
    # Test that driver has all expected mixins
    from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin

    assert isinstance(psycopg_async_driver, SQLTranslatorMixin)
    assert isinstance(psycopg_async_driver, AsyncArrowMixin)
    assert isinstance(psycopg_async_driver, ResultConverter)

    # Test mixin methods are available
    assert hasattr(psycopg_async_driver, "to_schema")
    assert hasattr(psycopg_async_driver, "returns_rows")


def test_psycopg_sync_driver_returns_rows_method(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver returns_rows method."""
    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert psycopg_sync_driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert psycopg_sync_driver.returns_rows(insert_stmt.expression) is False


def test_psycopg_async_driver_returns_rows_method(psycopg_async_driver: PsycopgAsyncDriver) -> None:
    """Test Psycopg async driver returns_rows method."""
    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert psycopg_async_driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert psycopg_async_driver.returns_rows(insert_stmt.expression) is False


def test_psycopg_sync_driver_named_parameters(
    psycopg_sync_driver: PsycopgSyncDriver, mock_psycopg_sync_connection: PsycopgSyncConnection
) -> None:
    """Test Psycopg sync driver named parameter handling."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_psycopg_sync_connection.cursor.return_value.__enter__.return_value = mock_cursor  # pyright: ignore
    mock_psycopg_sync_connection.cursor.return_value.__exit__.return_value = None  # pyright: ignore

    # Create SQL statement with named parameters
    statement = SQL(
        "SELECT * FROM users WHERE name = %(name)s AND age = %(age)s",
        parameters={"name": "John", "age": 30},
        config=psycopg_sync_driver.config,
    )

    # Execute
    psycopg_sync_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify named parameters were processed correctly
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM users WHERE name = %(name)s AND age = %(age)s", {"name": "John", "age": 30}
    )


@pytest.mark.asyncio
async def test_psycopg_async_driver_named_parameters(
    psycopg_async_driver: PsycopgAsyncDriver, mock_psycopg_async_connection: AsyncMock
) -> None:
    """Test Psycopg async driver named parameter handling."""
    # Setup mock cursor
    mock_cursor = AsyncMock()
    mock_psycopg_async_connection.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_psycopg_async_connection.cursor.return_value.__aexit__.return_value = None

    # Create SQL statement with named parameters
    statement = SQL(
        "SELECT * FROM users WHERE name = %(name)s AND age = %(age)s",
        parameters={"name": "John", "age": 30},
        config=psycopg_async_driver.config,
    )

    # Execute
    await psycopg_async_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify named parameters were processed correctly
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM users WHERE name = %(name)s AND age = %(age)s", {"name": "John", "age": 30}
    )


def test_psycopg_sync_driver_dict_row_handling(psycopg_sync_driver: PsycopgSyncDriver) -> None:
    """Test Psycopg sync driver DictRow handling."""
    # Create mock cursor with DictRow data
    mock_cursor = Mock()
    mock_row1 = {"id": 1, "name": "John"}
    mock_row2 = {"id": 2, "name": "Jane"}
    mock_cursor.fetchall.return_value = [mock_row1, mock_row2]
    mock_cursor.description = [Mock(name="id"), Mock(name="name")]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result
    result = psycopg_sync_driver._wrap_select_result(
        statement=statement,
        result=mock_cursor,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["id", "name"]
    # Data should be converted to dict format
    assert len(result.data) == 2
    assert all(isinstance(row, dict) for row in result.data)
    assert result.data[0]["id"] == 1
    assert result.data[0]["name"] == "John"
    assert result.data[1]["id"] == 2
    assert result.data[1]["name"] == "Jane"
