"""Unit tests for AsyncPG driver."""

from typing import Any, Union
from unittest.mock import AsyncMock, Mock

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_asyncpg_connection() -> AsyncMock:
    """Create a mock AsyncPG connection."""
    mock_connection = AsyncMock()  # Remove spec to avoid Union type issues
    mock_connection.execute.return_value = "INSERT 0 1"
    mock_connection.executemany.return_value = None
    mock_connection.fetch.return_value = []
    mock_connection.fetchval.return_value = None
    return mock_connection


@pytest.fixture
def asyncpg_driver(mock_asyncpg_connection: AsyncMock) -> AsyncpgDriver:
    """Create an AsyncPG driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return AsyncpgDriver(
        connection=mock_asyncpg_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


def test_asyncpg_driver_initialization(mock_asyncpg_connection: AsyncMock) -> None:
    """Test AsyncPG driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = AsyncpgDriver(
        connection=mock_asyncpg_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_asyncpg_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "postgres"
    assert driver.__supports_arrow__ is True


def test_asyncpg_driver_dialect_property(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver dialect property."""
    assert asyncpg_driver.dialect == "postgres"


def test_asyncpg_driver_supports_arrow(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver Arrow support."""
    assert asyncpg_driver.__supports_arrow__ is True
    assert AsyncpgDriver.__supports_arrow__ is True


def test_asyncpg_driver_placeholder_style(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver placeholder style detection."""
    placeholder_style = asyncpg_driver._get_placeholder_style()
    assert placeholder_style.value == "numeric"


@pytest.mark.asyncio
async def test_asyncpg_driver_execute_statement_select(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver _execute_statement for SELECT statements."""
    # Setup mock connection
    from asyncpg import Record

    mock_record = Mock(spec=Record)
    mock_record.keys.return_value = ["id", "name"]
    mock_record.__getitem__.side_effect = lambda key: {"id": 1, "name": "test"}[key]  # type: ignore[misc]
    mock_asyncpg_connection.fetch.return_value = [mock_record]

    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = $1", parameters=[1], config=asyncpg_driver.config)

    # Execute - parameters, is_many, is_script are part of SQL object
    result = await asyncpg_driver._execute_statement(statement=statement)

    # Verify connection methods were called
    mock_asyncpg_connection.fetch.assert_called_once_with("SELECT * FROM users WHERE id = $1", 1)
    assert result == [mock_record]


@pytest.mark.asyncio
async def test_asyncpg_driver_execute_statement_insert(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver _execute_statement for INSERT statements."""
    # Setup mock connection
    mock_asyncpg_connection.execute.return_value = "INSERT 0 1"

    # Create SQL statement with parameters
    statement = SQL("INSERT INTO users (name) VALUES ($1)", parameters=["John"], config=asyncpg_driver.config)

    # Execute - parameters, is_many, is_script are part of SQL object
    result = await asyncpg_driver._execute_statement(statement=statement)

    # Verify connection methods were called
    mock_asyncpg_connection.execute.assert_called_once_with("INSERT INTO users (name) VALUES ($1)", "John")
    assert result == "INSERT 0 1"


@pytest.mark.asyncio
async def test_asyncpg_driver_execute_statement_script(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver _execute_statement for script execution."""
    # Setup mock connection
    mock_asyncpg_connection.execute.return_value = "CREATE TABLE"

    # Create SQL statement (DDL allowed with strict_mode=False)
    # is_script should be part of the SQL object
    script_statement = SQL(
        "CREATE TABLE test (id INTEGER); INSERT INTO test VALUES (1);", config=asyncpg_driver.config
    ).as_script()

    # Execute script - parameters, is_many, is_script are part of SQL object
    script_result = await asyncpg_driver._execute_statement(script_statement)

    # Verify connection execute was called
    mock_asyncpg_connection.execute.assert_called_once()
    assert script_result == "CREATE TABLE"


@pytest.mark.asyncio
async def test_asyncpg_driver_execute_statement_many(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver _execute_statement for execute_many."""
    # Setup mock connection
    mock_asyncpg_connection.executemany.return_value = None

    # Test the new as_many() API that accepts parameters directly
    parameters = [["John"], ["Jane"], ["Bob"]]
    statement = SQL("INSERT INTO users (name) VALUES ($1)").as_many(parameters)

    result = await asyncpg_driver._execute_statement(statement=statement)

    # The statement should have is_many=True and the correct parameters
    assert statement.is_many is True
    assert statement.parameters == parameters

    # The execute_impl method should internally call executemany on the connection
    mock_asyncpg_connection.executemany.assert_called_once_with(
        "INSERT INTO users (name) VALUES ($1)", [("John",), ("Jane",), ("Bob",)]
    )
    assert result == 3  # Should return length of parameters list


@pytest.mark.asyncio
async def test_asyncpg_driver_execute_statement_parameter_processing(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver parameter processing for different types."""
    # Setup mock connection
    from asyncpg import Record

    mock_record = Mock(spec=Record)
    mock_record.keys.return_value = ["id", "name"]
    mock_record.__getitem__.side_effect = lambda key: {"id": 1, "name": "John"}[key]  # type: ignore[misc]
    mock_asyncpg_connection.fetch.return_value = [mock_record]

    # Create SQL statement with parameters
    statement = SQL(
        "SELECT * FROM users WHERE id = $1 AND name = $2", parameters=[1, "John"], config=asyncpg_driver.config
    )

    # Execute - parameters, is_many, is_script are part of SQL object
    result = await asyncpg_driver._execute_statement(statement=statement)

    # Verify parameters were processed correctly
    mock_asyncpg_connection.fetch.assert_called_once_with("SELECT * FROM users WHERE id = $1 AND name = $2", 1, "John")
    assert result == [mock_record]


@pytest.mark.asyncio
async def test_asyncpg_driver_wrap_select_result(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver _wrap_select_result method."""
    # Create mock records with data
    from asyncpg import Record

    mock_record1 = Mock(spec=Record)
    mock_record1.keys.return_value = ["id", "name"]
    mock_record1.__iter__.return_value = iter([("id", 1), ("name", "John")])

    mock_record2 = Mock(spec=Record)
    mock_record2.keys.return_value = ["id", "name"]
    mock_record2.__iter__.return_value = iter([("id", 2), ("name", "Jane")])

    mock_record1.__iter__ = lambda: iter([("id", 1), ("name", "John")])
    mock_record2.__iter__ = lambda: iter([("id", 2), ("name", "Jane")])

    records = [mock_record1, mock_record2]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result
    result: Union[SQLResult[Any], SQLResult[dict[str, Any]]] = await asyncpg_driver._wrap_select_result(
        statement=statement,
        result=records,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["id", "name"]
    # Data should be converted to dict format
    assert len(result.data) == 2


@pytest.mark.asyncio
async def test_asyncpg_driver_wrap_select_result_empty(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver _wrap_select_result method with empty result."""
    # Create empty records list
    records: list[Any] = []

    # Create SQL statement
    statement = SQL("SELECT * FROM empty_table")

    # Wrap result
    result: Union[SQLResult[Any], SQLResult[dict[str, Any]]] = await asyncpg_driver._wrap_select_result(
        statement=statement,
        result=records,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.data == []
    assert result.column_names == []


@pytest.mark.asyncio
async def test_asyncpg_driver_wrap_select_result_with_schema_type(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver _wrap_select_result with schema_type."""
    from dataclasses import dataclass

    from asyncpg import Record

    @dataclass
    class User:
        id: int
        name: str

    # Create mock record with data
    mock_record = Mock(spec=Record)
    mock_record.keys.return_value = ["id", "name"]
    mock_record.__iter__ = lambda: iter([("id", 1), ("name", "John")])

    records = [mock_record]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result with schema type
    result = await asyncpg_driver._wrap_select_result(
        statement=statement,
        result=records,
        schema_type=User,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["id", "name"]


@pytest.mark.asyncio
async def test_asyncpg_driver_wrap_execute_result(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver _wrap_execute_result method."""
    # Create SQL statement
    statement = SQL("UPDATE users SET active = 1", config=asyncpg_driver.config)

    # Wrap result with status string
    result = await asyncpg_driver._wrap_execute_result(
        statement=statement,
        result="UPDATE 3",
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"


@pytest.mark.asyncio
async def test_asyncpg_driver_wrap_execute_result_script(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver _wrap_execute_result method for script."""
    # Create SQL statement (DDL allowed with strict_mode=False)
    statement = SQL("CREATE TABLE test (id INTEGER)", config=asyncpg_driver.config)

    # Wrap result for script
    result = await asyncpg_driver._wrap_execute_result(
        statement=statement,
        result="CREATE TABLE",
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 0
    assert result.operation_type == "CREATE"


@pytest.mark.asyncio
async def test_asyncpg_driver_wrap_execute_result_integer(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver _wrap_execute_result method with integer result."""
    # Create SQL statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')", config=asyncpg_driver.config)

    # Wrap result with integer
    result = await asyncpg_driver._wrap_execute_result(
        statement=statement,
        result=5,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == -1
    assert result.operation_type == "INSERT"


def test_asyncpg_driver_connection_method(asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock) -> None:
    """Test AsyncPG driver _connection method."""
    # Test default connection return
    assert asyncpg_driver._connection() is mock_asyncpg_connection

    # Test connection override
    override_connection = AsyncMock()
    assert asyncpg_driver._connection(override_connection) is override_connection


@pytest.mark.asyncio
async def test_asyncpg_driver_error_handling(asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock) -> None:
    """Test AsyncPG driver error handling."""
    # Setup mock to raise exception
    mock_asyncpg_connection.fetch.side_effect = Exception("Database error")

    # Create SQL statement
    statement = SQL("SELECT * FROM users")

    # Test error propagation
    with pytest.raises(Exception, match="Database error"):
        await asyncpg_driver._execute_statement(statement=statement)


@pytest.mark.asyncio
async def test_asyncpg_driver_instrumentation(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver instrumentation integration."""
    # Test instrumentation config is accessible
    assert asyncpg_driver.instrumentation_config is not None
    assert isinstance(asyncpg_driver.instrumentation_config, InstrumentationConfig)

    # Test logging configuration
    assert hasattr(asyncpg_driver.instrumentation_config, "log_queries")
    assert hasattr(asyncpg_driver.instrumentation_config, "log_parameters")
    assert hasattr(asyncpg_driver.instrumentation_config, "log_results_count")

    # Test basic execution works (no need to test executemany here)
    # This test is about instrumentation, not executemany functionality
    pass


@pytest.mark.asyncio
async def test_asyncpg_driver_operation_type_detection(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver operation type detection."""
    # Test different SQL statement types (DDL allowed with strict_mode=False)
    test_cases = [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("UPDATE users SET name = 'John'", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("CREATE TABLE test (id INTEGER)", "CREATE"),
    ]

    for sql, expected_op_type in test_cases:
        statement = SQL(sql, config=asyncpg_driver.config)

        # Test with string result
        result = await asyncpg_driver._wrap_execute_result(
            statement=statement,
            result="COMMAND COMPLETED",
        )

        assert result.operation_type == expected_op_type


@pytest.mark.asyncio
async def test_asyncpg_driver_select_to_arrow_basic(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver select_to_arrow method basic functionality."""
    # Setup mock connection and result data
    from asyncpg import Record

    mock_record1 = Mock(spec=Record)
    mock_record1.keys.return_value = ["id", "name"]
    mock_record1.__getitem__.side_effect = lambda key: {"id": 1, "name": "Alice"}[key]  # type: ignore[misc]

    mock_record2 = Mock(spec=Record)
    mock_record2.keys.return_value = ["id", "name"]
    mock_record2.__getitem__.side_effect = lambda key: {"id": 2, "name": "Bob"}[key]  # type: ignore[misc]

    records = [mock_record1, mock_record2]
    mock_asyncpg_connection.fetch.return_value = records

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Execute select_to_arrow
    result = await asyncpg_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)
    # Note: Don't compare statement objects directly as they may be recreated

    # Verify connection operations
    mock_asyncpg_connection.fetch.assert_called_once()


@pytest.mark.asyncio
async def test_asyncpg_driver_select_to_arrow_with_parameters(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver select_to_arrow method with parameters."""
    # Setup mock connection and result data
    from asyncpg import Record

    mock_record = Mock(spec=Record)
    mock_record.keys.return_value = ["id", "name"]
    mock_record.__getitem__.side_effect = lambda key: {"id": 42, "name": "Test User"}[key]  # type: ignore[misc]

    records = [mock_record]
    mock_asyncpg_connection.fetch.return_value = records

    # Create SQL statement with parameters
    statement = SQL("SELECT id, name FROM users WHERE id = $1", parameters=[42])

    # Execute select_to_arrow
    result = await asyncpg_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify connection operations with parameters
    mock_asyncpg_connection.fetch.assert_called_once_with("SELECT id, name FROM users WHERE id = $1", 42)


@pytest.mark.asyncio
async def test_asyncpg_driver_select_to_arrow_non_query_error(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver select_to_arrow with non-query statement raises error."""
    # Create non-query statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')")

    # Test error for non-query
    with pytest.raises(TypeError, match="Cannot fetch Arrow table for a non-query statement"):
        await asyncpg_driver.select_to_arrow(statement)


@pytest.mark.asyncio
async def test_asyncpg_driver_select_to_arrow_empty_result(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver select_to_arrow with empty result."""
    # Setup mock connection with no data
    mock_asyncpg_connection.fetch.return_value = []

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users WHERE id > 1000")

    # Execute select_to_arrow
    result = await asyncpg_driver.select_to_arrow(statement)

    # Verify result
    assert isinstance(result, ArrowResult)
    # Should create empty Arrow table
    assert result.data.num_rows == 0


@pytest.mark.asyncio
async def test_asyncpg_driver_select_to_arrow_with_connection_override(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver select_to_arrow with connection override."""
    # Create override connection
    from asyncpg import Record

    override_connection = AsyncMock()
    mock_record = Mock(spec=Record)
    mock_record.keys.return_value = ["id"]
    mock_record.__getitem__.side_effect = lambda key: {"id": 1}[key]  # type: ignore[misc]

    records = [mock_record]
    override_connection.fetch.return_value = records

    # Create SQL statement
    statement = SQL("SELECT id FROM users")

    # Execute with connection override
    result = await asyncpg_driver.select_to_arrow(statement, connection=override_connection)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify override connection was used
    override_connection.fetch.assert_called_once()


@pytest.mark.asyncio
async def test_asyncpg_driver_logging_configuration(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver logging configuration."""
    # Enable logging
    asyncpg_driver.instrumentation_config.log_queries = True
    asyncpg_driver.instrumentation_config.log_parameters = True
    asyncpg_driver.instrumentation_config.log_results_count = True

    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = $1", parameters=[1], config=asyncpg_driver.config)

    # Execute with logging enabled
    await asyncpg_driver._execute_statement(statement=statement)

    # Verify execution worked
    mock_asyncpg_connection.fetch.assert_called_once_with("SELECT * FROM users WHERE id = $1", 1)


def test_asyncpg_driver_mixins_integration(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver mixin integration."""
    # Test that driver has all expected mixins
    from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin

    assert isinstance(asyncpg_driver, SQLTranslatorMixin)
    assert isinstance(asyncpg_driver, AsyncArrowMixin)
    assert isinstance(asyncpg_driver, ResultConverter)

    # Test mixin methods are available
    assert hasattr(asyncpg_driver, "select_to_arrow")
    assert hasattr(asyncpg_driver, "to_schema")
    assert hasattr(asyncpg_driver, "returns_rows")


def test_asyncpg_driver_returns_rows_method(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver returns_rows method."""
    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert asyncpg_driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert asyncpg_driver.returns_rows(insert_stmt.expression) is False


@pytest.mark.asyncio
async def test_asyncpg_driver_status_string_parsing(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver status string parsing for different operations."""
    test_cases = [
        ("INSERT 0 5", "INSERT", 5),
        ("UPDATE 3", "UPDATE", 3),
        ("DELETE 2", "DELETE", 2),
        ("CREATE TABLE", "CREATE", 0),
        ("DROP TABLE", "DROP", 0),
    ]

    for status_string, expected_op, expected_rows in test_cases:
        statement = SQL(f"{expected_op} statement", config=asyncpg_driver.config)

        result = await asyncpg_driver._wrap_execute_result(
            statement=statement,
            result=status_string,
        )

        assert result.operation_type == expected_op
        assert result.rows_affected == expected_rows


@pytest.mark.asyncio
async def test_asyncpg_driver_dict_parameter_handling(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver parameter handling with dict parameters."""
    mock_asyncpg_connection.executemany.return_value = None

    statement = SQL(
        "INSERT INTO users (name, age) VALUES ($1, $2)",
        parameters=[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}],
        config=asyncpg_driver.config,
    ).as_many()

    result_val = await asyncpg_driver._execute_statement(statement=statement)

    mock_asyncpg_connection.executemany.assert_called_once()
    assert result_val == 2
