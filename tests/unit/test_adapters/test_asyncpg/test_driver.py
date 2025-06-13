"""Unit tests for AsyncPG driver."""

from typing import Any, Union
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
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
    placeholder_style = asyncpg_driver.default_parameter_style
    assert placeholder_style == ParameterStyle.NUMERIC


@pytest.mark.asyncio
async def test_asyncpg_config_dialect_property() -> None:
    """Test AsyncPG config dialect property."""
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(
        host="localhost",
        port=5432,
        database="test",
        user="test",
        password="test",
    )
    assert config.dialect == "postgres"


@pytest.mark.asyncio
async def test_asyncpg_driver_execute_statement_select(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver _execute_statement for SELECT statements."""
    # Setup mock connection
    from asyncpg import Record

    mock_record = MagicMock(spec=Record)
    mock_record.keys.return_value = ["id", "name"]
    mock_record.__getitem__.side_effect = lambda key: {"id": 1, "name": "test"}[key]  # type: ignore[misc]
    mock_asyncpg_connection.fetch.return_value = [mock_record]

    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = $1", parameters=[1], config=asyncpg_driver.config)

    # Execute - parameters, is_many, is_script are part of SQL object
    result = await asyncpg_driver._execute_statement(statement=statement)

    # Verify connection methods were called
    mock_asyncpg_connection.fetch.assert_called_once_with("SELECT * FROM users WHERE id = $1", 1)
    
    # Result should be a SelectResultDict
    assert isinstance(result, dict)
    assert "data" in result
    assert "column_names" in result
    assert "rows_affected" in result
    assert result["data"] == [mock_record]
    assert result["column_names"] == ["id", "name"]
    assert result["rows_affected"] == 1


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
    
    # Result should be a DMLResultDict
    assert isinstance(result, dict)
    assert "rows_affected" in result
    assert "status_message" in result
    assert result["rows_affected"] == 1
    assert result["status_message"] == "INSERT 0 1"


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
    
    # Result should be a ScriptResultDict
    assert isinstance(script_result, dict)
    assert "statements_executed" in script_result
    assert "status_message" in script_result
    assert script_result["statements_executed"] == -1  # AsyncPG doesn't provide statement count
    assert script_result["status_message"] == "CREATE TABLE"


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
    
    # Result should be a DMLResultDict
    assert isinstance(result, dict)
    assert "rows_affected" in result
    assert "status_message" in result
    assert result["rows_affected"] == 0  # executemany returns None, so no rows parsed
    assert result["status_message"] == "OK"


@pytest.mark.asyncio
async def test_asyncpg_driver_execute_statement_parameter_processing(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver parameter processing for different types."""
    # Setup mock connection
    from asyncpg import Record

    mock_record = MagicMock(spec=Record)
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
    
    # Result should be a SelectResultDict
    assert isinstance(result, dict)
    assert result["data"] == [mock_record]
    assert result["column_names"] == ["id", "name"]
    assert result["rows_affected"] == 1


@pytest.mark.asyncio
async def test_asyncpg_driver_wrap_select_result(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver _wrap_select_result method."""
    # Create mock records with data
    from asyncpg import Record

    mock_record1 = MagicMock(spec=Record)
    mock_record1.keys.return_value = ["id", "name"]
    mock_record1.__iter__ = lambda: iter([("id", 1), ("name", "John")])

    mock_record2 = MagicMock(spec=Record)
    mock_record2.keys.return_value = ["id", "name"]
    mock_record2.__iter__ = lambda: iter([("id", 2), ("name", "Jane")])

    records = [mock_record1, mock_record2]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Create SelectResultDict input
    select_result: SelectResultDict = {
        "data": records,
        "column_names": ["id", "name"],
        "rows_affected": len(records),
    }
    
    # Wrap result
    result: Union[SQLResult[Any], SQLResult[dict[str, Any]]] = await asyncpg_driver._wrap_select_result(
        statement=statement,
        result=select_result,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["id", "name"]
    # Data should be converted to dict format
    assert result.num_rows == 2


@pytest.mark.asyncio
async def test_asyncpg_driver_wrap_select_result_empty(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver _wrap_select_result method with empty result."""
    # Create empty records list
    records: list[Any] = []

    # Create SQL statement
    statement = SQL("SELECT * FROM empty_table")

    # Create SelectResultDict input
    select_result: SelectResultDict = {
        "data": records,
        "column_names": [],
        "rows_affected": 0,
    }
    
    # Wrap result
    result: Union[SQLResult[Any], SQLResult[dict[str, Any]]] = await asyncpg_driver._wrap_select_result(
        statement=statement,
        result=select_result,
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
    mock_record = MagicMock(spec=Record)
    mock_record.keys.return_value = ["id", "name"]
    mock_record.__iter__ = lambda: iter([("id", 1), ("name", "John")])

    records = [mock_record]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Create SelectResultDict input
    select_result: SelectResultDict = {
        "data": records,
        "column_names": ["id", "name"],
        "rows_affected": len(records),
    }
    
    # Wrap result with schema type
    result = await asyncpg_driver._wrap_select_result(
        statement=statement,
        result=select_result,
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

    # Create DMLResultDict input
    dml_result: DMLResultDict = {
        "rows_affected": 3,
        "status_message": "UPDATE 3",
    }
    
    # Wrap result with DMLResultDict
    result = await asyncpg_driver._wrap_execute_result(
        statement=statement,
        result=dml_result,
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

    # Create ScriptResultDict input
    script_result: ScriptResultDict = {
        "statements_executed": -1,
        "status_message": "CREATE TABLE",
    }
    
    # Wrap result for script
    result = await asyncpg_driver._wrap_execute_result(
        statement=statement,
        result=script_result,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 0
    assert result.operation_type == "SCRIPT"  # Scripts are now labeled as "SCRIPT" not "CREATE"


@pytest.mark.asyncio
async def test_asyncpg_driver_wrap_execute_result_integer(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver _wrap_execute_result method with integer result."""
    # Create SQL statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')", config=asyncpg_driver.config)

    # Create DMLResultDict input
    dml_result: DMLResultDict = {
        "rows_affected": 5,
        "status_message": "INSERT 0 5",
    }
    
    # Wrap result with DMLResultDict
    result = await asyncpg_driver._wrap_execute_result(
        statement=statement,
        result=dml_result,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 5
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

        # Create DMLResultDict for each test case
        dml_result: DMLResultDict = {
            "rows_affected": 0,
            "status_message": "COMMAND COMPLETED",
        }
        
        # Test with DMLResultDict
        result = await asyncpg_driver._wrap_execute_result(
            statement=statement,
            result=dml_result,
        )

        assert result.operation_type == expected_op_type


@pytest.mark.asyncio
async def test_asyncpg_driver_fetch_arrow_table_basic(
    asyncpg_driver: AsyncpgDriver, mock_asyncpg_connection: AsyncMock
) -> None:
    """Test AsyncPG driver fetch_arrow_table method basic functionality."""
    # The fetch_arrow_table method is provided by AsyncStorageMixin
    # It internally creates a SQL object and tries various approaches
    # For unit testing, we'll just verify the method exists
    assert hasattr(asyncpg_driver, "fetch_arrow_table")
    assert callable(asyncpg_driver.fetch_arrow_table)


@pytest.mark.asyncio
async def test_asyncpg_driver_storage_methods(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver has all storage methods from AsyncStorageMixin."""
    # Verify all async storage methods are available
    storage_methods = [
        "fetch_arrow_table",
        "ingest_arrow_table",
        "export_to_storage",
        "import_from_storage",
    ]

    for method in storage_methods:
        assert hasattr(asyncpg_driver, method)
        assert callable(getattr(asyncpg_driver, method))


@pytest.mark.asyncio
async def test_asyncpg_driver_arrow_support_flag(asyncpg_driver: AsyncpgDriver) -> None:
    """Test AsyncPG driver declares Arrow support."""
    # AsyncPG should support Arrow operations
    assert asyncpg_driver.__supports_arrow__ is True
    assert hasattr(asyncpg_driver, "_rows_to_arrow_table")


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
    from sqlspec.driver.mixins import AsyncStorageMixin, SQLTranslatorMixin

    assert isinstance(asyncpg_driver, SQLTranslatorMixin)
    assert isinstance(asyncpg_driver, AsyncStorageMixin)

    # Test mixin methods are available
    assert hasattr(asyncpg_driver, "fetch_arrow_table")
    assert hasattr(asyncpg_driver, "ingest_arrow_table")
    assert hasattr(asyncpg_driver, "export_to_storage")
    assert hasattr(asyncpg_driver, "import_from_storage")
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
        ("INSERT 0 5", "INSERT INTO test VALUES (1)", "INSERT", 5),
        ("UPDATE 3", "UPDATE test SET col = 1", "UPDATE", 3),
        ("DELETE 2", "DELETE FROM test", "DELETE", 2),
        ("CREATE TABLE", "CREATE TABLE test (id INT)", "CREATE", 0),
        ("DROP TABLE", "DROP TABLE test", "DROP", 0),
    ]

    for status_string, sql_text, expected_op, expected_rows in test_cases:
        statement = SQL(sql_text, config=asyncpg_driver.config)

        # Create DMLResultDict for each test case
        dml_result: DMLResultDict = {
            "rows_affected": expected_rows,
            "status_message": status_string,
        }
        
        result = await asyncpg_driver._wrap_execute_result(
            statement=statement,
            result=dml_result,
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
    # Result should be a DMLResultDict
    assert isinstance(result_val, dict)
    assert result_val["rows_affected"] == 0  # executemany returns None, so no rows parsed
    assert result_val["status_message"] == "OK"
