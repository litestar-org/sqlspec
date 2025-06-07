"""Unit tests for PSQLPy driver."""

import tempfile
from typing import Any, Union
from unittest.mock import AsyncMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.psqlpy import PsqlpyConnection, PsqlpyDriver
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def mock_psqlpy_connection() -> AsyncMock:
    """Create a mock PSQLPy connection."""
    mock_connection = AsyncMock(spec=PsqlpyConnection)
    mock_connection.execute.return_value = []
    mock_connection.execute_many.return_value = None
    mock_connection.execute_script.return_value = None
    mock_connection.fetch_row.return_value = None
    mock_connection.fetch_all.return_value = []
    return mock_connection


@pytest.fixture
def psqlpy_driver(mock_psqlpy_connection: AsyncMock) -> PsqlpyDriver:
    """Create a PSQLPy driver with mocked connection."""
    config = SQLConfig(strict_mode=False)  # Disable strict mode for unit tests
    instrumentation_config = InstrumentationConfig()
    return PsqlpyDriver(
        connection=mock_psqlpy_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )


def test_psqlpy_driver_initialization(mock_psqlpy_connection: AsyncMock) -> None:
    """Test PSQLPy driver initialization."""
    config = SQLConfig()
    instrumentation_config = InstrumentationConfig(log_queries=True)

    driver = PsqlpyDriver(
        connection=mock_psqlpy_connection,
        config=config,
        instrumentation_config=instrumentation_config,
    )

    # Test driver attributes are set correctly
    assert driver.connection is mock_psqlpy_connection
    assert driver.config is config
    assert driver.instrumentation_config is instrumentation_config
    assert driver.dialect == "postgres"
    assert driver.__supports_arrow__ is True


def test_psqlpy_driver_dialect_property(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver dialect property."""
    assert psqlpy_driver.dialect == "postgres"


def test_psqlpy_driver_supports_arrow(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver Arrow support."""
    assert psqlpy_driver.__supports_arrow__ is True
    assert PsqlpyDriver.__supports_arrow__ is True


def test_psqlpy_driver_placeholder_style(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver placeholder style detection."""
    placeholder_style = psqlpy_driver._get_placeholder_style()
    assert placeholder_style.value == "numeric"


@pytest.mark.asyncio
async def test_psqlpy_driver_execute_statement_select(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver _execute_statement for SELECT statements."""
    # Setup mock connection
    mock_result = [{"id": 1, "name": "test"}]
    mock_psqlpy_connection.fetch_all.return_value = mock_result

    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = $1", parameters=[1], config=psqlpy_driver.config)

    # Execute
    result = await psqlpy_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify connection methods were called
    mock_psqlpy_connection.fetch_all.assert_called_once_with("SELECT * FROM users WHERE id = $1", [1])
    assert result == mock_result


@pytest.mark.asyncio
async def test_psqlpy_driver_execute_statement_insert(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver _execute_statement for INSERT statements."""
    # Setup mock connection
    mock_psqlpy_connection.execute.return_value = 1

    # Create SQL statement with parameters
    statement = SQL("INSERT INTO users (name) VALUES ($1)", parameters=["John"], config=psqlpy_driver.config)

    # Execute
    result = await psqlpy_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify connection methods were called
    mock_psqlpy_connection.execute.assert_called_once_with("INSERT INTO users (name) VALUES ($1)", ["John"])
    assert result == 1


@pytest.mark.asyncio
async def test_psqlpy_driver_execute_statement_script(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver _execute_statement for script execution."""
    # Setup mock connection
    mock_psqlpy_connection.execute_script.return_value = None

    # Create SQL statement (DDL allowed with strict_mode=False)
    statement = SQL(
        "CREATE TABLE test (id INTEGER); INSERT INTO test VALUES (1);", config=psqlpy_driver.config
    ).as_script()

    # Execute script
    result: Any = await psqlpy_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify connection execute_script was called
    mock_psqlpy_connection.execute_script.assert_called_once()
    assert result == "SCRIPT EXECUTED"


@pytest.mark.asyncio
async def test_psqlpy_driver_execute_statement_many(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver _execute_statement for execute_many."""
    # Setup mock connection
    mock_psqlpy_connection.execute_many.return_value = None

    # Create SQL statement with placeholder for parameters
    statement = SQL(
        "INSERT INTO users (name) VALUES ($1)",
        parameters=[["John"], ["Jane"], ["Bob"]],
        config=psqlpy_driver.config,
    ).as_many()

    # Execute many
    result: Any = await psqlpy_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify connection execute_many was called
    mock_psqlpy_connection.execute_many.assert_called_once_with(
        "INSERT INTO users (name) VALUES ($1)", [["John"], ["Jane"], ["Bob"]]
    )
    assert result == 3  # Should return length of parameters list


@pytest.mark.asyncio
async def test_psqlpy_driver_execute_statement_parameter_processing(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver parameter processing for different types."""
    # Setup mock connection
    mock_result = [{"id": 1, "name": "John"}]
    mock_psqlpy_connection.fetch_all.return_value = mock_result

    # Create SQL statement with parameters
    statement = SQL(
        "SELECT * FROM users WHERE id = $1 AND name = $2", parameters=[1, "John"], config=psqlpy_driver.config
    )

    # Execute
    result: Any = await psqlpy_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify parameters were processed correctly
    mock_psqlpy_connection.fetch_all.assert_called_once_with(
        "SELECT * FROM users WHERE id = $1 AND name = $2", [1, "John"]
    )
    assert result == mock_result


@pytest.mark.asyncio
async def test_psqlpy_driver_wrap_select_result(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver _wrap_select_result method."""
    # Create mock data
    mock_data: list[dict[str, Any]] = [
        {"id": 1, "name": "John"},
        {"id": 2, "name": "Jane"},
    ]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result
    result: SQLResult[dict[str, Any]] = await psqlpy_driver._wrap_select_result(
        statement=statement,
        result=mock_data,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["id", "name"]
    assert result.data == mock_data


@pytest.mark.asyncio
async def test_psqlpy_driver_wrap_select_result_empty(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver _wrap_select_result method with empty result."""
    # Create empty data
    mock_data: list[Any] = []

    # Create SQL statement
    statement = SQL("SELECT * FROM empty_table")

    # Wrap result
    result: Union[SQLResult[Any], SQLResult[dict[str, Any]]] = await psqlpy_driver._wrap_select_result(
        statement=statement,
        result=mock_data,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.data == []
    assert result.column_names == []


@pytest.mark.asyncio
async def test_psqlpy_driver_wrap_select_result_with_schema_type(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver _wrap_select_result with schema_type."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    # Create mock data
    mock_data = [
        {"id": 1, "name": "John"},
        {"id": 2, "name": "Jane"},
    ]

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Wrap result with schema type
    result = await psqlpy_driver._wrap_select_result(
        statement=statement,
        result=mock_data,
        schema_type=User,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.column_names == ["id", "name"]


@pytest.mark.asyncio
async def test_psqlpy_driver_wrap_execute_result(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver _wrap_execute_result method."""
    # Create SQL statement
    statement = SQL("UPDATE users SET active = 1", config=psqlpy_driver.config)

    # Wrap result with row count
    result: SQLResult[dict[str, Any]] = await psqlpy_driver._wrap_execute_result(
        statement=statement,
        result=3,
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"


@pytest.mark.asyncio
async def test_psqlpy_driver_wrap_execute_result_script(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver _wrap_execute_result method for script."""
    # Create SQL statement (DDL allowed with strict_mode=False)
    statement = SQL("CREATE TABLE test (id INTEGER)", config=psqlpy_driver.config)

    # Wrap result for script
    result = await psqlpy_driver._wrap_execute_result(
        statement=statement,
        result="SCRIPT EXECUTED",
    )

    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == 0
    assert result.operation_type == "CREATE"


@pytest.mark.asyncio
async def test_psqlpy_driver_wrap_execute_result_integer(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver _wrap_execute_result method with integer result."""
    # Create SQL statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')", config=psqlpy_driver.config)
    # Wrap result with integer
    result = await psqlpy_driver._wrap_execute_result(
        statement=statement,
        result=5,
    )
    # Verify result
    assert isinstance(result, SQLResult)
    assert result.statement is statement
    assert result.rows_affected == -1
    assert result.operation_type == "INSERT"


def test_psqlpy_driver_connection_method(psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock) -> None:
    """Test PSQLPy driver _connection method."""
    # Test default connection return
    assert psqlpy_driver._connection() is mock_psqlpy_connection

    # Test connection override
    override_connection = AsyncMock()
    assert psqlpy_driver._connection(override_connection) is override_connection


@pytest.mark.asyncio
async def test_psqlpy_driver_error_handling(psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock) -> None:
    """Test PSQLPy driver error handling."""
    # Setup mock to raise exception
    mock_psqlpy_connection.fetch_all.side_effect = Exception("Database error")

    # Create SQL statement
    statement = SQL("SELECT * FROM users")

    # Test error propagation
    with pytest.raises(Exception, match="Database error"):
        await psqlpy_driver._execute_statement(
            statement=statement,
            connection=None,
        )


def test_psqlpy_driver_instrumentation(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver instrumentation integration."""
    # Test instrumentation config is accessible
    assert psqlpy_driver.instrumentation_config is not None
    assert isinstance(psqlpy_driver.instrumentation_config, InstrumentationConfig)

    # Test logging configuration
    assert hasattr(psqlpy_driver.instrumentation_config, "log_queries")
    assert hasattr(psqlpy_driver.instrumentation_config, "log_parameters")
    assert hasattr(psqlpy_driver.instrumentation_config, "log_results_count")


@pytest.mark.asyncio
async def test_psqlpy_driver_operation_type_detection(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver operation type detection."""
    # Test different SQL statement types (DDL allowed with strict_mode=False)
    test_cases = [
        ("SELECT * FROM users", "SELECT"),
        ("INSERT INTO users VALUES (1)", "INSERT"),
        ("UPDATE users SET name = 'John'", "UPDATE"),
        ("DELETE FROM users WHERE id = 1", "DELETE"),
        ("CREATE TABLE test (id INTEGER)", "CREATE"),
    ]

    for sql, expected_op_type in test_cases:
        statement = SQL(sql, config=psqlpy_driver.config)

        # Test with integer result
        result = await psqlpy_driver._wrap_execute_result(
            statement=statement,
            result=1,
        )

        assert result.operation_type == expected_op_type


@pytest.mark.asyncio
async def test_psqlpy_driver_fetch_arrow_table_basic(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver fetch_arrow_table method basic functionality."""
    # Setup mock connection and result data
    mock_data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]
    mock_psqlpy_connection.fetch_all.return_value = mock_data

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users")

    # Execute fetch_arrow_table
    result = await psqlpy_driver.fetch_arrow_table(statement)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify connection operations
    mock_psqlpy_connection.fetch_all.assert_called_once()


@pytest.mark.asyncio
async def test_psqlpy_driver_fetch_arrow_table_with_parameters(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver fetch_arrow_table method with parameters."""
    # Setup mock connection and result data
    mock_data = [{"id": 42, "name": "Test User"}]
    mock_psqlpy_connection.fetch_all.return_value = mock_data

    # Create SQL statement with parameters
    statement = SQL("SELECT id, name FROM users WHERE id = $1", parameters=[42])

    # Execute fetch_arrow_table
    result = await psqlpy_driver.fetch_arrow_table(statement)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify connection operations with parameters
    mock_psqlpy_connection.fetch_all.assert_called_once_with("SELECT id, name FROM users WHERE id = $1", [42])


@pytest.mark.asyncio
async def test_psqlpy_driver_fetch_arrow_table_non_query_error(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver fetch_arrow_table with non-query statement raises error."""
    # Create non-query statement
    statement = SQL("INSERT INTO users VALUES (1, 'test')")

    # Test error for non-query
    with pytest.raises(TypeError, match="Cannot fetch Arrow table for a non-query statement"):
        await psqlpy_driver.fetch_arrow_table(statement)


@pytest.mark.asyncio
async def test_psqlpy_driver_fetch_arrow_table_empty_result(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver fetch_arrow_table with empty result."""
    # Setup mock connection with no data
    mock_psqlpy_connection.fetch_all.return_value = []

    # Create SQL statement
    statement = SQL("SELECT id, name FROM users WHERE id > 1000")

    # Execute fetch_arrow_table
    result = await psqlpy_driver.fetch_arrow_table(statement)

    # Verify result
    assert isinstance(result, ArrowResult)
    # Should create empty Arrow table
    assert result.data.num_rows == 0


@pytest.mark.asyncio
async def test_psqlpy_driver_fetch_arrow_table_with_connection_override(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver fetch_arrow_table with connection override."""
    # Create override connection
    override_connection = AsyncMock()
    mock_data = [{"id": 1}]
    override_connection.fetch_all.return_value = mock_data

    # Create SQL statement
    statement = SQL("SELECT id FROM users")

    # Execute with connection override
    result = await psqlpy_driver.fetch_arrow_table(statement, connection=override_connection)

    # Verify result
    assert isinstance(result, ArrowResult)

    # Verify override connection was used
    override_connection.fetch_all.assert_called_once()


@pytest.mark.asyncio
async def test_psqlpy_driver_logging_configuration(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver logging configuration."""
    # Enable logging
    psqlpy_driver.instrumentation_config.log_queries = True
    psqlpy_driver.instrumentation_config.log_parameters = True
    psqlpy_driver.instrumentation_config.log_results_count = True

    # Create SQL statement with parameters
    statement = SQL("SELECT * FROM users WHERE id = $1", parameters=[1], config=psqlpy_driver.config)

    # Execute with logging enabled
    await psqlpy_driver._execute_statement(
        statement=statement,
        connection=None,
    )

    # Verify execution worked
    mock_psqlpy_connection.fetch_all.assert_called_once_with("SELECT * FROM users WHERE id = $1", [1])


def test_psqlpy_driver_mixins_integration(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver mixin integration."""
    # Test that driver has all expected mixins
    from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin

    assert isinstance(psqlpy_driver, SQLTranslatorMixin)
    assert isinstance(psqlpy_driver, AsyncArrowMixin)
    assert isinstance(psqlpy_driver, ResultConverter)

    # Test mixin methods are available
    assert hasattr(psqlpy_driver, "fetch_arrow_table")
    assert hasattr(psqlpy_driver, "to_schema")
    assert hasattr(psqlpy_driver, "returns_rows")


def test_psqlpy_driver_returns_rows_method(psqlpy_driver: PsqlpyDriver) -> None:
    """Test PSQLPy driver returns_rows method."""
    # Test with SELECT statement
    select_stmt = SQL("SELECT * FROM users")
    assert psqlpy_driver.returns_rows(select_stmt.expression) is True

    # Test with INSERT statement
    insert_stmt = SQL("INSERT INTO users VALUES (1, 'test')")
    assert psqlpy_driver.returns_rows(insert_stmt.expression) is False


@pytest.mark.asyncio
async def test_psqlpy_driver_fetch_row_method(psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock) -> None:
    """Test PSQLPy driver with fetch_row for single row queries."""
    # Setup mock connection
    mock_data = {"id": 1, "name": "John"}
    mock_psqlpy_connection.fetch_row.return_value = mock_data

    # PSQLPy might use fetch_row for single results
    # This tests the connection method availability
    result = await mock_psqlpy_connection.fetch_row("SELECT * FROM users WHERE id = $1 LIMIT 1", [1])

    # Verify result
    assert result == mock_data
    mock_psqlpy_connection.fetch_row.assert_called_once_with("SELECT * FROM users WHERE id = $1 LIMIT 1", [1])


@pytest.mark.asyncio
async def test_psqlpy_driver_dict_parameter_handling(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver parameter handling with dict parameters."""
    # Setup mock connection
    mock_psqlpy_connection.execute_many.return_value = None

    # Create SQL statement with parameters
    statement = SQL(
        "INSERT INTO users (name, age) VALUES ($1, $2)", parameters=["dummy", 25], config=psqlpy_driver.config
    )

    # Test with dict parameters in many execution
    dict_params = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]

    # Execute many with dict parameters
    result = await psqlpy_driver._execute_statement(
        statement=statement,
        parameters=dict_params,
        connection=None,
        config=None,
        is_many=True,
        is_script=False,
    )

    # Verify execute_many was called
    mock_psqlpy_connection.execute_many.assert_called_once()
    assert result == 2


@pytest.mark.asyncio
async def test_psqlpy_driver_prepared_statements(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test PSQLPy driver with prepared statements features."""
    # PSQLPy supports prepared statements - test that the connection interface supports it
    mock_psqlpy_connection.execute.return_value = 1

    # Create SQL statement with parameters (PSQLPy can optimize with prepared statements)
    statement = SQL("INSERT INTO users (name) VALUES ($1)", parameters=["John"], config=psqlpy_driver.config)

    # Execute
    result = await psqlpy_driver._execute_statement(
        statement=statement,
        parameters=None,
        connection=None,
        config=None,
        is_many=False,
        is_script=False,
    )

    # Verify execution worked
    mock_psqlpy_connection.execute.assert_called_once_with("INSERT INTO users (name) VALUES ($1)", ["John"])
    assert result == 1


@pytest.mark.asyncio
async def test_psqlpy_driver_fetch_arrow_table_arrowresult(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock
) -> None:
    """Test fetch_arrow_table returns ArrowResult with correct pyarrow.Table (async)."""
    mock_psqlpy_connection.fetch_all.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    statement = SQL("SELECT id, name FROM users")
    result = await psqlpy_driver.fetch_arrow_table(statement)
    assert isinstance(result, ArrowResult)
    assert isinstance(result.data, pa.Table)
    assert result.data.num_rows == 2
    assert set(result.data.column_names) == {"id", "name"}


@pytest.mark.asyncio
async def test_psqlpy_driver_to_parquet(
    psqlpy_driver: PsqlpyDriver, mock_psqlpy_connection: AsyncMock, monkeypatch: "pytest.MonkeyPatch"
) -> None:
    """Test to_parquet writes correct data to a Parquet file (async)."""
    mock_psqlpy_connection.fetch_all.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    statement = SQL("SELECT id, name FROM users")
    called = {}

    def patched_write_table(table: Any, path: Any, **kwargs: Any) -> None:
        called["table"] = table
        called["path"] = path

    monkeypatch.setattr(pq, "write_table", patched_write_table)
    with tempfile.NamedTemporaryFile() as tmp:
        await psqlpy_driver.export_to_storage(statement, tmp.name)
        assert "table" in called
        assert called["path"] == tmp.name
        assert isinstance(called["table"], pa.Table)
