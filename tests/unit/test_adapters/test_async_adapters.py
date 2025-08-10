"""Tests for asynchronous database adapters based on CORE_ROUND_3 architecture."""

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.result import SQLResult
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.driver import ExecutionResult
from sqlspec.exceptions import NotFoundError, SQLSpecError
from tests.unit.test_adapters.conftest import MockAsyncConnection, MockAsyncCursor, MockAsyncDriver

if TYPE_CHECKING:
    pass

__all__ = ()


@pytest.mark.asyncio
async def test_async_driver_initialization(mock_async_connection: MockAsyncConnection) -> None:
    """Test basic async driver initialization."""
    driver = MockAsyncDriver(mock_async_connection)

    assert driver.connection is mock_async_connection
    assert driver.dialect == "sqlite"
    assert driver.statement_config.dialect == "sqlite"
    assert driver.statement_config.parameter_config.default_parameter_style == ParameterStyle.QMARK


@pytest.mark.asyncio
async def test_async_driver_with_custom_config(mock_async_connection: MockAsyncConnection) -> None:
    """Test async driver initialization with custom statement config."""
    custom_config = StatementConfig(
        dialect="postgresql",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC, supported_parameter_styles={ParameterStyle.NUMERIC}
        ),
    )

    driver = MockAsyncDriver(mock_async_connection, custom_config)
    assert driver.statement_config.dialect == "postgresql"
    assert driver.statement_config.parameter_config.default_parameter_style == ParameterStyle.NUMERIC


@pytest.mark.asyncio
async def test_async_driver_with_cursor(mock_async_driver: MockAsyncDriver) -> None:
    """Test async cursor context manager functionality."""
    async with mock_async_driver.with_cursor(mock_async_driver.connection) as cursor:
        # Test that cursor has expected attributes
        assert hasattr(cursor, "connection")
        assert hasattr(cursor, "execute")
        assert hasattr(cursor, "fetchall")
        assert cursor.connection is mock_async_driver.connection


@pytest.mark.asyncio
async def test_async_driver_database_exception_handling(mock_async_driver: MockAsyncDriver) -> None:
    """Test async database exception handling context manager."""
    async with mock_async_driver.handle_database_exceptions():
        pass  # No exception should be raised

    with pytest.raises(SQLSpecError, match="Mock async database error"):
        async with mock_async_driver.handle_database_exceptions():
            raise ValueError("Test async error")


@pytest.mark.asyncio
async def test_async_driver_execute_statement_select(mock_async_driver: MockAsyncDriver) -> None:
    """Test async _execute_statement method with SELECT query."""
    statement = SQL("SELECT id, name FROM users", statement_config=mock_async_driver.statement_config)
    async with mock_async_driver.with_cursor(mock_async_driver.connection) as cursor:
        result = await mock_async_driver._execute_statement(cursor, statement)

    assert isinstance(result, ExecutionResult)
    assert result.is_select_result is True
    assert result.is_script_result is False
    assert result.is_many_result is False
    assert result.selected_data == [{"id": 1, "name": "test"}, {"id": 2, "name": "example"}]
    assert result.column_names == ["id", "name"]
    assert result.data_row_count == 2


@pytest.mark.asyncio
async def test_async_driver_execute_statement_insert(mock_async_driver: MockAsyncDriver) -> None:
    """Test async _execute_statement method with INSERT query."""
    statement = SQL("INSERT INTO users (name) VALUES (?)", "test", statement_config=mock_async_driver.statement_config)

    async with mock_async_driver.with_cursor(mock_async_driver.connection) as cursor:
        result = await mock_async_driver._execute_statement(cursor, statement)
    assert isinstance(result, ExecutionResult)
    assert result.is_select_result is False
    assert result.is_script_result is False
    assert result.is_many_result is False
    assert result.rowcount_override == 1
    assert result.selected_data is None


@pytest.mark.asyncio
async def test_async_driver_execute_many(mock_async_driver: MockAsyncDriver) -> None:
    """Test async _execute_many method."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)",
        [["alice"], ["bob"], ["charlie"]],
        statement_config=mock_async_driver.statement_config,
        is_many=True,
    )
    async with mock_async_driver.with_cursor(mock_async_driver.connection) as cursor:
        result = await mock_async_driver._execute_statement(cursor, statement)
    assert isinstance(result, ExecutionResult)
    assert result.is_many_result is True
    assert result.is_select_result is False
    assert result.is_script_result is False
    assert result.rowcount_override == 3  # 3 parameter sets
    assert mock_async_driver.connection.execute_many_count == 1


@pytest.mark.asyncio
async def test_async_driver_execute_many_no_parameters(mock_async_driver: MockAsyncDriver) -> None:
    """Test async _execute_many method fails without parameters."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)", statement_config=mock_async_driver.statement_config, is_many=True
    )
    async with mock_async_driver.with_cursor(mock_async_driver.connection) as cursor:
        with pytest.raises(ValueError, match="execute_many requires parameters"):
            await mock_async_driver._execute_many(cursor, statement)


@pytest.mark.asyncio
async def test_async_driver_execute_script(mock_async_driver: MockAsyncDriver) -> None:
    """Test async _execute_script method."""
    script = """
    INSERT INTO users (name) VALUES ('alice');
    INSERT INTO users (name) VALUES ('bob');
    UPDATE users SET active = 1;
    """
    statement = SQL(script, statement_config=mock_async_driver.statement_config, is_script=True)
    async with mock_async_driver.with_cursor(mock_async_driver.connection) as cursor:
        result = await mock_async_driver._execute_statement(cursor, statement)
    assert isinstance(result, ExecutionResult)
    assert result.is_script_result is True
    assert result.is_select_result is False
    assert result.is_many_result is False
    assert result.statement_count == 3
    assert result.successful_statements == 3


@pytest.mark.asyncio
async def test_async_driver_dispatch_statement_execution_select(mock_async_driver: MockAsyncDriver) -> None:
    """Test async dispatch_statement_execution with SELECT statement."""
    statement = SQL("SELECT * FROM users", statement_config=mock_async_driver.statement_config)

    result = await mock_async_driver.dispatch_statement_execution(statement, mock_async_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"
    assert len(result.get_data()) == 2
    assert result.get_data()[0]["id"] == 1
    assert result.get_data()[0]["name"] == "test"


@pytest.mark.asyncio
async def test_async_driver_dispatch_statement_execution_insert(mock_async_driver: MockAsyncDriver) -> None:
    """Test async dispatch_statement_execution with INSERT statement."""
    statement = SQL("INSERT INTO users (name) VALUES (?)", "test", statement_config=mock_async_driver.statement_config)

    result = await mock_async_driver.dispatch_statement_execution(statement, mock_async_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 1
    assert len(result.get_data()) == 0


@pytest.mark.asyncio
async def test_async_driver_dispatch_statement_execution_script(mock_async_driver: MockAsyncDriver) -> None:
    """Test async dispatch_statement_execution with script."""
    script = "INSERT INTO users (name) VALUES ('alice'); INSERT INTO users (name) VALUES ('bob');"
    statement = SQL(script, statement_config=mock_async_driver.statement_config, is_script=True)

    result = await mock_async_driver.dispatch_statement_execution(statement, mock_async_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"
    assert result.total_statements == 2
    assert result.successful_statements == 2


@pytest.mark.asyncio
async def test_async_driver_dispatch_statement_execution_many(mock_async_driver: MockAsyncDriver) -> None:
    """Test async dispatch_statement_execution with execute_many."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)",
        [["alice"], ["bob"]],
        statement_config=mock_async_driver.statement_config,
        is_many=True,
    )

    result = await mock_async_driver.dispatch_statement_execution(statement, mock_async_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 2


@pytest.mark.asyncio
async def test_async_driver_transaction_management(mock_async_driver: MockAsyncDriver) -> None:
    """Test async transaction management methods."""
    connection = mock_async_driver.connection

    # Test begin
    await mock_async_driver.begin()
    assert connection.in_transaction is True

    # Test commit
    await mock_async_driver.commit()
    assert connection.in_transaction is False

    # Test rollback
    await mock_async_driver.begin()
    assert connection.in_transaction is True
    await mock_async_driver.rollback()
    assert connection.in_transaction is False


@pytest.mark.asyncio
async def test_async_driver_execute_method(mock_async_driver: MockAsyncDriver) -> None:
    """Test high-level async execute method."""
    result = await mock_async_driver.execute("SELECT * FROM users WHERE id = ?", 1)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"
    assert len(result.get_data()) == 2


@pytest.mark.asyncio
async def test_async_driver_execute_many_method(mock_async_driver: MockAsyncDriver) -> None:
    """Test high-level async execute_many method."""
    parameters = [["alice"], ["bob"], ["charlie"]]
    result = await mock_async_driver.execute_many("INSERT INTO users (name) VALUES (?)", parameters)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 3


@pytest.mark.asyncio
async def test_async_driver_execute_script_method(mock_async_driver: MockAsyncDriver) -> None:
    """Test high-level async execute_script method."""
    script = "INSERT INTO users (name) VALUES ('alice'); UPDATE users SET active = 1;"
    result = await mock_async_driver.execute_script(script)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"
    assert result.total_statements == 2
    assert result.successful_statements == 2


@pytest.mark.asyncio
async def test_async_driver_select_one(mock_async_driver: MockAsyncDriver) -> None:
    """Test async select_one method - expects error when multiple rows returned."""
    with pytest.raises(ValueError, match="Expected exactly one row, found 2"):
        await mock_async_driver.select_one("SELECT * FROM users WHERE id = ?", 1)


@pytest.mark.asyncio
async def test_async_driver_select_one_no_results(mock_async_driver: MockAsyncDriver) -> None:
    """Test async select_one method with no results."""
    # Mock empty result
    with patch.object(mock_async_driver, "execute", new_callable=AsyncMock) as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.get_data.return_value = []
        mock_execute.return_value = mock_result

        with pytest.raises(NotFoundError, match="No rows found"):
            await mock_async_driver.select_one("SELECT * FROM users WHERE id = ?", 999)


@pytest.mark.asyncio
async def test_async_driver_select_one_multiple_results(mock_async_driver: MockAsyncDriver) -> None:
    """Test async select_one method with multiple results."""
    # Mock multiple results
    with patch.object(mock_async_driver, "execute", new_callable=AsyncMock) as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.get_data.return_value = [{"id": 1}, {"id": 2}, {"id": 3}]
        mock_execute.return_value = mock_result

        with pytest.raises(ValueError, match="Expected exactly one row, found 3"):
            await mock_async_driver.select_one("SELECT * FROM users")


@pytest.mark.asyncio
async def test_async_driver_select_one_or_none(mock_async_driver: MockAsyncDriver) -> None:  # type: ignore[func-returns-value]
    """Test async select_one_or_none method - expects error when multiple rows returned."""
    with pytest.raises(ValueError, match="Expected at most one row, found 2"):
        await mock_async_driver.select_one_or_none("SELECT * FROM users WHERE id = ?", 1)  # type: ignore[func-returns-value]


@pytest.mark.asyncio
async def test_async_driver_select_one_or_none_no_results(mock_async_driver: MockAsyncDriver) -> None:  # type: ignore[func-returns-value]
    """Test async select_one_or_none method with no results."""
    with patch.object(mock_async_driver, "execute", new_callable=AsyncMock) as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.get_data.return_value = []
        mock_execute.return_value = mock_result

        result = await mock_async_driver.select_one_or_none("SELECT * FROM users WHERE id = ?", 999)  # type: ignore[func-returns-value]
        assert result is None


@pytest.mark.asyncio
async def test_async_driver_select_one_or_none_multiple_results(mock_async_driver: MockAsyncDriver) -> None:
    """Test async select_one_or_none method with multiple results."""
    with patch.object(mock_async_driver, "execute", new_callable=AsyncMock) as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.get_data.return_value = [{"id": 1}, {"id": 2}]
        mock_execute.return_value = mock_result

        with pytest.raises(ValueError, match="Expected at most one row, found 2"):
            await mock_async_driver.select_one_or_none("SELECT * FROM users")


@pytest.mark.asyncio
async def test_async_driver_select(mock_async_driver: MockAsyncDriver) -> None:
    """Test async select method."""
    result: list[dict[str, Any]] = await mock_async_driver.select("SELECT * FROM users")

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


@pytest.mark.asyncio
async def test_async_driver_select_value(mock_async_driver: MockAsyncDriver) -> None:
    """Test async select_value method."""
    # Mock single row, single column result
    with patch.object(mock_async_driver, "execute", new_callable=AsyncMock) as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.one.return_value = {"count": 42}
        mock_execute.return_value = mock_result

        result = await mock_async_driver.select_value("SELECT COUNT(*) as count FROM users")
        assert result == 42


@pytest.mark.asyncio
async def test_async_driver_select_value_no_results(mock_async_driver: MockAsyncDriver) -> None:
    """Test async select_value method with no results."""
    with patch.object(mock_async_driver, "execute", new_callable=AsyncMock) as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.one.side_effect = ValueError("No rows")
        mock_execute.return_value = mock_result

        with pytest.raises(NotFoundError, match="No rows found"):
            await mock_async_driver.select_value("SELECT COUNT(*) FROM users WHERE id = 999")


@pytest.mark.asyncio
async def test_async_driver_select_value_or_none(mock_async_driver: MockAsyncDriver) -> None:
    """Test async select_value_or_none method - expects error when multiple rows returned."""
    with pytest.raises(ValueError, match="Expected at most one row, found 2"):
        await mock_async_driver.select_value_or_none("SELECT * FROM users WHERE id = ?", 1)


@pytest.mark.asyncio
async def test_async_driver_select_value_or_none_no_results(mock_async_driver: MockAsyncDriver) -> None:
    """Test async select_value_or_none method with no results."""
    with patch.object(mock_async_driver, "execute", new_callable=AsyncMock) as mock_execute:
        mock_result = Mock(spec=SQLResult)
        mock_result.get_data.return_value = []
        mock_execute.return_value = mock_result

        result = await mock_async_driver.select_value_or_none("SELECT COUNT(*) FROM users WHERE id = 999")
        assert result is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parameter_style,expected_style",
    [
        pytest.param(ParameterStyle.QMARK, ParameterStyle.QMARK, id="qmark"),
        pytest.param(ParameterStyle.NUMERIC, ParameterStyle.NUMERIC, id="numeric"),
        pytest.param(ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_COLON, id="named_colon"),
        pytest.param(ParameterStyle.NAMED_PYFORMAT, ParameterStyle.NAMED_PYFORMAT, id="pyformat_named"),
    ],
)
async def test_async_driver_parameter_styles(
    mock_async_connection: MockAsyncConnection, parameter_style: ParameterStyle, expected_style: ParameterStyle
) -> None:
    """Test different parameter styles are handled correctly in async driver."""
    config = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=parameter_style,
            supported_parameter_styles={parameter_style},
            default_execution_parameter_style=parameter_style,
            supported_execution_parameter_styles={parameter_style},
        ),
    )

    driver = MockAsyncDriver(mock_async_connection, config)
    assert driver.statement_config.parameter_config.default_parameter_style == expected_style

    # Test execution with parameter style
    if parameter_style == ParameterStyle.QMARK:
        statement = SQL("SELECT * FROM users WHERE id = ?", 1, statement_config=config)
    elif parameter_style == ParameterStyle.NUMERIC:
        statement = SQL("SELECT * FROM users WHERE id = $1", 1, statement_config=config)
    elif parameter_style == ParameterStyle.NAMED_COLON:
        statement = SQL("SELECT * FROM users WHERE id = :id", {"id": 1}, statement_config=config)
    else:  # NAMED_PYFORMAT
        statement = SQL("SELECT * FROM users WHERE id = %(id)s", {"id": 1}, statement_config=config)

    result = await driver.dispatch_statement_execution(statement, driver.connection)
    assert isinstance(result, SQLResult)


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", ["sqlite", "postgres", "mysql"])
async def test_async_driver_different_dialects(mock_async_connection: MockAsyncConnection, dialect: str) -> None:
    """Test async driver works with different SQL dialects."""
    config = StatementConfig(
        dialect=dialect,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        ),
    )

    driver = MockAsyncDriver(mock_async_connection, config)
    assert driver.statement_config.dialect == dialect

    # Test basic query execution
    result = await driver.execute("SELECT 1 as test")
    assert isinstance(result, SQLResult)


@pytest.mark.asyncio
async def test_async_driver_create_execution_result(mock_async_driver: MockAsyncDriver) -> None:
    """Test async create_execution_result method."""
    cursor = mock_async_driver.with_cursor(mock_async_driver.connection)

    # Test SELECT result
    result = mock_async_driver.create_execution_result(
        cursor, selected_data=[{"id": 1}, {"id": 2}], column_names=["id"], data_row_count=2, is_select_result=True
    )

    assert result.is_select_result is True
    assert result.selected_data == [{"id": 1}, {"id": 2}]
    assert result.column_names == ["id"]
    assert result.data_row_count == 2

    # Test INSERT result
    result = mock_async_driver.create_execution_result(cursor, rowcount_override=1)
    assert result.is_select_result is False
    assert result.rowcount_override == 1

    # Test script result
    result = mock_async_driver.create_execution_result(
        cursor, statement_count=3, successful_statements=3, is_script_result=True
    )
    assert result.is_script_result is True
    assert result.statement_count == 3
    assert result.successful_statements == 3


@pytest.mark.asyncio
async def test_async_driver_build_statement_result(mock_async_driver: MockAsyncDriver) -> None:
    """Test async build_statement_result method."""
    statement = SQL("SELECT * FROM users", statement_config=mock_async_driver.statement_config)
    cursor = mock_async_driver.with_cursor(mock_async_driver.connection)

    # Test SELECT result
    execution_result = mock_async_driver.create_execution_result(
        cursor, selected_data=[{"id": 1}], column_names=["id"], data_row_count=1, is_select_result=True
    )

    sql_result = mock_async_driver.build_statement_result(statement, execution_result)
    assert isinstance(sql_result, SQLResult)
    assert sql_result.operation_type == "SELECT"
    assert sql_result.get_data() == [{"id": 1}]
    assert sql_result.column_names == ["id"]

    # Test script result
    script_statement = SQL(
        "INSERT INTO users (name) VALUES ('test');", statement_config=mock_async_driver.statement_config, is_script=True
    )
    script_execution_result = mock_async_driver.create_execution_result(
        cursor, statement_count=1, successful_statements=1, is_script_result=True
    )

    script_sql_result = mock_async_driver.build_statement_result(script_statement, script_execution_result)
    assert script_sql_result.operation_type == "SCRIPT"
    assert script_sql_result.total_statements == 1
    assert script_sql_result.successful_statements == 1


@pytest.mark.asyncio
async def test_async_driver_special_handling_integration(mock_async_driver: MockAsyncDriver) -> None:
    """Test that async _try_special_handling is called during dispatch."""
    statement = SQL("SELECT * FROM users", statement_config=mock_async_driver.statement_config)

    with patch.object(
        mock_async_driver, "_try_special_handling", new_callable=AsyncMock, return_value=None
    ) as mock_special:
        result = await mock_async_driver.dispatch_statement_execution(statement, mock_async_driver.connection)

        assert isinstance(result, SQLResult)
        mock_special.assert_called_once()


@pytest.mark.asyncio
async def test_async_driver_error_handling_in_dispatch(mock_async_driver: MockAsyncDriver) -> None:
    """Test error handling during async statement dispatch."""
    statement = SQL("SELECT * FROM users", statement_config=mock_async_driver.statement_config)

    # Mock an exception during execution
    with patch.object(
        mock_async_driver, "_execute_statement", new_callable=AsyncMock, side_effect=ValueError("Test async error")
    ):
        with pytest.raises(SQLSpecError, match="Mock async database error"):
            await mock_async_driver.dispatch_statement_execution(statement, mock_async_driver.connection)


@pytest.mark.asyncio
async def test_async_driver_statement_processing_integration(mock_async_driver: MockAsyncDriver) -> None:
    """Test async driver statement processing integration with CORE_ROUND_3."""
    statement = SQL("SELECT * FROM users WHERE active = ?", True, statement_config=mock_async_driver.statement_config)

    # Mock statement compilation method call on the SQL class
    with patch.object(SQL, "compile") as mock_compile:
        mock_compile.return_value = ("SELECT * FROM test", [])
        await mock_async_driver.dispatch_statement_execution(statement, mock_async_driver.connection)
        # compile should be called during execution via _get_compiled_sql
        assert mock_compile.called or statement.sql == "SELECT * FROM test"


@pytest.mark.asyncio
async def test_async_driver_context_manager_integration(mock_async_driver: MockAsyncDriver) -> None:
    """Test async context manager integration during execution."""
    statement = SQL("SELECT * FROM users", statement_config=mock_async_driver.statement_config)

    # Test that both context managers are used correctly
    with patch.object(mock_async_driver, "with_cursor") as mock_with_cursor:
        mock_cursor = MockAsyncCursor(mock_async_driver.connection)
        mock_with_cursor.return_value = mock_cursor

        with patch.object(mock_async_driver, "handle_database_exceptions") as mock_handle_exceptions:
            mock_context = AsyncMock()
            mock_handle_exceptions.return_value = mock_context

            result = await mock_async_driver.dispatch_statement_execution(statement, mock_async_driver.connection)

            assert isinstance(result, SQLResult)
            mock_with_cursor.assert_called_once()
            mock_handle_exceptions.assert_called_once()


@pytest.mark.asyncio
async def test_async_driver_resource_cleanup(mock_async_driver: MockAsyncDriver) -> None:
    """Test async resource cleanup during execution."""
    connection = mock_async_driver.connection
    cursor = await connection.cursor()

    assert cursor.closed is False

    # Test cursor cleanup after use
    await cursor.close()
    assert cursor.closed is True


@pytest.mark.asyncio
async def test_async_driver_concurrent_execution(mock_async_connection: MockAsyncConnection) -> None:
    """Test concurrent execution capability of async driver."""
    import asyncio

    driver = MockAsyncDriver(mock_async_connection)

    async def execute_query(query_id: int) -> SQLResult:
        return await driver.execute(f"SELECT {query_id} as id")

    # Execute multiple queries concurrently
    tasks = [execute_query(i) for i in range(3)]
    results = await asyncio.gather(*tasks)

    assert len(results) == 3
    for result in results:
        assert isinstance(result, SQLResult)
        assert result.operation_type == "SELECT"


@pytest.mark.asyncio
async def test_async_driver_with_transaction_context(mock_async_driver: MockAsyncDriver) -> None:
    """Test async driver transaction context usage."""
    connection = mock_async_driver.connection

    # Test transaction lifecycle
    await mock_async_driver.begin()
    assert connection.in_transaction is True

    # Execute some operations
    result = await mock_async_driver.execute("INSERT INTO users (name) VALUES (?)", "test")
    assert isinstance(result, SQLResult)

    # Commit transaction
    await mock_async_driver.commit()
    assert connection.in_transaction is False

    # Test rollback scenario
    await mock_async_driver.begin()
    assert connection.in_transaction is True

    await mock_async_driver.rollback()
    assert connection.in_transaction is False
