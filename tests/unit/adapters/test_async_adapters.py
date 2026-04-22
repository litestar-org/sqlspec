# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Tests for asynchronous database adapters."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import aiosqlite
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteDriver
from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, SQLResult, StatementConfig, get_default_config
from sqlspec.driver import ExecutionResult
from sqlspec.exceptions import NotFoundError, SQLSpecError
from sqlspec.typing import Empty

pytestmark = pytest.mark.xdist_group("adapter_unit")

__all__ = ()


async def test_async_driver_initialization() -> None:
    """Test basic async driver initialization."""
    conn = await aiosqlite.connect(":memory:")
    driver = AiosqliteDriver(conn)

    assert driver.connection is conn
    assert driver.dialect == "sqlite"
    assert driver.statement_config.dialect == "sqlite"
    assert driver.statement_config.parameter_config.default_parameter_style == ParameterStyle.QMARK
    await conn.close()


async def test_async_driver_with_custom_config() -> None:
    """Test async driver initialization with custom statement config."""
    conn = await aiosqlite.connect(":memory:")
    custom_config = StatementConfig(
        dialect="postgresql",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC, supported_parameter_styles={ParameterStyle.NUMERIC}
        ),
    )

    driver = AiosqliteDriver(conn, custom_config)
    assert driver.statement_config.dialect == "postgresql"
    assert driver.statement_config.parameter_config.default_parameter_style == ParameterStyle.NUMERIC
    await conn.close()


async def test_async_driver_with_cursor(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async cursor context manager functionality."""
    async with aiosqlite_async_driver.with_cursor(aiosqlite_async_driver.connection) as cursor:
        assert hasattr(cursor, "execute")
        assert hasattr(cursor, "fetchall")


async def test_async_driver_database_exception_handling(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async database exception handling with deferred exception pattern."""
    exc_handler = aiosqlite_async_driver.handle_database_exceptions()
    async with exc_handler:
        pass
    assert exc_handler.pending_exception is None

    exc_handler = aiosqlite_async_driver.handle_database_exceptions()
    async with exc_handler:
        raise aiosqlite.Error("Test async error")

    assert exc_handler.pending_exception is not None
    assert isinstance(exc_handler.pending_exception, SQLSpecError)


async def test_async_driver_dispatch_execute_select(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async dispatch_execute method with SELECT query."""
    statement = SQL("SELECT id, name FROM users", statement_config=aiosqlite_async_driver.statement_config)
    async with aiosqlite_async_driver.with_cursor(aiosqlite_async_driver.connection) as cursor:
        result = await aiosqlite_async_driver.dispatch_execute(cursor, statement)

    assert isinstance(result, ExecutionResult)
    assert result.is_select_result is True
    assert result.is_script_result is False
    assert result.is_many_result is False
    assert result.selected_data == [(1, "test"), (2, "example")]
    assert result.column_names == ["id", "name"]
    assert result.data_row_count == 2


async def test_async_driver_dispatch_execute_insert(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async dispatch_execute method with INSERT query."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)", "new_user", statement_config=aiosqlite_async_driver.statement_config
    )

    async with aiosqlite_async_driver.with_cursor(aiosqlite_async_driver.connection) as cursor:
        result = await aiosqlite_async_driver.dispatch_execute(cursor, statement)
    assert isinstance(result, ExecutionResult)
    assert result.is_select_result is False
    assert result.rowcount_override == 1


async def test_async_driver_execute_many(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async dispatch_execute_many method."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)",
        [["alice"], ["bob"], ["charlie"]],
        statement_config=aiosqlite_async_driver.statement_config,
        is_many=True,
    )
    async with aiosqlite_async_driver.with_cursor(aiosqlite_async_driver.connection) as cursor:
        result = await aiosqlite_async_driver.dispatch_execute_many(cursor, statement)
    assert isinstance(result, ExecutionResult)
    assert result.is_many_result is True
    assert result.rowcount_override == 3


async def test_async_driver_execute_many_no_parameters(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async _execute_many method fails without parameters."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)", statement_config=aiosqlite_async_driver.statement_config, is_many=True
    )
    async with aiosqlite_async_driver.with_cursor(aiosqlite_async_driver.connection) as cursor:
        with pytest.raises(ValueError, match="execute_many requires parameters"):
            await aiosqlite_async_driver.dispatch_execute_many(cursor, statement)


async def test_async_driver_execute_script(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async dispatch_execute_script method."""
    script = """
    INSERT INTO users (name) VALUES ('alice');
    INSERT INTO users (name) VALUES ('bob');
    UPDATE users SET name = 'updated';
    """
    statement = SQL(script, statement_config=aiosqlite_async_driver.statement_config, is_script=True)
    async with aiosqlite_async_driver.with_cursor(aiosqlite_async_driver.connection) as cursor:
        result = await aiosqlite_async_driver.dispatch_execute_script(cursor, statement)
    assert isinstance(result, ExecutionResult)
    assert result.is_script_result is True
    assert result.statement_count == 3
    assert result.successful_statements == 3


async def test_async_driver_dispatch_statement_execution_select(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async dispatch_statement_execution with SELECT statement."""
    statement = SQL("SELECT * FROM users", statement_config=aiosqlite_async_driver.statement_config)

    result = await aiosqlite_async_driver.dispatch_statement_execution(statement, aiosqlite_async_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"
    assert len(result.get_data()) == 2
    assert result.get_data()[0]["id"] == 1
    assert result.get_data()[0]["name"] == "test"


async def test_async_driver_dispatch_statement_execution_insert(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async dispatch_statement_execution with INSERT statement."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)", "new_user", statement_config=aiosqlite_async_driver.statement_config
    )

    result = await aiosqlite_async_driver.dispatch_statement_execution(statement, aiosqlite_async_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 1


async def test_async_driver_dispatch_statement_execution_script(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async dispatch_statement_execution with script."""
    script = "INSERT INTO users (name) VALUES ('alice'); INSERT INTO users (name) VALUES ('bob');"
    statement = SQL(script, statement_config=aiosqlite_async_driver.statement_config, is_script=True)

    result = await aiosqlite_async_driver.dispatch_statement_execution(statement, aiosqlite_async_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"
    assert result.total_statements == 2
    assert result.successful_statements == 2


async def test_async_driver_dispatch_statement_execution_many(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async dispatch_statement_execution with execute_many."""
    statement = SQL(
        "INSERT INTO users (name) VALUES (?)",
        [["alice"], ["bob"]],
        statement_config=aiosqlite_async_driver.statement_config,
        is_many=True,
    )

    result = await aiosqlite_async_driver.dispatch_statement_execution(statement, aiosqlite_async_driver.connection)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 2


async def test_async_driver_releases_pooled_statement(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Pooled statements should be reset after dispatch execution."""
    seed = "SELECT * FROM users WHERE id = ?"
    aiosqlite_async_driver.prepare_statement(
        seed, (1,), statement_config=aiosqlite_async_driver.statement_config, kwargs={}
    )
    pooled = aiosqlite_async_driver.prepare_statement(
        seed, (2,), statement_config=aiosqlite_async_driver.statement_config, kwargs={}
    )

    assert pooled._pooled is True

    await aiosqlite_async_driver.dispatch_statement_execution(pooled, aiosqlite_async_driver.connection)

    assert pooled._raw_sql == ""
    assert pooled._processed_state is Empty
    assert pooled._filters == []
    assert pooled._statement_config is get_default_config()


async def test_async_driver_transaction_management(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async transaction management methods."""
    await aiosqlite_async_driver.begin()
    await aiosqlite_async_driver.execute("INSERT INTO users (name) VALUES ('trans')")
    await aiosqlite_async_driver.commit()

    res = await aiosqlite_async_driver.select_value("SELECT COUNT(*) FROM users WHERE name = 'trans'")
    assert res == 1

    await aiosqlite_async_driver.begin()
    await aiosqlite_async_driver.execute("INSERT INTO users (name) VALUES ('rolledback')")
    await aiosqlite_async_driver.rollback()

    res = await aiosqlite_async_driver.select_value("SELECT COUNT(*) FROM users WHERE name = 'rolledback'")
    assert res == 0


async def test_async_driver_execute_method(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test high-level async execute method."""
    result = await aiosqlite_async_driver.execute("SELECT * FROM users WHERE id = ?", 1)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SELECT"
    assert len(result.get_data()) == 1


async def test_async_driver_execute_many_method(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test high-level async execute_many method."""
    parameters = [["alice"], ["bob"], ["charlie"]]
    result = await aiosqlite_async_driver.execute_many("INSERT INTO users (name) VALUES (?)", parameters)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "INSERT"
    assert result.rows_affected == 3


async def test_async_driver_execute_script_method(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test high-level async execute_script method."""
    script = "INSERT INTO users (name) VALUES ('alice'); UPDATE users SET name = 'updated';"
    result = await aiosqlite_async_driver.execute_script(script)

    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"
    assert result.total_statements == 2
    assert result.successful_statements == 2


@pytest.mark.parametrize(
    ("method_name", "call_args"),
    [
        pytest.param("execute", ("SELECT * FROM users WHERE id = ?", 1), id="execute"),
        pytest.param("execute_many", ("INSERT INTO users (name) VALUES (?)", [["alice"]]), id="execute_many"),
        pytest.param("execute_script", ("INSERT INTO users (name) VALUES ('alice');",), id="execute_script"),
    ],
)
async def test_async_driver_execution_wrappers_reraise_deferred_database_errors(
    aiosqlite_async_driver: AiosqliteDriver, method_name: str, call_args: tuple[Any, ...]
) -> None:
    """Test wrapper methods re-raise mapped errors after the exception context exits."""
    with patch.object(
        aiosqlite_async_driver,
        "dispatch_statement_execution",
        new_callable=AsyncMock,
        side_effect=aiosqlite.Error("Test async wrapper error"),
    ):
        method = getattr(aiosqlite_async_driver, method_name)

        with pytest.raises(SQLSpecError):
            await method(*call_args)


async def test_async_driver_select_one(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async select_one method - expects error when multiple rows returned."""
    with pytest.raises(ValueError, match="Multiple results found"):
        await aiosqlite_async_driver.select_one("SELECT * FROM users")


async def test_async_driver_select_one_no_results(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async select_one method with no results."""
    with pytest.raises(NotFoundError, match="No rows found"):
        await aiosqlite_async_driver.select_one("SELECT * FROM users WHERE id = ?", 999)


async def test_async_driver_select_one_or_none(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async select_one_or_none method - expects error when multiple rows returned."""
    with pytest.raises(ValueError, match="Multiple results found"):
        await aiosqlite_async_driver.select_one_or_none("SELECT * FROM users")


async def test_async_driver_select_one_or_none_no_results(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async select_one_or_none method with no results."""
    result = await aiosqlite_async_driver.select_one_or_none("SELECT * FROM users WHERE id = ?", 999)
    assert result is None


async def test_async_driver_select(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async select method."""
    result: list[dict[str, Any]] = await aiosqlite_async_driver.select("SELECT * FROM users")

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


async def test_async_driver_select_value(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async select_value method."""
    result = await aiosqlite_async_driver.select_value("SELECT COUNT(*) FROM users")
    assert result == 2


async def test_async_driver_select_value_no_results(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async select_value method with no results."""
    with pytest.raises(NotFoundError, match="No rows found"):
        await aiosqlite_async_driver.select_value("SELECT id FROM users WHERE id = 999")


async def test_async_driver_select_value_or_none_no_results(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async select_value_or_none method with no results."""
    result = await aiosqlite_async_driver.select_value_or_none("SELECT id FROM users WHERE id = 999")
    assert result is None


@pytest.mark.parametrize(
    "parameter_style,expected_style",
    [
        pytest.param(ParameterStyle.QMARK, ParameterStyle.QMARK, id="qmark"),
        pytest.param(ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_COLON, id="named_colon"),
    ],
)
async def test_async_driver_parameter_styles(
    aiosqlite_async_driver: AiosqliteDriver, parameter_style: ParameterStyle, expected_style: ParameterStyle
) -> None:
    """Test different parameter styles are handled correctly in async driver."""
    config = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=parameter_style,
            supported_parameter_styles={parameter_style},
            default_execution_parameter_style=ParameterStyle.QMARK,
            supported_execution_parameter_styles={ParameterStyle.QMARK},
        ),
    )

    aiosqlite_async_driver.statement_config = config
    assert aiosqlite_async_driver.statement_config.parameter_config.default_parameter_style == expected_style

    if parameter_style == ParameterStyle.QMARK:
        statement = SQL("SELECT * FROM users WHERE id = ?", 1, statement_config=config)
    else:
        statement = SQL("SELECT * FROM users WHERE id = :id", {"id": 1}, statement_config=config)

    result = await aiosqlite_async_driver.dispatch_statement_execution(statement, aiosqlite_async_driver.connection)
    assert isinstance(result, SQLResult)


async def test_async_driver_different_dialects(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async driver works with different SQL dialects."""
    config = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        ),
    )

    aiosqlite_async_driver.statement_config = config
    result = await aiosqlite_async_driver.execute("SELECT 1 as test")
    assert isinstance(result, SQLResult)


async def test_async_driver_create_execution_result(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async create_execution_result method."""
    async with aiosqlite_async_driver.with_cursor(aiosqlite_async_driver.connection) as cursor:
        result = aiosqlite_async_driver.create_execution_result(
            cursor,
            selected_data=[(1,), (2,)],
            column_names=["id"],
            data_row_count=2,
            is_select_result=True,
            row_format="tuple",
        )

        assert result.is_select_result is True
        assert result.selected_data == [(1,), (2,)]
        assert result.column_names == ["id"]
        assert result.data_row_count == 2

        result = aiosqlite_async_driver.create_execution_result(cursor, rowcount_override=1)
        assert result.is_select_result is False
        assert result.rowcount_override == 1


async def test_async_driver_build_statement_result(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test async build_statement_result method."""
    statement = SQL("SELECT * FROM users", statement_config=aiosqlite_async_driver.statement_config)
    async with aiosqlite_async_driver.with_cursor(aiosqlite_async_driver.connection) as cursor:
        execution_result = aiosqlite_async_driver.create_execution_result(
            cursor,
            selected_data=[(1,)],
            column_names=["id"],
            data_row_count=1,
            is_select_result=True,
            row_format="tuple",
        )

        sql_result = aiosqlite_async_driver.build_statement_result(statement, execution_result)
        assert isinstance(sql_result, SQLResult)
        assert sql_result.operation_type == "SELECT"
        assert sql_result.get_data() == [{"id": 1}]
        assert sql_result.column_names == ["id"]


async def test_async_driver_special_handling_integration(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test that async dispatch_special_handling is called during dispatch."""
    statement = SQL("SELECT * FROM users", statement_config=aiosqlite_async_driver.statement_config)

    with patch.object(
        aiosqlite_async_driver, "dispatch_special_handling", new_callable=AsyncMock, return_value=None
    ) as mock_special:
        result = await aiosqlite_async_driver.dispatch_statement_execution(statement, aiosqlite_async_driver.connection)

        assert isinstance(result, SQLResult)
        mock_special.assert_called_once()


async def test_async_driver_error_handling_in_dispatch(aiosqlite_async_driver: AiosqliteDriver) -> None:
    """Test error handling during async statement dispatch."""
    statement = SQL("SELECT * FROM users", statement_config=aiosqlite_async_driver.statement_config)

    with patch.object(
        aiosqlite_async_driver,
        "dispatch_execute",
        new_callable=AsyncMock,
        side_effect=aiosqlite.Error("Test async error"),
    ):
        with pytest.raises(SQLSpecError):
            await aiosqlite_async_driver.dispatch_statement_execution(statement, aiosqlite_async_driver.connection)


async def test_async_driver_concurrent_execution() -> None:
    """Test concurrent execution capability of async driver."""
    conn = await aiosqlite.connect(":memory:")
    driver = AiosqliteDriver(conn)

    async def execute_query(query_id: int) -> SQLResult:
        return await driver.execute(f"SELECT {query_id} as id")

    tasks = [execute_query(i) for i in range(3)]
    results = await asyncio.gather(*tasks)

    assert len(results) == 3
    for result in results:
        assert isinstance(result, SQLResult)
        assert result.operation_type == "SELECT"
    await conn.close()
