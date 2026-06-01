"""Unit tests for MySQL-family execute_script parameter handling."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.aiomysql.driver import AiomysqlDriver
from sqlspec.adapters.asyncmy.driver import AsyncmyDriver
from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver
from sqlspec.adapters.pymysql.driver import PyMysqlDriver
from sqlspec.exceptions import SQLSpecError

_ERROR_MESSAGE = "execute_script with parameters is not supported for multi-statement scripts"
_MULTI_STMT_SQL = "SELECT 1; SELECT 2"


def _make_statement(driver: Any) -> MagicMock:
    statement = MagicMock()
    statement.statement_config = driver.statement_config
    return statement


def _make_sync_cursor() -> MagicMock:
    cursor = MagicMock()
    cursor.rowcount = 1
    cursor.description = None
    cursor.lastrowid = None
    return cursor


def _make_async_cursor() -> AsyncMock:
    cursor = AsyncMock()
    cursor.rowcount = 1
    cursor.description = None
    cursor.lastrowid = None
    return cursor


@pytest.mark.parametrize("driver_factory", [PyMysqlDriver, MysqlConnectorSyncDriver])
def test_sync_mysql_execute_script_multi_statement_with_params_raises(driver_factory: type[Any]) -> None:
    driver = driver_factory(connection=MagicMock())
    statement = _make_statement(driver)

    with patch.object(driver_factory, "_get_compiled_sql", return_value=(_MULTI_STMT_SQL, (42,))):
        with pytest.raises(SQLSpecError, match=_ERROR_MESSAGE):
            driver.dispatch_execute_script(_make_sync_cursor(), statement)


@pytest.mark.parametrize("driver_factory", [PyMysqlDriver, MysqlConnectorSyncDriver])
def test_sync_mysql_execute_script_single_statement_with_params_executes(driver_factory: type[Any]) -> None:
    driver = driver_factory(connection=MagicMock())
    statement = _make_statement(driver)
    cursor = _make_sync_cursor()

    with patch.object(driver_factory, "_get_compiled_sql", return_value=("SELECT 1", (42,))):
        result = driver.dispatch_execute_script(cursor, statement)

    assert result.statement_count == 1
    cursor.execute.assert_called_once()


@pytest.mark.parametrize("driver_factory", [PyMysqlDriver, MysqlConnectorSyncDriver])
def test_sync_mysql_execute_script_multi_statement_without_params_executes(driver_factory: type[Any]) -> None:
    driver = driver_factory(connection=MagicMock())
    statement = _make_statement(driver)
    cursor = _make_sync_cursor()

    with patch.object(driver_factory, "_get_compiled_sql", return_value=(_MULTI_STMT_SQL, None)):
        result = driver.dispatch_execute_script(cursor, statement)

    assert result.statement_count == 2
    assert result.successful_statements == 2


@pytest.mark.parametrize("driver_factory", [AiomysqlDriver, AsyncmyDriver, MysqlConnectorAsyncDriver])
async def test_async_mysql_execute_script_multi_statement_with_params_raises(driver_factory: type[Any]) -> None:
    driver = driver_factory(connection=AsyncMock())
    statement = _make_statement(driver)

    with patch.object(driver_factory, "_get_compiled_sql", return_value=(_MULTI_STMT_SQL, (42,))):
        with pytest.raises(SQLSpecError, match=_ERROR_MESSAGE):
            await driver.dispatch_execute_script(_make_async_cursor(), statement)


@pytest.mark.parametrize("driver_factory", [AiomysqlDriver, AsyncmyDriver, MysqlConnectorAsyncDriver])
async def test_async_mysql_execute_script_single_statement_with_params_executes(driver_factory: type[Any]) -> None:
    driver = driver_factory(connection=AsyncMock())
    statement = _make_statement(driver)
    cursor = _make_async_cursor()

    with patch.object(driver_factory, "_get_compiled_sql", return_value=("SELECT 1", (42,))):
        result = await driver.dispatch_execute_script(cursor, statement)

    assert result.statement_count == 1
    cursor.execute.assert_awaited_once()


@pytest.mark.parametrize("driver_factory", [AiomysqlDriver, AsyncmyDriver, MysqlConnectorAsyncDriver])
async def test_async_mysql_execute_script_multi_statement_without_params_executes(driver_factory: type[Any]) -> None:
    driver = driver_factory(connection=AsyncMock())
    statement = _make_statement(driver)
    cursor = _make_async_cursor()

    with patch.object(driver_factory, "_get_compiled_sql", return_value=(_MULTI_STMT_SQL, None)):
        result = await driver.dispatch_execute_script(cursor, statement)

    assert result.statement_count == 2
    assert result.successful_statements == 2
