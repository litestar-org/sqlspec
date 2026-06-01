"""Unit tests for MySQL-family adapter core helpers."""

import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.aiomysql.driver import AiomysqlDriver
from sqlspec.adapters.asyncmy.driver import AsyncmyDriver
from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver
from sqlspec.adapters.pymysql.driver import PyMysqlDriver
from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool
from sqlspec.exceptions import SQLSpecError

MODULE_PATHS = (
    "sqlspec.adapters.aiomysql.core",
    "sqlspec.adapters.asyncmy.core",
    "sqlspec.adapters.mysqlconnector.core",
    "sqlspec.adapters.pymysql.core",
)
ASYNC_MYSQL_MODULE_PATHS = ("sqlspec.adapters.aiomysql.core", "sqlspec.adapters.asyncmy.core")


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_resolve_many_rowcount_prefers_driver_rowcount(module_path: str) -> None:
    core = importlib.import_module(module_path)
    cursor = SimpleNamespace(rowcount=7)
    assert core.resolve_many_rowcount(cursor, [{"id": 1}], fallback_count=1) == 7


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_resolve_many_rowcount_falls_back_to_parameter_count(module_path: str) -> None:
    core = importlib.import_module(module_path)
    cursor = SimpleNamespace(rowcount=0)
    assert core.resolve_many_rowcount(cursor, [{"id": 1}, {"id": 2}, {"id": 3}]) == 3


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_collect_rows_accepts_precomputed_column_names(module_path: str) -> None:
    core = importlib.import_module(module_path)
    description = [("id", 3), ("payload", 245)]
    column_names = ["id", "payload"]
    rows = [{"id": 1, "payload": json.dumps({"a": 1})}]
    (data, resolved_column_names, row_format) = core.collect_rows(
        rows, description, [1], json.loads, column_names=column_names
    )
    assert resolved_column_names is column_names
    assert row_format == "dict"
    assert data[0]["payload"] == {"a": 1}


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_detect_json_columns_uses_provided_description(module_path: str) -> None:
    core = importlib.import_module(module_path)
    description = [("id", 3), ("payload", 245)]
    cursor = SimpleNamespace(description=description)
    assert core.detect_json_columns(cursor, {245}, description=description) == [1]


@pytest.mark.parametrize("module_path", MODULE_PATHS)
def test_collect_rows_without_json_reuses_input_list(module_path: str) -> None:
    core = importlib.import_module(module_path)
    description = [("id", 3), ("name", 253)]
    column_names = ["id", "name"]
    rows = [{"id": 1, "name": "a"}]
    (data, resolved_column_names, row_format) = core.collect_rows(
        rows, description, [], json.loads, column_names=column_names
    )
    assert data is rows
    assert resolved_column_names is column_names
    assert row_format == "dict"


def test_asyncmy_uses_canonical_private_json_helper_names() -> None:
    core = importlib.import_module("sqlspec.adapters.asyncmy.core")
    assert hasattr(core, "_deserialize_json_dict_rows")
    assert hasattr(core, "_deserialize_json_tuple_rows")
    assert not hasattr(core, "_deserialize_asyncmy_json_dict_rows")
    assert not hasattr(core, "_deserialize_asyncmy_json_tuple_rows")


@pytest.mark.parametrize("module_path", ASYNC_MYSQL_MODULE_PATHS)
def test_async_mysql_core_has_keep_in_sync_comment(module_path: str) -> None:
    module_file = Path(*module_path.split(".")).with_suffix(".py")
    assert "Keep private helpers in sync with" in module_file.read_text()


def test_asyncmy_collect_rows_uses_canonical_dict_json_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    core = importlib.import_module("sqlspec.adapters.asyncmy.core")
    calls: list[tuple[list[str], list[dict[str, Any]], list[int]]] = []

    def deserialize(
        column_names: list[str],
        rows: list[dict[str, Any]],
        json_indexes: list[int],
        deserializer: object,
        *,
        logger: object | None = None,
    ) -> list[dict[str, Any]]:
        _ = (deserializer, logger)
        calls.append((column_names, rows, json_indexes))
        return rows

    monkeypatch.setattr(core, "_deserialize_json_dict_rows", deserialize)
    rows = [{"payload": json.dumps({"ok": True})}]
    (data, _columns, row_format) = core.collect_rows(rows, [("payload", 245)], [0], json.loads)
    assert row_format == "dict"
    assert data == rows
    assert calls == [(["payload"], rows, [0])]


def test_asyncmy_collect_rows_uses_canonical_tuple_json_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    core = importlib.import_module("sqlspec.adapters.asyncmy.core")
    calls: list[tuple[list[Any], list[int]]] = []

    def deserialize(
        rows: list[Any], json_indexes: list[int], deserializer: object, *, logger: object | None = None
    ) -> list[Any]:
        _ = (deserializer, logger)
        calls.append((rows, json_indexes))
        return rows

    monkeypatch.setattr(core, "_deserialize_json_tuple_rows", deserialize)
    rows = [(json.dumps({"ok": True}),)]
    (data, _columns, row_format) = core.collect_rows(rows, [("payload", 245)], [0], json.loads)
    assert row_format == "tuple"
    assert data == rows
    assert calls == [(rows, [0])]


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
def test_mysql_execute_script_sync_mysql_execute_script_multi_statement_with_params_raises(
    driver_factory: type[Any],
) -> None:
    driver = driver_factory(connection=MagicMock())
    statement = _make_statement(driver)
    with patch.object(driver_factory, "_get_compiled_sql", return_value=(_MULTI_STMT_SQL, (42,))):
        with pytest.raises(SQLSpecError, match=_ERROR_MESSAGE):
            driver.dispatch_execute_script(_make_sync_cursor(), statement)


@pytest.mark.parametrize("driver_factory", [PyMysqlDriver, MysqlConnectorSyncDriver])
def test_mysql_execute_script_sync_mysql_execute_script_single_statement_with_params_executes(
    driver_factory: type[Any],
) -> None:
    driver = driver_factory(connection=MagicMock())
    statement = _make_statement(driver)
    cursor = _make_sync_cursor()
    with patch.object(driver_factory, "_get_compiled_sql", return_value=("SELECT 1", (42,))):
        result = driver.dispatch_execute_script(cursor, statement)
    assert result.statement_count == 1
    cursor.execute.assert_called_once()


@pytest.mark.parametrize("driver_factory", [PyMysqlDriver, MysqlConnectorSyncDriver])
def test_mysql_execute_script_sync_mysql_execute_script_multi_statement_without_params_executes(
    driver_factory: type[Any],
) -> None:
    driver = driver_factory(connection=MagicMock())
    statement = _make_statement(driver)
    cursor = _make_sync_cursor()
    with patch.object(driver_factory, "_get_compiled_sql", return_value=(_MULTI_STMT_SQL, None)):
        result = driver.dispatch_execute_script(cursor, statement)
    assert result.statement_count == 2
    assert result.successful_statements == 2


@pytest.mark.parametrize("driver_factory", [AiomysqlDriver, AsyncmyDriver, MysqlConnectorAsyncDriver])
async def test_mysql_execute_script_async_mysql_execute_script_multi_statement_with_params_raises(
    driver_factory: type[Any],
) -> None:
    driver = driver_factory(connection=AsyncMock())
    statement = _make_statement(driver)
    with patch.object(driver_factory, "_get_compiled_sql", return_value=(_MULTI_STMT_SQL, (42,))):
        with pytest.raises(SQLSpecError, match=_ERROR_MESSAGE):
            await driver.dispatch_execute_script(_make_async_cursor(), statement)


@pytest.mark.parametrize("driver_factory", [AiomysqlDriver, AsyncmyDriver, MysqlConnectorAsyncDriver])
async def test_mysql_execute_script_async_mysql_execute_script_single_statement_with_params_executes(
    driver_factory: type[Any],
) -> None:
    driver = driver_factory(connection=AsyncMock())
    statement = _make_statement(driver)
    cursor = _make_async_cursor()
    with patch.object(driver_factory, "_get_compiled_sql", return_value=("SELECT 1", (42,))):
        result = await driver.dispatch_execute_script(cursor, statement)
    assert result.statement_count == 1
    cursor.execute.assert_awaited_once()


@pytest.mark.parametrize("driver_factory", [AiomysqlDriver, AsyncmyDriver, MysqlConnectorAsyncDriver])
async def test_mysql_execute_script_async_mysql_execute_script_multi_statement_without_params_executes(
    driver_factory: type[Any],
) -> None:
    driver = driver_factory(connection=AsyncMock())
    statement = _make_statement(driver)
    cursor = _make_async_cursor()
    with patch.object(driver_factory, "_get_compiled_sql", return_value=(_MULTI_STMT_SQL, None)):
        result = await driver.dispatch_execute_script(cursor, statement)
    assert result.statement_count == 2
    assert result.successful_statements == 2


class _PyMysqlConnectionPoolStub(PyMysqlConnectionPool):
    __slots__ = ("closed", "mock_connection")

    def __init__(self, mock_connection: MagicMock) -> None:
        super().__init__({"database": "testdb"})
        self.mock_connection = mock_connection
        self.closed: list[bool] = []

    def _get_thread_connection(self) -> MagicMock:
        return self.mock_connection

    def _close_thread_connection(self) -> None:
        self.closed.append(True)


def test_pymysql_pool_get_connection_yields_connection() -> None:
    mock_conn = MagicMock()
    pool = _PyMysqlConnectionPoolStub(mock_conn)
    with pool.get_connection() as conn:
        assert conn is mock_conn


def test_pymysql_pool_get_connection_closes_thread_connection_on_exception() -> None:
    mock_conn = MagicMock()
    pool = _PyMysqlConnectionPoolStub(mock_conn)
    with pytest.raises(ValueError, match="simulated error"):
        with pool.get_connection():
            raise ValueError("simulated error")
    assert pool.closed == [True]


def test_pymysql_pool_get_connection_keeps_thread_connection_on_success() -> None:
    mock_conn = MagicMock()
    pool = _PyMysqlConnectionPoolStub(mock_conn)
    with pool.get_connection() as conn:
        assert conn is mock_conn
    assert pool.closed == []
