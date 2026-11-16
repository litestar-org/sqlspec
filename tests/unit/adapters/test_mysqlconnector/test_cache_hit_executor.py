"""Unit tests for the mysql-connector cache-hit executors."""

from inspect import isawaitable
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.mysqlconnector._typing import MysqlConnectorError
from sqlspec.adapters.mysqlconnector.core import default_statement_config, execute_cache_hit, execute_cache_hit_async
from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver
from sqlspec.core import OperationProfile, ParameterProfile, ProcessedState
from sqlspec.core.cache import CachedQuery
from sqlspec.exceptions import DeadlockError
from sqlspec.utils.serializers import from_json

JSON_TYPE_CODE = 245

pytestmark = pytest.mark.parametrize("mode", ["sync", "async"])


def _state(cursor_options: "dict[str, Any] | None" = None) -> "tuple[Any, set[int], dict[str, Any] | None]":
    return (from_json, {JSON_TYPE_CODE}, cursor_options)


def _cached(compiled_sql: str, operation_type: Any, *, returns_rows: bool, param_count: int = 0) -> CachedQuery:
    return CachedQuery(
        compiled_sql=compiled_sql,
        parameter_profile=ParameterProfile(),
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type=operation_type,
        operation_profile=OperationProfile(returns_rows=returns_rows, modifies_rows=not returns_rows),
        param_count=param_count,
        processed_state=ProcessedState(
            compiled_sql=compiled_sql, execution_parameters=[], operation_type=operation_type
        ),
    )


def _mock(mode: str, **kwargs: Any) -> Any:
    return AsyncMock(**kwargs) if mode == "async" else MagicMock(**kwargs)


def _cursor(mode: str, *, rows: "list[Any] | None" = None, error: "BaseException | None" = None) -> MagicMock:
    cursor = MagicMock()
    cursor.execute = _mock(mode, side_effect=error)
    cursor.fetchall = _mock(mode, return_value=rows or [])
    cursor.close = _mock(mode)
    return cursor


def _connection(mode: str, cursor: MagicMock, *, unread_result: bool = False) -> MagicMock:
    connection = MagicMock(_cnx=None, unread_result=unread_result)
    connection.cursor = _mock(mode, return_value=cursor)
    connection.consume_results = _mock(mode)
    return connection


async def _execute(mode: str, connection: Any, sql: str, params: Any, cached: CachedQuery, state: Any) -> Any:
    executor = execute_cache_hit_async if mode == "async" else execute_cache_hit
    result = executor(connection, sql, params, cached, default_statement_config, state)
    return await result if isawaitable(result) else result


async def test_execute_cache_hit_returns_rows_with_json_decoded(mode: str) -> None:
    """A row-returning statement yields decoded tuple rows, column names, and the row count."""
    cursor = _cursor(mode, rows=[(1, '{"name": "alpha"}'), (2, None)])
    cursor.description = [("id", 3), ("payload", JSON_TYPE_CODE)]
    connection = _connection(mode, cursor)
    sql = "SELECT id, payload FROM items WHERE id > %s"
    cached = _cached(sql, "SELECT", returns_rows=True, param_count=1)

    result = await _execute(mode, connection, sql, (0,), cached, _state())

    assert result.data == [(1, {"name": "alpha"}), (2, None)]
    assert result.column_names == ["id", "payload"]
    assert result.rows_affected == 2
    connection.cursor.assert_called_once_with()
    cursor.execute.assert_called_once_with(sql, (0,))
    cursor.close.assert_called_once_with()
    connection.consume_results.assert_not_called()


async def test_execute_cache_hit_returns_affected_rows_for_dml(mode: str) -> None:
    """A statement without rows yields the cursor rowcount and omits parameters when none are bound."""
    cursor = _cursor(mode)
    cursor.rowcount = 3
    connection = _connection(mode, cursor)
    sql = "UPDATE items SET active = 0"
    cached = _cached(sql, "UPDATE", returns_rows=False)

    result = await _execute(mode, connection, sql, (), cached, _state())

    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"
    assert result.last_inserted_id is None
    cursor.execute.assert_called_once_with(sql)
    cursor.fetchall.assert_not_called()
    cursor.close.assert_called_once_with()


async def test_execute_cache_hit_forwards_cursor_options_and_keeps_dict_rows(mode: str) -> None:
    """Session cursor options reach the cursor factory and dictionary rows keep their shape."""
    cursor = _cursor(mode, rows=[{"id": 1, "payload": '{"name": "alpha"}'}])
    cursor.description = [("id", 3), ("payload", JSON_TYPE_CODE)]
    connection = _connection(mode, cursor)
    sql = "SELECT id, payload FROM items"
    cached = _cached(sql, "SELECT", returns_rows=True)

    result = await _execute(mode, connection, sql, (), cached, _state({"dictionary": True}))

    assert result.data == [{"id": 1, "payload": {"name": "alpha"}}]
    connection.cursor.assert_called_once_with(dictionary=True)


async def test_execute_cache_hit_maps_driver_errors_after_cursor_cleanup(mode: str) -> None:
    """A mysql-connector error is mapped after unread results are consumed and the cursor closes."""
    error = MysqlConnectorError(errno=1213, msg="Deadlock found when trying to get lock")
    cursor = _cursor(mode, error=error)
    connection = _connection(mode, cursor, unread_result=True)
    sql = "UPDATE items SET active = 0"
    cached = _cached(sql, "UPDATE", returns_rows=False)

    with pytest.raises(DeadlockError) as exc_info:
        await _execute(mode, connection, sql, (), cached, _state())

    assert exc_info.value.__cause__ is error
    connection.consume_results.assert_called_once_with()
    cursor.close.assert_called_once_with()


async def test_execute_cache_hit_propagates_unmapped_errors_and_ignores_close_failures(mode: str) -> None:
    """An error outside the mysql-connector hierarchy propagates unchanged even when cleanup fails."""
    cursor = _cursor(mode, error=RuntimeError("execute boom"))
    cursor.close = _mock(mode, side_effect=RuntimeError("close boom"))
    connection = _connection(mode, cursor, unread_result=True)
    connection.consume_results = _mock(mode, side_effect=RuntimeError("consume boom"))
    sql = "SELECT 1"
    cached = _cached(sql, "SELECT", returns_rows=True)

    with pytest.raises(RuntimeError, match="execute boom"):
        await _execute(mode, connection, sql, (), cached, _state())

    cursor.close.assert_called_once_with()


async def test_driver_runs_repeated_statement_through_executor(mode: str) -> None:
    """A repeated raw statement on the driver returns the same rows on the cached execution."""
    cursor = _cursor(mode, rows=[(1,)])
    cursor.description = [("value", 3)]
    connection = _connection(mode, cursor)
    connection.in_transaction = False
    driver_type: Any = MysqlConnectorAsyncDriver if mode == "async" else MysqlConnectorSyncDriver
    driver = driver_type(cast("Any", connection), driver_features={"cursor_options": {"buffered": True}})

    first = driver.execute("SELECT 1 AS value")
    first = await first if isawaitable(first) else first
    second = driver.execute("SELECT 1 AS value")
    second = await second if isawaitable(second) else second

    assert driver._cache_hit_executor is (execute_cache_hit_async if mode == "async" else execute_cache_hit)
    assert first.data == [(1,)]
    assert second.data == [(1,)]
    assert second.column_names == ["value"]
    assert [call.kwargs for call in connection.cursor.call_args_list] == [{"buffered": True}, {"buffered": True}]
    assert cursor.close.call_count == 2


async def test_execute_cache_hit_matches_driver_cache_hit_method(mode: str) -> None:
    """The executor and the driver cache-hit method produce the same rows, columns, and count."""
    cursor = _cursor(mode, rows=[(1, '{"name": "alpha"}')])
    cursor.description = [("id", 3), ("payload", JSON_TYPE_CODE)]
    connection = _connection(mode, cursor)
    connection.in_transaction = False
    driver_type: Any = MysqlConnectorAsyncDriver if mode == "async" else MysqlConnectorSyncDriver
    driver = driver_type(cast("Any", connection))
    sql = "SELECT id, payload FROM items"
    cached = _cached(sql, "SELECT", returns_rows=True)

    expected = driver._execute_cache_hit(sql, (), cached)
    expected = await expected if isawaitable(expected) else expected
    result = await _execute(mode, connection, sql, (), cached, driver._cache_hit_state)

    assert result.data == expected.data
    assert result.column_names == expected.column_names == ["id", "payload"]
    assert result.rows_affected == expected.rows_affected == 1
    assert cursor.execute.call_args_list[0] == cursor.execute.call_args_list[1]
