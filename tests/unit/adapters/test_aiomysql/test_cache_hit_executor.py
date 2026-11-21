"""Unit tests for the aiomysql cache-hit executor."""

from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.aiomysql.core import default_statement_config, execute_cache_hit
from sqlspec.adapters.aiomysql.driver import AiomysqlDriver
from sqlspec.core import OperationProfile, ParameterProfile, ProcessedState
from sqlspec.core.cache import CachedQuery
from sqlspec.exceptions import DeadlockError
from sqlspec.utils.serializers import from_json

pytest.importorskip("aiomysql", reason="aiomysql adapter requires the aiomysql package")

import pymysql  # pyright: ignore

JSON_TYPE_CODE = 245
STATE: "tuple[Any, set[int]]" = (from_json, {JSON_TYPE_CODE})


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


def _cursor() -> MagicMock:
    cursor = MagicMock()
    cursor.execute = AsyncMock(return_value=None)
    cursor.fetchall = AsyncMock(return_value=())
    cursor.close = AsyncMock(return_value=None)
    return cursor


def _connection(cursor: MagicMock) -> MagicMock:
    connection = MagicMock()
    connection.cursor = AsyncMock(return_value=cursor)
    return connection


async def test_execute_cache_hit_returns_rows_with_json_decoded() -> None:
    """A row-returning statement yields decoded tuple rows, column names, and the row count."""
    cursor = _cursor()
    cursor.description = [("id", 3), ("payload", JSON_TYPE_CODE)]
    cursor.fetchall = AsyncMock(return_value=((1, '{"name": "alpha"}'), (2, None)))
    connection = _connection(cursor)
    sql = "SELECT id, payload FROM items WHERE id > %s"
    cached = _cached(sql, "SELECT", returns_rows=True, param_count=1)

    result = await execute_cache_hit(connection, sql, (0,), cached, default_statement_config, STATE)

    assert result is not None
    assert result.data == [(1, {"name": "alpha"}), (2, None)]
    assert result.column_names == ["id", "payload"]
    assert result.rows_affected == 2
    cursor.execute.assert_awaited_once_with(sql, (0,))
    cursor.close.assert_awaited_once_with()


async def test_execute_cache_hit_returns_affected_rows_for_dml() -> None:
    """A statement without rows yields the cursor rowcount and omits parameters when none are bound."""
    cursor = _cursor()
    cursor.rowcount = 3
    connection = _connection(cursor)
    sql = "UPDATE items SET active = 0"
    cached = _cached(sql, "UPDATE", returns_rows=False)

    result = await execute_cache_hit(connection, sql, (), cached, default_statement_config, STATE)

    assert result is not None
    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"
    assert result.last_inserted_id is None
    cursor.execute.assert_awaited_once_with(sql)
    cursor.fetchall.assert_not_awaited()
    cursor.close.assert_awaited_once_with()


async def test_execute_cache_hit_maps_driver_errors_and_closes_cursor() -> None:
    """An aiomysql error is mapped to its SQLSpec exception after the cursor closes."""
    cursor = _cursor()
    error = pymysql.err.OperationalError(1213, "Deadlock found")
    cursor.execute.side_effect = error
    connection = _connection(cursor)
    sql = "UPDATE items SET active = 0"
    cached = _cached(sql, "UPDATE", returns_rows=False)

    with pytest.raises(DeadlockError) as exc_info:
        await execute_cache_hit(connection, sql, (), cached, default_statement_config, STATE)

    assert exc_info.value.__cause__ is error
    cursor.close.assert_awaited_once_with()


async def test_execute_cache_hit_propagates_unmapped_errors_and_ignores_close_failures() -> None:
    """An error outside the aiomysql hierarchy propagates unchanged even when closing the cursor fails."""
    cursor = _cursor()
    cursor.execute.side_effect = RuntimeError("execute boom")
    cursor.close.side_effect = RuntimeError("close boom")
    connection = _connection(cursor)
    sql = "SELECT 1"
    cached = _cached(sql, "SELECT", returns_rows=True)

    with pytest.raises(RuntimeError, match="execute boom"):
        await execute_cache_hit(connection, sql, (), cached, default_statement_config, STATE)


async def test_execute_cache_hit_declines_literal_percent_with_parameters() -> None:
    """A literal percent beside bound parameters declines before a cursor is opened."""
    connection = _connection(_cursor())
    sql = "SELECT '%s', %s"
    cached = _cached(sql, "SELECT", returns_rows=True, param_count=1)

    result = await execute_cache_hit(connection, sql, (1,), cached, default_statement_config, STATE)

    assert result is None
    connection.cursor.assert_not_called()


async def test_driver_runs_repeated_statement_through_executor() -> None:
    """A repeated raw statement on the driver returns the same rows on the cached execution."""
    cursor = _cursor()
    cursor.description = [("value", 3)]
    cursor.fetchall = AsyncMock(return_value=((1,),))
    connection = _connection(cursor)
    connection.server_status = 0
    driver = AiomysqlDriver(cast("Any", connection))

    first = await driver.execute("SELECT 1 AS value")
    second = await driver.execute("SELECT 1 AS value")

    assert driver._cache_hit_executor is execute_cache_hit
    assert first.data == [(1,)]
    assert second.data == [(1,)]
    assert second.column_names == ["value"]
    assert cursor.execute.call_count == 2
    assert cursor.close.call_count == 2


async def test_execute_cache_hit_matches_driver_cache_hit_method() -> None:
    """The executor and the driver cache-hit method produce the same rows, columns, and count."""
    cursor = _cursor()
    cursor.description = [("id", 3), ("payload", JSON_TYPE_CODE)]
    cursor.fetchall = AsyncMock(side_effect=[((1, '{"name": "alpha"}'),), ((1, '{"name": "alpha"}'),)])
    connection = _connection(cursor)
    connection.server_status = 0
    driver = AiomysqlDriver(cast("Any", connection))
    sql = "SELECT id, payload FROM items"
    cached = _cached(sql, "SELECT", returns_rows=True)

    expected = await driver._execute_cache_hit(sql, (), cached)
    result = await execute_cache_hit(connection, sql, (), cached, driver.statement_config, driver._cache_hit_state)

    assert result is not None
    assert result.data == expected.data == [(1, {"name": "alpha"})]
    assert result.column_names == expected.column_names
    assert result.rows_affected == expected.rows_affected
    assert cursor.execute.call_args_list[0] == cursor.execute.call_args_list[1]


@pytest.mark.parametrize(
    ("operation", "sql", "rowcount", "lastrowid", "expected_rows_affected", "expected_last_id"),
    [
        ("INSERT", "INSERT INTO items VALUES (%s)", 1, 42, 1, 42),
        ("INSERT", "INSERT INTO items VALUES (%s)", 2, 0, 2, 0),
        ("INSERT", "INSERT INTO items VALUES (%s)", 0, 42, 0, None),
        ("INSERT", "INSERT INTO items VALUES (%s)", 1, True, 1, None),
        ("INSERT", "INSERT INTO items VALUES (%s)", True, 42, 0, None),
        ("UPDATE", "UPDATE items SET active = %s", 1, 42, 1, None),
        ("DELETE", "DELETE FROM items WHERE active = %s", 1, 42, 1, None),
    ],
    ids=["insert-rowid", "insert-zero-id", "insert-no-rows", "insert-bool-id", "insert-bool-rc", "update", "delete"],
)
async def test_execute_cache_hit_preserves_insert_last_inserted_id(
    operation: str, sql: str, rowcount: Any, lastrowid: Any, expected_rows_affected: int, expected_last_id: Any
) -> None:
    """Cached DML populates last_inserted_id only for non-bool successful INSERT operations."""
    cursor = _cursor()
    cursor.rowcount = rowcount
    cursor.lastrowid = lastrowid
    connection = _connection(cursor)
    cached = _cached(sql, operation, returns_rows=False, param_count=1)

    result = await execute_cache_hit(connection, sql, (1,), cached, default_statement_config, STATE)

    assert result is not None
    assert type(result.rows_affected) is int
    assert result.rows_affected == expected_rows_affected
    assert (
        result.last_inserted_id is expected_last_id
        if expected_last_id is None
        else result.last_inserted_id == expected_last_id
    )
