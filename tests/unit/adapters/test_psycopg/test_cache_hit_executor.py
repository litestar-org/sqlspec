"""Unit tests for the psycopg cache-hit executors."""

import inspect
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from psycopg import errors as pg_errors

from sqlspec.adapters.psycopg.core import default_statement_config, execute_cache_hit, execute_cache_hit_async
from sqlspec.core import OperationProfile, ParameterProfile, ParameterStyle, ProcessedState
from sqlspec.core.cache import CachedQuery
from sqlspec.core.parameters import ParameterInfo
from sqlspec.exceptions import UniqueViolationError

pytestmark = pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])


def _cached(compiled_sql: str, operation_type: Any, *, returns_rows: bool, param_count: int = 0) -> CachedQuery:
    parameters = [
        ParameterInfo(None, ParameterStyle.POSITIONAL_PYFORMAT, position, position, "%s")
        for position in range(param_count)
    ]
    return CachedQuery(
        compiled_sql=compiled_sql,
        parameter_profile=ParameterProfile(parameters),
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


def _cursor(is_async: bool) -> MagicMock:
    cursor = MagicMock()
    if is_async:
        cursor.execute = AsyncMock()
        cursor.fetchall = AsyncMock()
        cursor.close = AsyncMock()
    return cursor


def _connection(cursor: MagicMock) -> MagicMock:
    connection = MagicMock()
    connection.cursor.return_value = cursor
    return connection


async def _execute(is_async: bool, connection: Any, sql: str, params: Any, cached: CachedQuery) -> Any:
    executor = execute_cache_hit_async if is_async else execute_cache_hit
    result = executor(connection, sql, params, cached, default_statement_config, None)
    return await result if inspect.isawaitable(result) else result


async def test_execute_cache_hit_returns_rows_and_column_names(is_async: bool) -> None:
    """A row-returning statement yields the fetched rows, the described column names, and the row count."""
    cursor = _cursor(is_async)
    cursor.description = [SimpleNamespace(name="id"), SimpleNamespace(name="name")]
    cursor.fetchall.return_value = [(1, "alpha"), (2, "beta")]
    connection = _connection(cursor)
    sql = "SELECT id, name FROM items WHERE id > %s"
    cached = _cached(sql, "SELECT", returns_rows=True, param_count=1)

    result = await _execute(is_async, connection, sql, (0,), cached)

    assert result.get_data() == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
    assert result.column_names == ["id", "name"]
    assert result.rows_affected == 2
    cursor.execute.assert_called_once_with(sql, (0,))
    cursor.close.assert_called_once_with()


async def test_execute_cache_hit_returns_affected_rows_for_dml(is_async: bool) -> None:
    """A statement without rows yields the cursor rowcount and omits parameters when none are bound."""
    cursor = _cursor(is_async)
    cursor.rowcount = 3
    connection = _connection(cursor)
    sql = "UPDATE items SET active = false"
    cached = _cached(sql, "UPDATE", returns_rows=False)

    result = await _execute(is_async, connection, sql, (), cached)

    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"
    cursor.execute.assert_called_once_with(sql)
    cursor.fetchall.assert_not_called()
    cursor.close.assert_called_once_with()


async def test_execute_cache_hit_maps_driver_errors_and_closes_cursor(is_async: bool) -> None:
    """A psycopg error is raised as its SQLSpec exception with the cause kept and the cursor closed."""
    cursor = _cursor(is_async)
    error = pg_errors.UniqueViolation("duplicate key value violates unique constraint")
    cursor.execute.side_effect = error
    connection = _connection(cursor)
    sql = "INSERT INTO items (id) VALUES (%s)"
    cached = _cached(sql, "INSERT", returns_rows=False, param_count=1)

    with pytest.raises(UniqueViolationError) as exc_info:
        await _execute(is_async, connection, sql, (1,), cached)

    assert exc_info.value.__cause__ is error
    cursor.close.assert_called_once_with()


async def test_execute_cache_hit_propagates_unmapped_errors_and_ignores_close_failures(is_async: bool) -> None:
    """An error outside the psycopg hierarchy propagates unchanged even when closing the cursor fails."""
    cursor = _cursor(is_async)
    cursor.execute.side_effect = RuntimeError("execute boom")
    cursor.close.side_effect = RuntimeError("close boom")
    connection = _connection(cursor)
    cached = _cached("SELECT 1", "SELECT", returns_rows=True)

    with pytest.raises(RuntimeError, match="execute boom"):
        await _execute(is_async, connection, "SELECT 1", (), cached)


@pytest.mark.parametrize(
    ("sql", "params", "param_count"),
    [("SELECT 10 % 3 AS value", (), 0), ("SELECT %s AS value WHERE name LIKE '100%'", (1,), 1)],
    ids=["no-parameters", "with-parameters"],
)
async def test_execute_cache_hit_declines_literal_percent(
    is_async: bool, sql: str, params: "tuple[Any, ...]", param_count: int
) -> None:
    """Compiled SQL holding a percent sign that is not a placeholder is declined before a cursor is opened."""
    connection = _connection(_cursor(is_async))
    cached = _cached(sql, "SELECT", returns_rows=True, param_count=param_count)

    result = await _execute(is_async, connection, sql, params, cached)

    assert result is None
    connection.cursor.assert_not_called()
