"""Unit tests for the CockroachDB psycopg cache-hit executors."""

import inspect
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from psycopg import errors as pg_errors

from sqlspec.adapters.cockroach_psycopg.core import execute_cache_hit, execute_cache_hit_async
from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
from sqlspec.core import OperationProfile, ParameterProfile, ProcessedState
from sqlspec.core.cache import CachedQuery
from sqlspec.exceptions import SerializationConflictError

pytestmark = pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])


def _cached(compiled_sql: str, operation_type: Any, *, returns_rows: bool) -> CachedQuery:
    return CachedQuery(
        compiled_sql=compiled_sql,
        parameter_profile=ParameterProfile(),
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type=operation_type,
        operation_profile=OperationProfile(
            returns_rows=returns_rows, modifies_rows=not returns_rows, is_read_only=returns_rows
        ),
        param_count=0,
        processed_state=ProcessedState(
            compiled_sql=compiled_sql, execution_parameters=[], operation_type=operation_type
        ),
    )


def _connection(is_async: bool) -> MagicMock:
    cursor = MagicMock()
    if is_async:
        cursor.execute = AsyncMock()
        cursor.fetchall = AsyncMock()
        cursor.close = AsyncMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor
    return connection


def _driver(is_async: bool, connection: Any, driver_features: "dict[str, Any] | None" = None) -> Any:
    driver_class = CockroachPsycopgAsyncDriver if is_async else CockroachPsycopgSyncDriver
    return driver_class(connection, driver_features=driver_features)


async def _execute(is_async: bool, driver: Any, sql: str, cached: CachedQuery) -> Any:
    executor = execute_cache_hit_async if is_async else execute_cache_hit
    result = executor(driver.connection, sql, (), cached, driver.statement_config, driver._cache_hit_state)
    return await result if inspect.isawaitable(result) else result


async def test_execute_cache_hit_returns_rows_and_column_names(is_async: bool) -> None:
    """A row-returning statement yields the fetched rows, the described column names, and the row count."""
    connection = _connection(is_async)
    cursor = connection.cursor.return_value
    cursor.description = [SimpleNamespace(name="id"), SimpleNamespace(name="name")]
    cursor.fetchall.return_value = [(1, "alpha"), (2, "beta")]
    sql = "SELECT id, name FROM items"

    result = await _execute(is_async, _driver(is_async, connection), sql, _cached(sql, "SELECT", returns_rows=True))

    assert result.get_data() == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
    assert result.column_names == ["id", "name"]
    assert result.rows_affected == 2
    cursor.execute.assert_called_once_with(sql)
    cursor.close.assert_called_once_with()


async def test_execute_cache_hit_returns_affected_rows_for_dml(is_async: bool) -> None:
    """A statement without rows yields the cursor rowcount."""
    connection = _connection(is_async)
    cursor = connection.cursor.return_value
    cursor.rowcount = 3
    sql = "UPDATE items SET active = false"

    result = await _execute(is_async, _driver(is_async, connection), sql, _cached(sql, "UPDATE", returns_rows=False))

    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"
    cursor.fetchall.assert_not_called()


async def test_execute_cache_hit_maps_driver_errors(is_async: bool) -> None:
    """A psycopg error is raised as its SQLSpec exception with the cause kept."""
    connection = _connection(is_async)
    error = pg_errors.SerializationFailure("restart transaction")
    connection.cursor.return_value.execute.side_effect = error
    sql = "UPDATE items SET active = false"

    with pytest.raises(SerializationConflictError) as exc_info:
        await _execute(is_async, _driver(is_async, connection), sql, _cached(sql, "UPDATE", returns_rows=False))

    assert exc_info.value.__cause__ is error


@pytest.mark.parametrize(
    ("driver_features", "returns_rows"),
    [
        ({"read_only_session": True}, True),
        ({"read_only_session": True}, False),
        ({"enable_follower_reads": True}, True),
    ],
    ids=["read-only-session-read", "read-only-session-write", "follower-read"],
)
async def test_execute_cache_hit_declines_follower_and_read_only_sessions(
    is_async: bool, driver_features: "dict[str, Any]", returns_rows: bool
) -> None:
    """Statements that need a follower-read transaction or a read-only check are declined untouched."""
    connection = _connection(is_async)
    cached = _cached("SELECT 1", "SELECT" if returns_rows else "UPDATE", returns_rows=returns_rows)

    result = await _execute(is_async, _driver(is_async, connection, driver_features), "SELECT 1", cached)

    assert result is None
    connection.cursor.assert_not_called()


async def test_execute_cache_hit_runs_writes_on_follower_read_sessions(is_async: bool) -> None:
    """A modifying statement on a follower-read session executes directly."""
    connection = _connection(is_async)
    connection.cursor.return_value.rowcount = 4
    sql = "DELETE FROM items"
    driver = _driver(is_async, connection, {"enable_follower_reads": True})

    result = await _execute(is_async, driver, sql, _cached(sql, "DELETE", returns_rows=False))

    assert result.rows_affected == 4
