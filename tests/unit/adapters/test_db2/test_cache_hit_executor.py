"""Unit tests for the Db2 compiled cache-hit executors."""

from typing import Any, cast

import pytest

from sqlspec.adapters.db2.core import execute_cache_hit, execute_cache_hit_async
from sqlspec.core.result._base import DMLResult
from sqlspec.exceptions import UniqueViolationError
from tests.unit.adapters.test_db2._fakes import (
    DriverMode,
    FakeDb2Connection,
    FakeDb2Cursor,
    FakeDb2IntegrityError,
    db2_description,
    db2_error,
)

SELECT_SQL = "SELECT id, name FROM users WHERE id = ?"
UPDATE_SQL = "UPDATE users SET name = ? WHERE id = ?"


async def _run_executor(db2_mode: DriverMode, driver: Any, statement: str, params: Any) -> Any:
    executor = execute_cache_hit_async if db2_mode.is_async else execute_cache_hit
    cached = cast("Any", driver._stmt_cache.get(statement))
    return await db2_mode.call(
        executor, driver.connection, statement, params, cached, driver.statement_config, driver._cache_hit_state
    )


async def test_repeated_select_runs_through_cache_hit_executor(
    db2_mode: DriverMode, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = FakeDb2Connection(
        lambda: FakeDb2Cursor(rows=[(1, "Ada")], description=db2_description("id", '"MixedCase"'))
    )
    driver = db2_mode.driver(connection)
    executor = driver._cache_hit_executor
    calls: list[tuple[Any, ...]] = []

    def recording_executor(*args: Any) -> Any:
        calls.append(args)
        return executor(*args)

    monkeypatch.setattr(driver, "_cache_hit_executor", recording_executor)

    await db2_mode.call(driver.execute, SELECT_SQL, 1)
    result = await db2_mode.call(driver.execute, SELECT_SQL, 2)

    assert len(calls) == 1
    assert result.get_data() == [{"id": 1, "MixedCase": "Ada"}]
    assert result.column_names == ["id", "MixedCase"]
    assert result.rows_affected == 1
    assert result.statement.sql == SELECT_SQL
    assert connection.cursors[-1].executed == [(SELECT_SQL, (2,))]
    assert connection.cursors[-1].closed is True


async def test_cache_hit_keeps_uppercase_columns_when_lowercasing_is_disabled(db2_mode: DriverMode) -> None:
    connection = FakeDb2Connection(lambda: FakeDb2Cursor(rows=[(1,)], description=db2_description("id")))
    driver = db2_mode.driver(connection, driver_features={"enable_lowercase_column_names": False})
    await db2_mode.call(driver.execute, "SELECT id FROM users WHERE id = ?", 1)

    result = await _run_executor(db2_mode, driver, "SELECT id FROM users WHERE id = ?", [2])

    assert result.get_data() == [{"ID": 1}]
    assert connection.cursors[-1].executed == [("SELECT id FROM users WHERE id = ?", (2,))]


async def test_cache_hit_without_parameters_executes_sql_only(db2_mode: DriverMode) -> None:
    statement = "SELECT id FROM users"
    connection = FakeDb2Connection(lambda: FakeDb2Cursor(rows=[(1,), (2,)], description=db2_description("id")))
    driver = db2_mode.driver(connection)
    await db2_mode.call(driver.execute, statement)

    result = await _run_executor(db2_mode, driver, statement, ())

    assert result.get_data() == [{"id": 1}, {"id": 2}]
    assert connection.cursors[-1].executed == [(statement, None)]


async def test_cache_hit_dml_returns_affected_rows(db2_mode: DriverMode) -> None:
    connection = FakeDb2Connection(lambda: FakeDb2Cursor(rowcount=3))
    driver = db2_mode.driver(connection)
    await db2_mode.call(driver.execute, UPDATE_SQL, "Ada", 1)

    result = await _run_executor(db2_mode, driver, UPDATE_SQL, ("Grace", 2))

    assert isinstance(result, DMLResult)
    assert result.operation_type == "UPDATE"
    assert result.rows_affected == 3
    assert connection.cursors[-1].executed == [(UPDATE_SQL, ("Grace", 2))]
    assert connection.cursors[-1].closed is True


async def test_cache_hit_maps_db2_errors_and_closes_cursor(db2_mode: DriverMode) -> None:
    cursors = [FakeDb2Cursor(rowcount=1)]
    connection = FakeDb2Connection(lambda: cursors[-1])
    driver = db2_mode.driver(connection)
    await db2_mode.call(driver.execute, UPDATE_SQL, "Ada", 1)
    failing = FakeDb2Cursor(
        error=db2_error(-803, "23505", "One or more values are not valid.", cls=FakeDb2IntegrityError)
    )
    cursors.append(failing)

    with pytest.raises(UniqueViolationError) as exc_info:
        await db2_mode.call(driver.execute, UPDATE_SQL, "Ada", 2)

    assert isinstance(exc_info.value.__cause__, FakeDb2IntegrityError)
    assert failing.closed is True


async def test_cache_hit_propagates_non_driver_errors_unchanged(db2_mode: DriverMode) -> None:
    cursors = [FakeDb2Cursor(rowcount=1)]
    connection = FakeDb2Connection(lambda: cursors[-1])
    driver = db2_mode.driver(connection)
    await db2_mode.call(driver.execute, UPDATE_SQL, "Ada", 1)
    cursors.append(FakeDb2Cursor(error=ValueError("not a driver error")))

    with pytest.raises(ValueError, match="not a driver error"):
        await _run_executor(db2_mode, driver, UPDATE_SQL, ("Ada", 2))
