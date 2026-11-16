"""Unit tests for the pymssql compiled cache-hit executor."""

from typing import Any, cast

import pytest
from pymssql import IntegrityError as PymssqlIntegrityError

from sqlspec.adapters.pymssql._typing import PymssqlConnection
from sqlspec.adapters.pymssql.core import execute_cache_hit
from sqlspec.adapters.pymssql.driver import PymssqlDriver
from sqlspec.core.result._base import DMLResult
from sqlspec.exceptions import UniqueViolationError
from tests.unit.adapters.test_pymssql._fakes import FakeConnection, FakeCursor

SELECT_SQL = "SELECT id, name FROM dbo.users WHERE id = ?"
UPDATE_SQL = "UPDATE dbo.users SET name = ? WHERE id = ?"


def _driver(cursor: FakeCursor) -> "tuple[PymssqlDriver, FakeConnection]":
    connection = FakeConnection(cursor)
    return PymssqlDriver(cast("PymssqlConnection", connection)), connection


def _run_executor(driver: PymssqlDriver, statement: str, params: Any) -> Any:
    cached = cast("Any", driver._stmt_cache.get(statement))
    return execute_cache_hit(
        driver.connection, statement, params, cached, driver.statement_config, driver._cache_hit_state
    )


def test_repeated_select_runs_through_cache_hit_executor(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = FakeCursor(rows=[(1, "Ada")], description=[("id",), ("name",)])
    driver, _ = _driver(cursor)
    outcomes: list[Any] = []

    def recording_executor(*args: Any) -> Any:
        outcome = execute_cache_hit(*args)
        outcomes.append(outcome)
        return outcome

    monkeypatch.setattr(driver, "_cache_hit_executor", recording_executor)

    driver.execute(SELECT_SQL, 1)
    result = driver.execute(SELECT_SQL, 2)

    assert len(outcomes) == 1
    assert outcomes[0] is result
    assert result.get_data() == [{"id": 1, "name": "Ada"}]
    assert result.column_names == ["id", "name"]
    assert result.rows_affected == 1
    assert result.statement.sql == SELECT_SQL
    assert cursor.calls[-1] == ("SELECT id, name FROM dbo.users WHERE id = %s", (2,))
    assert cursor.closed is True


def test_cache_hit_select_keeps_dict_rows_and_normalizes_list_parameters() -> None:
    cursor = FakeCursor(rows=[{"id": 1, "name": "Ada"}], description=[("id",), ("name",)])
    driver, _ = _driver(cursor)
    driver.execute(SELECT_SQL, 1)

    result = _run_executor(driver, SELECT_SQL, [7])

    assert result.get_data() == [{"id": 1, "name": "Ada"}]
    assert cursor.calls[-1] == ("SELECT id, name FROM dbo.users WHERE id = %s", (7,))


def test_cache_hit_dml_returns_affected_rows() -> None:
    cursor = FakeCursor(rowcount=3)
    driver, _ = _driver(cursor)
    driver.execute(UPDATE_SQL, "Ada", 1)

    result = _run_executor(driver, UPDATE_SQL, ("Grace", 2))

    assert isinstance(result, DMLResult)
    assert result.operation_type == "UPDATE"
    assert result.rows_affected == 3
    assert cursor.calls[-1] == ("UPDATE dbo.users SET name = %s WHERE id = %s", ("Grace", 2))
    assert cursor.closed is True


def test_cache_hit_maps_pymssql_errors_and_closes_cursor() -> None:
    driver, connection = _driver(FakeCursor(rowcount=1))
    driver.execute(UPDATE_SQL, "Ada", 1)
    failing = FakeCursor(error=PymssqlIntegrityError("Violation of UNIQUE KEY constraint (2627)"))
    connection.cursor_obj = failing

    with pytest.raises(UniqueViolationError) as exc_info:
        driver.execute(UPDATE_SQL, "Ada", 2)

    assert isinstance(exc_info.value.__cause__, PymssqlIntegrityError)
    assert failing.closed is True


def test_cache_hit_propagates_non_driver_errors_unchanged() -> None:
    driver, connection = _driver(FakeCursor(rowcount=1))
    driver.execute(UPDATE_SQL, "Ada", 1)
    connection.cursor_obj = FakeCursor(error=ValueError("not a driver error"))

    with pytest.raises(ValueError, match="not a driver error"):
        _run_executor(driver, UPDATE_SQL, ("Ada", 2))


def test_cache_hit_declines_statements_without_parameters() -> None:
    statement = "SELECT id, name FROM dbo.users"
    cursor = FakeCursor(rows=[(1, "Ada")], description=[("id",), ("name",)])
    driver, _ = _driver(cursor)
    driver.execute(statement)
    executed = len(cursor.calls)

    assert _run_executor(driver, statement, ()) is None
    assert len(cursor.calls) == executed
    assert driver.execute(statement).get_data() == [{"id": 1, "name": "Ada"}]


def test_cache_hit_declines_literal_percent_and_general_path_escapes_it() -> None:
    statement = "SELECT id, name FROM dbo.users WHERE name LIKE '50%' AND id = ?"
    cursor = FakeCursor(rows=[(1, "Ada")], description=[("id",), ("name",)])
    driver, _ = _driver(cursor)
    driver.execute(statement, 1)

    assert _run_executor(driver, statement, (2,)) is None

    result = driver.execute(statement, 3)

    assert result.get_data() == [{"id": 1, "name": "Ada"}]
    assert cursor.calls[-1] == ("SELECT id, name FROM dbo.users WHERE name LIKE '50%%' AND id = %s", (3,))
