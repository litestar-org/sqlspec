"""Unit tests for the mssql-python compiled cache-hit executor."""

from typing import Any, cast

import pytest
from mssql_python import Error as MssqlPythonError

from sqlspec.adapters.mssql_python.core import execute_cache_hit
from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver
from sqlspec.core.result._base import DMLResult
from sqlspec.exceptions import DeadlockError

SELECT_SQL = "SELECT id, name FROM dbo.users WHERE id = ?"
SELECT_BY_NAME_SQL = "SELECT id, name FROM dbo.users WHERE name = ?"
UPDATE_SQL = "UPDATE dbo.users SET name = ? WHERE id = ?"


class _Row:
    """Iterable mssql-python style row that is not a tuple."""

    def __init__(self, *values: Any) -> None:
        self.values = values

    def __iter__(self) -> Any:
        return iter(self.values)


class _Cursor:
    """Scripted cursor recording every execute call."""

    def __init__(
        self,
        rows: "list[Any] | None" = None,
        description: "list[tuple[str]] | None" = None,
        rowcount: int = -1,
        error: "BaseException | None" = None,
    ) -> None:
        self.rows = rows or []
        self.description = description
        self.rowcount = rowcount
        self.error = error
        self.closed = False
        self.executed: list[tuple[Any, ...]] = []

    def execute(self, *args: Any) -> None:
        self.executed.append(args)
        if self.error is not None:
            raise self.error

    def fetchall(self) -> "list[Any]":
        return list(self.rows)

    def close(self) -> None:
        self.closed = True


class _Connection:
    """Connection handing out one scripted cursor."""

    def __init__(self, cursor: _Cursor) -> None:
        self.cursor_obj = cursor
        self.autocommit = True

    def cursor(self) -> _Cursor:
        return self.cursor_obj


def _driver(cursor: _Cursor) -> "tuple[MssqlPythonDriver, _Connection]":
    connection = _Connection(cursor)
    return MssqlPythonDriver(cast("Any", connection)), connection


def _run_executor(driver: MssqlPythonDriver, statement: str, params: Any) -> Any:
    cached = cast("Any", driver._stmt_cache.get(statement))
    return execute_cache_hit(
        driver.connection, statement, params, cached, driver.statement_config, driver._cache_hit_state
    )


def test_repeated_select_runs_through_cache_hit_executor(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _Cursor(rows=[_Row(1, "Ada")], description=[("id",), ("name",)])
    driver, _ = _driver(cursor)
    outcomes: list[Any] = []

    def recording_executor(*args: Any) -> Any:
        outcome = execute_cache_hit(*args)
        outcomes.append(outcome)
        return outcome

    monkeypatch.setattr(driver, "_cache_hit_executor", recording_executor)

    driver.execute(SELECT_BY_NAME_SQL, "Ada")
    result = driver.execute(SELECT_BY_NAME_SQL, "Grace")

    assert len(outcomes) == 1
    assert outcomes[0] is result
    assert result.get_data() == [{"id": 1, "name": "Ada"}]
    assert result.column_names == ["id", "name"]
    assert result.rows_affected == 1
    assert result.statement.sql == SELECT_BY_NAME_SQL
    assert cursor.executed[-1] == (SELECT_BY_NAME_SQL, ("Grace",))
    assert cursor.closed is True


def test_cache_hit_without_parameters_executes_sql_only() -> None:
    statement = "SELECT id, name FROM dbo.users"
    cursor = _Cursor(rows=[(1, "Ada"), (2, "Grace")], description=[("id",), ("name",)])
    driver, _ = _driver(cursor)
    driver.execute(statement)

    result = _run_executor(driver, statement, ())

    assert result.get_data() == [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
    assert cursor.executed[-1] == (statement,)


def test_cache_hit_normalizes_list_parameters_to_tuple() -> None:
    cursor = _Cursor(rows=[(1, "Ada")], description=[("id",), ("name",)])
    driver, _ = _driver(cursor)
    driver.execute(SELECT_SQL, 1)

    _run_executor(driver, SELECT_SQL, [7])

    assert cursor.executed[-1] == (SELECT_SQL, (7,))


def test_cache_hit_dml_returns_affected_rows() -> None:
    cursor = _Cursor(rowcount=3)
    driver, _ = _driver(cursor)
    driver.execute(UPDATE_SQL, "Ada", 1)

    result = _run_executor(driver, UPDATE_SQL, ("Grace", 2))

    assert isinstance(result, DMLResult)
    assert result.operation_type == "UPDATE"
    assert result.rows_affected == 3
    assert cursor.executed[-1] == (UPDATE_SQL, ("Grace", 2))
    assert cursor.closed is True


def test_cache_hit_maps_mssql_python_errors_and_closes_cursor() -> None:
    driver, connection = _driver(_Cursor(rowcount=1))
    driver.execute(UPDATE_SQL, "Ada", 1)
    failing = _Cursor(error=MssqlPythonError("Transaction was deadlocked (1205)", "40001"))
    connection.cursor_obj = failing

    with pytest.raises(DeadlockError) as exc_info:
        driver.execute(UPDATE_SQL, "Ada", 2)

    assert isinstance(exc_info.value.__cause__, MssqlPythonError)
    assert failing.closed is True


def test_cache_hit_propagates_non_driver_errors_unchanged() -> None:
    driver, connection = _driver(_Cursor(rowcount=1))
    driver.execute(UPDATE_SQL, "Ada", 1)
    connection.cursor_obj = _Cursor(error=ValueError("not a driver error"))

    with pytest.raises(ValueError, match="not a driver error"):
        _run_executor(driver, UPDATE_SQL, ("Ada", 2))
