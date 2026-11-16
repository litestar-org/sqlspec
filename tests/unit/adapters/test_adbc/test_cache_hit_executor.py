"""Unit tests for the ADBC compiled cache-hit executor."""

from collections.abc import Generator
from typing import Any

import pytest

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.adapters.adbc.core import execute_cache_hit
from sqlspec.core.cache import CachedQuery
from sqlspec.exceptions import DataError, UniqueViolationError
from sqlspec.utils.correlation import CorrelationContext


class _Cursor:
    def __init__(self, rows: "list[Any]", error: "BaseException | None" = None) -> None:
        self.rows = rows
        self.error = error
        self.description = [("ID", None), ("NAME", None)]
        self.rowcount = 4
        self.executed: list[tuple[Any, ...]] = []
        self.closed = False

    def execute(self, *args: Any) -> None:
        self.executed.append(args)
        if self.error is not None:
            raise self.error

    def fetchall(self) -> "list[Any]":
        return self.rows

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor_obj = cursor
        self.cursor_kwargs: list[dict[str, Any]] = []

    def cursor(self, **kwargs: Any) -> _Cursor:
        self.cursor_kwargs.append(kwargs)
        return self.cursor_obj


@pytest.fixture
def session() -> "Generator[AdbcDriver, None, None]":
    pytest.importorskip("adbc_driver_sqlite")
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_sqlite", "uri": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE cache_hit_items (id TEXT PRIMARY KEY, name TEXT)")
        yield driver


def _executor_calls(session: AdbcDriver, monkeypatch: pytest.MonkeyPatch) -> "list[Any]":
    outcomes: list[Any] = []

    def recording_executor(*args: Any) -> Any:
        result = execute_cache_hit(*args)
        outcomes.append(result)
        return result

    monkeypatch.setattr(session, "_cache_hit_executor", recording_executor)
    return outcomes


def _cached(session: AdbcDriver, statement: str) -> CachedQuery:
    cached = session._stmt_cache.get(statement)
    assert isinstance(cached, CachedQuery)
    return cached


def test_cache_hit_select_returns_rows(session: AdbcDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    outcomes = _executor_calls(session, monkeypatch)
    session.execute("INSERT INTO cache_hit_items (id, name) VALUES ('1', 'a'), ('2', 'b')")
    statement = "SELECT id, name FROM cache_hit_items WHERE id >= ? ORDER BY id"

    session.execute(statement, ("1",))
    result = session.execute(statement, ("2",))
    empty = session.execute(statement, ("9",))

    assert result.get_data() == [{"id": "2", "name": "b"}]
    assert result.column_names == ["id", "name"]
    assert result.rows_affected == 1
    assert result.statement.sql == statement
    assert empty.get_data() == []
    assert empty.column_names == ["id", "name"]
    assert outcomes == [result, empty]


def test_cache_hit_dml_returns_cursor_rowcount(session: AdbcDriver, monkeypatch: pytest.MonkeyPatch) -> None:
    outcomes = _executor_calls(session, monkeypatch)
    statement = "INSERT INTO cache_hit_items (id, name) VALUES (?, ?)"

    session.execute(statement, ("1", "a"))
    result = session.execute(statement, ("2", "b"))

    assert outcomes == [result]
    assert result.operation_type == "INSERT"
    assert result.rows_affected == -1
    assert session.select_value("SELECT count(*) FROM cache_hit_items") == 2


def test_cache_hit_maps_adbc_errors(session: AdbcDriver) -> None:
    statement = "INSERT INTO cache_hit_items (id, name) VALUES (?, ?)"
    session.execute(statement, ("1", "a"))

    with pytest.raises(UniqueViolationError):
        session.execute(statement, ("1", "b"))


def test_cache_hit_reports_arrow_conversion_failures_as_data_errors(session: AdbcDriver) -> None:
    pa = pytest.importorskip("pyarrow")
    statement = "SELECT id, name FROM cache_hit_items WHERE id = ?"
    session.execute(statement, ("1",))
    cursor = _Cursor([], error=pa.ArrowInvalid("bad value"))

    with pytest.raises(DataError, match="could not convert a parameter or result value: bad value"):
        execute_cache_hit(
            _Connection(cursor),
            statement,
            ("1",),
            _cached(session, statement),
            session.statement_config,
            ("sqlite", {}, {}),
        )
    assert cursor.closed is True


def test_cache_hit_leaves_unmapped_errors_unchanged(session: AdbcDriver) -> None:
    statement = "SELECT id, name FROM cache_hit_items WHERE id = ?"
    session.execute(statement, ("1",))
    cursor = _Cursor([], error=RuntimeError("boom"))

    with pytest.raises(RuntimeError, match="boom"):
        execute_cache_hit(
            _Connection(cursor),
            statement,
            ("1",),
            _cached(session, statement),
            session.statement_config,
            ("sqlite", {}, {}),
        )
    assert cursor.closed is True


def test_cache_hit_declines_parameterized_db2_non_queries(session: AdbcDriver) -> None:
    statement = "INSERT INTO cache_hit_items (id, name) VALUES (?, ?)"
    session.execute(statement, ("1", "a"))
    connection = _Connection(_Cursor([]))

    result = execute_cache_hit(
        connection, statement, ("2", "b"), _cached(session, statement), session.statement_config, ("db2", {}, {})
    )

    assert result is None
    assert connection.cursor_kwargs == []


def test_cache_hit_normalizes_column_names_from_driver_features(session: AdbcDriver) -> None:
    statement = "SELECT id, name FROM cache_hit_items WHERE id = ?"
    session.execute(statement, ("1",))
    cursor = _Cursor([("1", "a")])

    result = execute_cache_hit(
        _Connection(cursor),
        statement,
        ("1",),
        _cached(session, statement),
        session.statement_config,
        ("db2", {"enable_lowercase_column_names": True}, {}),
    )

    assert result is not None
    assert result.column_names == ["id", "name"]
    assert result.get_data() == [{"id": "1", "name": "a"}]
    assert cursor.executed == [(statement, ("1",))]
    assert cursor.closed is True


def test_cache_hit_tags_snowflake_cursors_with_query_tag_and_correlation(session: AdbcDriver) -> None:
    statement = "SELECT id, name FROM cache_hit_items"
    session.execute(statement)
    connection = _Connection(_Cursor([("1", "a")]))
    state: tuple[str, dict[str, Any], dict[int, Any]] = ("snowflake", {"query_tag": " etl "}, {})

    with CorrelationContext.context("req-1"):
        result = execute_cache_hit(
            connection, statement, (), _cached(session, statement), session.statement_config, state
        )
    execute_cache_hit(connection, statement, (), _cached(session, statement), session.statement_config, state)

    assert result is not None
    assert result.rows_affected == 1
    assert connection.cursor_obj.executed == [(statement,), (statement,)]
    assert connection.cursor_kwargs == [
        {"adbc_stmt_kwargs": {"adbc.snowflake.statement.query_tag": "etl:req-1"}},
        {"adbc_stmt_kwargs": {"adbc.snowflake.statement.query_tag": "etl"}},
    ]
