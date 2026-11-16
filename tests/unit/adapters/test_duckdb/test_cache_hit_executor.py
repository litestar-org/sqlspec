"""Unit tests for the DuckDB compiled cache-hit executor."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.adapters.duckdb.core import ARROW_MIN_ROWS, execute_cache_hit
from sqlspec.exceptions import NotFoundError, UniqueViolationError


@contextmanager
def _session() -> "Generator[DuckDBDriver, None, None]":
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("CREATE TABLE cache_hit_items (id INTEGER PRIMARY KEY, name VARCHAR)")
            yield driver
    finally:
        config.close_pool()


def _executor_calls(session: DuckDBDriver, monkeypatch: pytest.MonkeyPatch) -> "list[Any]":
    outcomes: list[Any] = []

    def recording_executor(*args: Any) -> Any:
        result = execute_cache_hit(*args)
        outcomes.append(result)
        return result

    monkeypatch.setattr(session, "_cache_hit_executor", recording_executor)
    return outcomes


def test_cache_hit_select_returns_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    with _session() as session:
        outcomes = _executor_calls(session, monkeypatch)
        statement = "SELECT ? AS value, ? AS label"

        first = session.execute(statement, (1, "a"))
        second = session.execute(statement, (2, "b"))

        assert first.get_data() == [{"value": 1, "label": "a"}]
        assert second.get_data() == [{"value": 2, "label": "b"}]
        assert second.column_names == ["value", "label"]
        assert second.rows_affected == 1
        assert second.statement.sql == statement
        assert len(outcomes) == 1
        assert outcomes[0] is second


def test_cache_hit_select_without_rows_keeps_column_names() -> None:
    with _session() as session:
        statement = "SELECT id, name FROM cache_hit_items WHERE id = ?"

        session.execute(statement, (1,))
        result = session.execute(statement, (2,))

        assert result.get_data() == []
        assert result.column_names == ["id", "name"]
        assert result.rows_affected == 0


def test_cache_hit_large_select_materializes_through_arrow() -> None:
    with _session() as session:
        pytest.importorskip("pyarrow")
        statement = "SELECT i AS n FROM range(?) t(i) ORDER BY i"
        row_total = ARROW_MIN_ROWS * 2

        session.execute(statement, (row_total,))
        result = session.execute(statement, (row_total,))

        assert result.rows_affected == row_total
        assert result.get_data()[:2] == [{"n": 0}, {"n": 1}]
        assert session._cache_holder.get_row_count_hint(statement) == row_total


def test_cache_hit_select_tracks_row_count_hint() -> None:
    with _session() as session:
        statement = "SELECT i AS n FROM range(?) t(i)"

        session.execute(statement, (ARROW_MIN_ROWS * 2,))
        result = session.execute(statement, (3,))

        assert result.rows_affected == 3
        assert session._cache_holder.get_row_count_hint(statement) == 3
        assert session.execute(statement, (2,)).get_data() == [{"n": 0}, {"n": 1}]


def test_cache_hit_dml_returns_affected_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    with _session() as session:
        outcomes = _executor_calls(session, monkeypatch)
        statement = "INSERT INTO cache_hit_items (id, name) VALUES (?, ?)"

        session.execute(statement, (1, "a"))
        result = session.execute(statement, (2, "b"))
        updated = session.execute("UPDATE cache_hit_items SET name = 'z'")
        updated_again = session.execute("UPDATE cache_hit_items SET name = 'z'")

        assert result.rows_affected == 1
        assert result.operation_type == "INSERT"
        assert updated.rows_affected == 2
        assert updated_again.rows_affected == 2
        assert updated_again.operation_type == "UPDATE"
        assert outcomes == [result, updated_again]
        assert session.select_value("SELECT count(*) FROM cache_hit_items") == 2


def test_cache_hit_maps_duckdb_errors() -> None:
    with _session() as session:
        statement = "INSERT INTO cache_hit_items (id, name) VALUES (?, ?)"
        session.execute(statement, (1, "a"))

        with pytest.raises(UniqueViolationError):
            session.execute(statement, (1, "b"))

        lookup = "SELECT name FROM cache_hit_items WHERE id = ?"
        session.execute(lookup, (1,))
        session.execute("DROP TABLE cache_hit_items")
        with pytest.raises(NotFoundError):
            session.execute(lookup, (1,))


def test_cache_hit_declines_when_statement_profiling_is_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    config = DuckDBConfig(
        connection_config={"database": ":memory:"}, driver_features={"enable_statement_profiling": True}
    )
    try:
        with config.provide_session() as driver:
            outcomes = _executor_calls(driver, monkeypatch)
            statement = "SELECT ? AS value"
            driver.execute(statement, (1,))
            result = driver.execute(statement, (2,))
            assert result.get_data() == [{"value": 2}]
            assert outcomes == [None]
    finally:
        config.close_pool()
