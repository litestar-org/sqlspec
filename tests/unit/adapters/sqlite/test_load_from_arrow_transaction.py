"""SQLite load_from_arrow explicit transaction wrap."""

import sqlite3
from typing import Any, cast

import pyarrow as pa
import pytest

from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.exceptions import SQLSpecError

_CAPS: dict[str, Any] = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": [],
}


class _FakeCursor:
    def __init__(self, *, executemany_error: "Exception | None" = None) -> None:
        self.execute_calls: list[str] = []
        self.executemany_calls: list[tuple[str, Any]] = []
        self._executemany_error = executemany_error

    def execute(self, sql: str, *args: Any) -> None:
        self.execute_calls.append(sql)

    def executemany(self, sql: str, params: Any) -> None:
        if self._executemany_error is not None:
            raise self._executemany_error
        self.executemany_calls.append((sql, params))

    def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(self, *, in_transaction: bool = False, executemany_error: "Exception | None" = None) -> None:
        self.in_transaction = in_transaction
        self.events: list[str] = []
        self._cursor = _FakeCursor(executemany_error=executemany_error)

    def execute(self, sql: str, *args: Any) -> "_FakeCursor":
        self.events.append(sql)
        return self._cursor

    def cursor(self) -> "_FakeCursor":
        return self._cursor

    def commit(self) -> None:
        self.events.append("COMMIT")

    def rollback(self) -> None:
        self.events.append("ROLLBACK")


def _driver(conn: "_FakeConnection") -> SqliteDriver:
    return SqliteDriver(connection=cast("Any", conn), driver_features={"storage_capabilities": _CAPS})


def test_begin_immediate_issued_when_not_in_transaction() -> None:
    conn = _FakeConnection(in_transaction=False)
    _driver(conn).load_from_arrow("t", pa.table({"a": [1, 2]}))

    assert "BEGIN IMMEDIATE" in conn.events
    assert "COMMIT" in conn.events


def test_begin_immediate_skipped_when_already_in_transaction() -> None:
    conn = _FakeConnection(in_transaction=True)
    _driver(conn).load_from_arrow("t", pa.table({"a": [1, 2]}))

    assert "BEGIN IMMEDIATE" not in conn.events
    assert "COMMIT" not in conn.events


def test_overwrite_issues_delete_before_insert() -> None:
    conn = _FakeConnection(in_transaction=False)
    _driver(conn).load_from_arrow("t", pa.table({"a": [1, 2]}), overwrite=True)

    assert conn._cursor.execute_calls == ['DELETE FROM "t"']
    assert conn._cursor.executemany_calls


def test_rollback_on_error_when_owning_transaction() -> None:
    conn = _FakeConnection(in_transaction=False, executemany_error=sqlite3.OperationalError("boom"))
    with pytest.raises(SQLSpecError):
        _driver(conn).load_from_arrow("t", pa.table({"a": [1, 2]}))

    assert "BEGIN IMMEDIATE" in conn.events
    assert "ROLLBACK" in conn.events
    assert "COMMIT" not in conn.events
