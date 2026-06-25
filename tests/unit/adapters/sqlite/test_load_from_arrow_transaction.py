"""SQLite load_from_arrow explicit transaction wrap."""

from typing import Any, cast

import pyarrow as pa

from sqlspec.adapters.sqlite.driver import SqliteDriver

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
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, Any]] = []

    def execute(self, sql: str, *args: Any) -> None:
        pass

    def executemany(self, sql: str, params: Any) -> None:
        self.executemany_calls.append((sql, params))

    def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(self, *, in_transaction: bool = False) -> None:
        self.in_transaction = in_transaction
        self.events: list[str] = []
        self._cursor = _FakeCursor()

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
