"""aiosqlite load_from_arrow explicit transaction wrap."""

from typing import Any, cast

import pyarrow as pa
import pytest

from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver

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

    async def execute(self, sql: str, *args: Any) -> None:
        pass

    async def executemany(self, sql: str, params: Any) -> None:
        self.executemany_calls.append((sql, params))

    async def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(self, *, in_transaction: bool = False) -> None:
        self.in_transaction = in_transaction
        self.events: list[str] = []
        self._cursor = _FakeCursor()

    async def execute(self, sql: str, *args: Any) -> "_FakeCursor":
        self.events.append(sql)
        return self._cursor

    async def cursor(self) -> "_FakeCursor":
        return self._cursor

    async def commit(self) -> None:
        self.events.append("COMMIT")

    async def rollback(self) -> None:
        self.events.append("ROLLBACK")


def _driver(conn: "_FakeConnection") -> AiosqliteDriver:
    return AiosqliteDriver(connection=cast("Any", conn), driver_features={"storage_capabilities": _CAPS})


@pytest.mark.anyio
async def test_begin_immediate_issued_when_not_in_transaction() -> None:
    conn = _FakeConnection(in_transaction=False)
    await _driver(conn).load_from_arrow("t", pa.table({"a": [1, 2]}))

    assert "BEGIN IMMEDIATE" in conn.events
    assert "COMMIT" in conn.events


@pytest.mark.anyio
async def test_begin_immediate_skipped_when_already_in_transaction() -> None:
    conn = _FakeConnection(in_transaction=True)
    await _driver(conn).load_from_arrow("t", pa.table({"a": [1, 2]}))

    assert "BEGIN IMMEDIATE" not in conn.events
    assert "COMMIT" not in conn.events
