"""mssql_python load_from_arrow wiring over BulkCopy."""

from typing import Any, cast

import pyarrow as pa

from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver

_CAPS: dict[str, Any] = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": False,
    "parquet_import_enabled": False,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": [],
}


class _FakeCursor:
    def __init__(self) -> None:
        self.bulkcopy_calls: list[tuple[str, list[Any], dict[str, Any]]] = []
        self.execute_calls: list[str] = []
        self.rowcount = 0

    def bulkcopy(self, target_table: str, rows: Any, **kwargs: Any) -> dict[str, Any]:
        materialized = list(rows)
        self.bulkcopy_calls.append((target_table, materialized, kwargs))
        return {"rows_copied": len(materialized)}

    def execute(self, sql: str, *_args: Any) -> None:
        self.execute_calls.append(sql)

    def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(self) -> None:
        self._cursor = _FakeCursor()

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


def test_sync_load_from_arrow_uses_bulkcopy() -> None:
    conn = _FakeConnection()
    driver = MssqlPythonDriver(cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    job = driver.load_from_arrow("orders", pa.table({"id": [1, 2], "name": ["a", "b"]}))

    assert job.telemetry["rows_processed"] == 2
    target, rows, kwargs = conn._cursor.bulkcopy_calls[0]
    assert target == "orders"
    assert rows == [(1, "a"), (2, "b")]
    assert kwargs["column_mappings"] == ["id", "name"]
    assert conn._cursor.execute_calls == []


def test_sync_load_from_arrow_overwrite_deletes_first() -> None:
    conn = _FakeConnection()
    driver = MssqlPythonDriver(cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    driver.load_from_arrow("dbo.orders", pa.table({"id": [1]}), overwrite=True)

    assert conn._cursor.execute_calls == ['DELETE FROM "dbo"."orders"']
    assert conn._cursor.bulkcopy_calls


def test_sync_load_from_arrow_overwrite_preserves_quoted_dots() -> None:
    conn = _FakeConnection()
    driver = MssqlPythonDriver(cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    driver.load_from_arrow('"dbo.schema"."orders.table"', pa.table({"id": [1]}), overwrite=True)

    assert conn._cursor.execute_calls == ['DELETE FROM "dbo.schema"."orders.table"']
    assert conn._cursor.bulkcopy_calls
