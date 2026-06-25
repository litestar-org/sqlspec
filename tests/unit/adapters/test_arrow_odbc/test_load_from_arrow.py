"""arrow-odbc load_from_arrow wiring over bulk_insert_arrow."""

from typing import Any, cast

import pyarrow as pa

from sqlspec.adapters.arrow_odbc.driver import ArrowOdbcDriver

_CAPS: dict[str, Any] = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": False,
    "parquet_import_enabled": False,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": [],
}


class _FakeConn:
    dbms_name = "Microsoft SQL Server"

    def __init__(self) -> None:
        self.execute_calls: list[str] = []
        self.bulk_calls: list[tuple[str, int]] = []

    def execute(self, query: str, parameters: Any = None) -> None:
        self.execute_calls.append(query)

    def from_table_to_db(self, *, source: Any, target: str, chunk_size: int) -> None:
        self.bulk_calls.append((target, source.num_rows))


def _driver(conn: "_FakeConn") -> ArrowOdbcDriver:
    return ArrowOdbcDriver(cast("Any", conn), driver_features={"storage_capabilities": _CAPS})


def test_load_from_arrow_uses_bulk_insert() -> None:
    conn = _FakeConn()
    job = _driver(conn).load_from_arrow("orders", pa.table({"id": [1, 2, 3]}))

    assert job.telemetry["rows_processed"] == 3
    assert conn.bulk_calls == [("orders", 3)]
    assert conn.execute_calls == []


def test_load_from_arrow_overwrite_deletes_first() -> None:
    conn = _FakeConn()
    _driver(conn).load_from_arrow("dbo.orders", pa.table({"id": [1]}), overwrite=True)

    assert any("DELETE FROM" in q.upper() for q in conn.execute_calls)
    assert conn.bulk_calls
