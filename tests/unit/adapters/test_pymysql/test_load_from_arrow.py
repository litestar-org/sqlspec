"""PyMySQL load_from_arrow ingest paths (LOAD DATA LOCAL INFILE and executemany)."""

from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq

from sqlspec.adapters.pymysql.core import build_insert_statement
from sqlspec.adapters.pymysql.driver import PyMysqlDriver

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
        self.execute_calls: list[str] = []
        self.executemany_calls: list[tuple[str, list[Any]]] = []
        self.loaded_payload: bytes | None = None
        self.rowcount = 0

    def execute(self, sql: str, *_args: Any) -> None:
        self.execute_calls.append(sql)
        if sql.startswith("LOAD DATA"):
            path = sql.split("'")[1]
            self.loaded_payload = Path(path).read_bytes()

    def executemany(self, sql: str, params: Any) -> None:
        self.executemany_calls.append((sql, [tuple(row) for row in params]))

    def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(self) -> None:
        self._cursor = _FakeCursor()

    def cursor(self, *_args: Any, **_kwargs: Any) -> _FakeCursor:
        return self._cursor


def _make_driver(connection: _FakeConnection, *, enable_local_infile: bool) -> PyMysqlDriver:
    return PyMysqlDriver(
        connection=cast("Any", connection),
        driver_features={"storage_capabilities": _CAPS, "enable_local_infile_bulk_load": enable_local_infile},
    )


def test_load_from_arrow_local_infile_writes_tsv_and_loads() -> None:
    conn = _FakeConnection()
    driver = _make_driver(conn, enable_local_infile=True)

    job = driver.load_from_arrow("orders", pa.table({"id": [1, 2], "name": ["a", "b"]}))

    assert job.telemetry["rows_processed"] == 2
    assert conn._cursor.loaded_payload == b"1\ta\n2\tb\n"
    load_sql = conn._cursor.execute_calls[0]
    assert load_sql.startswith("LOAD DATA LOCAL INFILE")
    assert "INTO TABLE `orders`" in load_sql
    assert conn._cursor.executemany_calls == []


def test_load_from_arrow_without_feature_uses_executemany() -> None:
    conn = _FakeConnection()
    driver = _make_driver(conn, enable_local_infile=False)

    job = driver.load_from_arrow("orders", pa.table({"id": [1, 2], "name": ["a", "b"]}))

    assert job.telemetry["rows_processed"] == 2
    assert conn._cursor.execute_calls == []
    insert_sql, rows = conn._cursor.executemany_calls[0]
    assert insert_sql.startswith("INSERT INTO")
    assert rows == [(1, "a"), (2, "b")]


def test_load_from_arrow_overwrite_truncates_first() -> None:
    conn = _FakeConnection()
    driver = _make_driver(conn, enable_local_infile=True)

    driver.load_from_arrow("orders", pa.table({"id": [1]}), overwrite=True)

    assert conn._cursor.execute_calls[0] == "TRUNCATE TABLE `orders`"
    assert any(sql.startswith("LOAD DATA") for sql in conn._cursor.execute_calls)


def test_build_insert_statement_preserves_backtick_quoted_dots() -> None:
    statement = build_insert_statement("`analytics.db`.`orders.table`", ["id"])

    assert statement == "INSERT INTO `analytics.db`.`orders.table` (`id`) VALUES (%s)"


def test_load_from_storage_reads_parquet_and_delegates(tmp_path: Path) -> None:
    parquet_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2], "name": ["a", "b"]}), parquet_path)
    conn = _FakeConnection()
    driver = _make_driver(conn, enable_local_infile=False)

    job = driver.load_from_storage("orders", str(parquet_path), file_format="parquet")

    assert job.telemetry["rows_processed"] == 2
    insert_sql, rows = conn._cursor.executemany_calls[0]
    assert insert_sql.startswith("INSERT INTO")
    assert rows == [(1, "a"), (2, "b")]
