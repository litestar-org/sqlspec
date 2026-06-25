"""mysql-connector load_from_arrow ingest paths (LOAD DATA LOCAL INFILE and executemany)."""

from pathlib import Path
from typing import Any, cast

import anyio
import pyarrow as pa
import pyarrow.parquet as pq

from sqlspec.adapters.mysqlconnector.core import build_insert_statement
from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver

_CAPS: dict[str, Any] = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": False,
    "parquet_import_enabled": False,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": [],
}


class _FakeSyncCursor:
    def __init__(self) -> None:
        self.execute_calls: list[str] = []
        self.executemany_calls: list[tuple[str, list[Any]]] = []
        self.loaded_payload: bytes | None = None
        self.rowcount = 0

    def execute(self, sql: str, *_args: Any) -> None:
        self.execute_calls.append(sql)
        if sql.startswith("LOAD DATA"):
            self.loaded_payload = Path(sql.split("'")[1]).read_bytes()

    def executemany(self, sql: str, params: Any) -> None:
        self.executemany_calls.append((sql, [tuple(row) for row in params]))

    def close(self) -> None:
        pass


class _FakeSyncConnection:
    def __init__(self) -> None:
        self._cursor = _FakeSyncCursor()

    def cursor(self, *_args: Any, **_kwargs: Any) -> _FakeSyncCursor:
        return self._cursor


class _FakeAsyncCursor:
    def __init__(self) -> None:
        self.execute_calls: list[str] = []
        self.executemany_calls: list[tuple[str, list[Any]]] = []
        self.loaded_payload: bytes | None = None
        self.rowcount = 0

    async def execute(self, sql: str, *_args: Any) -> None:
        self.execute_calls.append(sql)
        if sql.startswith("LOAD DATA"):
            self.loaded_payload = await anyio.Path(sql.split("'")[1]).read_bytes()

    async def executemany(self, sql: str, params: Any) -> None:
        self.executemany_calls.append((sql, [tuple(row) for row in params]))

    async def close(self) -> None:
        pass


class _FakeAsyncConnection:
    def __init__(self) -> None:
        self._cursor = _FakeAsyncCursor()

    async def cursor(self, *_args: Any, **_kwargs: Any) -> _FakeAsyncCursor:
        return self._cursor


def test_sync_load_from_arrow_local_infile_writes_tsv_and_loads() -> None:
    conn = _FakeSyncConnection()
    driver = MysqlConnectorSyncDriver(
        connection=cast("Any", conn),
        driver_features={"storage_capabilities": _CAPS, "enable_local_infile_bulk_load": True},
    )

    job = driver.load_from_arrow("orders", pa.table({"id": [1, 2], "name": ["a", "b"]}))

    assert job.telemetry["rows_processed"] == 2
    assert conn._cursor.loaded_payload == b"1\ta\n2\tb\n"
    assert conn._cursor.execute_calls[0].startswith("LOAD DATA LOCAL INFILE")


def test_sync_load_from_arrow_without_feature_uses_executemany() -> None:
    conn = _FakeSyncConnection()
    driver = MysqlConnectorSyncDriver(connection=cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    driver.load_from_arrow("orders", pa.table({"id": [1, 2], "name": ["a", "b"]}))

    insert_sql, rows = conn._cursor.executemany_calls[0]
    assert insert_sql.startswith("INSERT INTO")
    assert rows == [(1, "a"), (2, "b")]


def test_sync_load_from_arrow_overwrite_truncates_first() -> None:
    conn = _FakeSyncConnection()
    driver = MysqlConnectorSyncDriver(
        connection=cast("Any", conn),
        driver_features={"storage_capabilities": _CAPS, "enable_local_infile_bulk_load": True},
    )

    driver.load_from_arrow("orders", pa.table({"id": [1]}), overwrite=True)

    assert conn._cursor.execute_calls[0] == "TRUNCATE TABLE `orders`"


def test_build_insert_statement_preserves_backtick_quoted_dots() -> None:
    statement = build_insert_statement("`analytics.db`.`orders.table`", ["id"])

    assert statement == "INSERT INTO `analytics.db`.`orders.table` (`id`) VALUES (%s)"


async def test_async_load_from_arrow_local_infile_writes_tsv_and_loads() -> None:
    conn = _FakeAsyncConnection()
    driver = MysqlConnectorAsyncDriver(
        connection=cast("Any", conn),
        driver_features={"storage_capabilities": _CAPS, "enable_local_infile_bulk_load": True},
    )

    job = await driver.load_from_arrow("orders", pa.table({"id": [1, 2], "name": ["a", "b"]}))

    assert job.telemetry["rows_processed"] == 2
    assert conn._cursor.loaded_payload == b"1\ta\n2\tb\n"
    assert conn._cursor.execute_calls[0].startswith("LOAD DATA LOCAL INFILE")


async def test_async_load_from_arrow_without_feature_uses_executemany() -> None:
    conn = _FakeAsyncConnection()
    driver = MysqlConnectorAsyncDriver(connection=cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    await driver.load_from_arrow("orders", pa.table({"id": [1, 2], "name": ["a", "b"]}))

    insert_sql, rows = conn._cursor.executemany_calls[0]
    assert insert_sql.startswith("INSERT INTO")
    assert rows == [(1, "a"), (2, "b")]


def test_sync_load_from_storage_reads_parquet_and_delegates(tmp_path: Path) -> None:
    parquet_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2], "name": ["a", "b"]}), parquet_path)
    conn = _FakeSyncConnection()
    driver = MysqlConnectorSyncDriver(connection=cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    job = driver.load_from_storage("orders", str(parquet_path), file_format="parquet")

    assert job.telemetry["rows_processed"] == 2
    assert conn._cursor.executemany_calls[0][1] == [(1, "a"), (2, "b")]


async def test_async_load_from_storage_reads_parquet_and_delegates(tmp_path: Path) -> None:
    parquet_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2], "name": ["a", "b"]}), parquet_path)
    conn = _FakeAsyncConnection()
    driver = MysqlConnectorAsyncDriver(connection=cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    job = await driver.load_from_storage("orders", str(parquet_path), file_format="parquet")

    assert job.telemetry["rows_processed"] == 2
    assert conn._cursor.executemany_calls[0][1] == [(1, "a"), (2, "b")]
