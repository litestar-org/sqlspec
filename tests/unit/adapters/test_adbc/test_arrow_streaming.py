"""Unit tests for ADBC Arrow streaming paths."""

from typing import TYPE_CHECKING, cast

import pyarrow as pa

from sqlspec.adapters.adbc.driver import AdbcDriver

if TYPE_CHECKING:
    from sqlspec.adapters.adbc._typing import AdbcConnection


_STORAGE_CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": False,
    "parquet_import_enabled": False,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": [],
}


class _AdbcStreamingCursor:
    def __init__(self) -> None:
        self.closed = False
        self.executed: list[tuple[str, object]] = []
        self.ingest_calls: list[tuple[str, object, str]] = []

    def execute(self, sql: str, parameters: object = None) -> None:
        self.executed.append((sql, parameters))

    def fetch_record_batch(self) -> pa.RecordBatchReader:
        table = pa.table({"x": [1, 2, 3]})
        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=2))

    def fetch_arrow_table(self) -> pa.Table:
        msg = "reader formats should use fetch_record_batch(), not fetch_arrow_table()"
        raise AssertionError(msg)

    def adbc_ingest(self, table: str, source: object, *, mode: str) -> int:
        self.ingest_calls.append((table, source, mode))
        return 3

    def close(self) -> None:
        self.closed = True


class _AdbcStreamingConnection:
    def __init__(self) -> None:
        self.cursor_obj = _AdbcStreamingCursor()

    def adbc_get_info(self) -> dict[str, str]:
        return {"vendor_name": "sqlite", "driver_name": "sqlite"}

    def cursor(self) -> _AdbcStreamingCursor:
        return self.cursor_obj


def test_select_to_arrow_reader_uses_fetch_record_batch_and_defers_cursor_close() -> None:
    connection = _AdbcStreamingConnection()
    driver = AdbcDriver(cast("AdbcConnection", connection), dialect="sqlite")

    result = driver.select_to_arrow("SELECT 1 AS x", return_format="reader")

    assert result.rows_affected == -1
    assert isinstance(result.data, pa.RecordBatchReader)
    assert connection.cursor_obj.closed is False
    assert result.data.read_all().to_pydict() == {"x": [1, 2, 3]}
    assert connection.cursor_obj.closed is True


def test_load_from_arrow_passes_record_batch_reader_to_adbc_ingest() -> None:
    connection = _AdbcStreamingConnection()
    driver = AdbcDriver(
        cast("AdbcConnection", connection),
        dialect="sqlite",
        driver_features={"storage_capabilities": _STORAGE_CAPABILITIES},
    )
    table = pa.table({"x": [1, 2, 3]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=2))

    job = driver.load_from_arrow("target_table", reader)

    target, source, mode = connection.cursor_obj.ingest_calls[0]
    assert target == "target_table"
    assert source is reader
    assert mode == "create_append"
    assert job.telemetry["rows_processed"] == 3
    assert job.telemetry["destination"] == "target_table"
