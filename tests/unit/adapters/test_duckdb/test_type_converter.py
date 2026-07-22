# pyright: reportArgumentType=false, reportIncompatibleMethodOverride=false
"""Unit tests for DuckDB Arrow paths."""

import pyarrow as pa

from sqlspec.adapters.duckdb.driver import DuckDBDriver

_STORAGE_CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": False,
    "parquet_import_enabled": False,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": [],
}


def test_load_from_arrow_registers_record_batch_reader_directly() -> None:
    import duckdb

    connection = duckdb.connect(":memory:")
    driver = DuckDBDriver(connection=connection, driver_features={"storage_capabilities": _STORAGE_CAPABILITIES})
    connection.execute("CREATE TABLE target (id INTEGER)")
    table = pa.table({"id": [1, 2, 3]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=2))

    job = driver.load_from_arrow("target", reader)

    assert connection.execute("SELECT id FROM target ORDER BY id").fetchall() == [(1,), (2,), (3,)]
    assert job.telemetry["rows_processed"] == 3
