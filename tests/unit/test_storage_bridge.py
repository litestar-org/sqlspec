"""Unit tests for storage bridge ingestion helpers."""

import duckdb
import pyarrow as pa
import pytest

from sqlspec.adapters.asyncpg.driver import AsyncpgDriver, asyncpg_statement_config
from sqlspec.adapters.duckdb.driver import DuckDBDriver, duckdb_statement_config

CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": ["fixed"],
}


class DummyAsyncpgConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[tuple[object, ...]], list[str]]] = []

    async def copy_records_to_table(
        self, table: str, *, records: list[tuple[object, ...]], columns: list[str]
    ) -> None:
        self.calls.append((table, records, columns))


@pytest.mark.asyncio
async def test_asyncpg_load_from_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    arrow_table = pa.table({"id": [1, 2], "name": ["alpha", "beta"]})

    async def _fake_read(*_: object, **__: object) -> tuple[pa.Table, dict[str, object]]:
        return arrow_table, {"destination": "file://tmp/part-0.parquet", "bytes_processed": 128}

    driver = AsyncpgDriver(
        connection=DummyAsyncpgConnection(),
        statement_config=asyncpg_statement_config,
        driver_features={"storage_capabilities": CAPABILITIES},
    )
    monkeypatch.setattr(driver, "_read_arrow_from_storage_async", _fake_read)

    job = await driver.load_from_storage("public.ingest_target", "file://tmp/part-0.parquet", file_format="parquet")

    assert driver.connection.calls[0][0] == "public.ingest_target"
    assert driver.connection.calls[0][2] == ["id", "name"]
    assert job.telemetry["rows_processed"] == arrow_table.num_rows
    assert job.telemetry["destination"] == "public.ingest_target"


def test_duckdb_load_from_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    arrow_table = pa.table({"id": [10, 11], "label": ["east", "west"]})

    def _fake_read(*_: object, **__: object) -> tuple[pa.Table, dict[str, object]]:
        return arrow_table, {"destination": "file://tmp/part-1.parquet", "bytes_processed": 256}

    connection = duckdb.connect(database=":memory:")
    connection.execute("CREATE TABLE ingest_target (id INTEGER, label TEXT)")

    driver = DuckDBDriver(
        connection=connection,
        statement_config=duckdb_statement_config,
        driver_features={"storage_capabilities": CAPABILITIES},
    )

    monkeypatch.setattr(driver, "_read_arrow_from_storage_sync", _fake_read)

    job = driver.load_from_storage(
        "ingest_target",
        "file://tmp/part-1.parquet",
        file_format="parquet",
        overwrite=True,
    )

    rows = connection.execute("SELECT id, label FROM ingest_target ORDER BY id").fetchall()
    assert rows == [(10, "east"), (11, "west")]
    assert job.telemetry["rows_processed"] == arrow_table.num_rows
    assert job.telemetry["destination"] == "ingest_target"
