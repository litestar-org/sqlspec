"""Unit tests for storage bridge ingestion helpers."""

import sqlite3
from pathlib import Path
from typing import Any, cast

import aiosqlite
import duckdb
import pyarrow as pa
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteDriver
from sqlspec.adapters.aiosqlite import default_statement_config as aiosqlite_statement_config
from sqlspec.adapters.asyncmy import AsyncmyConnection, AsyncmyDriver
from sqlspec.adapters.asyncmy import default_statement_config as asyncmy_statement_config
from sqlspec.adapters.asyncpg import AsyncpgConnection, AsyncpgDriver
from sqlspec.adapters.asyncpg import default_statement_config as asyncpg_statement_config
from sqlspec.adapters.duckdb import DuckDBDriver
from sqlspec.adapters.duckdb import default_statement_config as duckdb_statement_config
from sqlspec.adapters.psqlpy import PsqlpyConnection, PsqlpyDriver
from sqlspec.adapters.psqlpy import default_statement_config as psqlpy_statement_config
from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.adapters.sqlite import default_statement_config as sqlite_statement_config
from sqlspec.storage import SyncStoragePipeline, get_storage_bridge_diagnostics, reset_storage_bridge_metrics
from sqlspec.storage.pipeline import StorageDestination
from sqlspec.storage.registry import storage_registry
from sqlspec.utils.serializers import reset_serializer_cache, serialize_collection

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

    async def copy_records_to_table(self, table: str, *, records: list[tuple[object, ...]], columns: list[str]) -> None:
        self.calls.append((table, records, columns))


class DummyPsqlpyConnection:
    def __init__(self) -> None:
        self.copy_calls: list[dict[str, Any]] = []
        self.statements: list[str] = []

    async def binary_copy_to_table(
        self,
        source: list[tuple[object, ...]],
        table_name: str,
        *,
        columns: list[str] | None = None,
        schema_name: str | None = None,
    ) -> None:
        self.copy_calls.append({
            "table": table_name,
            "schema": schema_name,
            "columns": columns or [],
            "records": source,
        })

    async def execute(self, sql: str, params: "list[Any] | None" = None) -> None:
        _ = params
        self.statements.append(sql)


class DummyAsyncmyCursorImpl:
    def __init__(self, operations: "list[tuple[str, Any, Any | None]]") -> None:
        self.operations = operations

    async def executemany(self, sql: str, params: Any) -> None:
        self.operations.append(("executemany", sql, params))

    async def execute(self, sql: str, params: Any | None = None) -> None:
        self.operations.append(("execute", sql, params))

    async def close(self) -> None:
        return None


class DummyAsyncmyConnection:
    def __init__(self) -> None:
        self.operations: list[tuple[str, Any, Any | None]] = []

    def cursor(self) -> DummyAsyncmyCursorImpl:
        return DummyAsyncmyCursorImpl(self.operations)


@pytest.mark.asyncio
async def test_asyncpg_load_from_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    arrow_table = pa.table({"id": [1, 2], "name": ["alpha", "beta"]})

    async def _fake_read(self, *_: object, **__: object) -> tuple[pa.Table, dict[str, object]]:
        return arrow_table, {"destination": "file://tmp/part-0.parquet", "bytes_processed": 128}

    driver = AsyncpgDriver(
        connection=cast(AsyncpgConnection, DummyAsyncpgConnection()),
        statement_config=aiosqlite_statement_config,
        driver_features={"storage_capabilities": CAPABILITIES},
    )
    monkeypatch.setattr(AsyncpgDriver, "_read_arrow_from_storage_async", _fake_read)

    job = await driver.load_from_storage("public.ingest_target", "file://tmp/part-0.parquet", file_format="parquet")

    assert driver.connection.calls[0][0] == "public.ingest_target"
    assert driver.connection.calls[0][2] == ["id", "name"]
    assert job.telemetry["rows_processed"] == arrow_table.num_rows
    assert job.telemetry["destination"] == "public.ingest_target"


def test_duckdb_load_from_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    arrow_table = pa.table({"id": [10, 11], "label": ["east", "west"]})

    def _fake_read(self, *_: object, **__: object) -> tuple[pa.Table, dict[str, object]]:
        return arrow_table, {"destination": "file://tmp/part-1.parquet", "bytes_processed": 256}

    connection = duckdb.connect(database=":memory:")
    connection.execute("CREATE TABLE ingest_target (id INTEGER, label TEXT)")

    driver = DuckDBDriver(
        connection=connection,
        statement_config=asyncmy_statement_config,
        driver_features={"storage_capabilities": CAPABILITIES},
    )

    monkeypatch.setattr(DuckDBDriver, "_read_arrow_from_storage_sync", _fake_read)

    job = driver.load_from_storage("ingest_target", "file://tmp/part-1.parquet", file_format="parquet", overwrite=True)

    rows = connection.execute("SELECT id, label FROM ingest_target ORDER BY id").fetchall()
    assert rows == [(10, "east"), (11, "west")]
    assert job.telemetry["rows_processed"] == arrow_table.num_rows
    assert job.telemetry["destination"] == "ingest_target"


@pytest.mark.asyncio
async def test_psqlpy_load_from_arrow_overwrite() -> None:
    arrow_table = pa.table({"id": [7, 8], "name": ["east", "west"]})
    dummy_connection = DummyPsqlpyConnection()
    driver = PsqlpyDriver(
        connection=cast(PsqlpyConnection, dummy_connection),
        statement_config=asyncpg_statement_config,
        driver_features={"storage_capabilities": CAPABILITIES},
    )

    job = await driver.load_from_arrow("analytics.ingest_target", arrow_table, overwrite=True)

    assert dummy_connection.statements == ['TRUNCATE TABLE "analytics"."ingest_target"']
    assert dummy_connection.copy_calls[0]["table"] == "ingest_target"
    assert dummy_connection.copy_calls[0]["schema"] == "analytics"
    payload = dummy_connection.copy_calls[0]["records"]
    if isinstance(payload, bytes):
        assert payload == b"7\teast\n8\twest\n"
    else:
        assert payload == [(7, "east"), (8, "west")]
    assert job.telemetry["destination"] == "analytics.ingest_target"
    assert job.telemetry["rows_processed"] == arrow_table.num_rows


@pytest.mark.asyncio
async def test_psqlpy_load_from_storage_merges_telemetry(monkeypatch: pytest.MonkeyPatch) -> None:
    arrow_table = pa.table({"id": [1, 2], "name": ["north", "south"]})
    dummy_connection = DummyPsqlpyConnection()
    driver = PsqlpyDriver(
        connection=cast(PsqlpyConnection, dummy_connection),
        statement_config=duckdb_statement_config,
        driver_features={"storage_capabilities": CAPABILITIES},
    )

    async def _fake_read(self, *_: object, **__: object) -> tuple[pa.Table, dict[str, object]]:
        return arrow_table, {"destination": "s3://bucket/part-2.parquet", "bytes_processed": 512}

    monkeypatch.setattr(PsqlpyDriver, "_read_arrow_from_storage_async", _fake_read)

    job = await driver.load_from_storage("public.delta_load", "s3://bucket/part-2.parquet", file_format="parquet")

    assert dummy_connection.copy_calls[0]["table"] == "delta_load"
    assert dummy_connection.copy_calls[0]["columns"] == ["id", "name"]
    assert job.telemetry["destination"] == "public.delta_load"
    assert job.telemetry["extra"]["source"]["destination"] == "s3://bucket/part-2.parquet"  # type: ignore[index]


@pytest.mark.asyncio
async def test_aiosqlite_load_from_arrow_overwrite() -> None:
    connection = await aiosqlite.connect(":memory:")
    try:
        await connection.execute("CREATE TABLE ingest (id INTEGER, name TEXT)")
        await connection.execute("INSERT INTO ingest (id, name) VALUES (99, 'stale')")
        await connection.commit()

        driver = AiosqliteDriver(
            connection=connection,
            statement_config=psqlpy_statement_config,
            driver_features={"storage_capabilities": CAPABILITIES},
        )
        arrow_table = pa.table({"id": [1, 2], "name": ["alpha", "beta"]})

        job = await driver.load_from_arrow("ingest", arrow_table, overwrite=True)

        async with connection.execute("SELECT id, name FROM ingest ORDER BY id") as cursor:
            rows = await cursor.fetchall()
        assert rows == [(1, "alpha"), (2, "beta")]  # type: ignore[comparison-overlap]
        assert job.telemetry["destination"] == "ingest"
        assert job.telemetry["rows_processed"] == arrow_table.num_rows
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_aiosqlite_load_from_storage_includes_source(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = await aiosqlite.connect(":memory:")
    try:
        await connection.execute("CREATE TABLE raw_data (id INTEGER, label TEXT)")
        await connection.commit()

        driver = AiosqliteDriver(
            connection=connection,
            statement_config=sqlite_statement_config,
            driver_features={"storage_capabilities": CAPABILITIES},
        )
        arrow_table = pa.table({"id": [5], "label": ["gamma"]})

        async def _fake_read(self, *_: object, **__: object) -> tuple[pa.Table, dict[str, object]]:
            return arrow_table, {"destination": "file:///tmp/chunk.parquet", "bytes_processed": 64}

        monkeypatch.setattr(AiosqliteDriver, "_read_arrow_from_storage_async", _fake_read)

        job = await driver.load_from_storage("raw_data", "file:///tmp/chunk.parquet", file_format="parquet")

        async with connection.execute("SELECT id, label FROM raw_data") as cursor:
            rows = await cursor.fetchall()
        assert rows == [(5, "gamma")]  # type: ignore[comparison-overlap]
        assert job.telemetry["extra"]["source"]["destination"] == "file:///tmp/chunk.parquet"  # type: ignore[index]
    finally:
        await connection.close()


def test_sqlite_load_from_arrow_overwrite() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("CREATE TABLE staging (id INTEGER, description TEXT)")
        connection.execute("INSERT INTO staging (id, description) VALUES (42, 'legacy')")

        driver = SqliteDriver(
            connection=connection,
            statement_config=sqlite_statement_config,
            driver_features={"storage_capabilities": CAPABILITIES},
        )
        arrow_table = pa.table({"id": [10, 11], "description": ["north", "south"]})

        job = driver.load_from_arrow("staging", arrow_table, overwrite=True)

        rows = connection.execute("SELECT id, description FROM staging ORDER BY id").fetchall()
        normalized_rows = [tuple(row) for row in rows]
        assert normalized_rows == [(10, "north"), (11, "south")]
        assert job.telemetry["rows_processed"] == arrow_table.num_rows
    finally:
        connection.close()


def test_sqlite_load_from_storage_merges_source(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("CREATE TABLE metrics (val INTEGER)")

        driver = SqliteDriver(
            connection=connection,
            statement_config=sqlite_statement_config,
            driver_features={"storage_capabilities": CAPABILITIES},
        )
        arrow_table = pa.table({"val": [99]})

        def _fake_read(self, *_: object, **__: object) -> tuple[pa.Table, dict[str, object]]:
            return arrow_table, {"destination": "s3://bucket/segment.parquet", "bytes_processed": 32}

        monkeypatch.setattr(SqliteDriver, "_read_arrow_from_storage_sync", _fake_read)

        job = driver.load_from_storage("metrics", "s3://bucket/segment.parquet", file_format="parquet")

        rows = connection.execute("SELECT val FROM metrics").fetchall()
        normalized_rows = [tuple(row) for row in rows]
        assert normalized_rows == [(99,)]
        assert job.telemetry["extra"]["source"]["destination"] == "s3://bucket/segment.parquet"  # type: ignore[index]
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_asyncmy_load_from_arrow_overwrite() -> None:
    connection = DummyAsyncmyConnection()
    driver = AsyncmyDriver(
        connection=cast(AsyncmyConnection, connection),
        statement_config=psqlpy_statement_config,
        driver_features={"storage_capabilities": CAPABILITIES},
    )
    arrow_table = pa.table({"id": [3], "score": [9.5]})

    job = await driver.load_from_arrow("analytics.scores", arrow_table, overwrite=True)

    assert connection.operations[0][1].startswith("TRUNCATE TABLE `analytics`.`scores`")
    assert connection.operations[1][0] == "executemany"
    assert job.telemetry["destination"] == "analytics.scores"


@pytest.mark.asyncio
async def test_asyncmy_load_from_storage_merges_source(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = DummyAsyncmyConnection()
    driver = AsyncmyDriver(
        connection=cast(AsyncmyConnection, connection),
        statement_config=sqlite_statement_config,
        driver_features={"storage_capabilities": CAPABILITIES},
    )
    arrow_table = pa.table({"id": [11], "score": [8.2]})

    async def _fake_read(self, *_: object, **__: object) -> tuple[pa.Table, dict[str, object]]:
        return arrow_table, {"destination": "s3://bucket/segment.parquet", "bytes_processed": 48, "backend": "fsspec"}

    monkeypatch.setattr(AsyncmyDriver, "_read_arrow_from_storage_async", _fake_read)

    job = await driver.load_from_storage("analytics.scores", "s3://bucket/segment.parquet", file_format="parquet")

    assert job.telemetry["extra"]["source"]["backend"] == "fsspec"  # type: ignore[index]


def test_sync_pipeline_write_rows_includes_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    pipeline = SyncStoragePipeline()

    class _Backend:
        backend_type = "test-backend"

        def __init__(self) -> None:
            self.payloads: list[tuple[str, bytes]] = []

        def write_bytes(self, path: str, payload: bytes) -> None:
            self.payloads.append((path, payload))

    backend = _Backend()

    def _fake_resolve(
        self: SyncStoragePipeline, destination: "StorageDestination", backend_options: "dict[str, Any] | None"
    ) -> tuple[_Backend, str]:
        return backend, "objects/data.jsonl"

    monkeypatch.setattr(SyncStoragePipeline, "_resolve_backend", _fake_resolve)

    telemetry = pipeline.write_rows([{"id": 1}], "alias://data")
    assert telemetry["backend"] == "test-backend"


def test_sync_pipeline_supports_alias_destinations(tmp_path: "Path") -> None:
    storage_registry.clear()
    alias_name = "storage_bridge_unit_tests"
    storage_registry.register_alias(alias_name, f"file://{tmp_path}", backend="local")
    pipeline = SyncStoragePipeline()

    telemetry = pipeline.write_rows([{"id": 1}], f"alias://{alias_name}/payload.jsonl")

    assert telemetry["destination"].endswith("payload.jsonl")
    storage_registry.clear()


def test_storage_bridge_diagnostics_include_serializer_metrics() -> None:
    reset_storage_bridge_metrics()
    reset_serializer_cache()
    serialize_collection([{"id": 1}])
    diagnostics = get_storage_bridge_diagnostics()
    assert "serializer.size" in diagnostics
