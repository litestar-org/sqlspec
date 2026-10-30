# pyright: reportPrivateUsage=false
"""Unit tests for vectorized PGCOPY binary encoding and streaming COPY across PostgreSQL & CockroachDB drivers."""

import datetime as dt
import struct
import uuid
from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver, default_statement_config as asyncpg_statement_config
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver
from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
from sqlspec.adapters.cockroach_psycopg.core import build_copy_from_command as crdb_build_copy_from_command
from sqlspec.adapters.psqlpy import PsqlpyDriver, default_statement_config as psqlpy_statement_config
from sqlspec.adapters.psycopg import PsycopgAsyncDriver, PsycopgSyncDriver, default_statement_config as psycopg_statement_config
from sqlspec.adapters.psycopg.core import build_copy_from_command as pg_build_copy_from_command
from sqlspec.utils.arrow_helpers import (
    PG_BINARY_COPY_HEADER,
    PG_BINARY_COPY_TRAILER,
    encode_arrow_batch_pg_binary,
    iter_pgcopy_binary_chunks,
)

_CAPS: dict[str, Any] = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "partition_strategies": ["fixed"],
}


def test_pg_binary_copy_constants() -> None:
    """PGCOPY binary header and trailer match the PostgreSQL wire specification."""
    assert len(PG_BINARY_COPY_HEADER) == 19
    assert PG_BINARY_COPY_HEADER[:11] == b"PGCOPY\n\xff\r\n\x00"
    assert PG_BINARY_COPY_HEADER[11:15] == b"\x00\x00\x00\x00"
    assert PG_BINARY_COPY_HEADER[15:19] == b"\x00\x00\x00\x00"
    assert PG_BINARY_COPY_TRAILER == b"\xff\xff"


def test_encode_arrow_batch_pg_binary_fast_path_1_fixed_width_and_pgvector() -> None:
    """Fast Path 1 packs non-null fixed-width and FixedSizeList vector columns via structured NumPy dtype."""
    uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([True, False], type=pa.bool_()),
            pa.array([10, -20], type=pa.int16()),
            pa.array([1000, -2000], type=pa.int32()),
            pa.array([100_000, -200_000], type=pa.int64()),
            pa.array([1.5, -2.5], type=pa.float32()),
            pa.array([3.125, -6.25], type=pa.float64()),
            pa.array([dt.date(2000, 1, 1), dt.date(2000, 1, 3)], type=pa.date32()),
            pa.array(
                [
                    dt.datetime(2000, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2000, 1, 1, 0, 0, 2, tzinfo=dt.timezone.utc),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            pa.array([uid.bytes, uid.bytes], type=pa.binary(16)),
            pa.FixedSizeListArray.from_arrays(pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], type=pa.float32()), 3),
            pa.FixedSizeListArray.from_arrays(
                pa.array([np.float16(0.5), np.float16(1.5), np.float16(2.5), np.float16(3.5)], type=pa.float16()), 2
            ),
        ],
        names=[
            "flag",
            "i16",
            "i32",
            "i64",
            "f32",
            "f64",
            "dt",
            "ts",
            "uid",
            "vec_f32",
            "vec_f16",
        ],
    )

    payload = encode_arrow_batch_pg_binary(batch, include_header=True, include_trailer=True)
    assert payload.startswith(PG_BINARY_COPY_HEADER)
    assert payload.endswith(PG_BINARY_COPY_TRAILER)

    body = payload[len(PG_BINARY_COPY_HEADER) : -len(PG_BINARY_COPY_TRAILER)]
    expected_row_0 = b"".join([
        struct.pack(">h", 11),
        struct.pack(">iB", 1, 1),
        struct.pack(">ih", 2, 10),
        struct.pack(">ii", 4, 1000),
        struct.pack(">iq", 8, 100_000),
        struct.pack(">if", 4, 1.5),
        struct.pack(">id", 8, 3.125),
        struct.pack(">ii", 4, 0),
        struct.pack(">iq", 8, 0),
        struct.pack(">i16s", 16, uid.bytes),
        struct.pack(">iHH3f", 16, 3, 0, 1.0, 2.0, 3.0),
        struct.pack(">iHH2e", 8, 2, 0, 0.5, 1.5),
    ])
    expected_row_1 = b"".join([
        struct.pack(">h", 11),
        struct.pack(">iB", 1, 0),
        struct.pack(">ih", 2, -20),
        struct.pack(">ii", 4, -2000),
        struct.pack(">iq", 8, -200_000),
        struct.pack(">if", 4, -2.5),
        struct.pack(">id", 8, -6.25),
        struct.pack(">ii", 4, 2),
        struct.pack(">iq", 8, 2_000_000),
        struct.pack(">i16s", 16, uid.bytes),
        struct.pack(">iHH3f", 16, 3, 0, 4.0, 5.0, 6.0),
        struct.pack(">iHH2e", 8, 2, 0, 2.5, 3.5),
    ])
    assert body == expected_row_0 + expected_row_1


def test_encode_arrow_batch_pg_binary_fast_path_2_variable_width_jsonb_and_nulls() -> None:
    """Fast Path 2 slices Arrow UTF-8/binary buffers, prepends jsonb version byte, and writes -1 for NULLs."""
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, None, 3], type=pa.int64()),
            pa.array(["alpha", None, "gamma"], type=pa.string()),
            pa.array(['{"a":1}', None, '{"b":2}'], type=pa.string()),
            pa.array([[1.0, 2.0], None, [3.0, 4.0]], type=pa.list_(pa.float32())),
        ],
        names=["id", "label", "payload", "embedding"],
    )

    payload = encode_arrow_batch_pg_binary(
        batch,
        jsonb_columns=frozenset({"payload"}),
        vector_columns=frozenset({"embedding"}),
        include_header=False,
        include_trailer=False,
    )

    expected_row_0 = b"".join([
        struct.pack(">h", 4),
        struct.pack(">iq", 8, 1),
        struct.pack(">i5s", 5, b"alpha"),
        struct.pack(">iB7s", 8, 1, b'{"a":1}'),
        struct.pack(">iHH2f", 12, 2, 0, 1.0, 2.0),
    ])
    expected_row_1 = b"".join([
        struct.pack(">h", 4),
        struct.pack(">i", -1),
        struct.pack(">i", -1),
        struct.pack(">i", -1),
        struct.pack(">i", -1),
    ])
    expected_row_2 = b"".join([
        struct.pack(">h", 4),
        struct.pack(">iq", 8, 3),
        struct.pack(">i5s", 5, b"gamma"),
        struct.pack(">iB7s", 8, 1, b'{"b":2}'),
        struct.pack(">iHH2f", 12, 2, 0, 3.0, 4.0),
    ])
    assert payload == expected_row_0 + expected_row_1 + expected_row_2


def test_iter_pgcopy_binary_chunks_streams_reader_batches() -> None:
    """iter_pgcopy_binary_chunks emits header, batch payloads, and trailer across RecordBatchReader."""
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batches = [
        pa.RecordBatch.from_pydict({"id": [1, 2], "name": ["a", "b"]}, schema=schema),
        pa.RecordBatch.from_pydict({"id": [3, 4], "name": ["c", "d"]}, schema=schema),
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)

    chunks = list(iter_pgcopy_binary_chunks(reader, chunk_bytes=16))
    combined = b"".join(chunks)

    assert combined.startswith(PG_BINARY_COPY_HEADER)
    assert combined.endswith(PG_BINARY_COPY_TRAILER)
    assert len(chunks) >= 2


def test_build_copy_from_command_supports_binary_and_text_modes() -> None:
    """psycopg and cockroach_psycopg build_copy_from_command emit FORMAT BINARY when binary=True."""
    pg_bin = pg_build_copy_from_command("public.items", ["id", "name"], binary=True)
    pg_txt = pg_build_copy_from_command("public.items", ["id", "name"], binary=False)
    crdb_bin = crdb_build_copy_from_command("public.items", ["id", "name"], binary=True)

    assert "WITH (FORMAT BINARY)" in repr(pg_bin)
    assert "WITH (FORMAT BINARY)" not in repr(pg_txt)
    assert "WITH (FORMAT BINARY)" in repr(crdb_bin)


class _DummyPsycopgCopy:
    def __init__(self, writes: list[bytes], rows: list[tuple[Any, ...]]) -> None:
        self._writes = writes
        self._rows = rows

    def __enter__(self) -> "_DummyPsycopgCopy":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    async def __aenter__(self) -> "_DummyPsycopgCopy":
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    def write(self, data: bytes) -> None:
        self._writes.append(bytes(data))

    def write_row(self, row: tuple[Any, ...]) -> None:
        self._rows.append(tuple(row))


class _DummyPsycopgAsyncCopy:
    def __init__(self, writes: list[bytes], rows: list[tuple[Any, ...]]) -> None:
        self._writes = writes
        self._rows = rows

    async def __aenter__(self) -> "_DummyPsycopgAsyncCopy":
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def write(self, data: bytes) -> None:
        self._writes.append(bytes(data))

    async def write_row(self, row: tuple[Any, ...]) -> None:
        self._rows.append(tuple(row))


class _DummyPsycopgSyncCursor:
    def __init__(self, conn: "_DummyPsycopgSyncConn") -> None:
        self._conn = conn

    def __enter__(self) -> "_DummyPsycopgSyncCursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, sql: Any) -> None:
        self._conn.executed.append(sql)

    def copy(self, sql: Any) -> _DummyPsycopgCopy:
        self._conn.copy_sqls.append(sql)
        return _DummyPsycopgCopy(self._conn.binary_writes, self._conn.row_writes)


class _DummyPsycopgSyncConn:
    def __init__(self) -> None:
        self.executed: list[Any] = []
        self.copy_sqls: list[Any] = []
        self.binary_writes: list[bytes] = []
        self.row_writes: list[tuple[Any, ...]] = []
        self.info = SimpleNamespace(transaction_status=0)

    def cursor(self) -> _DummyPsycopgSyncCursor:
        return _DummyPsycopgSyncCursor(self)


class _DummyPsycopgAsyncCursor:
    def __init__(self, conn: "_DummyPsycopgAsyncConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_DummyPsycopgAsyncCursor":
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def execute(self, sql: Any) -> None:
        self._conn.executed.append(sql)

    def copy(self, sql: Any) -> _DummyPsycopgAsyncCopy:
        self._conn.copy_sqls.append(sql)
        return _DummyPsycopgAsyncCopy(self._conn.binary_writes, self._conn.row_writes)


class _DummyPsycopgAsyncConn:
    def __init__(self) -> None:
        self.executed: list[Any] = []
        self.copy_sqls: list[Any] = []
        self.binary_writes: list[bytes] = []
        self.row_writes: list[tuple[Any, ...]] = []
        self.info = SimpleNamespace(transaction_status=0)

    def cursor(self) -> _DummyPsycopgAsyncCursor:
        return _DummyPsycopgAsyncCursor(self)


def test_psycopg_and_cockroach_sync_load_from_arrow_streams_binary_copy() -> None:
    """PsycopgSyncDriver and CockroachPsycopgSyncDriver stream RecordBatchReader via binary COPY."""
    schema = pa.schema([("id", pa.int64()), ("label", pa.string())])
    for driver_cls in (PsycopgSyncDriver, CockroachPsycopgSyncDriver):
        conn = _DummyPsycopgSyncConn()
        driver = driver_cls(
            connection=cast("Any", conn),
            statement_config=psycopg_statement_config,
            driver_features={"storage_capabilities": _CAPS},
        )
        reader = pa.RecordBatchReader.from_batches(
            schema,
            [
                pa.RecordBatch.from_pydict({"id": [1, 2], "label": ["a", "b"]}, schema=schema),
                pa.RecordBatch.from_pydict({"id": [3], "label": ["c"]}, schema=schema),
            ],
        )
        job = driver.load_from_arrow("public.items", reader)

        assert job.telemetry["rows_processed"] == 3
        assert len(conn.copy_sqls) == 1
        assert "WITH (FORMAT BINARY)" in repr(conn.copy_sqls[0])
        combined = b"".join(conn.binary_writes)
        assert combined.startswith(PG_BINARY_COPY_HEADER)
        assert combined.endswith(PG_BINARY_COPY_TRAILER)
        assert conn.row_writes == []


async def test_psycopg_and_cockroach_async_load_from_arrow_streams_binary_copy() -> None:
    """PsycopgAsyncDriver and CockroachPsycopgAsyncDriver stream RecordBatchReader via async binary COPY."""
    schema = pa.schema([("id", pa.int64()), ("label", pa.string())])
    for driver_cls in (PsycopgAsyncDriver, CockroachPsycopgAsyncDriver):
        conn = _DummyPsycopgAsyncConn()
        driver = driver_cls(
            connection=cast("Any", conn),
            statement_config=psycopg_statement_config,
            driver_features={"storage_capabilities": _CAPS},
        )
        reader = pa.RecordBatchReader.from_batches(
            schema,
            [pa.RecordBatch.from_pydict({"id": [10, 20], "label": ["x", "y"]}, schema=schema)],
        )
        job = await driver.load_from_arrow("public.items", reader)

        assert job.telemetry["rows_processed"] == 2
        assert len(conn.copy_sqls) == 1
        assert "WITH (FORMAT BINARY)" in repr(conn.copy_sqls[0])
        combined = b"".join(conn.binary_writes)
        assert combined.startswith(PG_BINARY_COPY_HEADER)
        assert combined.endswith(PG_BINARY_COPY_TRAILER)


class _DummyAsyncpgBinaryConn:
    def __init__(self) -> None:
        self.copy_to_table_calls: list[dict[str, Any]] = []
        self.chunks: list[bytes] = []

    async def copy_to_table(
        self,
        table_name: str,
        *,
        source: Any,
        columns: list[str] | None = None,
        schema_name: str | None = None,
        format: str | None = None,
    ) -> None:
        async for chunk in source:
            self.chunks.append(bytes(chunk))
        self.copy_to_table_calls.append({
            "table_name": table_name,
            "columns": columns,
            "schema_name": schema_name,
            "format": format,
        })


async def test_asyncpg_and_cockroach_asyncpg_load_from_arrow_streams_binary_copy() -> None:
    """AsyncpgDriver and CockroachAsyncpgDriver stream RecordBatchReader into copy_to_table(format='binary')."""
    schema = pa.schema([("id", pa.int64()), ("vec", pa.list_(pa.float32(), 2))])
    for driver_cls in (AsyncpgDriver, CockroachAsyncpgDriver):
        conn = _DummyAsyncpgBinaryConn()
        driver = driver_cls(
            connection=cast("Any", conn),
            statement_config=asyncpg_statement_config,
            driver_features={"storage_capabilities": _CAPS},
        )
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2], type=pa.int64()),
                pa.FixedSizeListArray.from_arrays(pa.array([1.0, 2.0, 3.0, 4.0], type=pa.float32()), 2),
            ],
            schema=schema,
        )
        reader = pa.RecordBatchReader.from_batches(schema, [batch])
        job = await driver.load_from_arrow("analytics.embeddings", reader)

        assert job.telemetry["rows_processed"] == 2
        assert len(conn.copy_to_table_calls) == 1
        assert conn.copy_to_table_calls[0] == {
            "table_name": "embeddings",
            "columns": ["id", "vec"],
            "schema_name": "analytics",
            "format": "binary",
        }
        combined = b"".join(conn.chunks)
        assert combined.startswith(PG_BINARY_COPY_HEADER)
        assert combined.endswith(PG_BINARY_COPY_TRAILER)


class _DummyPsqlpyBinaryConn:
    def __init__(self) -> None:
        self.binary_calls: list[dict[str, Any]] = []

    async def binary_copy_to_table(
        self,
        source: bytes,
        table_name: str,
        *,
        columns: list[str] | None = None,
        schema_name: str | None = None,
    ) -> int:
        self.binary_calls.append({
            "source": bytes(source),
            "table_name": table_name,
            "columns": columns,
            "schema_name": schema_name,
        })
        return 2

    async def fetch(self, _sql: str, _params: Any = None) -> Any:
        return SimpleNamespace(result=list)


async def test_psqlpy_load_from_arrow_supports_binary_copy_to_table() -> None:
    """PsqlpyDriver streams PGCOPY binary payloads to binary_copy_to_table when enable_pgcopy_binary=True."""
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [pa.RecordBatch.from_pydict({"id": [1, 2], "name": ["alpha", "beta"]}, schema=schema)],
    )
    conn = _DummyPsqlpyBinaryConn()
    driver = PsqlpyDriver(
        connection=cast("Any", conn),
        statement_config=psqlpy_statement_config,
        driver_features={"storage_capabilities": _CAPS, "enable_pgcopy_binary": True},
    )

    job = await driver.load_from_arrow("public.items", reader)

    assert job.telemetry["rows_processed"] == 2
    assert len(conn.binary_calls) == 1
    assert conn.binary_calls[0]["table_name"] == "items"
    assert conn.binary_calls[0]["columns"] == ["id", "name"]
    assert conn.binary_calls[0]["source"].startswith(PG_BINARY_COPY_HEADER)
    assert conn.binary_calls[0]["source"].endswith(PG_BINARY_COPY_TRAILER)
