# pyright: reportPrivateUsage=false
"""Unit tests for streaming storage pipeline batches, vector Parquet defaults, schema-pinned JSONL/CSV/IPC, and ObStoreSink safety."""

import asyncio
import contextlib
import threading
from collections.abc import AsyncIterator, Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import pytest

from sqlspec.core.result import ArrowResult, SQLResult
from sqlspec.exceptions import FileExistsInStorageError
from sqlspec.protocols import AsyncWritableBytesSink
from sqlspec.storage import StorageRegistry, get_storage_bridge_metrics, reset_storage_bridge_metrics
from sqlspec.storage._arrow_payload import decode_arrow_payload, encode_arrow_payload, resolve_parquet_use_dictionary
from sqlspec.storage.backends.local import LocalStore
from sqlspec.storage.backends.obstore import ObStoreBackend, _ObstoreSink
from sqlspec.storage.pipeline import AsyncStoragePipeline, SyncStoragePipeline


def test_resolve_parquet_use_dictionary_disables_float_vector_columns() -> None:
    """resolve_parquet_use_dictionary disables dictionary encoding on float vector columns while preserving scalar columns."""
    schema = pa.schema(
        cast(
            "Any",
            [
                ("id", pa.int64()),
                ("category", pa.string()),
                ("embedding_f32", pa.list_(pa.float32(), 4)),
                ("embedding_var", pa.list_(pa.float32())),
                ("tags", pa.list_(pa.string())),
            ],
        )
    )

    resolved = resolve_parquet_use_dictionary(schema)
    assert resolved == ["id", "category", "tags"]

    explicit = resolve_parquet_use_dictionary(schema, explicit_use_dictionary=False)
    assert explicit is False

    scalar_only_schema = pa.schema(cast("Any", [("id", pa.int64()), ("category", pa.string())]))
    assert resolve_parquet_use_dictionary(scalar_only_schema) is True


def test_encode_arrow_payload_applies_vector_use_dictionary_and_write_options() -> None:
    """encode_arrow_payload forwards write_options and disables dictionary encoding for float vector columns."""
    schema = pa.schema(
        cast("Any", [("id", pa.int64()), ("category", pa.string()), ("embedding", pa.list_(pa.float32(), 2))])
    )
    table = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["a", "a", "b"], type=pa.string()),
            pa.FixedSizeListArray.from_arrays(pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], type=pa.float32()), 2),
        ],
        schema=schema,
    )

    payload = encode_arrow_payload(table, "parquet", compression="zstd", write_options={"row_group_size": 2})
    pf = pq.ParquetFile(pa.BufferReader(payload))
    assert pf.num_row_groups == 2


def test_obstore_sink_converts_mutable_bytearray_to_bytes() -> None:
    """_ObstoreSink.write converts bytearray/memoryview to immutable bytes so callers can clear buffers safely."""
    retained: list[Any] = []

    class _RetainingWriter:
        def closed(self) -> bool:
            return False

        def write(self, data: Any) -> int:
            assert isinstance(data, bytes)
            retained.append(memoryview(data))
            return len(data)

        def flush(self) -> None:
            return None

        def close(self) -> None:
            return None

    sink = _ObstoreSink(_RetainingWriter())
    buf = bytearray(b"hello world")
    written = sink.write(buf)
    buf.clear()

    assert written == 11
    assert bytes(retained[0]) == b"hello world"


def test_sync_pipeline_write_and_read_arrow_batches_parquet_and_jsonl(tmp_path: Path) -> None:
    """SyncStoragePipeline writes and streams Parquet, JSONL (with arrow_schema float32 pinning), CSV, and IPC batches."""
    pipeline = SyncStoragePipeline()
    parquet_path = tmp_path / "vectors.parquet"
    schema = pa.schema([("id", pa.int64()), ("embedding", pa.list_(pa.float32(), 2))])
    batches = [
        pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2], type=pa.int64()),
                pa.FixedSizeListArray.from_arrays(pa.array([1.0, 2.0, 3.0, 4.0], type=pa.float32()), 2),
            ],
            schema=schema,
        ),
        pa.RecordBatch.from_arrays(
            [
                pa.array([3], type=pa.int64()),
                pa.FixedSizeListArray.from_arrays(pa.array([5.0, 6.0], type=pa.float32()), 2),
            ],
            schema=schema,
        ),
    ]

    telemetry = pipeline.write_arrow_batches(parquet_path, iter(batches), file_format="parquet")
    assert telemetry["rows_processed"] == 3
    assert parquet_path.exists()

    streamed = list(pipeline.read_arrow_batches(parquet_path, file_format="parquet", batch_size=2))
    assert sum(b.num_rows for b in streamed) == 3

    jsonl_path = tmp_path / "vectors.jsonl"
    jsonl_path.write_text('{"id": 1, "embedding": [0.25, 0.75]}\n{"id": 2, "embedding": [1.25, 1.75]}\n')
    jsonl_schema = pa.schema(cast("Any", [("id", pa.int64()), ("embedding", pa.list_(pa.float32()))]))
    jsonl_batches = list(
        pipeline.read_arrow_batches(jsonl_path, file_format="jsonl", arrow_schema=jsonl_schema, batch_size=10)
    )
    assert sum(b.num_rows for b in jsonl_batches) == 2
    assert jsonl_batches[0].schema.field("embedding").type == pa.list_(pa.float32())
    assert np.allclose(cast("Any", jsonl_batches[0].column("embedding").to_pylist()[0]), [0.25, 0.75])


async def test_async_pipeline_write_and_read_arrow_batches_and_arrow_result(tmp_path: Path) -> None:
    """AsyncStoragePipeline and ArrowResult stream batches without full-table materialization."""
    pipeline = AsyncStoragePipeline()
    parquet_path = tmp_path / "async_items.parquet"
    schema = pa.schema(cast("Any", [("id", pa.int64()), ("name", pa.string())]))
    batch = pa.RecordBatch.from_pydict({"id": [10, 20], "name": ["x", "y"]}, schema=schema)

    telemetry = await pipeline.write_arrow_batches_async(parquet_path, [batch], file_format="parquet")
    assert telemetry["rows_processed"] == 2

    read_batches = [
        b async for b in pipeline.read_arrow_batches_async(parquet_path, file_format="parquet", batch_size=1)
    ]
    assert [b.num_rows for b in read_batches] == [1, 1]

    reader = pa.RecordBatchReader.from_batches(schema, [batch])
    arrow_result = ArrowResult(statement=cast("Any", None), data=reader)
    out_path = tmp_path / "from_reader.parquet"
    res_telemetry = await arrow_result.write_to_storage_async(out_path, pipeline=pipeline)
    assert res_telemetry["rows_processed"] == 2


class _MapRegistry:
    def __init__(self, mapping: dict[str, Any]) -> None:
        self.mapping = mapping

    def get(self, key: str, **kwargs: Any) -> Any:
        return self.mapping[key]


async def test_pipeline_format_and_telemetry_parity(tmp_path: Path) -> None:
    """Format and telemetry parity across direct obstore, local, and non-direct buffered backends with decimal64."""

    class _BufferedMemoryBackend:
        backend_type = "buffered_mem"
        protocol = "bufmem"

        def __init__(self) -> None:
            self.objects: dict[str, bytes] = {}

        def write_bytes_sync(self, path: str, payload: bytes) -> None:
            self.objects[path] = payload

        async def write_bytes_async(self, path: str, payload: bytes) -> None:
            self.objects[path] = payload

        def read_bytes_sync(self, path: str) -> bytes:
            return self.objects[path]

        async def read_bytes_async(self, path: str) -> bytes:
            return self.objects[path]

    mem_backend = ObStoreBackend("memory://")
    loc_backend = LocalStore(str(tmp_path))
    buf_backend = _BufferedMemoryBackend()
    registry = cast("StorageRegistry", _MapRegistry({"mem": mem_backend, "loc": loc_backend, "buf": buf_backend}))
    sync_p = SyncStoragePipeline(registry=registry)
    async_p = AsyncStoragePipeline(registry=registry)

    schema_scalar = pa.schema(cast("Any", [("id", pa.int64()), ("amount", pa.decimal64(10, 2))]))
    table_scalar = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            pa.array(
                [Decimal("1.25"), Decimal("2.50"), Decimal("3.75"), Decimal("5.00"), Decimal("6.25")],
                type=pa.decimal64(10, 2),
            ),
        ],
        schema=schema_scalar,
    )

    schema_vector = pa.schema(
        cast("Any", [("id", pa.int64()), ("amount", pa.decimal64(10, 2)), ("vec", pa.list_(pa.float32(), 2))])
    )
    table_vector = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            pa.array(
                [Decimal("1.25"), Decimal("2.50"), Decimal("3.75"), Decimal("5.00"), Decimal("6.25")],
                type=pa.decimal64(10, 2),
            ),
            pa.FixedSizeListArray.from_arrays(
                pa.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], type=pa.float32()), 2
            ),
        ],
        schema=schema_vector,
    )

    cases = [("parquet", table_vector), ("arrow-ipc", table_vector), ("csv", table_scalar)]

    for fmt, tbl in cases:
        bytes_by_backend: dict[str, int] = {}
        for dest_alias, expected_backend in [
            ("alias://mem", "obstore"),
            ("alias://loc", "local"),
            ("alias://buf", "buffered_mem"),
        ]:
            sync_dest = f"{dest_alias}/sync_pop_{fmt}.{fmt}"
            t_sync = sync_p.write_arrow(tbl, sync_dest, format_hint=cast("Any", fmt))
            assert t_sync["rows_processed"] == 5
            assert t_sync["bytes_processed"] > 0
            assert t_sync["backend"] == expected_backend
            assert t_sync["format"] == fmt
            bytes_by_backend[expected_backend] = t_sync["bytes_processed"]

            decoded_sync, _ = sync_p.read_arrow(sync_dest, file_format=cast("Any", fmt))
            assert decoded_sync.num_rows == 5
            if fmt == "arrow-ipc":
                assert decoded_sync.schema.field("amount").type == pa.decimal64(10, 2)
                assert decoded_sync.column("amount").to_pylist() == tbl.column("amount").to_pylist()
            elif fmt == "parquet":
                assert decoded_sync.schema.field("amount").type == pa.decimal128(10, 2)
                assert decoded_sync.column("amount").to_pylist() == tbl.column("amount").to_pylist()

            async_dest = f"{dest_alias}/async_pop_{fmt}.{fmt}"
            t_async = await async_p.write_arrow(tbl, async_dest, format_hint=cast("Any", fmt))
            assert t_async["rows_processed"] == 5
            assert t_async["bytes_processed"] == t_sync["bytes_processed"]
            assert t_async["backend"] == expected_backend
            assert t_async["format"] == fmt

            zero_tbl = tbl.schema.empty_table()
            zero_dest = f"{dest_alias}/zero_{fmt}.{fmt}"
            t_zero = sync_p.write_arrow(zero_tbl, zero_dest, format_hint=cast("Any", fmt))
            assert t_zero["rows_processed"] == 0
            assert t_zero["backend"] == expected_backend

            batch = tbl.to_batches()[0]
            ar_table = ArrowResult(statement=cast("Any", None), data=tbl)
            ar_batch = ArrowResult(statement=cast("Any", None), data=batch)

            t_ar_tbl = ar_table.write_to_storage_sync(
                f"{dest_alias}/ar_tbl_{fmt}.{fmt}", format_hint=cast("Any", fmt), pipeline=sync_p
            )
            assert t_ar_tbl["rows_processed"] == 5

            t_ar_btch = await ar_batch.write_to_storage_async(
                f"{dest_alias}/ar_btch_{fmt}.{fmt}", format_hint=cast("Any", fmt), pipeline=async_p
            )
            assert t_ar_btch["rows_processed"] == 5

        assert bytes_by_backend["obstore"] == bytes_by_backend["buffered_mem"]
        assert bytes_by_backend["local"] == bytes_by_backend["buffered_mem"]


async def test_pipeline_backpressure_stops_generator() -> None:
    """AsyncStoragePipeline propagates backpressure from slow async sink to batch generator."""
    gate = asyncio.Event()

    class _GatedSink:
        def __init__(self) -> None:
            self.data = bytearray()
            self.first = True

        async def write(self, data: bytes, /) -> int | None:
            if self.first:
                self.first = False
                await gate.wait()
            self.data.extend(data)
            return len(data)

    gated_sink = _GatedSink()

    class _GatedBackend:
        backend_type = "gated"
        protocol = "memory"

        def supports_direct_arrow_write(self, file_format: str, *, overwrite: bool = True) -> bool:
            return file_format == "arrow-ipc" and overwrite

        @contextlib.contextmanager
        def open_write_stream_sync(self, path: str, *, overwrite: bool = True, **kwargs: Any) -> Iterator[Any]:
            raise NotImplementedError

        @contextlib.asynccontextmanager
        async def open_write_stream_async(
            self, path: str, *, overwrite: bool = True, **kwargs: Any
        ) -> AsyncIterator[AsyncWritableBytesSink]:
            yield gated_sink

    registry = cast("StorageRegistry", _MapRegistry({"gate": _GatedBackend()}))
    pipeline = AsyncStoragePipeline(registry=registry)

    schema = pa.schema(cast("Any", [("id", pa.int64()), ("payload", pa.binary())]))
    yield_count = 0

    def _generate_batches() -> Iterator[pa.RecordBatch]:
        nonlocal yield_count
        for i in range(40):
            yield_count += 1
            yield pa.RecordBatch.from_pydict({"id": [i], "payload": [b"x" * 131_072]}, schema=schema)

    task = asyncio.create_task(
        pipeline.write_arrow_batches_async("alias://gate/stream.ipc", _generate_batches(), file_format="arrow-ipc")
    )

    await asyncio.sleep(0.05)
    first_poll = yield_count
    await asyncio.sleep(0.05)
    second_poll = yield_count

    assert first_poll == second_poll
    assert first_poll < 40

    gate.set()
    telemetry = await task
    assert telemetry["rows_processed"] == 40
    assert len(gated_sink.data) > 0
    decoded = decode_arrow_payload(bytes(gated_sink.data), "arrow-ipc")
    assert decoded.num_rows == 40


def test_pipeline_csv_vector_column_raises() -> None:
    """CSV export with fixed-size list vector column raises pa.ArrowInvalid and does not write object."""
    backend = ObStoreBackend("memory://")
    registry = cast("StorageRegistry", _MapRegistry({"mem": backend}))
    pipeline = SyncStoragePipeline(registry=registry)

    schema = pa.schema([("id", pa.int64()), ("embedding", pa.list_(pa.float32(), 2))])
    table = pa.Table.from_arrays(
        [
            pa.array([1, 2], type=pa.int64()),
            pa.FixedSizeListArray.from_arrays(pa.array([1.0, 2.0, 3.0, 4.0], type=pa.float32()), 2),
        ],
        schema=schema,
    )

    with pytest.raises(pa.ArrowInvalid, match="Unsupported Type"):
        pipeline.write_arrow(table, "alias://mem/vectors.csv", format_hint="csv")

    assert not backend.is_path_sync("vectors.csv")


async def test_pipeline_minimal_stub_backend_buffered_path() -> None:
    """Storage pipelines route to buffered path for 2-parameter stub backends without overwrite kwarg."""

    class _MinimalStubBackend:
        backend_type = "minimal_stub"
        protocol = "stub"

        def __init__(self) -> None:
            self.written_sync: dict[str, bytes] = {}
            self.written_async: dict[str, bytes] = {}

        def write_bytes_sync(self, path: str, payload: bytes) -> None:
            self.written_sync[path] = payload

        async def write_bytes_async(self, path: str, payload: bytes) -> None:
            self.written_async[path] = payload

    backend = _MinimalStubBackend()
    registry = cast("StorageRegistry", _MapRegistry({"stub": backend}))
    sync_p = SyncStoragePipeline(registry=registry)
    async_p = AsyncStoragePipeline(registry=registry)

    schema = pa.schema([("id", pa.int64())])
    table = pa.Table.from_arrays([pa.array([100, 200], type=pa.int64())], schema=schema)

    t_sync = sync_p.write_arrow(table, "alias://stub/sync.parquet")
    assert t_sync["rows_processed"] == 2
    assert "sync.parquet" in backend.written_sync

    t_sync_batches = sync_p.write_arrow_batches("alias://stub/sync_b.parquet", table.to_batches())
    assert t_sync_batches["rows_processed"] == 2
    assert "sync_b.parquet" in backend.written_sync

    t_async = await async_p.write_arrow(table, "alias://stub/async.parquet")
    assert t_async["rows_processed"] == 2
    assert "async.parquet" in backend.written_async

    t_async_batches = await async_p.write_arrow_batches_async("alias://stub/async_b.parquet", table.to_batches())
    assert t_async_batches["rows_processed"] == 2
    assert "async_b.parquet" in backend.written_async


async def test_pipeline_overwrite_false_raises_across_backends(tmp_path: Path) -> None:
    """overwrite=False raises FileExistsInStorageError and preserves original payload."""
    mem_backend = ObStoreBackend("memory://")
    file_backend = ObStoreBackend(f"file://{tmp_path / 'obstore_dir'}")
    local_backend = LocalStore(str(tmp_path / "local_dir"))

    registry = cast("StorageRegistry", _MapRegistry({"mem": mem_backend, "obfile": file_backend, "loc": local_backend}))

    sync_p = SyncStoragePipeline(registry=registry)
    async_p = AsyncStoragePipeline(registry=registry)

    schema = pa.schema([("id", pa.int64())])
    original_table = pa.Table.from_arrays([pa.array([1], type=pa.int64())], schema=schema)
    new_table = pa.Table.from_arrays([pa.array([999], type=pa.int64())], schema=schema)

    for alias in ["alias://mem", "alias://obfile", "alias://loc"]:
        parquet_dest = f"{alias}/item.parquet"
        sync_p.write_arrow(original_table, parquet_dest, overwrite=False)
        with pytest.raises(FileExistsInStorageError):
            sync_p.write_arrow(new_table, parquet_dest, overwrite=False)

        read_back, _ = sync_p.read_arrow(parquet_dest, file_format="parquet")
        assert read_back.column("id").to_pylist() == [1]

        rows_dest = f"{alias}/rows.jsonl"
        await async_p.write_rows([{"id": 1}], rows_dest, overwrite=False)
        with pytest.raises(FileExistsInStorageError):
            await async_p.write_rows([{"id": 999}], rows_dest, overwrite=False)

        batch_dest = f"{alias}/batch.parquet"
        await async_p.write_arrow_batches_async(batch_dest, original_table.to_batches(), overwrite=False)
        with pytest.raises(FileExistsInStorageError):
            await async_p.write_arrow_batches_async(batch_dest, new_table.to_batches(), overwrite=False)

        ar_table = ArrowResult(statement=cast("Any", None), data=original_table)
        ar_new = ArrowResult(statement=cast("Any", None), data=new_table)
        ar_dest = f"{alias}/ar.parquet"
        ar_table.write_to_storage_sync(ar_dest, overwrite=False, pipeline=sync_p)
        with pytest.raises(FileExistsInStorageError):
            ar_new.write_to_storage_sync(ar_dest, overwrite=False, pipeline=sync_p)
        with pytest.raises(FileExistsInStorageError):
            await ar_new.write_to_storage_async(ar_dest, overwrite=False, pipeline=async_p)


async def test_pipeline_midstream_failure_and_cancellation_preserves_object(tmp_path: Path) -> None:
    """Mid-stream errors and asyncio.CancelledError preserve pre-existing object across memory:// and file://."""
    mem_backend = ObStoreBackend("memory://")
    file_backend = ObStoreBackend(f"file://{tmp_path}")
    registry = cast("StorageRegistry", _MapRegistry({"mem": mem_backend, "obfile": file_backend}))
    sync_p = SyncStoragePipeline(registry=registry)
    async_p = AsyncStoragePipeline(registry=registry)

    schema = pa.schema([("id", pa.int64())])
    initial_table = pa.Table.from_arrays([pa.array([1], type=pa.int64())], schema=schema)

    for alias_prefix in ("alias://mem", "alias://obfile"):
        dest = f"{alias_prefix}/existing.parquet"
        sync_p.write_arrow(initial_table, dest)

        def _failing_batches() -> Iterator[pa.RecordBatch]:
            yield pa.RecordBatch.from_pydict({"id": [10]}, schema=schema)
            raise ValueError("bad batch")

        reset_storage_bridge_metrics()
        with pytest.raises(ValueError, match="bad batch"):
            sync_p.write_arrow_batches(dest, _failing_batches())

        assert get_storage_bridge_metrics().get("storage_bridge.bytes_written", 0) == 0
        read_back, _ = sync_p.read_arrow(dest, file_format="parquet")
        assert read_back.column("id").to_pylist() == [1]

        def _failing_batches_async() -> Iterator[pa.RecordBatch]:
            yield pa.RecordBatch.from_pydict({"id": [20]}, schema=schema)
            raise ValueError("bad async batch")

        reset_storage_bridge_metrics()
        with pytest.raises(ValueError, match="bad async batch"):
            await async_p.write_arrow_batches_async(dest, _failing_batches_async())

        assert get_storage_bridge_metrics().get("storage_bridge.bytes_written", 0) == 0
        read_back, _ = sync_p.read_arrow(dest, file_format="parquet")
        assert read_back.column("id").to_pylist() == [1]

        started = asyncio.Event()
        stop_event = threading.Event()
        loop = asyncio.get_running_loop()

        def _slow_batches_async(
            ready: asyncio.Event = started, release: threading.Event = stop_event
        ) -> Iterator[pa.RecordBatch]:
            yield pa.RecordBatch.from_pydict({"id": [30]}, schema=schema)
            loop.call_soon_threadsafe(ready.set)
            release.wait(timeout=0.5)
            yield pa.RecordBatch.from_pydict({"id": [31]}, schema=schema)

        reset_storage_bridge_metrics()
        task = asyncio.create_task(async_p.write_arrow_batches_async(dest, _slow_batches_async()))
        await started.wait()
        task.cancel()
        stop_event.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert get_storage_bridge_metrics().get("storage_bridge.bytes_written", 0) == 0
        read_back, _ = sync_p.read_arrow(dest, file_format="parquet")
        assert read_back.column("id").to_pylist() == [1]


async def test_pipeline_write_options_precedence_and_stripping(tmp_path: Path) -> None:
    """write_options precedence, real StorageRegistry kwarg stripping, and single _backend resolution per call."""

    class _CountingRealRegistry(StorageRegistry):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[tuple[str, str | None, dict[str, Any]]] = []

        def get(self, uri_or_alias: str | Path, *, backend: str | None = None, **kwargs: Any) -> Any:
            self.calls.append((str(uri_or_alias), backend, dict(kwargs)))
            return super().get(uri_or_alias, backend=backend, **kwargs)

    registry = _CountingRealRegistry()
    registry.register_alias("ob", f"file://{tmp_path}", backend="obstore")
    pipeline = SyncStoragePipeline(registry=registry, storage_options={"write_options": {"delimiter": "|"}})
    async_pipeline = AsyncStoragePipeline(registry=registry, storage_options={"write_options": {"delimiter": "|"}})

    schema = pa.schema([("id", pa.int64())])
    table = pa.Table.from_arrays([pa.array(list(range(6)), type=pa.int64())], schema=schema)

    registry.calls.clear()
    pipeline.write_arrow(table, "alias://ob/call_opt.parquet", storage_options={"write_options": {"row_group_size": 3}})
    assert len(registry.calls) == 1
    assert "write_options" not in registry.calls[0][2]
    pf1 = pq.ParquetFile(tmp_path / "call_opt.parquet")
    assert pf1.num_row_groups == 2

    direct_file_uri = f"file://{tmp_path / 'direct_uri_opt.parquet'}"
    registry.calls.clear()
    pipeline.write_arrow(
        table, direct_file_uri, storage_options={"backend": "obstore", "write_options": {"row_group_size": 3}}
    )
    assert len(registry.calls) == 1
    assert registry.calls[0] == (direct_file_uri, "obstore", {})
    assert pq.ParquetFile(tmp_path / "direct_uri_opt.parquet").num_row_groups == 2

    buffered_file_uri = f"file://{tmp_path / 'buffered_once.parquet'}"
    registry.calls.clear()
    await async_pipeline.write_arrow(
        table,
        buffered_file_uri,
        storage_options={"backend": "obstore", "write_options": {"row_group_size": 2}},
        overwrite=False,
    )
    assert len(registry.calls) == 1
    assert registry.calls[0] == (buffered_file_uri, "obstore", {})

    pipeline.write_arrow(
        table,
        "alias://ob/explicit_opt.parquet",
        storage_options={"write_options": {"row_group_size": 3}},
        write_options={"row_group_size": 2},
    )
    pf2 = pq.ParquetFile(tmp_path / "explicit_opt.parquet")
    assert pf2.num_row_groups == 3

    pipeline.write_arrow(table, "alias://ob/plain.parquet")
    pf3 = pq.ParquetFile(tmp_path / "plain.parquet")
    assert pf3.num_row_groups == 1

    pipeline.write_arrow(table, "alias://ob/delim.csv", format_hint="csv")
    csv_text = (tmp_path / "delim.csv").read_text()
    assert '"id"' in csv_text or "id" in csv_text
    table_from_csv = pa_csv.read_csv(tmp_path / "delim.csv", parse_options=pa_csv.ParseOptions(delimiter="|"))
    assert table_from_csv.num_rows == 6


async def test_arrow_result_write_to_storage_batches_iterables_and_zero_row_mapping(tmp_path: Path) -> None:
    """ArrowResult.write_to_storage_* streams list/Iterator[RecordBatch] and preserves 0-row typed Mapping schema."""
    registry = StorageRegistry()
    registry.register_alias("ob", f"file://{tmp_path}", backend="obstore")
    sync_p = SyncStoragePipeline(registry=registry)
    async_p = AsyncStoragePipeline(registry=registry)

    schema = pa.table({"id": [1], "label": ["a"]}).schema
    b1 = pa.RecordBatch.from_pydict({"id": [1, 2], "label": ["a", "b"]}, schema=schema)
    b2 = pa.RecordBatch.from_pydict({"id": [3], "label": ["c"]}, schema=schema)

    res_list_sync = ArrowResult(statement=cast("Any", None), data=[b1, b2])
    t1 = res_list_sync.write_to_storage_sync("alias://ob/list_sync.parquet", pipeline=sync_p)
    assert t1["rows_processed"] == 3
    loaded_1, _ = sync_p.read_arrow("alias://ob/list_sync.parquet", file_format="parquet")
    assert loaded_1.to_pylist() == [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}, {"id": 3, "label": "c"}]

    res_list_async = ArrowResult(statement=cast("Any", None), data=[b1, b2])
    t2 = await res_list_async.write_to_storage_async("alias://ob/list_async.parquet", pipeline=async_p)
    assert t2["rows_processed"] == 3
    loaded_2, _ = await async_p.read_arrow_async("alias://ob/list_async.parquet", file_format="parquet")
    assert loaded_2.to_pylist() == [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}, {"id": 3, "label": "c"}]

    res_iter_sync = ArrowResult(statement=cast("Any", None), data=iter([b1, b2]))
    t3 = res_iter_sync.write_to_storage_sync("alias://ob/iter_sync.ipc", format_hint="arrow-ipc", pipeline=sync_p)
    assert t3["rows_processed"] == 3
    loaded_3, _ = sync_p.read_arrow("alias://ob/iter_sync.ipc", file_format="arrow-ipc")
    assert loaded_3.to_pylist() == [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}, {"id": 3, "label": "c"}]

    res_iter_async = ArrowResult(statement=cast("Any", None), data=iter([b1, b2]))
    t4 = await res_iter_async.write_to_storage_async(
        "alias://ob/iter_async.ipc", format_hint="arrow-ipc", pipeline=async_p
    )
    assert t4["rows_processed"] == 3
    loaded_4, _ = await async_p.read_arrow_async("alias://ob/iter_async.ipc", file_format="arrow-ipc")
    assert loaded_4.to_pylist() == [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}, {"id": 3, "label": "c"}]

    empty_typed_mapping = {"id": pa.array([], type=pa.int64()), "label": pa.array([], type=pa.string())}
    for fmt in ("parquet", "arrow-ipc", "csv"):
        res_map_sync = ArrowResult(statement=cast("Any", None), data=empty_typed_mapping)
        t_map_s = res_map_sync.write_to_storage_sync(
            f"alias://ob/zero_map_sync.{fmt}", format_hint=fmt, pipeline=sync_p
        )
        assert t_map_s["rows_processed"] == 0
        loaded_ms, _ = sync_p.read_arrow(f"alias://ob/zero_map_sync.{fmt}", file_format=fmt)
        assert loaded_ms.num_rows == 0
        assert loaded_ms.column_names == ["id", "label"]
        if fmt != "csv":
            assert loaded_ms.schema.field("id").type == pa.int64()

        res_map_async = ArrowResult(statement=cast("Any", None), data=empty_typed_mapping)
        t_map_a = await res_map_async.write_to_storage_async(
            f"alias://ob/zero_map_async.{fmt}", format_hint=fmt, pipeline=async_p
        )
        assert t_map_a["rows_processed"] == 0
        loaded_ma, _ = await async_p.read_arrow_async(f"alias://ob/zero_map_async.{fmt}", file_format=fmt)
        assert loaded_ma.num_rows == 0
        assert loaded_ma.column_names == ["id", "label"]

    res_dicts = ArrowResult(statement=cast("Any", None), data=[{"id": 7, "label": "row"}])
    t_dicts = res_dicts.write_to_storage_sync("alias://ob/dicts.parquet", pipeline=sync_p)
    assert t_dicts["rows_processed"] == 1

    res_empty_list = ArrowResult(statement=cast("Any", None), data=[])
    t_empty = await res_empty_list.write_to_storage_async("alias://ob/empty_list.parquet", pipeline=async_p)
    assert t_empty["rows_processed"] == 0


async def test_sql_result_write_to_storage_overwrite_false() -> None:
    """SQLResult write_to_storage_sync and write_to_storage_async honor overwrite=False."""
    registry = cast("StorageRegistry", _MapRegistry({"mem": ObStoreBackend("memory://")}))
    sync_p = SyncStoragePipeline(registry=registry)
    async_p = AsyncStoragePipeline(registry=registry)

    result = SQLResult(statement=None, data=[{"id": 1, "name": "first"}])
    result2 = SQLResult(statement=None, data=[{"id": 2, "name": "second"}])

    result.write_to_storage_sync("alias://mem/rows_sync.jsonl", overwrite=False, pipeline=sync_p)
    with pytest.raises(FileExistsInStorageError):
        result2.write_to_storage_sync("alias://mem/rows_sync.jsonl", overwrite=False, pipeline=sync_p)

    await result.write_to_storage_async("alias://mem/rows_async.jsonl", overwrite=False, pipeline=async_p)
    with pytest.raises(FileExistsInStorageError):
        await result2.write_to_storage_async("alias://mem/rows_async.jsonl", overwrite=False, pipeline=async_p)


async def test_pipeline_routes_local_store_direct_stream_and_create_only(tmp_path: Path) -> None:
    """Sync and async pipelines stream directly through LocalStore stage files and enforce overwrite=False."""
    registry = StorageRegistry()
    registry.register_alias("loc", f"file://{tmp_path}", backend="local")
    sync_p = SyncStoragePipeline(registry=registry)
    async_p = AsyncStoragePipeline(registry=registry)

    schema = pa.table({"id": [1]}).schema
    batch_1 = pa.RecordBatch.from_pydict({"id": [1, 2]}, schema=schema)
    batch_2 = pa.RecordBatch.from_pydict({"id": [3, 4]}, schema=schema)

    observed_at_batch_2: dict[str, Any] = {}

    def _inspect_stage_batches() -> Iterator[pa.RecordBatch]:
        yield batch_1
        names = sorted(p.name for p in tmp_path.iterdir())
        observed_at_batch_2["dest_exists"] = "staged_batches.parquet" in names
        observed_at_batch_2["stages"] = [n for n in names if n.startswith(".sqlspec-stage-")]
        yield batch_2

    telemetry = sync_p.write_arrow_batches("alias://loc/staged_batches.parquet", _inspect_stage_batches())
    assert telemetry["backend"] == "local"
    assert telemetry["rows_processed"] == 4
    assert observed_at_batch_2["dest_exists"] is False
    assert len(observed_at_batch_2["stages"]) == 1

    tbl = pa.table({"id": [10, 20]})
    sync_p.write_arrow(tbl, "alias://loc/create_only_sync.parquet", overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        sync_p.write_arrow(tbl, "alias://loc/create_only_sync.parquet", overwrite=False)

    await async_p.write_arrow(tbl, "alias://loc/create_only_async.parquet", overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        await async_p.write_arrow(tbl, "alias://loc/create_only_async.parquet", overwrite=False)


async def test_pipeline_routes_fsspec_memory_direct_stream_and_create_only(tmp_path: Path) -> None:
    """Sync and async pipelines stream batches to FSSpecBackend memory alias via staging and enforce overwrite=False."""
    pytest.importorskip("fsspec")

    registry = StorageRegistry()
    mem_base = f"/pipe_fsspec_{tmp_path.name}"
    registry.register_alias("fsmem", "memory://", backend="fsspec", base_path=mem_base)
    sync_p = SyncStoragePipeline(registry=registry)
    async_p = AsyncStoragePipeline(registry=registry)

    backend = registry.get("fsmem")
    schema = pa.table({"id": [1]}).schema
    batch_1 = pa.RecordBatch.from_pydict({"id": [1, 2]}, schema=schema)
    batch_2 = pa.RecordBatch.from_pydict({"id": [3, 4]}, schema=schema)

    observed_at_batch_2: dict[str, Any] = {}

    def _inspect_fsspec_batches() -> Iterator[pa.RecordBatch]:
        yield batch_1
        observed_at_batch_2["dest_exists"] = backend.exists_sync("batches.parquet")
        observed_at_batch_2["objects"] = backend.list_objects_sync()
        yield batch_2

    telemetry = sync_p.write_arrow_batches("alias://fsmem/batches.parquet", _inspect_fsspec_batches())
    assert telemetry["backend"] == "fsspec"
    assert telemetry["rows_processed"] == 4
    assert observed_at_batch_2["dest_exists"] is False
    assert any(".sqlspec-stage-" in item for item in observed_at_batch_2["objects"])

    read_back, _ = sync_p.read_arrow("alias://fsmem/batches.parquet", file_format="parquet")
    assert read_back.column("id").to_pylist() == [1, 2, 3, 4]

    tbl = pa.table({"id": [10, 20]})
    sync_p.write_arrow(tbl, "alias://fsmem/exclusive_sync.parquet", overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        sync_p.write_arrow(tbl, "alias://fsmem/exclusive_sync.parquet", overwrite=False)

    await async_p.write_arrow_batches_async("alias://fsmem/exclusive_async.parquet", tbl.to_batches(), overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        await async_p.write_arrow_batches_async(
            "alias://fsmem/exclusive_async.parquet", tbl.to_batches(), overwrite=False
        )
