# pyright: reportPrivateUsage=false
"""Focused storage payload codec and write-format contract tests."""

import asyncio
import gc
import os
import threading
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

import sqlspec.storage.pipeline as storage_pipeline
from sqlspec.exceptions import StorageCapabilityError
from sqlspec.storage._arrow_payload import (
    StorageFormat,
    decode_arrow_payload,
    encode_arrow_payload,
    encode_arrow_to_async_sink,
    encode_arrow_to_sink,
    join_shielded,
    validate_arrow_write_options,
)
from sqlspec.storage.pipeline import (
    AsyncStoragePipeline,
    SyncStoragePipeline,
    get_recent_storage_events,
    reset_storage_bridge_events,
)
from sqlspec.utils.serializers import to_json


class _TrackingBackend:
    backend_type = "tracking"

    def __init__(self) -> None:
        self.writes: list[tuple[str, bytes]] = []

    def write_bytes_sync(self, path: str, payload: bytes) -> None:
        self.writes.append((path, payload))

    async def write_bytes_async(self, path: str, payload: bytes) -> None:
        self.writes.append((path, payload))


def _install_backend(monkeypatch: pytest.MonkeyPatch, pipeline_type: type[Any]) -> _TrackingBackend:
    backend = _TrackingBackend()
    monkeypatch.setattr(pipeline_type, "_backend", lambda *_args, **_kwargs: (backend, "payload", backend.backend_type))
    return backend


def test_decode_jsonl_uses_native_timestamp_inference() -> None:
    payload = b'{"id":1,"created_at":"2026-08-01T12:34:56"}\n'

    table = decode_arrow_payload(payload, "jsonl")

    assert table.schema.field("created_at").type == pa.timestamp("s")
    assert table.to_pylist() == [{"id": 1, "created_at": table.column("created_at")[0].as_py()}]


def test_decode_empty_jsonl_preserves_empty_table() -> None:
    table = decode_arrow_payload(b"", "jsonl")

    assert table.equals(pa.table({}))


def test_decode_whitespace_only_jsonl_is_delegated_to_pyarrow() -> None:
    table = decode_arrow_payload(b" \n\t", "jsonl")

    assert table.equals(pa.table({}))


def test_decode_jsonl_row_larger_than_default_block_size() -> None:
    blob = "x" * (2 * 1024 * 1024)
    payload = b'{"id":1,"blob":"' + blob.encode() + b'"}\n'

    table = decode_arrow_payload(payload, "jsonl")

    assert table.num_rows == 1
    assert table.to_pylist()[0]["blob"] == blob


def test_decode_jsonl_round_trips_rows_spanning_the_block_boundary() -> None:
    rows = [{"id": index, "blob": "y" * 400_000} for index in range(6)]
    payload = b"".join(to_json(row, as_bytes=True) + b"\n" for row in rows)

    table = decode_arrow_payload(payload, "jsonl")

    assert table.to_pylist() == rows


def test_json_array_decode_keeps_existing_shape() -> None:
    table = decode_arrow_payload(b'[{"id":1},{"id":2}]', "json")

    assert table.to_pylist() == [{"id": 1}, {"id": 2}]


@pytest.mark.parametrize("format_choice", ["parquet", "arrow-ipc", "csv"])
def test_arrow_payload_round_trip_has_expected_signature(format_choice: StorageFormat) -> None:
    table = pa.table({"id": [1, 2], "name": ["alpha", "beta"]})

    payload = encode_arrow_payload(table, format_choice, compression=None)
    restored = decode_arrow_payload(payload, format_choice)

    signatures = {"parquet": b"PAR1", "arrow-ipc": b"ARROW1", "csv": b'"id","name"'}
    assert payload.startswith(signatures[format_choice])
    assert restored.to_pylist() == table.to_pylist()


@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc", "csv"])
def test_sync_row_write_rejects_arrow_formats_before_encoding_or_io(
    monkeypatch: pytest.MonkeyPatch, format_hint: StorageFormat
) -> None:
    pipeline = SyncStoragePipeline()
    backend = _install_backend(monkeypatch, SyncStoragePipeline)
    monkeypatch.setattr(storage_pipeline, "serialize_collection", pytest.fail)
    monkeypatch.setattr(storage_pipeline, "_encode_row_payload", pytest.fail)
    reset_storage_bridge_events()

    with pytest.raises(StorageCapabilityError, match="Row storage writes support only JSON and JSONL"):
        pipeline.write_rows([{"id": 1}], "payload", format_hint=format_hint)

    assert backend.writes == []
    assert get_recent_storage_events() == []


@pytest.mark.parametrize("format_hint", ["json", "jsonl"])
def test_sync_arrow_write_rejects_row_formats_before_encoding_or_io(
    monkeypatch: pytest.MonkeyPatch, format_hint: StorageFormat
) -> None:
    pipeline = SyncStoragePipeline()
    backend = _install_backend(monkeypatch, SyncStoragePipeline)
    monkeypatch.setattr(storage_pipeline, "_encode_arrow_source_to_bytes", pytest.fail)
    monkeypatch.setattr(storage_pipeline, "encode_arrow_to_sink", pytest.fail)
    reset_storage_bridge_events()

    with pytest.raises(StorageCapabilityError, match="Arrow storage writes support only Parquet, Arrow IPC, and CSV"):
        pipeline.write_arrow(pa.table({"id": [1]}), "payload", format_hint=format_hint)

    assert backend.writes == []
    assert get_recent_storage_events() == []


@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc", "csv"])
async def test_async_row_write_rejects_arrow_formats_before_io(
    monkeypatch: pytest.MonkeyPatch, format_hint: StorageFormat
) -> None:
    pipeline = AsyncStoragePipeline()
    backend = _install_backend(monkeypatch, AsyncStoragePipeline)
    monkeypatch.setattr(storage_pipeline, "serialize_collection", pytest.fail)
    monkeypatch.setattr(storage_pipeline, "_encode_row_payload", pytest.fail)
    reset_storage_bridge_events()

    with pytest.raises(StorageCapabilityError, match="Row storage writes support only JSON and JSONL"):
        await pipeline.write_rows([{"id": 1}], "payload", format_hint=format_hint)

    assert backend.writes == []
    assert get_recent_storage_events() == []


@pytest.mark.parametrize("format_hint", ["json", "jsonl"])
async def test_async_arrow_write_rejects_row_formats_before_io(
    monkeypatch: pytest.MonkeyPatch, format_hint: StorageFormat
) -> None:
    pipeline = AsyncStoragePipeline()
    backend = _install_backend(monkeypatch, AsyncStoragePipeline)
    monkeypatch.setattr(storage_pipeline, "_encode_arrow_source_to_bytes", pytest.fail)
    monkeypatch.setattr(storage_pipeline, "encode_arrow_to_async_sink", pytest.fail)
    reset_storage_bridge_events()

    with pytest.raises(StorageCapabilityError, match="Arrow storage writes support only Parquet, Arrow IPC, and CSV"):
        await pipeline.write_arrow(pa.table({"id": [1]}), "payload", format_hint=format_hint)

    assert backend.writes == []
    assert get_recent_storage_events() == []


class _TrackingBytesSink:
    """Synchronous in-memory sink tracking writes and close calls."""

    def __init__(self) -> None:
        self.data = bytearray()
        self.closed_calls = 0

    def write(self, data: bytes, /) -> int | None:
        self.data.extend(data)
        return len(data)

    def close(self) -> None:
        self.closed_calls += 1


class _AsyncTrackingBytesSink:
    """Asynchronous in-memory sink tracking chunks and close calls."""

    def __init__(self) -> None:
        self.chunks: list[bytes] = []
        self.closed_calls = 0

    async def write(self, data: bytes, /) -> int | None:
        self.chunks.append(data)
        return len(data)

    def close(self) -> None:
        self.closed_calls += 1

    @property
    def data(self) -> bytes:
        return b"".join(self.chunks)


class _CStreamWrapper:
    """Wrapper exposing __arrow_c_stream__ from an underlying reader."""

    def __init__(self, reader: pa.RecordBatchReader) -> None:
        self._reader = reader

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        stream_fn: Any = getattr(self._reader, "__arrow_c_stream__")
        return stream_fn(requested_schema)


def _make_source(kind: str) -> Any:
    schema_fields: list[tuple[str, pa.DataType]] = [("id", pa.int64()), ("name", pa.string())]
    schema = pa.schema(schema_fields)
    b1 = pa.record_batch([pa.array([1, 2]), pa.array(["a", "b"])], schema=schema)
    b2 = pa.record_batch([pa.array([3]), pa.array(["c"])], schema=schema)
    if kind == "table":
        return pa.Table.from_batches([b1, b2])
    if kind == "batch":
        return pa.record_batch([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)
    if kind == "reader":
        return pa.RecordBatchReader.from_batches(schema, [b1, b2])
    if kind == "c_stream":
        return _CStreamWrapper(pa.RecordBatchReader.from_batches(schema, [b1, b2]))
    if kind == "iterable":
        return iter([b1, b2])
    raise ValueError(kind)


@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc", "csv"])
@pytest.mark.parametrize("source_kind", ["table", "batch", "reader", "c_stream", "iterable"])
def test_encode_arrow_to_sink_sources_roundtrip(format_hint: StorageFormat, source_kind: str) -> None:
    source = _make_source(source_kind)
    sink = _TrackingBytesSink()
    nbytes, rows = encode_arrow_to_sink(source, sink, format_hint=format_hint)
    assert nbytes == len(sink.data)
    assert rows == 3
    restored = decode_arrow_payload(bytes(sink.data), format_hint)
    assert restored.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]


@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc", "csv"])
@pytest.mark.parametrize("source_kind", ["table", "batch", "reader", "c_stream", "iterable"])
async def test_encode_arrow_to_async_sink_sources_roundtrip(format_hint: StorageFormat, source_kind: str) -> None:
    source = _make_source(source_kind)
    sink = _AsyncTrackingBytesSink()
    nbytes, rows = await encode_arrow_to_async_sink(source, sink, format_hint=format_hint, chunk_size=32)
    assert nbytes == len(sink.data)
    assert rows == 3
    restored = decode_arrow_payload(bytes(sink.data), format_hint)
    assert restored.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]


def test_sink_receives_bytes_incrementally_before_stream_exhausted() -> None:
    schema = pa.schema([("id", pa.int64())])
    b1 = pa.record_batch([pa.array([1, 2])], schema=schema)
    b2 = pa.record_batch([pa.array([3, 4])], schema=schema)
    b3 = pa.record_batch([pa.array([5, 6])], schema=schema)

    sink = _TrackingBytesSink()
    recorded_len_at_batch_3 = -1

    def batch_generator() -> Iterator[pa.RecordBatch]:
        nonlocal recorded_len_at_batch_3
        yield b1
        yield b2
        recorded_len_at_batch_3 = len(sink.data)
        yield b3

    nbytes, rows = encode_arrow_to_sink(batch_generator(), sink, format_hint="arrow-ipc")
    assert rows == 6
    assert recorded_len_at_batch_3 > 0
    assert nbytes > recorded_len_at_batch_3


@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc", "csv"])
def test_native_sink_bypass_and_byte_parity(format_hint: StorageFormat) -> None:
    table = pa.table({"x": [10, 20], "y": ["hello", "world"]})
    native_sink = pa.BufferOutputStream()
    native_bytes_written, native_rows = encode_arrow_to_sink(table, native_sink, format_hint=format_hint)
    assert native_sink.closed is False
    buffer = native_sink.getvalue()
    assert native_bytes_written == buffer.size
    assert native_rows == 2

    tracking_sink = _TrackingBytesSink()
    tracking_bytes_written, tracking_rows = encode_arrow_to_sink(table, tracking_sink, format_hint=format_hint)
    assert tracking_bytes_written == native_bytes_written
    assert bytes(tracking_sink.data) == buffer.to_pybytes()
    assert tracking_rows == 2


def test_parquet_write_options_row_groups() -> None:
    import pyarrow.parquet as pq

    table = pa.table({"id": [1, 2, 3, 4]})
    sink = _TrackingBytesSink()
    _nbytes, rows = encode_arrow_to_sink(
        table, sink, format_hint="parquet", compression="zstd", write_options={"row_group_size": 2}
    )
    assert rows == 4
    parquet_file = pq.ParquetFile(pa.BufferReader(bytes(sink.data)))
    assert parquet_file.num_row_groups == 2


def test_arrow_ipc_write_options_compression() -> None:
    table = pa.table({"id": list(range(100))})
    sink = _TrackingBytesSink()
    _nbytes, rows = encode_arrow_to_sink(table, sink, format_hint="arrow-ipc", write_options={"compression": "lz4"})
    assert rows == 100
    restored = decode_arrow_payload(bytes(sink.data), "arrow-ipc")
    assert restored.to_pylist() == table.to_pylist()


def test_csv_write_options_custom_delimiter_and_header() -> None:
    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    sink = _TrackingBytesSink()
    _nbytes, rows = encode_arrow_to_sink(
        table, sink, format_hint="csv", write_options={"delimiter": "|", "include_header": False}
    )
    assert rows == 3
    lines = bytes(sink.data).decode().strip().split("\n")
    assert lines == ['1|"a"', '2|"b"', '3|"c"']


@pytest.mark.parametrize("as_reader", [False, True])
@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc"])
def test_float_vector_and_decimal64_fidelity(as_reader: bool, format_hint: StorageFormat) -> None:
    schema = pa.schema([("embedding", pa.list_(pa.float32(), 2)), ("amount", pa.decimal64(10, 2))])
    rows = [
        {"embedding": [1.0, 2.0], "amount": Decimal("12.34")},
        {"embedding": [3.0, 4.0], "amount": Decimal("-56.78")},
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    source = pa.RecordBatchReader.from_batches(schema, table.to_batches()) if as_reader else table

    sink = _TrackingBytesSink()
    _nbytes, count = encode_arrow_to_sink(source, sink, format_hint=format_hint)
    assert count == 2

    restored = decode_arrow_payload(bytes(sink.data), format_hint)
    if format_hint == "arrow-ipc":
        assert restored.schema.field("amount").type == pa.decimal64(10, 2)
    else:
        assert restored.schema.field("amount").type == pa.decimal128(10, 2)
    assert restored.to_pylist() == rows


def test_decimal_only_csv_and_vector_rejection() -> None:
    dec_schema = pa.schema([("amount", pa.decimal64(10, 2))])
    dec_table = pa.Table.from_pylist([{"amount": Decimal("12.34")}, {"amount": Decimal("-56.78")}], schema=dec_schema)
    sink = _TrackingBytesSink()
    _nbytes, count = encode_arrow_to_sink(dec_table, sink, format_hint="csv")
    assert count == 2
    assert b"12.34" in bytes(sink.data)
    assert b"-56.78" in bytes(sink.data)

    vec_schema = pa.schema([("vec", pa.list_(pa.float32(), 2))])
    vec_table = pa.Table.from_pylist([{"vec": [1.0, 2.0]}], schema=vec_schema)
    vec_sink = _TrackingBytesSink()
    with pytest.raises(pa.ArrowInvalid, match="Unsupported Type"):
        encode_arrow_to_sink(vec_table, vec_sink, format_hint="csv")
    assert len(vec_sink.data) == 0


@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc", "csv"])
def test_zero_row_schema_retention(format_hint: StorageFormat) -> None:
    schema_fields: list[tuple[str, pa.DataType]] = [("id", pa.int64()), ("label", pa.string())]
    schema = pa.schema(schema_fields)
    empty_reader = pa.RecordBatchReader.from_batches(schema, [])
    sink = _TrackingBytesSink()
    nbytes, count = encode_arrow_to_sink(empty_reader, sink, format_hint=format_hint)
    assert count == 0
    assert nbytes > 0
    restored = decode_arrow_payload(bytes(sink.data), format_hint)
    assert restored.num_rows == 0
    assert restored.column_names == ["id", "label"]


@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc", "csv"])
def test_empty_list_fallback(format_hint: StorageFormat) -> None:
    sink = _TrackingBytesSink()
    nbytes, count = encode_arrow_to_sink([], sink, format_hint=format_hint)
    assert count == 0
    if format_hint == "csv":
        assert nbytes == 0
        assert len(sink.data) == 0
    else:
        assert nbytes > 0
        restored = decode_arrow_payload(bytes(sink.data), format_hint)
        assert restored.num_rows == 0
        assert restored.num_columns == 0


def _run_failing_encoder(format_hint: StorageFormat) -> tuple[int, _TrackingBytesSink]:
    schema = pa.schema([("id", pa.int64())])
    b1 = pa.record_batch([pa.array([1, 2])], schema=schema)
    sink = _TrackingBytesSink()
    before_len = 0

    def failing_batches() -> Iterator[pa.RecordBatch]:
        nonlocal before_len
        yield b1
        before_len = len(sink.data)
        raise RuntimeError("mid-batch boom")

    with pytest.raises(RuntimeError, match="mid-batch boom"):
        encode_arrow_to_sink(failing_batches(), sink, format_hint=format_hint)

    return before_len, sink


@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc"])
def test_poisoned_failure_writes_nothing_further(format_hint: StorageFormat) -> None:
    before_len, sink = _run_failing_encoder(format_hint)
    gc.collect()

    assert before_len > 4
    assert len(sink.data) == before_len
    if format_hint == "parquet":
        assert not bytes(sink.data).endswith(b"PAR1")
    assert sink.closed_calls == 0


@pytest.mark.parametrize(
    ("format_hint", "bad_options"),
    [
        ("parquet", {"bogus": 1}),
        ("parquet", {"delimiter": "|"}),
        ("arrow-ipc", {"row_group_size": 5}),
        ("csv", {"row_group_size": 10}),
    ],
)
def test_validate_arrow_write_options_unsupported(format_hint: StorageFormat, bad_options: dict[str, Any]) -> None:
    sink = _TrackingBytesSink()
    table = pa.table({"a": [1]})
    with pytest.raises(TypeError, match=r"^Unsupported write option\(s\)"):
        encode_arrow_to_sink(table, sink, format_hint=format_hint, write_options=bad_options)
    assert len(sink.data) == 0


def test_validate_arrow_write_options_direct() -> None:
    validate_arrow_write_options("parquet", None)
    validate_arrow_write_options("parquet", {})
    validate_arrow_write_options("parquet", {"row_group_size": 100})
    validate_arrow_write_options("arrow-ipc", {"compression": "lz4"})
    validate_arrow_write_options("csv", {"include_header": True})
    with pytest.raises(TypeError, match=r"^Unsupported write option\(s\) for csv: bogus"):
        validate_arrow_write_options("csv", {"bogus": True})


async def test_join_shielded_survives_repeated_cancellation() -> None:
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[int] = loop.create_future()

    task = asyncio.create_task(join_shielded(fut))
    await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.sleep(0.02)
    fut.set_result(42)

    with pytest.raises(asyncio.CancelledError):
        await task

    assert fut.done() is True
    assert fut.result() == 42


async def test_async_backpressure() -> None:
    schema = pa.schema([pa.field("payload", pa.binary())])
    large_chunk = os.urandom(2048)
    batch = pa.record_batch([pa.array([large_chunk])], schema=schema)

    yielded_count = 0

    def batch_gen() -> Iterator[pa.RecordBatch]:
        nonlocal yielded_count
        for _ in range(40):
            yielded_count += 1
            yield batch

    gate_event = asyncio.Event()

    class _GatedAsyncSink:
        def __init__(self) -> None:
            self.first_write = True

        async def write(self, data: bytes, /) -> int | None:
            if self.first_write:
                self.first_write = False
                await gate_event.wait()
            return len(data)

    sink = _GatedAsyncSink()
    encoder_task = asyncio.create_task(
        encode_arrow_to_async_sink(batch_gen(), sink, chunk_size=1024, format_hint="arrow-ipc")
    )

    await asyncio.sleep(0.05)
    first_poll = yielded_count
    await asyncio.sleep(0.05)
    second_poll = yielded_count

    assert first_poll == second_poll
    assert yielded_count < 10

    gate_event.set()
    nbytes, rows = await encoder_task
    assert rows == 40
    assert nbytes > 0


async def test_async_cancellation_and_sink_failure_cleanup() -> None:
    schema = pa.schema([pa.field("payload", pa.binary())])
    large_chunk = os.urandom(2048)
    batch = pa.record_batch([pa.array([large_chunk])], schema=schema)

    worker_finished = threading.Event()

    def tracking_batch_gen() -> Iterator[pa.RecordBatch]:
        try:
            for _ in range(50):
                yield batch
        finally:
            worker_finished.set()

    gate_event = asyncio.Event()

    class _GatedSink:
        async def write(self, data: bytes, /) -> int | None:
            await gate_event.wait()
            return len(data)

    sink = _GatedSink()
    task = asyncio.create_task(
        encode_arrow_to_async_sink(tracking_batch_gen(), sink, chunk_size=1024, format_hint="arrow-ipc")
    )

    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert worker_finished.is_set() is True


async def test_async_sink_write_failure_cleanup() -> None:
    schema = pa.schema([pa.field("payload", pa.binary())])
    large_chunk = os.urandom(2048)
    batch = pa.record_batch([pa.array([large_chunk])], schema=schema)

    worker_finished = threading.Event()

    def tracking_batch_gen() -> Iterator[pa.RecordBatch]:
        try:
            for _ in range(50):
                yield batch
        finally:
            worker_finished.set()

    class _FailingSink:
        def __init__(self) -> None:
            self.write_count = 0
            self.chunks: list[bytes] = []

        async def write(self, data: bytes, /) -> int | None:
            self.write_count += 1
            if self.write_count == 2:
                raise OSError("upload broken")
            self.chunks.append(data)
            return len(data)

    sink = _FailingSink()
    with pytest.raises(OSError, match="upload broken"):
        await encode_arrow_to_async_sink(tracking_batch_gen(), sink, chunk_size=1024, format_hint="arrow-ipc")

    assert worker_finished.is_set() is True
    assert len(sink.chunks) == 1
    assert len(sink.chunks[0]) == 1024


def test_reject_row_formats_and_invalid_chunk_size() -> None:
    table = pa.table({"a": [1]})
    sink = _TrackingBytesSink()

    with pytest.raises(StorageCapabilityError, match="Arrow storage writes support only Parquet, Arrow IPC, and CSV"):
        encode_arrow_to_sink(table, sink, format_hint="jsonl")

    assert len(sink.data) == 0


async def test_async_reject_invalid_chunk_size() -> None:
    table = pa.table({"a": [1]})
    async_sink = _AsyncTrackingBytesSink()

    with pytest.raises(ValueError, match="chunk_size must be a positive integer"):
        await encode_arrow_to_async_sink(table, async_sink, chunk_size=0)

    with pytest.raises(ValueError, match="chunk_size must be a positive integer"):
        await encode_arrow_to_async_sink(table, async_sink, chunk_size=-10)

    assert len(async_sink.data) == 0


@pytest.mark.parametrize("format_hint", ["parquet", "arrow-ipc", "csv"])
def test_poisonable_counting_sink_loops_on_partial_writes(format_hint: StorageFormat) -> None:
    """_PoisonableCountingSink loops until all bytes are written when sink.write returns partial counts."""

    class _PartialWriteSink:
        def __init__(self, max_step: int) -> None:
            self._max_step = max_step
            self.data = bytearray()
            self.write_calls = 0

        def write(self, data: bytes, /) -> int:
            assert isinstance(data, bytes)
            self.write_calls += 1
            step = min(len(data), self._max_step)
            self.data.extend(data[:step])
            return step

    table = pa.table({"id": [1, 2, 3, 4], "name": ["alpha", "beta", "gamma", "delta"]})
    partial_sink = _PartialWriteSink(max_step=7)
    nbytes, rows = encode_arrow_to_sink(table, partial_sink, format_hint=format_hint)

    assert rows == 4
    assert nbytes == len(partial_sink.data)
    assert partial_sink.write_calls > 1
    expected = encode_arrow_payload(table, format_hint, compression=None)
    assert bytes(partial_sink.data) == expected


async def test_async_abort_halts_worker_before_second_batch_consumed() -> None:
    """Cancelling an async encode halts the worker immediately without consuming subsequent batches."""
    schema = pa.table({"id": [1], "name": ["a"]}).schema
    batch_1 = pa.record_batch([pa.array([1]), pa.array(["first"])], schema=schema)
    batch_2 = pa.record_batch([pa.array([2]), pa.array(["second"])], schema=schema)

    entered_gen = threading.Event()
    proceed_after_abort = threading.Event()
    batches_yielded = 0

    def tracked_batches() -> Iterator[pa.RecordBatch]:
        nonlocal batches_yielded
        entered_gen.set()
        proceed_after_abort.wait(timeout=5.0)
        batches_yielded += 1
        yield batch_1
        batches_yielded += 1
        yield batch_2

    sink = _AsyncTrackingBytesSink()
    task = asyncio.create_task(
        encode_arrow_to_async_sink(tracked_batches(), sink, chunk_size=1_048_576, format_hint="arrow-ipc")
    )

    await asyncio.to_thread(entered_gen.wait, 5.0)
    task.cancel()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    proceed_after_abort.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert batches_yielded == 1
    assert sink.chunks == []


async def test_join_shielded_suppresses_delayed_worker_exception_and_preserves_cancel() -> None:
    """join_shielded suppresses delayed worker exceptions when suppress_result=True and prioritizes CancelledError."""
    loop = asyncio.get_running_loop()

    suppressed_fut: asyncio.Future[None] = loop.create_future()
    suppress_task = asyncio.create_task(join_shielded(suppressed_fut, suppress_result=True))
    await asyncio.sleep(0)
    suppressed_fut.set_exception(RuntimeError("delayed worker failure"))
    assert await suppress_task is None

    cancel_fut: asyncio.Future[None] = loop.create_future()
    cancel_task = asyncio.create_task(join_shielded(cancel_fut, suppress_result=False))
    await asyncio.sleep(0)
    cancel_task.cancel()
    await asyncio.sleep(0)
    cancel_fut.set_exception(RuntimeError("delayed worker failure after cancel"))
    with pytest.raises(asyncio.CancelledError):
        await cancel_task


async def test_direct_stream_pipeline_bypasses_buffered_encoding_seams(monkeypatch: pytest.MonkeyPatch) -> None:
    """Direct-stream pipeline writes bypass _encode_arrow_source_to_bytes and backend write_bytes."""
    from sqlspec.storage import StorageRegistry
    from sqlspec.storage.backends.obstore import ObStoreBackend

    monkeypatch.setattr(ObStoreBackend, "write_bytes_sync", pytest.fail)
    monkeypatch.setattr(ObStoreBackend, "write_bytes_async", pytest.fail)
    monkeypatch.setattr(storage_pipeline, "_encode_arrow_source_to_bytes", pytest.fail)

    registry = StorageRegistry()
    registry.register_alias("mem_direct", "memory://", backend="obstore")

    sync_pipeline = SyncStoragePipeline(registry=registry)
    async_pipeline = AsyncStoragePipeline(registry=registry)
    table = pa.table({"id": [1, 2]})

    t_sync = sync_pipeline.write_arrow(table, "alias://mem_direct/sync.parquet")
    assert t_sync["rows_processed"] == 2
    t_async = await async_pipeline.write_arrow(table, "alias://mem_direct/async.parquet")
    assert t_async["rows_processed"] == 2
    assert registry.get("mem_direct").read_arrow_sync("sync.parquet").equals(table)
    assert (await registry.get("mem_direct").read_arrow_async("async.parquet")).equals(table)
