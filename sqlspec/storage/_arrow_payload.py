"""Interpreted PyArrow payload helpers for storage pipelines."""

import asyncio
import collections
import contextlib
import itertools
import threading
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlspec.exceptions import StorageCapabilityError
from sqlspec.storage._utils import import_pyarrow, import_pyarrow_csv, import_pyarrow_json, import_pyarrow_parquet
from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from sqlspec.protocols import AsyncWritableBytesSink
    from sqlspec.typing import ArrowTable

__all__ = (
    "StorageFormat",
    "decode_arrow_payload",
    "encode_arrow_payload",
    "encode_arrow_to_async_sink",
    "encode_arrow_to_sink",
    "join_shielded",
    "resolve_parquet_use_dictionary",
    "validate_arrow_write_options",
)


StorageFormat = Literal["jsonl", "json", "parquet", "arrow-ipc", "csv"]

_PYARROW_JSON_BLOCK_SIZE = 1 << 20
_MAX_PENDING_CHUNKS = 2

_PARQUET_ALLOWED_WRITE_OPTIONS = frozenset({
    "allow_truncated_timestamps",
    "bloom_filter_options",
    "coerce_timestamps",
    "column_encoding",
    "compression",
    "compression_level",
    "data_page_size",
    "data_page_version",
    "dictionary_pagesize_limit",
    "encryption_properties",
    "flavor",
    "max_rows_per_page",
    "row_group_size",
    "sorting_columns",
    "store_decimal_as_integer",
    "store_schema",
    "use_byte_stream_split",
    "use_compliant_nested_type",
    "use_content_defined_chunking",
    "use_deprecated_int96_timestamps",
    "use_dictionary",
    "version",
    "write_batch_size",
    "write_page_checksum",
    "write_page_index",
    "write_statistics",
    "write_time_adjusted_to_utc",
    "writer_engine_version",
})

_ARROW_IPC_ALLOWED_WRITE_OPTIONS = frozenset({
    "allow_64bit",
    "compression",
    "emit_dictionary_deltas",
    "metadata_version",
    "unify_dictionaries",
    "use_legacy_format",
    "use_threads",
})

_CSV_ALLOWED_WRITE_OPTIONS = frozenset({"batch_size", "delimiter", "include_header", "quoting_header", "quoting_style"})

_ALLOWED_WRITE_OPTIONS: dict[str, frozenset[str]] = {
    "arrow-ipc": _ARROW_IPC_ALLOWED_WRITE_OPTIONS,
    "csv": _CSV_ALLOWED_WRITE_OPTIONS,
    "parquet": _PARQUET_ALLOWED_WRITE_OPTIONS,
}


class _BridgeAbortedError(Exception):
    """Internal signal indicating an async sink bridge was aborted."""


class _PoisonableCountingSink:
    """Wrap a WritableBytesSink for pyarrow.PythonFile without closing the backend sink."""

    __slots__ = ("_closed", "_sink", "bytes_written")

    def __init__(self, sink: Any) -> None:
        self._sink = sink
        self._closed = False
        self.bytes_written = 0

    @property
    def closed(self) -> bool:
        return self._closed

    def writable(self) -> bool:
        return not self._closed

    def poison(self) -> None:
        self._closed = True

    def write(self, data: Any, /) -> int:
        if self._closed:
            return 0
        payload = data if type(data) is bytes else bytes(data)
        nbytes = len(payload)
        if nbytes == 0:
            return 0
        written = self._sink.write(payload)
        if written is not None and written < nbytes:
            if written <= 0:
                msg = "Writable sink returned 0 bytes for non-empty write"
                raise OSError(msg)
            view = memoryview(payload)
            offset = written
            while offset < nbytes:
                if self._closed:
                    return offset
                step = self._sink.write(view[offset:].tobytes())
                if step is None:
                    offset = nbytes
                    break
                if step <= 0:
                    msg = "Writable sink returned 0 bytes for non-empty write"
                    raise OSError(msg)
                offset += step
        self.bytes_written += nbytes
        return nbytes

    def flush(self) -> None:
        if self._closed:
            return
        flush_fn = getattr(self._sink, "flush", None)
        if callable(flush_fn):
            flush_fn()

    def close(self) -> None:
        self._closed = True


class _AsyncSinkBridge:
    """Bounded queue bridging threaded sink writes to an asynchronous consumer."""

    __slots__ = (
        "_aborted",
        "_buffer",
        "_chunk_size",
        "_cond",
        "_data_available",
        "_failed",
        "_loop",
        "_queue",
        "_worker_done",
    )

    def __init__(self, loop: asyncio.AbstractEventLoop, chunk_size: int) -> None:
        self._loop = loop
        self._chunk_size = chunk_size
        self._buffer = bytearray()
        self._queue: collections.deque[bytes] = collections.deque()
        self._cond = threading.Condition()
        self._data_available = asyncio.Event()
        self._worker_done = False
        self._failed = False
        self._aborted = False

    def write(self, data: Any, /) -> int:
        if self._aborted:
            raise _BridgeAbortedError
        payload = data if type(data) is bytes else bytes(data)
        total_len = len(payload)
        if total_len == 0:
            return 0

        offset = 0
        chunk_size = self._chunk_size
        while offset < total_len:
            if self._aborted:
                raise _BridgeAbortedError
            needed = chunk_size - len(self._buffer)
            available = total_len - offset
            take = min(needed, available)
            self._buffer.extend(payload[offset : offset + take])
            offset += take
            if len(self._buffer) == chunk_size:
                self._enqueue(bytes(self._buffer))
                self._buffer.clear()

        if self._aborted:
            raise _BridgeAbortedError
        return total_len

    def _enqueue(self, chunk: bytes) -> None:
        with self._cond:
            while len(self._queue) >= _MAX_PENDING_CHUNKS and not self._aborted:
                self._cond.wait()
            if self._aborted:
                raise _BridgeAbortedError
            self._queue.append(chunk)
            with contextlib.suppress(RuntimeError):
                self._loop.call_soon_threadsafe(self._data_available.set)

    def finish(self) -> None:
        with self._cond:
            if self._aborted:
                raise _BridgeAbortedError
            if self._buffer:
                self._queue.append(bytes(self._buffer))
                self._buffer.clear()
            self._worker_done = True
            with contextlib.suppress(RuntimeError):
                self._loop.call_soon_threadsafe(self._data_available.set)
            self._cond.notify_all()

    def mark_aborted_done(self) -> None:
        with self._cond:
            self._worker_done = True
            with contextlib.suppress(RuntimeError):
                self._loop.call_soon_threadsafe(self._data_available.set)
            self._cond.notify_all()

    def fail(self) -> None:
        with self._cond:
            self._buffer.clear()
            self._queue.clear()
            self._failed = True
            self._worker_done = True
            with contextlib.suppress(RuntimeError):
                self._loop.call_soon_threadsafe(self._data_available.set)
            self._cond.notify_all()

    def abort(self) -> None:
        with self._cond:
            self._aborted = True
            self._buffer.clear()
            self._queue.clear()
            self._cond.notify_all()

    async def dequeue(self) -> bytes | None:
        while True:
            with self._cond:
                if self._failed:
                    return None
                if self._queue:
                    chunk = self._queue.popleft()
                    self._cond.notify()
                    return chunk
                if self._worker_done:
                    return None
                self._data_available.clear()
            await self._data_available.wait()


def _normalize_arrow_schema(schema: Any, pa: Any) -> tuple[Any, bool]:
    new_fields = []
    has_change = False
    for field in schema:
        if str(field.type).startswith(("decimal32", "decimal64")):
            new_fields.append(field.with_type(pa.decimal128(field.type.precision, field.type.scale)))
            has_change = True
        else:
            new_fields.append(field)
    if has_change:
        return pa.schema(new_fields, metadata=schema.metadata), True
    return schema, False


def validate_arrow_write_options(format_hint: StorageFormat, options: "dict[str, Any] | None") -> None:
    """Validate format-specific write options against allowed option sets.

    Args:
        format_hint: Target storage format (parquet, arrow-ipc, or csv).
        options: Dictionary of write options to validate.

    Raises:
        TypeError: If any options are unsupported for the target format.
    """
    if not options:
        return
    allowed = _ALLOWED_WRITE_OPTIONS.get(format_hint)
    if allowed is None:
        return
    unsupported = sorted(set(options) - allowed)
    if unsupported:
        names = ", ".join(unsupported)
        msg = f"Unsupported write option(s) for {format_hint}: {names}"
        raise TypeError(msg)


def resolve_parquet_use_dictionary(
    schema: Any, explicit_use_dictionary: "bool | list[str] | None" = None
) -> "bool | list[str]":
    """Resolve Parquet dictionary encoding, disabling dictionary construction on float vector columns.

    Args:
        schema: PyArrow Schema of the table or batch being written.
        explicit_use_dictionary: Caller-supplied ``use_dictionary`` override.

    Returns:
        ``True`` when no float vector columns exist, ``explicit_use_dictionary`` when provided,
        or the list of non-vector column names to dictionary-encode.
    """
    if explicit_use_dictionary is not None:
        return explicit_use_dictionary
    pa = import_pyarrow()
    has_float_vector = any(_is_float_vector_field(field, pa) for field in schema)
    if not has_float_vector:
        return True
    return [field.name for field in schema if not _is_float_vector_field(field, pa)]


def _resolve_arrow_source(data: Any, pa: Any) -> tuple[Any, Any, Any, Any]:
    if isinstance(data, pa.Table):
        return data, data.schema, None, None
    if isinstance(data, pa.RecordBatch):
        return None, data.schema, iter((data,)), None
    if isinstance(data, pa.RecordBatchReader):
        return None, data.schema, iter(data), None
    if hasattr(data, "__arrow_c_stream__"):
        reader = pa.RecordBatchReader.from_stream(data)
        return None, reader.schema, iter(reader), None
    iterator = iter(data)
    first_batch = next(iterator, None)
    if first_batch is None:
        empty_table = pa.table({})
        return empty_table, empty_table.schema, None, iterator
    return (None, first_batch.schema, itertools.chain((first_batch,), iterator), iterator)


def _create_format_writer(
    format_hint: StorageFormat,
    pa_sink: Any,
    target_schema: Any,
    *,
    compression: str | None,
    write_options: "dict[str, Any] | None",
) -> tuple[Any, Any]:
    if format_hint == "parquet":
        pq = import_pyarrow_parquet()
        parquet_opts = dict(write_options) if write_options else {}
        row_group_size = parquet_opts.pop("row_group_size", None)
        resolved_compression = compression if compression is not None else parquet_opts.pop("compression", None)
        parquet_opts.pop("compression", None)
        parquet_opts["use_dictionary"] = resolve_parquet_use_dictionary(
            target_schema, parquet_opts.get("use_dictionary")
        )
        writer = pq.ParquetWriter(
            pa_sink,
            target_schema,
            compression="NONE" if resolved_compression is None else resolved_compression,
            **parquet_opts,
        )
        return writer, row_group_size
    if format_hint == "arrow-ipc":
        pa = import_pyarrow()
        ipc_kwargs = dict(write_options) if write_options else {}
        if compression is not None:
            ipc_kwargs["compression"] = compression
        ipc_options = pa.ipc.IpcWriteOptions(**ipc_kwargs) if ipc_kwargs else None
        return pa.ipc.new_file(pa_sink, target_schema, options=ipc_options), None
    pa_csv = import_pyarrow_csv()
    csv_opts = pa_csv.WriteOptions(**write_options) if write_options else None
    return pa_csv.CSVWriter(pa_sink, target_schema, write_options=csv_opts), None


def _write_table_or_batches(
    writer: Any,
    table: Any,
    batches_iter: Any,
    target_schema: Any,
    *,
    format_hint: StorageFormat,
    schema_changed: bool,
    row_group_size: Any,
) -> int:
    if table is not None:
        if schema_changed:
            table = table.cast(target_schema)
        if format_hint == "parquet":
            writer.write_table(table, row_group_size=row_group_size)
        else:
            writer.write_table(table)
        return cast("int", table.num_rows)

    batches_written = 0
    rows_processed = 0
    for batch in batches_iter:
        if schema_changed:
            batch = batch.cast(target_schema)
        if format_hint == "parquet":
            writer.write_batch(batch, row_group_size=row_group_size)
        else:
            writer.write_batch(batch)
        rows_processed += batch.num_rows
        batches_written += 1
    if batches_written == 0:
        writer.write_table(target_schema.empty_table())
    return rows_processed


def encode_arrow_to_sink(
    data: "ArrowTable | Any",
    sink: Any,
    *,
    format_hint: StorageFormat = "parquet",
    compression: str | None = None,
    write_options: "dict[str, Any] | None" = None,
) -> tuple[int, int]:
    """Stream an Arrow table or record-batch source to a synchronous byte sink.

    Args:
        data: Arrow Table, RecordBatch, RecordBatchReader, Arrow stream producer,
            or iterable of RecordBatch objects.
        sink: Writable byte sink accepting bytes chunks via write(data).
        format_hint: Target serialization format (parquet, arrow-ipc, or csv).
        compression: Optional codec name forwarded to Parquet or Arrow IPC writers.
        write_options: Optional format-specific PyArrow writer options.

    Returns:
        Tuple of (bytes_written, rows_processed).

    Raises:
        StorageCapabilityError: If format_hint is not a supported Arrow write format.
    """
    if format_hint not in {"parquet", "arrow-ipc", "csv"}:
        msg = "Arrow storage writes support only Parquet, Arrow IPC, and CSV formats"
        raise StorageCapabilityError(
            msg, capability="arrow_write", remediation="Write row payloads with the row storage APIs instead."
        )

    validate_arrow_write_options(format_hint, write_options)

    pa = import_pyarrow()
    table, schema, batches_iter, iterator = _resolve_arrow_source(data, pa)

    if format_hint in {"parquet", "csv"}:
        target_schema, schema_changed = _normalize_arrow_schema(schema, pa)
    else:
        target_schema = schema
        schema_changed = False

    if format_hint == "csv" and len(target_schema) == 0:
        return (0, 0)

    is_native = isinstance(sink, pa.NativeFile) and not isinstance(sink, pa.PythonFile)
    proxy: _PoisonableCountingSink | None = None
    if is_native:
        pa_sink = sink
        start_offset = sink.tell()
    else:
        proxy = _PoisonableCountingSink(sink)
        pa_sink = pa.PythonFile(proxy, mode="w")
        start_offset = 0

    writer: Any = None
    try:
        writer, row_group_size = _create_format_writer(
            format_hint, pa_sink, target_schema, compression=compression, write_options=write_options
        )
        rows_processed = _write_table_or_batches(
            writer,
            table,
            batches_iter,
            target_schema,
            format_hint=format_hint,
            schema_changed=schema_changed,
            row_group_size=row_group_size,
        )
        writer.close()
        if proxy is not None:
            proxy.flush()
            proxy.close()
    except BaseException:
        if proxy is not None:
            proxy.poison()
        if writer is not None:
            with contextlib.suppress(Exception):
                writer.close()
        raise
    else:
        bytes_written = (pa_sink.tell() - start_offset) if proxy is None else proxy.bytes_written
        return (bytes_written, rows_processed)
    finally:
        source_close = getattr(iterator or data, "close", None)
        if callable(source_close):
            with contextlib.suppress(Exception):
                source_close()


async def join_shielded(future: "asyncio.Future[Any] | asyncio.Task[Any]", *, suppress_result: bool = False) -> Any:
    """Await future under shield until complete, propagating any cancellation afterward.

    Args:
        future: The future or task to await.
        suppress_result: Whether to discard the result value and return None.

    Returns:
        The result of the future if not suppressed and no cancellation occurred.

    Raises:
        asyncio.CancelledError: If cancellation was received during the join.
    """
    cancelled_exc: asyncio.CancelledError | None = None
    while not future.done():
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError as exc:
            if not future.cancelled():
                cancelled_exc = exc
        except Exception:
            if not suppress_result and cancelled_exc is None:
                if not future.cancelled():
                    with contextlib.suppress(Exception):
                        future.exception()
                raise

    if cancelled_exc is not None:
        if not future.cancelled():
            with contextlib.suppress(Exception):
                future.exception()
        raise cancelled_exc

    if suppress_result:
        if not future.cancelled():
            with contextlib.suppress(Exception):
                future.exception()
        return None

    return future.result()


async def encode_arrow_to_async_sink(
    data: "ArrowTable | Any",
    sink: "AsyncWritableBytesSink",
    *,
    format_hint: StorageFormat = "parquet",
    compression: str | None = None,
    write_options: "dict[str, Any] | None" = None,
    chunk_size: int = 1048576,
) -> tuple[int, int]:
    """Stream an Arrow table or record-batch source to an async byte sink with bounded buffering.

    Args:
        data: Arrow Table, RecordBatch, RecordBatchReader, Arrow stream producer,
            or iterable of RecordBatch objects.
        sink: Asynchronous writable byte sink receiving coalesced byte chunks.
        format_hint: Target serialization format (parquet, arrow-ipc, or csv).
        compression: Optional codec name forwarded to Parquet or Arrow IPC writers.
        write_options: Optional format-specific PyArrow writer options.
        chunk_size: Coalesced staging chunk size in bytes before enqueuing to sink.

    Returns:
        Tuple of (bytes_written, rows_processed).

    Raises:
        StorageCapabilityError: If format_hint is not a supported Arrow write format.
        ValueError: If chunk_size is not a positive integer.
    """
    if format_hint not in {"parquet", "arrow-ipc", "csv"}:
        msg = "Arrow storage writes support only Parquet, Arrow IPC, and CSV formats"
        raise StorageCapabilityError(
            msg, capability="arrow_write", remediation="Write row payloads with the row storage APIs instead."
        )
    if chunk_size <= 0:
        msg = "chunk_size must be a positive integer"
        raise ValueError(msg)

    validate_arrow_write_options(format_hint, write_options)

    loop = asyncio.get_running_loop()
    bridge = _AsyncSinkBridge(loop, chunk_size)

    def _worker() -> tuple[int, int]:
        try:
            result = encode_arrow_to_sink(
                data, bridge, format_hint=format_hint, compression=compression, write_options=write_options
            )
            bridge.finish()
        except _BridgeAbortedError:
            bridge.mark_aborted_done()
            return (0, 0)
        except BaseException:
            bridge.fail()
            raise
        else:
            return result

    worker_future = loop.run_in_executor(None, _worker)
    try:
        while True:
            chunk = await bridge.dequeue()
            if chunk is None:
                break
            await sink.write(chunk)
        return await worker_future
    finally:
        bridge.abort()
        await join_shielded(worker_future, suppress_result=True)


def encode_arrow_payload(
    table: "ArrowTable",
    format_choice: StorageFormat,
    *,
    compression: str | None,
    write_options: "dict[str, Any] | None" = None,
) -> bytes:
    """Encode an Arrow table to bytes using optional PyArrow dependencies."""
    pa = import_pyarrow()
    sink = pa.BufferOutputStream()
    encode_arrow_to_sink(table, sink, format_hint=format_choice, compression=compression, write_options=write_options)
    buffer = sink.getvalue()
    result_bytes: bytes = buffer.to_pybytes()
    return result_bytes


def decode_arrow_payload(payload: bytes, format_choice: StorageFormat, *, arrow_schema: Any = None) -> "ArrowTable":
    """Decode bytes into an Arrow table using optional PyArrow dependencies."""
    pa = import_pyarrow()
    if format_choice == "parquet":
        pq = import_pyarrow_parquet()
        table = cast("ArrowTable", pq.read_table(pa.BufferReader(payload)))
        return table.cast(arrow_schema) if arrow_schema is not None else table
    if format_choice == "arrow-ipc":
        with pa.ipc.open_file(pa.BufferReader(payload)) as reader:
            table = cast("ArrowTable", reader.read_all())
            return table.cast(arrow_schema) if arrow_schema is not None else table
    if format_choice == "csv":
        pa_csv = import_pyarrow_csv()
        convert_opts = pa_csv.ConvertOptions(column_types=arrow_schema) if arrow_schema is not None else None
        return cast("ArrowTable", pa_csv.read_csv(pa.BufferReader(payload), convert_options=convert_opts))
    if format_choice == "json":
        if not payload.strip():
            return cast("ArrowTable", pa.table({}))
        data = from_json(payload)
        rows = data if isinstance(data, list) else [data]
        return cast("ArrowTable", pa.Table.from_pylist(rows, schema=arrow_schema))
    if format_choice == "jsonl":
        if payload == b"":
            return cast("ArrowTable", pa.table({}))
        pa_json = import_pyarrow_json()
        read_options = pa_json.ReadOptions(block_size=max(_PYARROW_JSON_BLOCK_SIZE, len(payload)))
        parse_options = pa_json.ParseOptions(explicit_schema=arrow_schema) if arrow_schema is not None else None
        return cast(
            "ArrowTable",
            pa_json.read_json(pa.BufferReader(payload), read_options=read_options, parse_options=parse_options),
        )
    msg = f"Unsupported storage format for Arrow decoding: {format_choice}"
    raise ValueError(msg)


def _is_float_vector_field(field: Any, pa: Any) -> bool:
    dtype = field.type
    if pa.types.is_fixed_size_list(dtype) or pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return bool(pa.types.is_floating(dtype.value_type))
    return False
