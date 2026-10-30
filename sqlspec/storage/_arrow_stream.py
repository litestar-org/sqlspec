"""Shared helpers for bounded Arrow batch streaming."""

from pathlib import PurePath
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import StorageCapabilityError
from sqlspec.storage._utils import import_pyarrow, import_pyarrow_csv, import_pyarrow_json

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from sqlspec.typing import ArrowRecordBatch

__all__ = ("iter_parquet_row_groups", "iter_streamed_arrow_batches", "validate_parquet_stream_options")

_NON_PARQUET_SUFFIXES = frozenset({".arrow", ".csv", ".feather", ".ipc", ".json", ".jsonl", ".ndjson"})

_STREAM_REMEDIATION = "Read this format with the Arrow read APIs instead of batch streaming."

_JSONL_CHUNK_TARGET_BYTES = 8 * 1024 * 1024


def validate_parquet_stream_options(pattern: str, file_format: str, batch_size: int) -> None:
    """Validate a Parquet streaming request before storage is accessed.

    Args:
        pattern: Glob pattern selecting objects to stream.
        file_format: Requested storage format.
        batch_size: Maximum number of rows in each yielded record batch.

    Raises:
        StorageCapabilityError: If a non-Parquet format is requested.
        ValueError: If ``batch_size`` is not greater than zero.
    """
    if file_format != "parquet":
        msg = f"Arrow batch streaming supports only Parquet files; received file_format={file_format!r}"
        raise StorageCapabilityError(msg, capability="arrow_batch_streaming", remediation=_STREAM_REMEDIATION)
    if batch_size <= 0:
        msg = f"batch_size must be greater than zero; received {batch_size}"
        raise ValueError(msg)

    suffix = PurePath(pattern).suffix.lower()
    if suffix in _NON_PARQUET_SUFFIXES:
        msg = f"Arrow batch streaming supports only Parquet files; pattern {pattern!r} selects {suffix} files"
        raise StorageCapabilityError(msg, capability="arrow_batch_streaming", remediation=_STREAM_REMEDIATION)


def iter_parquet_row_groups(
    parquet_file: Any, *, batch_size: int, arrow_schema: Any = None, **kwargs: Any
) -> "Iterator[ArrowRecordBatch]":
    """Yield batches while limiting each PyArrow read to one row group."""
    for row_group in range(parquet_file.num_row_groups):
        for batch in parquet_file.iter_batches(batch_size=batch_size, row_groups=[row_group], **kwargs):
            yield batch.cast(arrow_schema) if arrow_schema is not None else batch


def iter_streamed_arrow_batches(
    byte_chunks: "Iterable[bytes]", *, file_format: str, batch_size: int = 65_536, arrow_schema: Any = None
) -> "Iterator[ArrowRecordBatch]":
    """Stream RecordBatches from byte chunks for JSONL, CSV, and Arrow IPC formats.

    Args:
        byte_chunks: Iterable of raw byte chunks from a storage backend.
        file_format: Storage format (``"jsonl"``, ``"ndjson"``, ``"csv"``, ``"arrow-ipc"``, ``"arrow"``, ``"ipc"``).
        batch_size: Maximum rows per yielded RecordBatch.
        arrow_schema: Optional explicit ``pyarrow.Schema`` to pin column types (such as ``float32`` vectors).

    Yields:
        Arrow ``RecordBatch`` instances.
    """
    if batch_size <= 0:
        msg = f"batch_size must be greater than zero; received {batch_size}"
        raise ValueError(msg)
    pa = import_pyarrow()
    normalized = file_format.lower()
    if normalized in {"jsonl", "ndjson"}:
        pa_json = import_pyarrow_json()
        parse_options = pa_json.ParseOptions(explicit_schema=arrow_schema) if arrow_schema is not None else None
        pending = bytearray()
        for chunk in byte_chunks:
            if not chunk:
                continue
            pending.extend(chunk)
            if len(pending) < _JSONL_CHUNK_TARGET_BYTES:
                continue
            last_nl = pending.rfind(b"\n")
            if last_nl <= 0:
                continue
            complete_block = bytes(pending[: last_nl + 1])
            del pending[: last_nl + 1]
            if complete_block.strip():
                read_options = pa_json.ReadOptions(block_size=max(1 << 20, len(complete_block)))
                table = pa_json.read_json(
                    pa.BufferReader(complete_block), read_options=read_options, parse_options=parse_options
                )
                yield from table.to_batches(max_chunksize=batch_size)
        if pending.strip():
            final_block = bytes(pending)
            read_options = pa_json.ReadOptions(block_size=max(1 << 20, len(final_block)))
            table = pa_json.read_json(
                pa.BufferReader(final_block), read_options=read_options, parse_options=parse_options
            )
            yield from table.to_batches(max_chunksize=batch_size)
        return

    payload = b"".join(byte_chunks)
    if not payload:
        return
    if normalized == "csv":
        pa_csv = import_pyarrow_csv()
        convert_options = pa_csv.ConvertOptions(column_types=arrow_schema) if arrow_schema is not None else None
        reader = pa_csv.open_csv(pa.BufferReader(payload), convert_options=convert_options)
        for batch in reader:
            yield from pa.Table.from_batches([batch]).to_batches(max_chunksize=batch_size)
        return

    if normalized in {"arrow-ipc", "arrow", "ipc"}:
        try:
            with pa.ipc.open_file(pa.BufferReader(payload)) as ipc_reader:
                for idx in range(ipc_reader.num_record_batches):
                    batch = ipc_reader.get_batch(idx)
                    if arrow_schema is not None:
                        batch = batch.cast(arrow_schema)
                    yield from pa.Table.from_batches([batch]).to_batches(max_chunksize=batch_size)
        except Exception:
            with pa.ipc.open_stream(pa.BufferReader(payload)) as stream_reader:
                for batch in stream_reader:
                    if arrow_schema is not None:
                        batch = batch.cast(arrow_schema)
                    yield from pa.Table.from_batches([batch]).to_batches(max_chunksize=batch_size)
        return

    msg = f"Unsupported streaming Arrow format: {file_format!r}"
    raise ValueError(msg)
