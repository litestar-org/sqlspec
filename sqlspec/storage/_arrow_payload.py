"""Interpreted PyArrow payload helpers for storage pipelines."""

from typing import TYPE_CHECKING, Any, Literal, cast

from sqlspec.storage._utils import import_pyarrow, import_pyarrow_csv, import_pyarrow_json, import_pyarrow_parquet
from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from sqlspec.typing import ArrowTable

__all__ = ("decode_arrow_payload", "encode_arrow_payload")


StorageFormat = Literal["jsonl", "json", "parquet", "arrow-ipc", "csv"]

_PYARROW_JSON_BLOCK_SIZE = 1 << 20


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
    if format_choice == "arrow-ipc":
        writer = pa.ipc.new_file(sink, table.schema)
        writer.write_table(table)
        writer.close()
    elif format_choice == "csv":
        pa_csv = import_pyarrow_csv()
        csv_opts: Any = None
        if write_options:
            csv_opts = pa_csv.WriteOptions(**write_options)
        pa_csv.write_csv(table, sink, write_options=csv_opts)
    else:
        pq = import_pyarrow_parquet()
        pq.write_table(table, sink, compression=compression)
    buffer = sink.getvalue()
    result_bytes: bytes = buffer.to_pybytes()
    return result_bytes


def decode_arrow_payload(payload: bytes, format_choice: StorageFormat) -> "ArrowTable":
    """Decode bytes into an Arrow table using optional PyArrow dependencies."""

    pa = import_pyarrow()
    if format_choice == "parquet":
        pq = import_pyarrow_parquet()
        return cast("ArrowTable", pq.read_table(pa.BufferReader(payload)))
    if format_choice == "arrow-ipc":
        reader = pa.ipc.open_file(pa.BufferReader(payload))
        return cast("ArrowTable", reader.read_all())
    if format_choice == "csv":
        pa_csv = import_pyarrow_csv()
        return cast("ArrowTable", pa_csv.read_csv(pa.BufferReader(payload)))
    if format_choice == "json":
        data = from_json(payload.decode())
        rows = data if isinstance(data, list) else [data]
        return cast("ArrowTable", pa.Table.from_pylist(rows))
    if format_choice == "jsonl":
        if payload == b"":
            return cast("ArrowTable", pa.table({}))
        pa_json = import_pyarrow_json()
        read_options = pa_json.ReadOptions(block_size=max(_PYARROW_JSON_BLOCK_SIZE, len(payload)))
        return cast("ArrowTable", pa_json.read_json(pa.BufferReader(payload), read_options=read_options))
    msg = f"Unsupported storage format for Arrow decoding: {format_choice}"
    raise ValueError(msg)
