"""Interpreted PyArrow payload helpers for storage pipelines."""

import re
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlspec.storage._utils import import_pyarrow, import_pyarrow_csv, import_pyarrow_json, import_pyarrow_parquet
from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from sqlspec.typing import ArrowTable

__all__ = (
    "StorageFormat",
    "batches_to_arrow_table",
    "decode_arrow_payload",
    "encode_arrow_payload",
    "resolve_parquet_use_dictionary",
    "write_arrow_payload",
)


StorageFormat = Literal["jsonl", "json", "parquet", "arrow-ipc", "csv"]

_PYARROW_JSON_BLOCK_SIZE = 1 << 20


def batches_to_arrow_table(batches: Any) -> "ArrowTable":
    """Coalesce Arrow batches, tables, readers, or C stream exporters into a table."""
    pa = import_pyarrow()
    if isinstance(batches, pa.Table):
        return cast("ArrowTable", batches)
    if isinstance(batches, pa.RecordBatchReader):
        return cast("ArrowTable", batches.read_all())
    if isinstance(batches, pa.RecordBatch):
        return cast("ArrowTable", pa.Table.from_batches([batches]))
    if hasattr(batches, "__arrow_c_stream__"):
        return cast("ArrowTable", pa.RecordBatchReader.from_stream(batches).read_all())
    batch_list = list(batches)
    if not batch_list:
        return cast("ArrowTable", pa.table({}))
    return cast("ArrowTable", pa.Table.from_batches(batch_list, schema=batch_list[0].schema))


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


def write_arrow_payload(
    table: "ArrowTable",
    sink: Any,
    format_choice: StorageFormat,
    *,
    compression: str | None = None,
    write_options: "dict[str, Any] | None" = None,
) -> None:
    """Write an Arrow table directly to a sink using optional PyArrow dependencies.

    Args:
        table: PyArrow Table to write.
        sink: Target file-like object, buffer, or NativeFile.
        format_choice: Target storage format ("parquet", "arrow-ipc", or "csv").
        compression: Optional compression codec.
        write_options: Format-specific write options.

    Raises:
        ValueError: If format_choice is not supported for Arrow encoding.
    """
    pa = import_pyarrow()
    target_sink = sink if isinstance(sink, pa.NativeFile) else pa.PythonFile(sink, mode="w")

    if format_choice == "arrow-ipc":
        ipc_options = pa.ipc.IpcWriteOptions(compression=compression) if compression else None
        with pa.ipc.new_file(target_sink, table.schema, options=ipc_options) as writer:
            writer.write_table(table)
    elif format_choice == "csv":
        pa_csv = import_pyarrow_csv()
        csv_opts: Any = None
        if write_options:
            csv_opts = pa_csv.WriteOptions(**write_options)
        pa_csv.write_csv(table, target_sink, write_options=csv_opts)
    elif format_choice == "parquet":
        pq = import_pyarrow_parquet()
        schema = table.schema
        if any(str(f.type).startswith("decimal64") for f in schema):
            new_fields = []
            for field in schema:
                if str(field.type).startswith("decimal64"):
                    match = re.match(r"decimal64\((\d+),\s*(\d+)\)", str(field.type))
                    if match:
                        precision, scale = int(match.group(1)), int(match.group(2))
                        new_fields.append(pa.field(field.name, pa.decimal128(precision, scale)))
                    else:
                        new_fields.append(field)
                else:
                    new_fields.append(field)
            table = table.cast(pa.schema(new_fields))
        parquet_opts = dict(write_options) if write_options else {}
        parquet_opts["use_dictionary"] = resolve_parquet_use_dictionary(
            table.schema, parquet_opts.get("use_dictionary")
        )
        row_group_size = parquet_opts.pop("row_group_size", None)
        if row_group_size is not None:
            writer = pq.ParquetWriter(target_sink, table.schema, compression=compression, **parquet_opts)
            try:
                writer.write_table(table, row_group_size=row_group_size)
            finally:
                writer.close()
        else:
            pq.write_table(table, target_sink, compression=compression, **parquet_opts)
    else:
        msg = f"Unsupported storage format for Arrow encoding: {format_choice}"
        raise ValueError(msg)

    if target_sink is not sink:
        target_sink.flush()


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
    write_arrow_payload(table, sink, format_choice, compression=compression, write_options=write_options)
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
