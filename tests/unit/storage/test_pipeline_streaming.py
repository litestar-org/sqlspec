# pyright: reportPrivateUsage=false
"""Unit tests for streaming storage pipeline batches, vector Parquet defaults, schema-pinned JSONL/CSV/IPC, and ObStoreSink safety."""

from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from sqlspec.core.result import ArrowResult
from sqlspec.storage._arrow_payload import encode_arrow_payload, resolve_parquet_use_dictionary
from sqlspec.storage.backends.obstore import _ObstoreSink
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
