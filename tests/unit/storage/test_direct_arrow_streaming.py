"""Regression tests for direct Arrow pipeline streaming to capable backends (Issue #898)."""

import io
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import pytest

from sqlspec.exceptions import StorageOperationFailedError
from sqlspec.storage._arrow_payload import decode_arrow_payload, encode_arrow_payload, write_arrow_payload
from sqlspec.storage.backends.base import ObjectStoreBase
from sqlspec.storage.backends.local import LocalStore
from sqlspec.storage.backends.obstore import ObStoreBackend, _ObstoreSink
from sqlspec.storage.pipeline import AsyncStoragePipeline, SyncStoragePipeline
from sqlspec.storage.registry import StorageRegistry
from sqlspec.typing import OBSTORE_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils.type_guards import supports_direct_arrow_write


class _DummyCustomStore(ObjectStoreBase):
    """Dummy backend that does not implement DirectArrowWriteProtocol."""

    __slots__ = ("_data", "backend_type", "protocol")

    def __init__(self) -> None:
        self.protocol = "memory"
        self.backend_type = "dummy"
        self._data: dict[str, bytes] = {}

    @property
    def supports_signing(self) -> bool:
        return False

    def resolve_uri(self, path: "str | Path") -> str:
        return str(path)

    def read_bytes_sync(self, path: "str | Path", **kwargs: Any) -> bytes:
        return self._data.get(str(path), b"")

    def write_bytes_sync(self, path: "str | Path", data: bytes, *, overwrite: bool = True, **kwargs: Any) -> None:
        self._data[str(path)] = data

    def stream_read_sync(self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any) -> Any:
        yield self._data.get(str(path), b"")

    def read_text_sync(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        return self._data.get(str(path), b"").decode(encoding)

    def write_text_sync(
        self, path: "str | Path", data: str, encoding: str = "utf-8", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        self._data[str(path)] = data.encode(encoding)

    def list_objects_sync(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        return list(self._data.keys())

    def exists_sync(self, path: "str | Path", **kwargs: Any) -> bool:
        return str(path) in self._data

    def delete_sync(self, path: "str | Path", **kwargs: Any) -> None:
        self._data.pop(str(path), None)

    def copy_sync(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        self._data[str(destination)] = self._data[str(source)]

    def move_sync(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        self._data[str(destination)] = self._data.pop(str(source))

    def glob_sync(self, pattern: str, **kwargs: Any) -> list[str]:
        return list(self._data.keys())

    def get_metadata_sync(self, path: "str | Path", **kwargs: Any) -> dict[str, object]:
        return {"size": len(self._data.get(str(path), b""))}

    def is_object_sync(self, path: "str | Path") -> bool:
        return str(path) in self._data

    def is_path_sync(self, path: "str | Path") -> bool:
        return False

    def read_arrow_sync(self, path: "str | Path", **kwargs: Any) -> Any:
        return decode_arrow_payload(self._data[str(path)], "parquet")

    def write_arrow_sync(self, path: "str | Path", table: Any, **kwargs: Any) -> None:
        self._data[str(path)] = encode_arrow_payload(table, "parquet", compression=None)

    def stream_arrow_sync(self, pattern: str, **kwargs: Any) -> Any:
        raise NotImplementedError

    def sign_sync(self, paths: Any, expires_in: int = 3600, for_upload: bool = False) -> Any:
        raise NotImplementedError

    async def read_bytes_async(self, path: "str | Path", **kwargs: Any) -> bytes:
        return self.read_bytes_sync(path, **kwargs)

    async def write_bytes_async(
        self, path: "str | Path", data: bytes, *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        self.write_bytes_sync(path, data, overwrite=overwrite, **kwargs)

    async def read_text_async(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        return self.read_text_sync(path, encoding=encoding, **kwargs)

    async def write_text_async(
        self, path: "str | Path", data: str, encoding: str = "utf-8", *, overwrite: bool = True, **kwargs: Any
    ) -> None:
        self.write_text_sync(path, data, encoding=encoding, overwrite=overwrite, **kwargs)

    async def stream_read_async(self, path: "str | Path", chunk_size: "int | None" = None, **kwargs: Any) -> Any:
        for chunk in self.stream_read_sync(path, chunk_size=chunk_size, **kwargs):
            yield chunk

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        return self.list_objects_sync(prefix=prefix, recursive=recursive, **kwargs)

    async def exists_async(self, path: "str | Path", **kwargs: Any) -> bool:
        return self.exists_sync(path, **kwargs)

    async def delete_async(self, path: "str | Path", **kwargs: Any) -> None:
        self.delete_sync(path, **kwargs)

    async def copy_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        self.copy_sync(source, destination, **kwargs)

    async def move_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        self.move_sync(source, destination, **kwargs)

    async def get_metadata_async(self, path: "str | Path", **kwargs: Any) -> dict[str, object]:
        return self.get_metadata_sync(path, **kwargs)

    async def read_arrow_async(self, path: "str | Path", **kwargs: Any) -> Any:
        return self.read_arrow_sync(path, **kwargs)

    async def write_arrow_async(self, path: "str | Path", table: Any, **kwargs: Any) -> None:
        self.write_arrow_sync(path, table, **kwargs)

    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def sign_async(self, paths: Any, expires_in: int = 3600, for_upload: bool = False) -> Any:
        raise NotImplementedError


def _sample_table() -> Any:
    return pa.table({"id": [1, 2, 3], "name": ["Alpha", "Beta", "Gamma"], "score": [10.5, 20.0, 31.25]})


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_supports_direct_arrow_write_type_guard(tmp_path: Path) -> None:
    """Verifies that supports_direct_arrow_write correctly identifies capable backends."""
    obstore_backend = ObStoreBackend("memory://")
    assert supports_direct_arrow_write(obstore_backend) is True

    local_backend = LocalStore(str(tmp_path))
    assert supports_direct_arrow_write(local_backend) is False

    dummy = _DummyCustomStore()
    assert supports_direct_arrow_write(dummy) is False


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_obstore_backend_declares_supported_direct_formats() -> None:
    """Verifies format declarations on ObStoreBackend."""
    store = ObStoreBackend("memory://")
    assert store.supports_arrow_direct_write("parquet") is True
    assert store.supports_arrow_direct_write("arrow-ipc") is True
    assert store.supports_arrow_direct_write("csv") is True
    assert store.supports_arrow_direct_write("json") is False
    assert store.supports_arrow_direct_write("jsonl") is False
    assert store.supports_arrow_direct_write("unknown") is False


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="PyArrow missing")
def test_write_arrow_payload_to_sink_direct() -> None:
    """Verifies write_arrow_payload encodes directly into sinks for all supported formats."""
    table = _sample_table()

    parquet_sink = io.BytesIO()
    write_arrow_payload(table, parquet_sink, "parquet", compression="snappy")
    assert len(parquet_sink.getvalue()) > 0
    read_parquet = pq.read_table(io.BytesIO(parquet_sink.getvalue()))
    assert read_parquet.num_rows == 3

    ipc_sink = io.BytesIO()
    write_arrow_payload(table, ipc_sink, "arrow-ipc")
    assert len(ipc_sink.getvalue()) > 0
    with pa.ipc.open_file(io.BytesIO(ipc_sink.getvalue())) as reader:
        read_ipc = reader.read_all()
    assert read_ipc.num_rows == 3

    csv_sink = io.BytesIO()
    write_arrow_payload(table, csv_sink, "csv", write_options={"delimiter": ";"})
    csv_bytes = csv_sink.getvalue()
    assert b";" in csv_bytes
    read_csv = pa_csv.read_csv(io.BytesIO(csv_bytes), parse_options=pa_csv.ParseOptions(delimiter=";"))
    assert read_csv.num_rows == 3

    with pytest.raises(ValueError, match="Unsupported storage format"):
        write_arrow_payload(table, io.BytesIO(), cast("Any", "unsupported"))


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_sync_pipeline_write_arrow_direct_all_formats() -> None:
    """SyncStoragePipeline routes write_arrow directly to ObStoreBackend writer for parquet, ipc, and csv."""
    registry = StorageRegistry()
    registry.register_alias("direct_store", "memory://direct_sync/")
    pipeline = SyncStoragePipeline(registry=registry)
    table = _sample_table()

    for fmt, ext in [("parquet", "table.parquet"), ("arrow-ipc", "table.ipc"), ("csv", "table.csv")]:
        destination = f"alias://direct_store/{ext}"
        telemetry = pipeline.write_arrow(table, destination, format_hint=cast("Any", fmt))

        assert telemetry["rows_processed"] == 3
        assert telemetry["bytes_processed"] > 0
        assert telemetry["format"] == fmt
        assert telemetry["backend"] == "obstore"
        assert telemetry["duration_s"] >= 0

        read_table, read_telemetry = pipeline.read_arrow(destination, file_format=cast("Any", fmt))
        assert read_table.num_rows == 3
        assert read_telemetry["rows_processed"] == 3
        assert read_telemetry["format"] == fmt


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_sync_pipeline_write_arrow_batches_direct() -> None:
    """SyncStoragePipeline routes write_arrow_batches directly to ObStoreBackend."""
    registry = StorageRegistry()
    registry.register_alias("direct_store_batches", "memory://direct_batches_sync/")
    pipeline = SyncStoragePipeline(registry=registry)
    table = _sample_table()
    batches = table.to_batches(max_chunksize=2)

    telemetry = pipeline.write_arrow_batches(
        "alias://direct_store_batches/streamed.parquet", batches, file_format="parquet"
    )
    assert telemetry["rows_processed"] == 3
    assert telemetry["bytes_processed"] > 0
    assert telemetry["backend"] == "obstore"

    loaded, _ = pipeline.read_arrow("alias://direct_store_batches/streamed.parquet", file_format="parquet")
    assert loaded.num_rows == 3


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
async def test_async_pipeline_write_arrow_direct_all_formats() -> None:
    """AsyncStoragePipeline routes write_arrow directly to ObStoreBackend writer asynchronously."""
    registry = StorageRegistry()
    registry.register_alias("direct_store_async", "memory://direct_async/")
    pipeline = AsyncStoragePipeline(registry=registry)
    table = _sample_table()

    for fmt, ext in [("parquet", "async_table.parquet"), ("arrow-ipc", "async_table.ipc"), ("csv", "async_table.csv")]:
        destination = f"alias://direct_store_async/{ext}"
        telemetry = await pipeline.write_arrow(table, destination, format_hint=cast("Any", fmt))

        assert telemetry["rows_processed"] == 3
        assert telemetry["bytes_processed"] > 0
        assert telemetry["format"] == fmt
        assert telemetry["backend"] == "obstore"

        read_table, read_telemetry = await pipeline.read_arrow_async(destination, file_format=cast("Any", fmt))
        assert read_table.num_rows == 3
        assert read_telemetry["rows_processed"] == 3
        assert read_telemetry["format"] == fmt


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
async def test_async_pipeline_write_arrow_batches_async_direct() -> None:
    """AsyncStoragePipeline routes write_arrow_batches_async directly to ObStoreBackend."""
    registry = StorageRegistry()
    registry.register_alias("direct_store_batches_async", "memory://direct_batches_async/")
    pipeline = AsyncStoragePipeline(registry=registry)
    table = _sample_table()
    batches = table.to_batches(max_chunksize=1)

    telemetry = await pipeline.write_arrow_batches_async(
        "alias://direct_store_batches_async/async_streamed.parquet", batches, file_format="parquet"
    )
    assert telemetry["rows_processed"] == 3
    assert telemetry["bytes_processed"] > 0
    assert telemetry["backend"] == "obstore"

    loaded, _ = await pipeline.read_arrow_async(
        "alias://direct_store_batches_async/async_streamed.parquet", file_format="parquet"
    )
    assert loaded.num_rows == 3


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_direct_write_failure_aborts_and_preserves_existing_object() -> None:
    """A direct write failure aborts upload without corrupting or modifying an existing object."""
    store = ObStoreBackend("memory://abort_case/")
    store.write_bytes_sync("existing.parquet", b"original-content-preserved")

    registry = StorageRegistry()
    registry.register_alias("abort_store", "memory://abort_case/")
    pipeline = SyncStoragePipeline(registry=registry)

    unwritable = pa.table({
        "bad_col": pa.UnionArray.from_sparse(pa.array([0, 1], pa.int8()), [pa.array([1, 2]), pa.array(["a", "b"])])
    })

    with pytest.raises(StorageOperationFailedError):
        pipeline.write_arrow(unwritable, "alias://abort_store/existing.parquet")

    assert store.read_bytes_sync("existing.parquet") == b"original-content-preserved"

    with pytest.raises(StorageOperationFailedError):
        pipeline.write_arrow(unwritable, "alias://abort_store/new_file.parquet")

    assert "new_file.parquet" not in store.list_objects_sync()


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="PyArrow missing")
def test_pipeline_fallback_to_buffered_when_backend_lacks_direct_hook() -> None:
    """Pipelines fall back to the buffered path when backends do not implement direct writes."""
    dummy_backend = _DummyCustomStore()

    class _DummyRegistry(StorageRegistry):
        def get(self, *args: Any, **kwargs: Any) -> Any:
            return dummy_backend

    pipeline = SyncStoragePipeline(registry=_DummyRegistry())
    table = _sample_table()

    telemetry = pipeline.write_arrow(table, "alias://dummy/buffered.parquet", format_hint="parquet")
    assert telemetry["rows_processed"] == 3
    assert telemetry["bytes_processed"] > 0
    assert telemetry["backend"] == "dummy"
    assert dummy_backend.exists_sync("buffered.parquet") is True

    loaded, _ = pipeline.read_arrow("alias://dummy/buffered.parquet", file_format="parquet")
    assert loaded.num_rows == 3


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_sink_tracks_bytes_written_and_idempotent_close() -> None:
    """_ObstoreSink tracks bytes written accurately and prevents double-close errors."""
    store = ObStoreBackend("memory://sink_test/")
    obs = store.store

    import obstore

    raw_writer = obstore.open_writer(obs, "test_file.bin")
    sink = _ObstoreSink(raw_writer)

    first_len = sink.write(b"part1")
    second_len = sink.write(bytearray(b"part2"))
    assert sink.bytes_written == first_len + second_len

    sink.flush()
    sink.close()
    sink.close()

    assert sink.closed is True
