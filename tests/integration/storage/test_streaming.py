"""Integration tests for sync and async storage streaming across local and multi-cloud backends."""

import asyncio
import threading
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import pytest

from sqlspec.exceptions import FileExistsInStorageError
from sqlspec.storage.pipeline import AsyncStoragePipeline, SyncStoragePipeline
from sqlspec.storage.registry import StorageRegistry
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils.module_loader import import_optional, import_optional_attr
from tests.fixtures.rustfs import rustfs_fsspec_kwargs, rustfs_obstore_kwargs
from tests.integration.fixtures.cloud_storage import floci_gcs_obstore_kwargs

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_databases.docker.azure_blob import AzureBlobService
    from pytest_databases.docker.floci import FlociGCPService
    from pytest_databases.docker.rustfs import RustfsService

    from sqlspec.protocols import ObjectStoreProtocol
    from sqlspec.storage.backends.fsspec import FSSpecBackend
    from sqlspec.storage.backends.obstore import ObStoreBackend
else:
    FSSpecBackend = import_optional_attr("sqlspec.storage.backends.fsspec", "FSSpecBackend")
    ObStoreBackend = import_optional_attr("sqlspec.storage.backends.obstore", "ObStoreBackend")

pyarrow: Any = import_optional("pyarrow")

STREAM_CHUNK_SIZE = 1024
STREAM_PAYLOAD = b"sqlspec-streaming-chunk-" * 4000


@pytest.fixture
def test_file(tmp_path: "Path") -> "Path":
    """Create a multi-kilobyte local file for chunked streaming verification."""
    target = tmp_path / "test.txt"
    target.write_bytes(STREAM_PAYLOAD)
    return target


def test_sync_stream_read_local(test_file: "Path") -> None:
    """Verify SyncStoragePipeline streams local file bytes across multiple chunks."""
    pipeline = SyncStoragePipeline()
    chunks = list(pipeline.stream_read(test_file, chunk_size=STREAM_CHUNK_SIZE))
    assert len(chunks) > 1
    assert b"".join(chunks) == STREAM_PAYLOAD


async def test_async_stream_read_local(test_file: "Path") -> None:
    """Verify AsyncStoragePipeline streams local file bytes across multiple chunks."""
    pipeline = AsyncStoragePipeline()
    stream = await pipeline.stream_read_async(test_file, chunk_size=STREAM_CHUNK_SIZE)
    chunks = [chunk async for chunk in stream]
    assert len(chunks) > 1
    assert b"".join(chunks) == STREAM_PAYLOAD


async def test_async_stream_read_local_explicit_uri(test_file: "Path") -> None:
    """Verify AsyncStoragePipeline streams file:// URIs across multiple chunks."""
    pipeline = AsyncStoragePipeline()
    stream = await pipeline.stream_read_async(f"file://{test_file}", chunk_size=STREAM_CHUNK_SIZE)
    chunks = [chunk async for chunk in stream]
    assert len(chunks) > 1
    assert b"".join(chunks) == STREAM_PAYLOAD


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_sync_stream_read_fsspec(test_file: "Path") -> None:
    """Verify FSSpecBackend and SyncStoragePipeline stream local files across multiple chunks."""
    backend = FSSpecBackend(f"file://{test_file.parent}")
    direct_chunks = list(backend.stream_read_sync(test_file.name, chunk_size=STREAM_CHUNK_SIZE))
    assert len(direct_chunks) > 1
    assert b"".join(direct_chunks) == STREAM_PAYLOAD

    registry = StorageRegistry()
    registry.register_alias("local-fsspec-stream", f"file://{test_file.parent}", backend="fsspec")
    pipeline = SyncStoragePipeline(registry=registry)
    pipeline_chunks = list(
        pipeline.stream_read(f"alias://local-fsspec-stream/{test_file.name}", chunk_size=STREAM_CHUNK_SIZE)
    )
    assert len(pipeline_chunks) > 1
    assert b"".join(pipeline_chunks) == STREAM_PAYLOAD


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_async_stream_read_fsspec(test_file: "Path") -> None:
    """Verify FSSpecBackend async chunked streaming reassembles payloads accurately."""
    backend = FSSpecBackend(f"file://{test_file.parent}")
    stream = await backend.stream_read_async(test_file.name, chunk_size=STREAM_CHUNK_SIZE)
    chunks = [chunk async for chunk in stream]
    assert len(chunks) > 1
    assert b"".join(chunks) == STREAM_PAYLOAD


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_rustfs_s3_fsspec_streaming(rustfs_service: "RustfsService", rustfs_bucket_name: str) -> None:
    """Verify sync and async chunked streaming against RustFS S3 via FSSpecBackend."""
    pytest.importorskip("s3fs")
    backend = FSSpecBackend(f"s3://{rustfs_bucket_name}", **rustfs_fsspec_kwargs(rustfs_service))
    await _assert_backend_streaming_and_arrow(backend, "streaming/rustfs_fsspec")


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_rustfs_s3_obstore_streaming(rustfs_service: "RustfsService", rustfs_bucket_name: str) -> None:
    """Verify sync and async chunked streaming against RustFS S3 via ObStoreBackend."""
    backend = ObStoreBackend(f"s3://{rustfs_bucket_name}", **rustfs_obstore_kwargs(rustfs_service))
    await _assert_backend_streaming_and_arrow(backend, "streaming/rustfs_obstore")

    registry = StorageRegistry()
    registry.register_alias(
        "rustfs-s3-stream",
        f"s3://{rustfs_bucket_name}",
        backend="obstore",
        **rustfs_obstore_kwargs(rustfs_service),
    )
    await _assert_pipeline_direct_arrow_streaming(
        registry,
        "rustfs-s3-stream",
        "streaming/pipeline_arrow_s3",
        check_overwrite_false_conflict=True,
        check_multipart_12mb=True,
    )


@pytest.mark.xdist_group("floci_gcp")
async def test_floci_gcs_obstore_streaming_and_pipeline(
    floci_gcp_service: "FlociGCPService", floci_gcs_bucket_name: str, floci_gcs_obstore: "ObjectStoreProtocol"
) -> None:
    """Verify ObStoreBackend and StoragePipeline streaming and Arrow round-trips on Floci GCS."""
    await _assert_backend_streaming_and_arrow(floci_gcs_obstore, "streaming/floci_gcs_obstore")

    registry = StorageRegistry()
    registry.register_alias(
        "floci-gcs-stream",
        f"gs://{floci_gcs_bucket_name}",
        backend="obstore",
        **floci_gcs_obstore_kwargs(floci_gcp_service),
    )
    object_path = "streaming/pipeline_gcs.bin"
    floci_gcs_obstore.write_bytes_sync(object_path, STREAM_PAYLOAD)

    sync_pipeline = SyncStoragePipeline(registry=registry)
    sync_chunks = list(
        sync_pipeline.stream_read(f"alias://floci-gcs-stream/{object_path}", chunk_size=STREAM_CHUNK_SIZE)
    )
    assert len(sync_chunks) > 1
    assert b"".join(sync_chunks) == STREAM_PAYLOAD

    async_pipeline = AsyncStoragePipeline(registry=registry)
    async_stream = await async_pipeline.stream_read_async(
        f"alias://floci-gcs-stream/{object_path}", chunk_size=STREAM_CHUNK_SIZE
    )
    async_chunks = [chunk async for chunk in async_stream]
    assert b"".join(async_chunks) == STREAM_PAYLOAD

    await _assert_pipeline_direct_arrow_streaming(
        registry,
        "floci-gcs-stream",
        "streaming/pipeline_arrow_gcs",
        check_overwrite_false_conflict=False,
        check_multipart_12mb=False,
    )


@pytest.mark.xdist_group("floci_gcp")
async def test_floci_gcs_fsspec_streaming(floci_gcs_fsspec: "ObjectStoreProtocol") -> None:
    """Verify FSSpecBackend streaming and Arrow round-trips on Floci GCS."""
    await _assert_backend_streaming_and_arrow(floci_gcs_fsspec, "streaming/floci_gcs_fsspec")


@pytest.mark.xdist_group("azure_blob")
async def test_azurite_obstore_streaming_and_pipeline(
    azure_blob_service: "AzureBlobService",
    azure_blob_default_container_name: str,
    azurite_obstore: "ObjectStoreProtocol",
) -> None:
    """Verify ObStoreBackend and StoragePipeline streaming and Arrow round-trips on Azurite."""
    await _assert_backend_streaming_and_arrow(azurite_obstore, "streaming/azurite_obstore")

    registry = StorageRegistry()
    registry.register_alias(
        "azurite-stream",
        f"az://{azure_blob_default_container_name}",
        backend="obstore",
        account_name=azure_blob_service.account_name,
        account_key=azure_blob_service.account_key,
        endpoint=azure_blob_service.account_url,
        client_options={"allow_http": True},
    )
    object_path = "streaming/pipeline_az.bin"
    azurite_obstore.write_bytes_sync(object_path, STREAM_PAYLOAD)

    sync_pipeline = SyncStoragePipeline(registry=registry)
    sync_chunks = list(sync_pipeline.stream_read(f"alias://azurite-stream/{object_path}", chunk_size=STREAM_CHUNK_SIZE))
    assert len(sync_chunks) > 1
    assert b"".join(sync_chunks) == STREAM_PAYLOAD

    await _assert_pipeline_direct_arrow_streaming(
        registry,
        "azurite-stream",
        "streaming/pipeline_arrow_az",
        check_overwrite_false_conflict=True,
        check_multipart_12mb=True,
    )


@pytest.mark.xdist_group("azure_blob")
async def test_azurite_fsspec_streaming(azurite_fsspec: "ObjectStoreProtocol") -> None:
    """Verify FSSpecBackend streaming and Arrow round-trips on Azurite."""
    await _assert_backend_streaming_and_arrow(azurite_fsspec, "streaming/azurite_fsspec")


async def _assert_backend_streaming_and_arrow(backend: "ObjectStoreProtocol", prefix: str) -> None:
    """Assert sync/async chunked byte streaming and Arrow Parquet round-trip on an object store backend."""
    blob_path = f"{prefix}/payload.bin"
    backend.write_bytes_sync(blob_path, STREAM_PAYLOAD)

    sync_chunks = list(backend.stream_read_sync(blob_path, chunk_size=STREAM_CHUNK_SIZE))
    assert len(sync_chunks) > 1
    assert b"".join(sync_chunks) == STREAM_PAYLOAD

    async_stream = await backend.stream_read_async(blob_path, chunk_size=STREAM_CHUNK_SIZE)
    async_chunks = [chunk async for chunk in async_stream]
    assert len(async_chunks) >= 1
    assert b"".join(async_chunks) == STREAM_PAYLOAD

    if PYARROW_INSTALLED and pyarrow is not None:
        arrow_path = f"{prefix}/records.parquet"
        expected_table = pyarrow.table({"id": [1, 2, 3], "name": ["alpha", "beta", "gamma"]})
        backend.write_arrow_sync(arrow_path, expected_table)
        loaded_table = backend.read_arrow_sync(arrow_path)
        assert loaded_table.equals(expected_table)


async def _assert_pipeline_direct_arrow_streaming(
    registry: StorageRegistry,
    alias_name: str,
    prefix: str,
    *,
    check_overwrite_false_conflict: bool = True,
    check_multipart_12mb: bool = False,
) -> None:
    """Assert pipeline direct Arrow streaming, abort semantics, and overwrite protection."""
    if not OBSTORE_INSTALLED or not PYARROW_INSTALLED or pyarrow is None:
        return

    backend = registry.get(alias_name)
    sync_pipeline = SyncStoragePipeline(registry=registry)
    async_pipeline = AsyncStoragePipeline(registry=registry)

    sample_table = pyarrow.table({"id": [1, 2, 3, 4], "label": ["a", "b", "c", "d"]})

    for fmt in ("parquet", "arrow-ipc", "csv"):
        dest_sync = f"alias://{alias_name}/{prefix}/sync_table_{fmt}.{fmt}"
        t_sync = sync_pipeline.write_arrow(sample_table, dest_sync, format_hint=fmt)
        assert t_sync["backend"] == "obstore"
        assert t_sync["rows_processed"] == 4
        assert t_sync["bytes_processed"] > 0
        assert t_sync["format"] == fmt

        reader = pyarrow.RecordBatchReader.from_batches(sample_table.schema, sample_table.to_batches())
        dest_sync_b = f"alias://{alias_name}/{prefix}/sync_reader_{fmt}.{fmt}"
        t_sync_b = sync_pipeline.write_arrow_batches(dest_sync_b, reader, file_format=fmt)
        assert t_sync_b["backend"] == "obstore"
        assert t_sync_b["rows_processed"] == 4

        dest_async = f"alias://{alias_name}/{prefix}/async_table_{fmt}.{fmt}"
        t_async = await async_pipeline.write_arrow(sample_table, dest_async, format_hint=fmt)
        assert t_async["backend"] == "obstore"
        assert t_async["rows_processed"] == 4

        dest_async_b = f"alias://{alias_name}/{prefix}/async_batches_{fmt}.{fmt}"
        t_async_b = await async_pipeline.write_arrow_batches_async(
            dest_async_b, sample_table.to_batches(), file_format=fmt
        )
        assert t_async_b["backend"] == "obstore"
        assert t_async_b["rows_processed"] == 4

        if fmt == "parquet":
            read_back = backend.read_arrow_sync(f"{prefix}/sync_table_parquet.parquet")
            assert read_back.equals(sample_table)

    v1 = pyarrow.table({"id": [1], "label": ["v1"]})
    sync_pipeline.write_arrow(v1, f"alias://{alias_name}/{prefix}/existing.parquet")

    def _failing_batches() -> Iterator[Any]:
        yield pyarrow.RecordBatch.from_pydict({"id": [10], "label": ["fail"]}, schema=v1.schema)
        raise RuntimeError("emulator stream abort")

    with pytest.raises(RuntimeError, match="emulator stream abort"):
        sync_pipeline.write_arrow_batches(f"alias://{alias_name}/{prefix}/existing.parquet", _failing_batches())

    assert backend.read_arrow_sync(f"{prefix}/existing.parquet").equals(v1)

    with pytest.raises(RuntimeError, match="emulator stream abort"):
        sync_pipeline.write_arrow_batches(f"alias://{alias_name}/{prefix}/fresh_aborted.parquet", _failing_batches())

    assert not backend.exists_sync(f"{prefix}/fresh_aborted.parquet")

    with pytest.raises(RuntimeError, match="emulator stream abort"):
        await async_pipeline.write_arrow_batches_async(
            f"alias://{alias_name}/{prefix}/existing.parquet", _failing_batches()
        )

    assert backend.read_arrow_sync(f"{prefix}/existing.parquet").equals(v1)

    with pytest.raises(RuntimeError, match="emulator stream abort"):
        await async_pipeline.write_arrow_batches_async(
            f"alias://{alias_name}/{prefix}/fresh_aborted_async.parquet", _failing_batches()
        )

    assert not backend.exists_sync(f"{prefix}/fresh_aborted_async.parquet")

    cancel_existing_dest = f"alias://{alias_name}/{prefix}/cancel_existing.parquet"
    sync_pipeline.write_arrow(v1, cancel_existing_dest)

    first_batch_emitted = threading.Event()
    release_generator = threading.Event()

    def _cancellable_batches() -> Iterator[Any]:
        yield pyarrow.RecordBatch.from_pydict({"id": [100], "label": ["c1"]}, schema=v1.schema)
        first_batch_emitted.set()
        release_generator.wait(timeout=5.0)
        yield pyarrow.RecordBatch.from_pydict({"id": [101], "label": ["c2"]}, schema=v1.schema)

    task_existing = asyncio.create_task(
        async_pipeline.write_arrow_batches_async(cancel_existing_dest, _cancellable_batches())
    )
    await asyncio.to_thread(first_batch_emitted.wait, 5.0)
    task_existing.cancel()
    release_generator.set()
    with pytest.raises(asyncio.CancelledError):
        await task_existing

    assert backend.read_arrow_sync(f"{prefix}/cancel_existing.parquet").equals(v1)

    cancel_fresh_dest = f"alias://{alias_name}/{prefix}/cancel_fresh.parquet"
    first_batch_fresh = threading.Event()
    release_fresh = threading.Event()

    def _cancellable_fresh_batches() -> Iterator[Any]:
        yield pyarrow.RecordBatch.from_pydict({"id": [200], "label": ["f1"]}, schema=v1.schema)
        first_batch_fresh.set()
        release_fresh.wait(timeout=5.0)
        yield pyarrow.RecordBatch.from_pydict({"id": [201], "label": ["f2"]}, schema=v1.schema)

    task_fresh = asyncio.create_task(
        async_pipeline.write_arrow_batches_async(cancel_fresh_dest, _cancellable_fresh_batches())
    )
    await asyncio.to_thread(first_batch_fresh.wait, 5.0)
    task_fresh.cancel()
    release_fresh.set()
    with pytest.raises(asyncio.CancelledError):
        await task_fresh

    assert not await backend.exists_async(f"{prefix}/cancel_fresh.parquet")

    if check_overwrite_false_conflict:
        once_dest = f"alias://{alias_name}/{prefix}/once.parquet"
        sync_pipeline.write_arrow(v1, once_dest, overwrite=False)
        with pytest.raises(FileExistsInStorageError):
            sync_pipeline.write_arrow(v1, once_dest, overwrite=False)
        with pytest.raises(FileExistsInStorageError):
            await async_pipeline.write_arrow(v1, once_dest, overwrite=False)
        assert backend.read_arrow_sync(f"{prefix}/once.parquet").equals(v1)

    if check_multipart_12mb:
        chunk_data = b"x" * (1024 * 1024)
        large_batches = [
            pyarrow.RecordBatch.from_pydict({"chunk": [chunk_data]}) for _ in range(12)
        ]
        large_dest = f"alias://{alias_name}/{prefix}/multipart_12mb.parquet"
        t_large = sync_pipeline.write_arrow_batches(
            large_dest,
            large_batches,
        )
        assert t_large["rows_processed"] == 12
        assert t_large["bytes_processed"] > 10_485_760
        loaded_large = backend.read_arrow_sync(f"{prefix}/multipart_12mb.parquet")
        assert loaded_large.num_rows == 12
