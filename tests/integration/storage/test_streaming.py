"""Integration tests for sync and async storage streaming across local and multi-cloud backends."""

from typing import TYPE_CHECKING, Any

import pytest

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
