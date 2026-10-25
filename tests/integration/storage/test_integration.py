"""Integration tests for storage backends across local, RustFS S3, Floci GCS, Floci AWS S3, and Azurite."""

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import pytest

from sqlspec.exceptions import FileNotFoundInStorageError
from sqlspec.storage.backends.local import LocalStore
from sqlspec.storage.errors import execute_sync_storage_operation
from sqlspec.storage.registry import storage_registry
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils.module_loader import import_optional, import_optional_attr
from tests.fixtures.rustfs import rustfs_fsspec_kwargs, rustfs_obstore_kwargs

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_databases.docker.rustfs import RustfsService

    from sqlspec.protocols import ObjectStoreProtocol
    from sqlspec.storage.backends.fsspec import FSSpecBackend
    from sqlspec.storage.backends.obstore import ObStoreBackend
else:
    FSSpecBackend = import_optional_attr("sqlspec.storage.backends.fsspec", "FSSpecBackend")
    ObStoreBackend = import_optional_attr("sqlspec.storage.backends.obstore", "ObStoreBackend")

pyarrow: Any = import_optional("pyarrow")

TEST_TEXT_CONTENT = "Hello, SQLSpec storage integration test!"
TEST_BINARY_CONTENT = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
)


@pytest.fixture
def local_test_setup(tmp_path: "Path") -> "Path":
    """Create test directory with sample files."""
    test_dir = tmp_path / "storage_test"
    test_dir.mkdir()

    (test_dir / "test.txt").write_text(TEST_TEXT_CONTENT)
    (test_dir / "test.bin").write_bytes(TEST_BINARY_CONTENT)

    subdir = test_dir / "subdir"
    subdir.mkdir()
    (subdir / "nested.txt").write_text("Nested file content")

    return test_dir


@pytest.fixture
def fsspec_s3_backend(rustfs_service: "RustfsService", rustfs_bucket_name: str) -> "ObjectStoreProtocol":
    """Set up FSSpec S3 backend for testing."""
    pytest.importorskip("s3fs")
    return FSSpecBackend(uri=f"s3://{rustfs_bucket_name}/", **rustfs_fsspec_kwargs(rustfs_service))


@pytest.fixture
def obstore_s3_backend(rustfs_service: "RustfsService", rustfs_bucket_name: str) -> "ObjectStoreProtocol":
    """Set up ObStore S3 backend for testing."""
    s3_uri = f"s3://{rustfs_bucket_name}"
    return ObStoreBackend(s3_uri, **rustfs_obstore_kwargs(rustfs_service))


@pytest.fixture
def local_backend(tmp_path: "Path") -> "ObjectStoreProtocol":
    """Create LocalStore backend."""
    return LocalStore(str(tmp_path))


@pytest.fixture
def fsspec_s3_backend_optional(rustfs_service: "RustfsService", rustfs_bucket_name: str) -> "ObjectStoreProtocol":
    """Create FSSpec S3 backend if available."""
    if not FSSPEC_INSTALLED:
        pytest.skip("fsspec missing")
    pytest.importorskip("s3fs")
    return FSSpecBackend.from_config({
        "protocol": "s3",
        "fs_config": rustfs_fsspec_kwargs(rustfs_service),
        "base_path": rustfs_bucket_name,
    })


@pytest.fixture
def obstore_s3_backend_optional(rustfs_service: "RustfsService", rustfs_bucket_name: str) -> "ObjectStoreProtocol":
    """Create ObStore S3 backend if available."""
    if not OBSTORE_INSTALLED:
        pytest.skip("obstore missing")
    s3_uri = f"s3://{rustfs_bucket_name}"
    return ObStoreBackend(s3_uri, **rustfs_obstore_kwargs(rustfs_service))


def test_local_store_file_operations(local_test_setup: "Path") -> None:
    """Test LocalStore basic file operations."""
    store = LocalStore(str(local_test_setup))

    assert store.exists_sync("test.txt")
    assert not store.exists_sync("nonexistent.txt")

    text_content = store.read_text_sync("test.txt")
    assert text_content == TEST_TEXT_CONTENT

    binary_content = store.read_bytes_sync("test.bin")
    assert binary_content == TEST_BINARY_CONTENT


def test_local_store_write_operations(local_test_setup: "Path") -> None:
    """Test LocalStore write operations."""
    store = LocalStore(str(local_test_setup))

    new_text = "New text content"
    store.write_text_sync("new.txt", new_text)
    assert store.read_text_sync("new.txt") == new_text

    new_bytes = b"New binary content"
    store.write_bytes_sync("new.bin", new_bytes)
    assert store.read_bytes_sync("new.bin") == new_bytes


def test_local_store_listing_operations(local_test_setup: "Path") -> None:
    """Test LocalStore listing operations."""
    store = LocalStore(str(local_test_setup))

    objects = store.list_objects_sync()
    assert "test.txt" in objects
    assert "test.bin" in objects
    assert "subdir/nested.txt" in objects


def test_local_store_url_signing_not_supported(local_test_setup: "Path") -> None:
    """Test LocalStore URL signing raises NotImplementedError."""
    store = LocalStore(str(local_test_setup))

    assert store.supports_signing is False

    with pytest.raises(NotImplementedError, match="URL signing is not applicable"):
        store.sign_sync("test.txt", expires_in=3600)


async def test_local_store_async_operations(local_test_setup: "Path") -> None:
    """Test LocalStore async operations."""
    store = LocalStore(str(local_test_setup))

    exists = await store.exists_async("test.txt")
    assert exists

    text_content = await store.read_text_async("test.txt")
    assert text_content == TEST_TEXT_CONTENT

    binary_content = await store.read_bytes_async("test.bin")
    assert binary_content == TEST_BINARY_CONTENT

    new_text = "Async new text content"
    await store.write_text_async("async_new.txt", new_text)
    assert await store.read_text_async("async_new.txt") == new_text


def test_storage_missing_logging_format(caplog: pytest.LogCaptureFixture) -> None:
    """Test structured logging fields emitted when a storage object is missing."""
    caplog.set_level(logging.INFO, logger="sqlspec.storage.errors")

    with pytest.raises(FileNotFoundInStorageError):
        execute_sync_storage_operation(_raise_missing, backend="fsspec", operation="read", path="missing.txt")

    record = _find_log_record(caplog.records, "storage.object.missing", "sqlspec.storage.errors")
    extra_fields = record.__dict__.get("extra_fields")
    assert isinstance(extra_fields, dict)
    assert extra_fields.get("backend_type") == "fsspec"
    assert extra_fields.get("operation") == "read"
    assert extra_fields.get("path") == "missing.txt"
    assert extra_fields.get("exception_type") == "FileNotFoundError"
    assert extra_fields.get("retryable") is False


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_basic_operations(fsspec_s3_backend: "ObjectStoreProtocol") -> None:
    """Test FSSpec S3 backend basic operations."""
    test_path = "integration_test/test.txt"
    fsspec_s3_backend.write_text_sync(test_path, TEST_TEXT_CONTENT)

    content = fsspec_s3_backend.read_text_sync(test_path)
    assert content == TEST_TEXT_CONTENT

    assert fsspec_s3_backend.exists_sync(test_path)
    assert not fsspec_s3_backend.exists_sync("nonexistent.txt")


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_binary_operations(fsspec_s3_backend: "ObjectStoreProtocol") -> None:
    """Test FSSpec S3 backend binary operations."""
    test_path = "integration_test/binary.bin"
    fsspec_s3_backend.write_bytes_sync(test_path, TEST_BINARY_CONTENT)

    content = fsspec_s3_backend.read_bytes_sync(test_path)
    assert content == TEST_BINARY_CONTENT


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_fsspec_s3_async_operations(fsspec_s3_backend: "ObjectStoreProtocol") -> None:
    """Test FSSpec S3 backend async operations."""
    test_path = "integration_test/async_test.txt"

    await fsspec_s3_backend.write_text_async(test_path, TEST_TEXT_CONTENT)
    content = await fsspec_s3_backend.read_text_async(test_path)
    assert content == TEST_TEXT_CONTENT

    exists = await fsspec_s3_backend.exists_async(test_path)
    assert exists


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_listing_operations(fsspec_s3_backend: "ObjectStoreProtocol") -> None:
    """Test FSSpec S3 backend listing operations."""
    test_files = ["list_test/file1.txt", "list_test/file2.txt", "list_test/subdir/file3.txt"]
    for file_path in test_files:
        fsspec_s3_backend.write_text_sync(file_path, f"Content of {file_path}")

    objects = fsspec_s3_backend.list_objects_sync("list_test/")
    assert len(objects) >= 3
    assert any("file1.txt" in obj for obj in objects)
    assert any("file2.txt" in obj for obj in objects)
    assert any("file3.txt" in obj for obj in objects)


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_copy_move_operations(fsspec_s3_backend: "ObjectStoreProtocol") -> None:
    """Test FSSpec S3 backend copy and move operations."""
    source_path = "copy_test/source.txt"
    copy_path = "copy_test/copy.txt"
    move_source_path = "move_test/source.txt"
    move_dest_path = "move_test/moved.txt"

    fsspec_s3_backend.write_text_sync(source_path, TEST_TEXT_CONTENT)
    fsspec_s3_backend.write_text_sync(move_source_path, TEST_TEXT_CONTENT)

    fsspec_s3_backend.copy_sync(source_path, copy_path)
    assert fsspec_s3_backend.exists_sync(source_path)
    assert fsspec_s3_backend.exists_sync(copy_path)
    assert fsspec_s3_backend.read_text_sync(copy_path) == TEST_TEXT_CONTENT

    fsspec_s3_backend.move_sync(move_source_path, move_dest_path)
    assert not fsspec_s3_backend.exists_sync(move_source_path)
    assert fsspec_s3_backend.exists_sync(move_dest_path)
    assert fsspec_s3_backend.read_text_sync(move_dest_path) == TEST_TEXT_CONTENT


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_basic_operations(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test ObStore S3 backend basic operations."""
    test_path = "integration_test/obstore_test.txt"

    obstore_s3_backend.write_text_sync(test_path, TEST_TEXT_CONTENT)
    content = obstore_s3_backend.read_text_sync(test_path)
    assert content == TEST_TEXT_CONTENT

    assert obstore_s3_backend.exists_sync(test_path)


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_binary_operations(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test ObStore S3 backend binary operations."""
    test_path = "integration_test/obstore_binary.bin"

    obstore_s3_backend.write_bytes_sync(test_path, TEST_BINARY_CONTENT)
    content = obstore_s3_backend.read_bytes_sync(test_path)
    assert content == TEST_BINARY_CONTENT


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_obstore_s3_async_operations(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test ObStore S3 backend async operations."""
    test_path = "integration_test/obstore_async.txt"

    await obstore_s3_backend.write_text_async(test_path, TEST_TEXT_CONTENT)
    content = await obstore_s3_backend.read_text_async(test_path)
    assert content == TEST_TEXT_CONTENT

    exists = await obstore_s3_backend.exists_async(test_path)
    assert exists


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_listing_operations(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test ObStore S3 backend listing operations."""
    test_files = ["obstore_list/file1.txt", "obstore_list/file2.txt", "obstore_list/subdir/file3.txt"]
    for file_path in test_files:
        obstore_s3_backend.write_text_sync(file_path, f"ObStore content of {file_path}")

    objects = obstore_s3_backend.list_objects_sync("obstore_list/")
    assert len(objects) >= 3
    assert any("file1.txt" in obj for obj in objects)
    assert any("file2.txt" in obj for obj in objects)


def test_registry_uri_resolution_local(tmp_path: "Path") -> None:
    """Test storage registry URI resolution for local files."""
    test_file = tmp_path / "registry_test.txt"
    test_file.write_text(TEST_TEXT_CONTENT)

    file_uri = f"file://{test_file}"
    backend = storage_registry.get(file_uri)
    expected_types = (ObStoreBackend, LocalStore) if OBSTORE_INSTALLED else (LocalStore,)
    assert isinstance(backend, expected_types)

    content = backend.read_text_sync("registry_test.txt")
    assert content == TEST_TEXT_CONTENT


def test_registry_path_resolution(tmp_path: "Path") -> None:
    """Test storage registry resolution for raw paths."""
    test_file = tmp_path / "path_test.txt"
    test_file.write_text(TEST_TEXT_CONTENT)

    backend = storage_registry.get(tmp_path)
    expected_types = (ObStoreBackend, LocalStore) if OBSTORE_INSTALLED else (LocalStore,)
    assert isinstance(backend, expected_types)

    content = backend.read_text_sync("path_test.txt")
    assert content == TEST_TEXT_CONTENT


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_registry_s3_fsspec_resolution(rustfs_service: "RustfsService", rustfs_bucket_name: str) -> None:
    """Test storage registry S3 resolution with FSSpec backend."""
    pytest.importorskip("s3fs")
    s3_uri = f"s3://{rustfs_bucket_name}/registry_test/"

    backend = storage_registry.get(s3_uri, backend="fsspec", **rustfs_fsspec_kwargs(rustfs_service))
    assert isinstance(backend, FSSpecBackend)

    test_path = "registry_fsspec_test.txt"
    backend.write_text_sync(test_path, TEST_TEXT_CONTENT)
    content = backend.read_text_sync(test_path)
    assert content == TEST_TEXT_CONTENT


@pytest.mark.xdist_group("rustfs")
def test_registry_alias_registration(
    rustfs_service: "RustfsService", rustfs_bucket_name: str, tmp_path: "Path"
) -> None:
    """Test storage registry alias registration and usage."""
    storage_registry.clear()

    try:
        storage_registry.register_alias("test-local", uri=f"file://{tmp_path / 'test_data'}")

        backend = storage_registry.get("test-local")
        expected_types = (ObStoreBackend, LocalStore) if OBSTORE_INSTALLED else (LocalStore,)
        assert isinstance(backend, expected_types)

        backend.write_text_sync("alias_test.txt", TEST_TEXT_CONTENT)
        content = backend.read_text_sync("alias_test.txt")
        assert content == TEST_TEXT_CONTENT

        if FSSPEC_INSTALLED:
            pytest.importorskip("s3fs")
            storage_registry.register_alias(
                "test-s3", uri=f"s3://{rustfs_bucket_name}/", backend="fsspec", **rustfs_fsspec_kwargs(rustfs_service)
            )

            s3_backend = storage_registry.get("test-s3")
            assert isinstance(s3_backend, FSSpecBackend)

            s3_backend.write_text_sync("s3_alias_test.txt", TEST_TEXT_CONTENT)
            s3_content = s3_backend.read_text_sync("s3_alias_test.txt")
            assert s3_content == TEST_TEXT_CONTENT

    finally:
        storage_registry.clear()


@pytest.mark.xdist_group("rustfs")
@pytest.mark.parametrize("backend_name", ["local_backend", "fsspec_s3_backend_optional", "obstore_s3_backend_optional"])
def test_backend_consistency(request: pytest.FixtureRequest, backend_name: str) -> None:
    """Test that all backends provide consistent behavior."""
    backend = request.getfixturevalue(backend_name)
    if backend is None:
        pytest.skip(f"Backend {backend_name} missing")

    test_path = f"consistency_test_{backend_name}.txt"

    backend.write_text_sync(test_path, TEST_TEXT_CONTENT)
    content = backend.read_text_sync(test_path)
    assert content == TEST_TEXT_CONTENT

    assert backend.exists_sync(test_path)

    if backend.supports_signing:
        signed_url = backend.sign_sync(test_path, expires_in=3600)
        assert isinstance(signed_url, str)
        assert len(signed_url) > 0
    else:
        with pytest.raises(NotImplementedError):
            backend.sign_sync(test_path, expires_in=3600)


@pytest.mark.xdist_group("rustfs")
def test_s3_backend_resolved_uri_consistency(
    fsspec_s3_backend_optional: "ObjectStoreProtocol",
    obstore_s3_backend_optional: "ObjectStoreProtocol",
    rustfs_bucket_name: str,
) -> None:
    """Fsspec and obstore expose the same URI after equivalent S3 operations."""
    test_path = "resolved_uri_consistency.txt"

    for backend in (fsspec_s3_backend_optional, obstore_s3_backend_optional):
        backend.write_text_sync(test_path, TEST_TEXT_CONTENT)
        assert backend.read_text_sync(test_path) == TEST_TEXT_CONTENT

    expected = f"s3://{rustfs_bucket_name}/{test_path}"
    assert fsspec_s3_backend_optional.resolve_uri(test_path) == expected
    assert obstore_s3_backend_optional.resolve_uri(test_path) == expected


@pytest.mark.xdist_group("rustfs")
@pytest.mark.parametrize("backend_name", ["local_backend", "fsspec_s3_backend_optional", "obstore_s3_backend_optional"])
async def test_backend_async_consistency(request: pytest.FixtureRequest, backend_name: str) -> None:
    """Test that all backends provide consistent async behavior."""
    backend = request.getfixturevalue(backend_name)
    if backend is None:
        pytest.skip(f"Backend {backend_name} missing")

    test_path = f"async_consistency_{backend_name}.txt"

    await backend.write_text_async(test_path, TEST_TEXT_CONTENT)
    content = await backend.read_text_async(test_path)
    assert content == TEST_TEXT_CONTENT

    exists = await backend.exists_async(test_path)
    assert exists


def test_local_backend_error_handling(tmp_path: "Path") -> None:
    """Test LocalStore error handling for invalid operations."""
    backend = LocalStore(str(tmp_path))

    with pytest.raises(FileNotFoundError):
        backend.read_text_sync("nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        backend.read_bytes_sync("nonexistent.txt")


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_error_handling(rustfs_service: "RustfsService", rustfs_bucket_name: str) -> None:
    """Test FSSpec S3 backend error handling."""
    pytest.importorskip("s3fs")
    backend = FSSpecBackend.from_config({
        "protocol": "s3",
        "fs_config": rustfs_fsspec_kwargs(rustfs_service),
        "base_path": rustfs_bucket_name,
    })

    with pytest.raises(FileNotFoundInStorageError):
        backend.read_text_sync("nonexistent.txt")


async def test_async_error_handling(tmp_path: "Path") -> None:
    """Test async error handling."""
    backend = LocalStore(str(tmp_path))

    with pytest.raises(FileNotFoundError):
        await backend.read_text_async("nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await backend.read_bytes_async("nonexistent.txt")


def test_registry_caching_behavior(tmp_path: "Path") -> None:
    """Test that storage registry properly caches backend instances."""
    storage_registry.clear()

    try:
        uri = f"file://{tmp_path}"

        backend1 = storage_registry.get(uri)
        backend2 = storage_registry.get(uri)

        assert backend1 is backend2

        storage_registry.clear_cache(uri)
        backend3 = storage_registry.get(uri)

        assert backend1 is not backend3

    finally:
        storage_registry.clear()


def test_registry_alias_management(tmp_path: "Path") -> None:
    """Test storage registry alias management features."""
    storage_registry.clear()

    try:
        alias_name = "test-management"
        storage_registry.register_alias(alias_name, uri=f"file://{tmp_path}")

        assert storage_registry.is_alias_registered(alias_name)
        assert not storage_registry.is_alias_registered("nonexistent-alias")

        aliases = storage_registry.list_aliases()
        assert alias_name in aliases

        storage_registry.clear_aliases()
        assert not storage_registry.is_alias_registered(alias_name)

    finally:
        storage_registry.clear()


@pytest.mark.xdist_group("rustfs")
def test_registry_backend_fallback_order(
    tmp_path: "Path", rustfs_service: "RustfsService", rustfs_bucket_name: str
) -> None:
    """Test that registry follows correct backend fallback order."""
    storage_registry.clear()

    try:
        local_uri = f"file://{tmp_path}"
        local_backend = storage_registry.get(local_uri)
        expected_types = (ObStoreBackend, LocalStore) if OBSTORE_INSTALLED else (LocalStore,)
        assert isinstance(local_backend, expected_types)

        s3_uri = f"s3://{rustfs_bucket_name}"
        storage_kwargs = (
            rustfs_obstore_kwargs(rustfs_service) if OBSTORE_INSTALLED else rustfs_fsspec_kwargs(rustfs_service)
        )
        s3_backend = storage_registry.get(s3_uri, **storage_kwargs)

        if OBSTORE_INSTALLED:
            assert isinstance(s3_backend, ObStoreBackend)
        elif FSSPEC_INSTALLED:
            assert isinstance(s3_backend, FSSpecBackend)

    finally:
        storage_registry.clear()


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow missing")
def test_local_arrow_operations(tmp_path: "Path") -> None:
    """Test LocalStore Arrow operations if pyarrow is available."""
    backend = LocalStore(str(tmp_path))

    data: dict[str, Any] = {"col1": [1, 2, 3, 4], "col2": ["a", "b", "c", "d"], "col3": [1.1, 2.2, 3.3, 4.4]}
    table = pyarrow.table(data)

    arrow_path = "arrow_test.parquet"
    backend.write_arrow_sync(arrow_path, table)

    read_table = backend.read_arrow_sync(arrow_path)
    assert read_table.equals(table)

    assert backend.exists_sync(arrow_path)


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow missing")
def test_fsspec_s3_arrow_operations(rustfs_service: "RustfsService", rustfs_bucket_name: str) -> None:
    """Test FSSpec S3 backend Arrow operations if pyarrow is available."""
    pytest.importorskip("s3fs")
    backend = FSSpecBackend.from_config({
        "protocol": "s3",
        "fs_config": rustfs_fsspec_kwargs(rustfs_service),
        "base_path": rustfs_bucket_name,
    })

    data: dict[str, Any] = {
        "integers": [1, 2, 3, 4, 5],
        "strings": ["hello", "world", "storage", "test", "arrow"],
        "floats": [1.1, 2.2, 3.3, 4.4, 5.5],
        "booleans": [True, False, True, False, True],
    }
    table = pyarrow.table(data)

    s3_arrow_path = "s3_arrow_test.parquet"
    backend.write_arrow_sync(s3_arrow_path, table)

    read_table = backend.read_arrow_sync(s3_arrow_path)
    assert read_table.equals(table)


def test_local_backend_large_file_operations(tmp_path: "Path") -> None:
    """Test LocalStore with larger file operations."""
    backend = LocalStore(str(tmp_path))

    large_text = "Large file content line\n" * 1000
    large_binary = b"Binary data chunk" * 1000

    large_text_path = "large_test.txt"
    backend.write_text_sync(large_text_path, large_text)
    read_content = backend.read_text_sync(large_text_path)
    assert read_content == large_text
    assert len(read_content) == len(large_text)

    large_binary_path = "large_test.bin"
    backend.write_bytes_sync(large_binary_path, large_binary)
    read_binary = backend.read_bytes_sync(large_binary_path)
    assert read_binary == large_binary
    assert len(read_binary) == len(large_binary)


async def test_concurrent_storage_operations(tmp_path: "Path") -> None:
    """Test concurrent storage operations."""
    backend = LocalStore(str(tmp_path))

    async def write_test_file(index: int) -> None:
        """Write a test file asynchronously."""
        path = f"concurrent_test_{index}.txt"
        content = f"Concurrent test content {index}"
        await backend.write_text_async(path, content)

        read_content = await backend.read_text_async(path)
        assert read_content == content

    tasks = [write_test_file(i) for i in range(10)]
    await asyncio.gather(*tasks)

    for i in range(10):
        assert backend.exists_sync(f"concurrent_test_{i}.txt")


def test_local_metadata_operations(tmp_path: "Path") -> None:
    """Test LocalStore metadata retrieval."""
    backend = LocalStore(str(tmp_path))

    test_path = "metadata_test.txt"
    backend.write_text_sync(test_path, TEST_TEXT_CONTENT)

    metadata = backend.get_metadata_sync(test_path)
    assert metadata is not None
    assert "size" in metadata
    assert metadata["size"] == len(TEST_TEXT_CONTENT.encode())

    binary_path = "metadata_binary.bin"
    backend.write_bytes_sync(binary_path, TEST_BINARY_CONTENT)

    binary_metadata = backend.get_metadata_sync(binary_path)
    assert binary_metadata is not None
    assert binary_metadata["size"] == len(TEST_BINARY_CONTENT)


@pytest.mark.xdist_group("rustfs")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_metadata_operations(rustfs_service: "RustfsService", rustfs_bucket_name: str) -> None:
    """Test FSSpec S3 backend metadata operations."""
    pytest.importorskip("s3fs")
    backend = FSSpecBackend.from_config({
        "protocol": "s3",
        "fs_config": rustfs_fsspec_kwargs(rustfs_service),
        "base_path": rustfs_bucket_name,
    })

    test_path = "s3_metadata_test.txt"
    backend.write_text_sync(test_path, TEST_TEXT_CONTENT)

    metadata = backend.get_metadata_sync(test_path)
    assert metadata is not None
    assert "size" in metadata


@pytest.mark.xdist_group("floci_gcp")
async def test_floci_gcs_obstore_operations(floci_gcs_obstore: "ObjectStoreProtocol") -> None:
    """Verify sync/async object lifecycle and Arrow operations on Floci GCS via ObStoreBackend."""
    await _assert_cloud_backend_operations(floci_gcs_obstore, "floci_gcs_obstore")


@pytest.mark.xdist_group("floci_gcp")
async def test_floci_gcs_fsspec_operations(floci_gcs_fsspec: "ObjectStoreProtocol") -> None:
    """Verify sync/async object lifecycle and Arrow operations on Floci GCS via FSSpecBackend."""
    await _assert_cloud_backend_operations(floci_gcs_fsspec, "floci_gcs_fsspec")


@pytest.mark.xdist_group("floci_aws")
async def test_floci_aws_s3_obstore_operations(floci_aws_s3_obstore: "ObjectStoreProtocol") -> None:
    """Verify sync/async object lifecycle and Arrow operations on Floci AWS S3 via ObStoreBackend."""
    await _assert_cloud_backend_operations(floci_aws_s3_obstore, "floci_aws_s3_obstore")


@pytest.mark.xdist_group("azure_blob")
async def test_azurite_obstore_operations(azurite_obstore: "ObjectStoreProtocol") -> None:
    """Verify sync/async object lifecycle and Arrow operations on Azurite via ObStoreBackend."""
    await _assert_cloud_backend_operations(azurite_obstore, "azurite_obstore")


@pytest.mark.xdist_group("azure_blob")
async def test_azurite_fsspec_operations(azurite_fsspec: "ObjectStoreProtocol") -> None:
    """Verify sync/async object lifecycle and Arrow operations on Azurite via FSSpecBackend."""
    await _assert_cloud_backend_operations(azurite_fsspec, "azurite_fsspec")


async def _assert_cloud_backend_operations(backend: "ObjectStoreProtocol", prefix: str) -> None:
    """Verify sync/async bytes, existence, listing, copy, delete, and Arrow Parquet round-trips."""
    sync_path = f"{prefix}/sync.bin"
    async_path = f"{prefix}/async.bin"
    copy_path = f"{prefix}/copied.bin"

    backend.write_bytes_sync(sync_path, TEST_BINARY_CONTENT)
    assert backend.exists_sync(sync_path) is True
    assert backend.read_bytes_sync(sync_path) == TEST_BINARY_CONTENT

    await backend.write_bytes_async(async_path, TEST_TEXT_CONTENT.encode("utf-8"))
    assert await backend.exists_async(async_path) is True
    assert await backend.read_bytes_async(async_path) == TEST_TEXT_CONTENT.encode("utf-8")

    backend.copy_sync(sync_path, copy_path)
    assert backend.exists_sync(copy_path) is True
    assert backend.read_bytes_sync(copy_path) == TEST_BINARY_CONTENT

    listed = backend.list_objects_sync(f"{prefix}/")
    assert any("sync.bin" in entry for entry in listed)
    assert any("async.bin" in entry for entry in listed)
    assert any("copied.bin" in entry for entry in listed)

    backend.delete_sync(copy_path)
    assert backend.exists_sync(copy_path) is False

    if PYARROW_INSTALLED and pyarrow is not None:
        arrow_path = f"{prefix}/table.parquet"
        expected_table = pyarrow.table({
            "id": [10, 20, 30],
            "label": ["gcs", "s3", "azure"],
            "active": [True, False, True],
        })
        backend.write_arrow_sync(arrow_path, expected_table)
        loaded_table = backend.read_arrow_sync(arrow_path)
        assert loaded_table.equals(expected_table)


def _find_log_record(records: "list[logging.LogRecord]", message: str, logger_name: str) -> "logging.LogRecord":
    """Locate a log record matching exact message and logger name."""
    for record in records:
        if record.name != logger_name:
            continue
        if record.getMessage() == message:
            return record
    msg = f"Expected log message '{message}' from '{logger_name}' not found"
    raise AssertionError(msg)


def _raise_missing() -> None:
    """Raise FileNotFoundError to test storage error logging translation."""
    raise FileNotFoundError("missing")
