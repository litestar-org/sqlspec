"""Integration tests for storage backends using minio fixtures.

Tests storage backend operations against S3-compatible storage using pytest-databases minio fixtures.
Follows Advanced Alchemy patterns for comprehensive storage testing.
"""

import logging
from pathlib import Path
from typing import Any

import pytest
from minio import Minio
from pytest_databases.docker.minio import MinioService

from sqlspec.exceptions import FileNotFoundInStorageError
from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage.errors import execute_sync_storage_operation
from sqlspec.storage.registry import storage_registry
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED, PYARROW_INSTALLED

# Test data
TEST_TEXT_CONTENT = "Hello, SQLSpec storage integration test!"
TEST_BINARY_CONTENT = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
)


def _find_log_record(records: "list[logging.LogRecord]", message: str, logger_name: str) -> "logging.LogRecord":
    for record in records:
        if record.name != logger_name:
            continue
        if record.getMessage() == message:
            return record
    msg = f"Expected log message '{message}' from '{logger_name}' not found"
    raise AssertionError(msg)


def _raise_missing() -> None:
    raise FileNotFoundError("missing")


@pytest.fixture
def local_test_setup(tmp_path: "Path") -> "Path":
    """Create test directory with sample files."""
    test_dir = tmp_path / "storage_test"
    test_dir.mkdir()

    # Create sample files
    (test_dir / "test.txt").write_text(TEST_TEXT_CONTENT)
    (test_dir / "test.bin").write_bytes(TEST_BINARY_CONTENT)

    # Create subdirectory structure
    subdir = test_dir / "subdir"
    subdir.mkdir()
    (subdir / "nested.txt").write_text("Nested file content")

    return test_dir


@pytest.fixture
def fsspec_s3_backend(
    minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str
) -> "ObjectStoreProtocol":
    """Set up FSSpec S3 backend for testing."""
    _ = minio_client  # Ensures bucket is created
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    return FSSpecBackend(
        uri=f"s3://{minio_default_bucket_name}/",
        endpoint_url=f"http://{minio_service.endpoint}",
        key=minio_service.access_key,
        secret=minio_service.secret_key,
        use_ssl=False,
        client_kwargs={"verify": False, "use_ssl": False},
    )


@pytest.fixture
def obstore_s3_backend(
    minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str
) -> "ObjectStoreProtocol":
    """Set up ObStore S3 backend for testing."""
    _ = minio_client  # Ensures bucket is created
    from sqlspec.storage.backends.obstore import ObStoreBackend

    s3_uri = f"s3://{minio_default_bucket_name}"
    return ObStoreBackend(
        s3_uri,
        aws_endpoint=f"http://{minio_service.endpoint}",
        aws_access_key_id=minio_service.access_key,
        aws_secret_access_key=minio_service.secret_key,
        aws_virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )


# Local storage tests


@pytest.mark.xdist_group("storage")
def test_local_store_file_operations(local_test_setup: Path) -> None:
    """Test LocalStore basic file operations."""
    from sqlspec.storage.backends.local import LocalStore

    store = LocalStore(str(local_test_setup))

    # Test exists
    assert store.exists("test.txt")
    assert not store.exists("nonexistent.txt")

    # Test read operations
    text_content = store.read_text("test.txt")
    assert text_content == TEST_TEXT_CONTENT

    binary_content = store.read_bytes("test.bin")
    assert binary_content == TEST_BINARY_CONTENT


@pytest.mark.xdist_group("storage")
def test_local_store_write_operations(local_test_setup: Path) -> None:
    """Test LocalStore write operations."""
    from sqlspec.storage.backends.local import LocalStore

    store = LocalStore(str(local_test_setup))

    # Test write text
    new_text = "New text content"
    store.write_text("new.txt", new_text)
    assert store.read_text("new.txt") == new_text

    # Test write bytes
    new_bytes = b"New binary content"
    store.write_bytes("new.bin", new_bytes)
    assert store.read_bytes("new.bin") == new_bytes


@pytest.mark.xdist_group("storage")
def test_local_store_listing_operations(local_test_setup: Path) -> None:
    """Test LocalStore listing operations."""
    from sqlspec.storage.backends.local import LocalStore

    store = LocalStore(str(local_test_setup))

    # Test list_objects
    objects = store.list_objects()
    assert "test.txt" in objects
    assert "test.bin" in objects
    assert "subdir/nested.txt" in objects


@pytest.mark.xdist_group("storage")
def test_local_store_url_signing_not_supported(local_test_setup: Path) -> None:
    """Test LocalStore URL signing raises NotImplementedError."""
    from sqlspec.storage.backends.local import LocalStore

    store = LocalStore(str(local_test_setup))

    # Local storage does not support URL signing
    assert store.supports_signing is False

    with pytest.raises(NotImplementedError, match="URL signing is not applicable"):
        store.sign_sync("test.txt", expires_in=3600)


@pytest.mark.xdist_group("storage")
async def test_local_store_async_operations(local_test_setup: Path) -> None:
    """Test LocalStore async operations."""
    from sqlspec.storage.backends.local import LocalStore

    store = LocalStore(str(local_test_setup))

    # Test async exists
    exists = await store.exists_async("test.txt")
    assert exists

    # Test async read operations
    text_content = await store.read_text_async("test.txt")
    assert text_content == TEST_TEXT_CONTENT

    binary_content = await store.read_bytes_async("test.bin")
    assert binary_content == TEST_BINARY_CONTENT

    # Test async write operations
    new_text = "Async new text content"
    await store.write_text_async("async_new.txt", new_text)
    assert await store.read_text_async("async_new.txt") == new_text


def test_storage_missing_logging_format(caplog) -> None:
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


# FSSpec S3 backend tests


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_basic_operations(
    fsspec_s3_backend: "ObjectStoreProtocol", minio_client: "Minio", minio_default_bucket_name: str
) -> None:
    """Test FSSpec S3 backend basic operations."""
    # Ensure bucket exists (following Advanced Alchemy pattern)
    assert minio_client.bucket_exists(bucket_name=minio_default_bucket_name), (
        f"Bucket {minio_default_bucket_name} does not exist"
    )

    # Test write and read text
    test_path = "integration_test/test.txt"
    fsspec_s3_backend.write_text(test_path, TEST_TEXT_CONTENT)

    content = fsspec_s3_backend.read_text(test_path)
    assert content == TEST_TEXT_CONTENT

    # Test exists
    assert fsspec_s3_backend.exists(test_path)
    assert not fsspec_s3_backend.exists("nonexistent.txt")


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_binary_operations(fsspec_s3_backend: "ObjectStoreProtocol") -> None:
    """Test FSSpec S3 backend binary operations."""
    test_path = "integration_test/binary.bin"
    fsspec_s3_backend.write_bytes(test_path, TEST_BINARY_CONTENT)

    content = fsspec_s3_backend.read_bytes(test_path)
    assert content == TEST_BINARY_CONTENT


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_fsspec_s3_async_operations(fsspec_s3_backend: "ObjectStoreProtocol") -> None:
    """Test FSSpec S3 backend async operations."""
    test_path = "integration_test/async_test.txt"

    # Test async operations
    await fsspec_s3_backend.write_text_async(test_path, TEST_TEXT_CONTENT)
    content = await fsspec_s3_backend.read_text_async(test_path)
    assert content == TEST_TEXT_CONTENT

    # Test async exists
    exists = await fsspec_s3_backend.exists_async(test_path)
    assert exists


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_listing_operations(fsspec_s3_backend: "ObjectStoreProtocol") -> None:
    """Test FSSpec S3 backend listing operations."""
    # Write multiple test files
    test_files = ["list_test/file1.txt", "list_test/file2.txt", "list_test/subdir/file3.txt"]
    for file_path in test_files:
        fsspec_s3_backend.write_text(file_path, f"Content of {file_path}")

    # Test list_objects
    objects = fsspec_s3_backend.list_objects("list_test/")
    assert len(objects) >= 3
    assert any("file1.txt" in obj for obj in objects)
    assert any("file2.txt" in obj for obj in objects)
    assert any("file3.txt" in obj for obj in objects)


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_copy_move_operations(fsspec_s3_backend: "ObjectStoreProtocol") -> None:
    """Test FSSpec S3 backend copy and move operations."""
    # Setup source file
    source_path = "copy_test/source.txt"
    copy_path = "copy_test/copy.txt"
    move_source_path = "move_test/source.txt"
    move_dest_path = "move_test/moved.txt"

    fsspec_s3_backend.write_text(source_path, TEST_TEXT_CONTENT)
    fsspec_s3_backend.write_text(move_source_path, TEST_TEXT_CONTENT)

    # Test copy
    fsspec_s3_backend.copy(source_path, copy_path)
    assert fsspec_s3_backend.exists(source_path)  # Original should still exist
    assert fsspec_s3_backend.exists(copy_path)
    assert fsspec_s3_backend.read_text(copy_path) == TEST_TEXT_CONTENT

    # Test move
    fsspec_s3_backend.move(move_source_path, move_dest_path)
    assert not fsspec_s3_backend.exists(move_source_path)  # Original should be gone
    assert fsspec_s3_backend.exists(move_dest_path)
    assert fsspec_s3_backend.read_text(move_dest_path) == TEST_TEXT_CONTENT


# ObStore S3 backend tests


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_basic_operations(
    obstore_s3_backend: "ObjectStoreProtocol", minio_client: "Minio", minio_default_bucket_name: str
) -> None:
    """Test ObStore S3 backend basic operations."""
    # Ensure bucket exists (following Advanced Alchemy pattern)
    assert minio_client.bucket_exists(bucket_name=minio_default_bucket_name), (
        f"Bucket {minio_default_bucket_name} does not exist"
    )

    test_path = "integration_test/obstore_test.txt"

    # Test write and read
    obstore_s3_backend.write_text(test_path, TEST_TEXT_CONTENT)
    content = obstore_s3_backend.read_text(test_path)
    assert content == TEST_TEXT_CONTENT

    # Test exists
    assert obstore_s3_backend.exists(test_path)


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_binary_operations(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test ObStore S3 backend binary operations."""
    test_path = "integration_test/obstore_binary.bin"

    obstore_s3_backend.write_bytes(test_path, TEST_BINARY_CONTENT)
    content = obstore_s3_backend.read_bytes(test_path)
    assert content == TEST_BINARY_CONTENT


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_obstore_s3_async_operations(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test ObStore S3 backend async operations."""
    test_path = "integration_test/obstore_async.txt"

    # Test async operations
    await obstore_s3_backend.write_text_async(test_path, TEST_TEXT_CONTENT)
    content = await obstore_s3_backend.read_text_async(test_path)
    assert content == TEST_TEXT_CONTENT

    exists = await obstore_s3_backend.exists_async(test_path)
    assert exists


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_listing_operations(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test ObStore S3 backend listing operations."""
    # Write test files in different paths
    test_files = ["obstore_list/file1.txt", "obstore_list/file2.txt", "obstore_list/subdir/file3.txt"]
    for file_path in test_files:
        obstore_s3_backend.write_text(file_path, f"ObStore content of {file_path}")

    # Test list_objects
    objects = obstore_s3_backend.list_objects("obstore_list/")
    assert len(objects) >= 3
    assert any("file1.txt" in obj for obj in objects)
    assert any("file2.txt" in obj for obj in objects)


# Storage registry tests


@pytest.mark.xdist_group("storage")
def test_registry_uri_resolution_local(tmp_path: "Path") -> None:
    """Test storage registry URI resolution for local files."""
    from sqlspec.storage.backends.local import LocalStore
    from sqlspec.storage.backends.obstore import ObStoreBackend

    # Test file URI resolution
    test_file = tmp_path / "registry_test.txt"
    test_file.write_text(TEST_TEXT_CONTENT)

    # Test file:// URI
    file_uri = f"file://{test_file}"
    backend = storage_registry.get(file_uri)
    # Registry prefers obstore for file:// URIs when available, otherwise LocalStore
    assert isinstance(backend, (ObStoreBackend, LocalStore))

    content = backend.read_text("registry_test.txt")
    assert content == TEST_TEXT_CONTENT


@pytest.mark.xdist_group("storage")
def test_registry_path_resolution(tmp_path: "Path") -> None:
    """Test storage registry resolution for raw paths."""
    from sqlspec.storage.backends.local import LocalStore
    from sqlspec.storage.backends.obstore import ObStoreBackend

    # Test Path object resolution
    test_file = tmp_path / "path_test.txt"
    test_file.write_text(TEST_TEXT_CONTENT)

    backend = storage_registry.get(tmp_path)
    # Registry prefers obstore for local paths when available, otherwise LocalStore
    assert isinstance(backend, (ObStoreBackend, LocalStore))

    content = backend.read_text("path_test.txt")
    assert content == TEST_TEXT_CONTENT


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_registry_s3_fsspec_resolution(
    minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str
) -> None:
    """Test storage registry S3 resolution with FSSpec backend."""
    _ = minio_client  # Ensures bucket is created
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    s3_uri = f"s3://{minio_default_bucket_name}/registry_test/"

    backend = storage_registry.get(
        s3_uri,
        backend="fsspec",
        endpoint_url=f"http://{minio_service.endpoint}",
        key=minio_service.access_key,
        secret=minio_service.secret_key,
        use_ssl=False,
        client_kwargs={"verify": False, "use_ssl": False},
    )

    # Should get FSSpec backend for S3
    assert isinstance(backend, FSSpecBackend)

    # Test basic operations
    test_path = "registry_fsspec_test.txt"
    backend.write_text(test_path, TEST_TEXT_CONTENT)
    content = backend.read_text(test_path)
    assert content == TEST_TEXT_CONTENT


@pytest.mark.xdist_group("storage")
def test_registry_alias_registration(
    minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str, tmp_path: "Path"
) -> None:
    """Test storage registry alias registration and usage."""
    _ = minio_client  # Ensures bucket is created
    from sqlspec.storage.backends.local import LocalStore
    from sqlspec.storage.backends.obstore import ObStoreBackend

    # Clear registry to avoid test interference
    storage_registry.clear()

    try:
        # Register local alias
        storage_registry.register_alias("test-local", uri=f"file://{tmp_path / 'test_data'}")

        # Test local alias
        backend = storage_registry.get("test-local")
        # Registry prefers obstore for local paths when available, otherwise LocalStore
        assert isinstance(backend, (ObStoreBackend, LocalStore))

        # Create test data
        backend.write_text("alias_test.txt", TEST_TEXT_CONTENT)
        content = backend.read_text("alias_test.txt")
        assert content == TEST_TEXT_CONTENT

        # Register S3 alias if fsspec available
        if FSSPEC_INSTALLED:
            from sqlspec.storage.backends.fsspec import FSSpecBackend

            storage_registry.register_alias(
                "test-s3",
                uri=f"s3://{minio_default_bucket_name}/",
                backend="fsspec",
                endpoint_url=f"http://{minio_service.endpoint}",
                key=minio_service.access_key,
                secret=minio_service.secret_key,
                use_ssl=False,
                client_kwargs={"verify": False, "use_ssl": False},
            )

            s3_backend = storage_registry.get("test-s3")
            assert isinstance(s3_backend, FSSpecBackend)

            # Test S3 alias operations
            s3_backend.write_text("s3_alias_test.txt", TEST_TEXT_CONTENT)
            s3_content = s3_backend.read_text("s3_alias_test.txt")
            assert s3_content == TEST_TEXT_CONTENT

    finally:
        # Clean up registry
        storage_registry.clear()


# Backend comparison tests


@pytest.fixture
def local_backend(tmp_path: "Path") -> "ObjectStoreProtocol":
    """Create LocalStore backend."""
    from sqlspec.storage.backends.local import LocalStore

    return LocalStore(str(tmp_path))


@pytest.fixture
def fsspec_s3_backend_optional(
    minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str
) -> "ObjectStoreProtocol":
    """Create FSSpec S3 backend if available."""
    _ = minio_client  # Ensures bucket is created
    if not FSSPEC_INSTALLED:
        pytest.skip("fsspec missing")

    from sqlspec.storage.backends.fsspec import FSSpecBackend

    return FSSpecBackend.from_config({
        "protocol": "s3",
        "fs_config": {
            "endpoint_url": f"http://{minio_service.host}:{minio_service.port}",
            "key": minio_service.access_key,
            "secret": minio_service.secret_key,
        },
        "base_path": minio_default_bucket_name,
    })


@pytest.fixture
def obstore_s3_backend_optional(
    minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str
) -> "ObjectStoreProtocol":
    """Create ObStore S3 backend if available."""
    _ = minio_client  # Ensures bucket is created
    if not OBSTORE_INSTALLED:
        pytest.skip("obstore missing")

    from sqlspec.storage.backends.obstore import ObStoreBackend

    s3_uri = f"s3://{minio_default_bucket_name}"
    return ObStoreBackend(
        s3_uri,
        aws_endpoint=f"http://{minio_service.endpoint}",
        aws_access_key_id=minio_service.access_key,
        aws_secret_access_key=minio_service.secret_key,
        aws_virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )


@pytest.mark.xdist_group("storage")
@pytest.mark.parametrize("backend_name", ["local_backend", "fsspec_s3_backend_optional", "obstore_s3_backend_optional"])
def test_backend_consistency(request: pytest.FixtureRequest, backend_name: str) -> None:
    """Test that all backends provide consistent behavior."""
    backend = request.getfixturevalue(backend_name)
    if backend is None:
        pytest.skip(f"Backend {backend_name} missing")

    test_path = f"consistency_test_{backend_name}.txt"

    # Test write/read consistency
    backend.write_text(test_path, TEST_TEXT_CONTENT)
    content = backend.read_text(test_path)
    assert content == TEST_TEXT_CONTENT

    # Test exists consistency
    assert backend.exists(test_path)

    # Test URL signing consistency (only for backends that support signing)
    if backend.supports_signing:
        signed_url = backend.sign_sync(test_path, expires_in=3600)
        assert isinstance(signed_url, str)
        assert len(signed_url) > 0
    else:
        with pytest.raises(NotImplementedError):
            backend.sign_sync(test_path, expires_in=3600)


@pytest.mark.xdist_group("storage")
@pytest.mark.parametrize("backend_name", ["local_backend", "fsspec_s3_backend_optional", "obstore_s3_backend_optional"])
async def test_backend_async_consistency(request: pytest.FixtureRequest, backend_name: str) -> None:
    """Test that all backends provide consistent async behavior."""
    backend = request.getfixturevalue(backend_name)
    if backend is None:
        pytest.skip(f"Backend {backend_name} missing")

    test_path = f"async_consistency_{backend_name}.txt"

    # Test async write/read consistency
    await backend.write_text_async(test_path, TEST_TEXT_CONTENT)
    content = await backend.read_text_async(test_path)
    assert content == TEST_TEXT_CONTENT

    # Test async exists consistency
    exists = await backend.exists_async(test_path)
    assert exists


# Error handling tests


@pytest.mark.xdist_group("storage")
def test_local_backend_error_handling(tmp_path: "Path") -> None:
    """Test LocalStore error handling for invalid operations."""
    from sqlspec.storage.backends.local import LocalStore

    backend = LocalStore(str(tmp_path))

    # Test reading nonexistent file
    with pytest.raises(FileNotFoundError):
        backend.read_text("nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        backend.read_bytes("nonexistent.txt")


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_error_handling(
    minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str
) -> None:
    """Test FSSpec S3 backend error handling."""
    _ = minio_client  # Ensures bucket is created
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    backend = FSSpecBackend.from_config({
        "protocol": "s3",
        "fs_config": {
            "endpoint_url": f"http://{minio_service.host}:{minio_service.port}",
            "key": minio_service.access_key,
            "secret": minio_service.secret_key,
        },
        "base_path": minio_default_bucket_name,
    })

    # Test reading nonexistent file
    with pytest.raises(FileNotFoundInStorageError):
        backend.read_text("nonexistent.txt")


@pytest.mark.xdist_group("storage")
async def test_async_error_handling(tmp_path: "Path") -> None:
    """Test async error handling."""
    from sqlspec.storage.backends.local import LocalStore

    backend = LocalStore(str(tmp_path))

    # Test async reading nonexistent file
    with pytest.raises(FileNotFoundError):
        await backend.read_text_async("nonexistent.txt")

    with pytest.raises(FileNotFoundError):
        await backend.read_bytes_async("nonexistent.txt")


# Registry advanced tests


@pytest.mark.xdist_group("storage")
def test_registry_caching_behavior(tmp_path: "Path") -> None:
    """Test that storage registry properly caches backend instances."""
    storage_registry.clear()

    try:
        uri = f"file://{tmp_path}"

        # Get same URI twice
        backend1 = storage_registry.get(uri)
        backend2 = storage_registry.get(uri)

        # Should return the same instance (cached)
        assert backend1 is backend2

        # Clear cache and get again
        storage_registry.clear_cache(uri)
        backend3 = storage_registry.get(uri)

        # Should be different instance after cache clear
        assert backend1 is not backend3

    finally:
        storage_registry.clear()


@pytest.mark.xdist_group("storage")
def test_registry_alias_management(tmp_path: "Path") -> None:
    """Test storage registry alias management features."""
    storage_registry.clear()

    try:
        # Register alias
        alias_name = "test-management"
        storage_registry.register_alias(alias_name, uri=f"file://{tmp_path}")

        # Test alias registration check
        assert storage_registry.is_alias_registered(alias_name)
        assert not storage_registry.is_alias_registered("nonexistent-alias")

        # Test list aliases
        aliases = storage_registry.list_aliases()
        assert alias_name in aliases

        # Test clearing aliases
        storage_registry.clear_aliases()
        assert not storage_registry.is_alias_registered(alias_name)

    finally:
        storage_registry.clear()


@pytest.mark.xdist_group("storage")
def test_registry_backend_fallback_order(
    tmp_path: "Path", minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str
) -> None:
    """Test that registry follows correct backend fallback order."""
    _ = minio_client  # Ensures bucket is created
    from sqlspec.storage.backends.local import LocalStore
    from sqlspec.storage.backends.obstore import ObStoreBackend

    storage_registry.clear()

    try:
        # Test local file resolution (prefers ObStore > FSSpec > LocalStore)
        local_uri = f"file://{tmp_path}"
        local_backend = storage_registry.get(local_uri)
        # Should get ObStore if available, else FSSpec, else LocalStore
        assert isinstance(local_backend, (ObStoreBackend, LocalStore))

        # Test S3 resolution (should prefer ObStore > FSSpec if available)
        s3_uri = f"s3://{minio_default_bucket_name}"
        s3_backend = storage_registry.get(
            s3_uri,
            endpoint_url=f"http://{minio_service.host}:{minio_service.port}",
            aws_access_key_id=minio_service.access_key,
            aws_secret_access_key=minio_service.secret_key,
        )

        # Should get ObStore if available, else FSSpec, else error
        if OBSTORE_INSTALLED:
            from sqlspec.storage.backends.obstore import ObStoreBackend

            assert isinstance(s3_backend, ObStoreBackend)
        elif FSSPEC_INSTALLED:
            from sqlspec.storage.backends.fsspec import FSSpecBackend

            assert isinstance(s3_backend, FSSpecBackend)
        else:
            # Should raise MissingDependencyError if no cloud backends available
            pass

    finally:
        storage_registry.clear()


# Arrow integration tests


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow missing")
def test_local_arrow_operations(tmp_path: "Path") -> None:
    """Test LocalStore Arrow operations if pyarrow is available."""
    from sqlspec.storage.backends.local import LocalStore

    backend = LocalStore(str(tmp_path))

    # Create test Arrow data
    import pyarrow as pa

    data: dict[str, Any] = {"col1": [1, 2, 3, 4], "col2": ["a", "b", "c", "d"], "col3": [1.1, 2.2, 3.3, 4.4]}
    table = pa.table(data)

    # Test write/read Arrow table
    arrow_path = "arrow_test.parquet"
    backend.write_arrow(arrow_path, table)

    read_table = backend.read_arrow(arrow_path)
    assert read_table.equals(table)

    # Test exists for Arrow file
    assert backend.exists(arrow_path)


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow missing")
def test_fsspec_s3_arrow_operations(
    minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str
) -> None:
    """Test FSSpec S3 backend Arrow operations if pyarrow is available."""
    _ = minio_client  # Ensures bucket is created
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    backend = FSSpecBackend.from_config({
        "protocol": "s3",
        "fs_config": {
            "endpoint_url": f"http://{minio_service.host}:{minio_service.port}",
            "key": minio_service.access_key,
            "secret": minio_service.secret_key,
        },
        "base_path": minio_default_bucket_name,
    })

    import pyarrow as pa

    # Create test data with different types
    data: dict[str, Any] = {
        "integers": [1, 2, 3, 4, 5],
        "strings": ["hello", "world", "storage", "test", "arrow"],
        "floats": [1.1, 2.2, 3.3, 4.4, 5.5],
        "booleans": [True, False, True, False, True],
    }
    table = pa.table(data)

    # Test S3 Arrow operations
    s3_arrow_path = "s3_arrow_test.parquet"
    backend.write_arrow(s3_arrow_path, table)

    read_table = backend.read_arrow(s3_arrow_path)
    assert read_table.equals(table)


# Performance tests


@pytest.mark.xdist_group("storage")
def test_local_backend_large_file_operations(tmp_path: "Path") -> None:
    """Test LocalStore with larger file operations."""
    from sqlspec.storage.backends.local import LocalStore

    backend = LocalStore(str(tmp_path))

    # Create larger test content
    large_text = "Large file content line\n" * 1000
    large_binary = b"Binary data chunk" * 1000

    # Test large text operations
    large_text_path = "large_test.txt"
    backend.write_text(large_text_path, large_text)
    read_content = backend.read_text(large_text_path)
    assert read_content == large_text
    assert len(read_content) == len(large_text)

    # Test large binary operations
    large_binary_path = "large_test.bin"
    backend.write_bytes(large_binary_path, large_binary)
    read_binary = backend.read_bytes(large_binary_path)
    assert read_binary == large_binary
    assert len(read_binary) == len(large_binary)


@pytest.mark.xdist_group("storage")
async def test_concurrent_storage_operations(tmp_path: "Path") -> None:
    """Test concurrent storage operations."""
    import asyncio

    from sqlspec.storage.backends.local import LocalStore

    backend = LocalStore(str(tmp_path))

    async def write_test_file(index: int) -> None:
        """Write a test file asynchronously."""
        path = f"concurrent_test_{index}.txt"
        content = f"Concurrent test content {index}"
        await backend.write_text_async(path, content)

        # Verify the write
        read_content = await backend.read_text_async(path)
        assert read_content == content

    # Run multiple concurrent writes
    tasks = [write_test_file(i) for i in range(10)]
    await asyncio.gather(*tasks)

    # Verify all files exist
    for i in range(10):
        assert backend.exists(f"concurrent_test_{i}.txt")


# Metadata tests


@pytest.mark.xdist_group("storage")
def test_local_metadata_operations(tmp_path: "Path") -> None:
    """Test LocalStore metadata retrieval."""
    from sqlspec.storage.backends.local import LocalStore

    backend = LocalStore(str(tmp_path))

    # Create test file
    test_path = "metadata_test.txt"
    backend.write_text(test_path, TEST_TEXT_CONTENT)

    # Test metadata retrieval
    metadata = backend.get_metadata(test_path)
    assert metadata is not None
    assert "size" in metadata
    assert metadata["size"] == len(TEST_TEXT_CONTENT.encode())

    # Test metadata for binary file
    binary_path = "metadata_binary.bin"
    backend.write_bytes(binary_path, TEST_BINARY_CONTENT)

    binary_metadata = backend.get_metadata(binary_path)
    assert binary_metadata is not None
    assert binary_metadata["size"] == len(TEST_BINARY_CONTENT)


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_s3_metadata_operations(
    minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str
) -> None:
    """Test FSSpec S3 backend metadata operations."""
    _ = minio_client  # Ensures bucket is created
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    backend = FSSpecBackend.from_config({
        "protocol": "s3",
        "fs_config": {
            "endpoint_url": f"http://{minio_service.host}:{minio_service.port}",
            "key": minio_service.access_key,
            "secret": minio_service.secret_key,
        },
        "base_path": minio_default_bucket_name,
    })

    # Test S3 metadata
    test_path = "s3_metadata_test.txt"
    backend.write_text(test_path, TEST_TEXT_CONTENT)

    metadata = backend.get_metadata(test_path)
    assert metadata is not None
    assert "size" in metadata
