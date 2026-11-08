# pyright: reportPrivateUsage=false
"""Unit tests for ObStoreBackend."""

import asyncio
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Self

import pytest

from sqlspec.exceptions import (
    FileExistsInStorageError,
    MissingDependencyError,
    StorageCapabilityError,
    StorageOperationFailedError,
    StoragePathTraversalError,
)
from sqlspec.storage.backends.obstore import _SIGNABLE_PROTOCOLS, ObStoreBackend, _AsyncObstoreWriteStream
from sqlspec.typing import OBSTORE_INSTALLED, PYARROW_INSTALLED
from sqlspec.utils.type_guards import supports_direct_stream_write


class _FakeBytes(bytes):
    def __new__(cls) -> Self:
        return super().__new__(cls, b"parquet")

    def to_bytes(self) -> bytes:
        return bytes(self)


class _FakeGetResult:
    def bytes(self) -> _FakeBytes:
        return _FakeBytes()

    async def bytes_async(self) -> _FakeBytes:
        return _FakeBytes()


class _FakeStore:
    def __init__(self) -> None:
        self.get_paths: list[str] = []

    def get(self, path: str) -> _FakeGetResult:
        self.get_paths.append(path)
        return _FakeGetResult()

    async def get_async(self, path: str) -> _FakeGetResult:
        self.get_paths.append(path)
        return _FakeGetResult()


def _new_obstore_for_signing(protocol: str = "s3", base_path: str = "prefix") -> "ObStoreBackend":
    store = ObStoreBackend.__new__(ObStoreBackend)
    store.protocol = protocol
    store.base_path = base_path
    store.store = _FakeStore()
    return store


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_signable_protocols_exported_constant() -> None:
    assert _SIGNABLE_PROTOCOLS == frozenset({"s3", "gs", "gcs", "az", "azure"})


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_prepare_sign_request_rejects_unsupported_protocol() -> None:
    store = _new_obstore_for_signing(protocol="file")

    with pytest.raises(NotImplementedError, match="URL signing is not supported"):
        store._prepare_sign_request("object.txt", 3600, False)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_prepare_sign_request_rejects_excessive_expiration() -> None:
    store = _new_obstore_for_signing()

    with pytest.raises(ValueError, match="expires_in cannot exceed 604800"):
        store._prepare_sign_request("object.txt", 604801, False)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_prepare_sign_request_resolves_single_download_path() -> None:
    store = _new_obstore_for_signing(base_path="tenant")

    method, expires_delta, resolved_paths, is_single = store._prepare_sign_request("object.txt", 60, False)

    assert method == "GET"
    assert expires_delta.total_seconds() == 60
    assert resolved_paths == ["tenant/object.txt"]
    assert is_single is True


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_prepare_sign_request_resolves_list_upload_paths() -> None:
    store = _new_obstore_for_signing(base_path="tenant")

    method, expires_delta, resolved_paths, is_single = store._prepare_sign_request(["a.txt", "b.txt"], 120, True)

    assert method == "PUT"
    assert expires_delta.total_seconds() == 120
    assert resolved_paths == ["tenant/a.txt", "tenant/b.txt"]
    assert is_single is False


class _FakeListingStore:
    def __init__(self) -> None:
        self.list_paths: list[str] = []
        self.list_with_delimiter_paths: list[str] = []

    def list(self, path: str) -> Any:
        self.list_paths.append(path)
        return iter([
            [{"path": "file1.txt", "size": 5, "last_modified": None, "e_tag": None, "version": None}],
            [
                {"path": "subdir/file2.txt", "size": 8, "last_modified": None, "e_tag": None, "version": None},
                {"path": "subdir/file3.txt", "size": 12, "last_modified": None, "e_tag": None, "version": None},
            ],
        ])

    def list_with_delimiter(self, path: str) -> dict[str, Any]:
        self.list_with_delimiter_paths.append(path)
        return {
            "objects": [
                {"path": "dir/file_a.txt", "size": 10, "last_modified": None, "e_tag": None, "version": None},
                {"path": "dir/file_b.txt", "size": 20, "last_modified": None, "e_tag": None, "version": None},
            ],
            "common_prefixes": ["dir/subdir/"],
        }


class _FakeHeadStore:
    def __init__(self, metadata: dict[str, object] | None = None, *, missing: bool = False) -> None:
        self.head_paths: list[str] = []
        self.head_async_paths: list[str] = []
        self.metadata = metadata or {
            "path": "object.txt",
            "size": 12,
            "last_modified": None,
            "e_tag": "etag",
            "version": "1",
            "metadata": {"owner": "sqlspec"},
        }
        self.missing = missing

    def head(self, path: str) -> dict[str, object]:
        self.head_paths.append(path)
        if self.missing:
            raise FileNotFoundError(path)
        return self.metadata

    async def head_async(self, path: str) -> dict[str, object]:
        self.head_async_paths.append(path)
        if self.missing:
            raise FileNotFoundError(path)
        return self.metadata


class _FakeParquet:
    def __init__(self, result: Any) -> None:
        self.result = result

    def read_table(self, source: Any, **kwargs: Any) -> Any:
        return self.result

    def write_table(self, table: Any, sink: Any, **kwargs: Any) -> None:
        sink.write(b"parquet")


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_init_with_file_uri(tmp_path: Path) -> None:
    """Test initialization with file:// URI."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    assert store.base_path == ""


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_from_config(tmp_path: Path) -> None:
    """Test from_config class method."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    data_dir = f"{tmp_path}/data"
    config = {"store_uri": f"file://{data_dir}", "store_options": {}}
    store = ObStoreBackend.from_config(config)
    assert store.base_path == ""


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_write_and_read_bytes(tmp_path: Path) -> None:
    """Test write and read bytes operations."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    test_data = b"test data content"

    store.write_bytes_sync("test_file.bin", test_data)
    result = store.read_bytes_sync("test_file.bin")

    assert result == test_data


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_write_to_nonexistent_file_uri(tmp_path: Path) -> None:
    """A file:// URI naming a not-yet-created file roots the store at its parent directory."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    destination = tmp_path / "exports" / "out.parquet"
    store = ObStoreBackend(f"file://{destination}")
    store.write_bytes_sync(str(destination), b"payload")

    assert destination.read_bytes() == b"payload"
    assert store.read_bytes_sync(str(destination)) == b"payload"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_write_and_read_text(tmp_path: Path) -> None:
    """Test write and read text operations."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    test_text = "test text content\nwith multiple lines"

    store.write_text_sync("test_file.txt", test_text)
    result = store.read_text_sync("test_file.txt")

    assert result == test_text


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_exists(tmp_path: Path) -> None:
    """Test exists operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    assert not store.exists_sync("nonexistent.txt")

    store.write_text_sync("existing.txt", "content")
    assert store.exists_sync("existing.txt")


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_delete(tmp_path: Path) -> None:
    """Test delete operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    store.write_text_sync("to_delete.txt", "content")
    assert store.exists_sync("to_delete.txt")

    store.delete_sync("to_delete.txt")
    assert not store.exists_sync("to_delete.txt")


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_copy(tmp_path: Path) -> None:
    """Test copy operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    original_content = "original content"

    store.write_text_sync("original.txt", original_content)
    store.copy_sync("original.txt", "copied.txt")

    assert store.exists_sync("copied.txt")
    assert store.read_text_sync("copied.txt") == original_content


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_move(tmp_path: Path) -> None:
    """Test move operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    original_content = "content to move"

    store.write_text_sync("original.txt", original_content)
    store.move_sync("original.txt", "moved.txt")

    assert not store.exists_sync("original.txt")
    assert store.exists_sync("moved.txt")
    assert store.read_text_sync("moved.txt") == original_content


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_list_objects(tmp_path: Path) -> None:
    """Test list_objects operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    # Create test files
    store.write_text_sync("file1.txt", "content1")
    store.write_text_sync("file2.txt", "content2")
    store.write_text_sync("subdir/file3.txt", "content3")

    # List all objects
    all_objects = store.list_objects_sync()
    assert any("file1.txt" in obj for obj in all_objects)
    assert any("file2.txt" in obj for obj in all_objects)
    assert any("file3.txt" in obj for obj in all_objects)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_list_objects_sync_non_recursive_uses_list_with_delimiter(tmp_path: Path) -> None:
    """Non-recursive list results are ListResult dicts, not ListStream batches."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    fake_store = _FakeListingStore()
    store.store = fake_store

    result = store.list_objects_sync(recursive=False)

    assert fake_store.list_with_delimiter_paths == [""]
    assert fake_store.list_paths == []
    assert result == ["dir/file_a.txt", "dir/file_b.txt"]


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_list_objects_sync_recursive_uses_list_stream(tmp_path: Path) -> None:
    """Recursive lists still consume the ListStream batch iterator."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    fake_store = _FakeListingStore()
    store.store = fake_store

    result = store.list_objects_sync(recursive=True)

    assert fake_store.list_paths == [""]
    assert fake_store.list_with_delimiter_paths == []
    assert result == ["file1.txt", "subdir/file2.txt", "subdir/file3.txt"]


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_glob(tmp_path: Path) -> None:
    """Test glob pattern matching."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    # Create test files
    store.write_text_sync("test1.sql", "SELECT 1")
    store.write_text_sync("test2.sql", "SELECT 2")
    store.write_text_sync("config.json", "{}")

    # Test glob patterns
    sql_files = store.glob_sync("*.sql")
    assert any("test1.sql" in obj for obj in sql_files)
    assert any("test2.sql" in obj for obj in sql_files)
    assert not any("config.json" in obj for obj in sql_files)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_get_metadata(tmp_path: Path) -> None:
    """Test get_metadata operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    test_content = "test content for metadata"

    store.write_text_sync("test_file.txt", test_content)
    metadata = store.get_metadata_sync("test_file.txt")

    assert "exists" in metadata
    assert metadata["exists"] is True


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_get_metadata_sync_uses_local_store_path_resolution(tmp_path: Path) -> None:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}", base_path="data")
    fake_store = _FakeHeadStore()
    store.store = fake_store

    metadata = store.get_metadata_sync("nested/object.txt")

    assert fake_store.head_paths == ["nested/object.txt"]
    assert metadata["path"] == "nested/object.txt"
    assert metadata["exists"] is True


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_get_metadata_sync_returns_typed_dict_keys(tmp_path: Path) -> None:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    store.store = _FakeHeadStore()

    metadata = store.get_metadata_sync("object.txt")

    assert metadata == {
        "path": "object.txt",
        "exists": True,
        "size": 12,
        "last_modified": None,
        "e_tag": "etag",
        "version": "1",
        "custom_metadata": {"owner": "sqlspec"},
    }


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_get_metadata_sync_returns_absent_result_for_missing_object(tmp_path: Path) -> None:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    store.store = _FakeHeadStore(missing=True)

    metadata = store.get_metadata_sync("missing.txt")

    assert metadata == {"path": "missing.txt", "exists": False}


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_is_object_and_is_path(tmp_path: Path) -> None:
    """Test is_object and is_path operations."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    store.write_text_sync("file.txt", "content")
    # Create directory by writing file inside it
    store.write_text_sync("subdir/nested.txt", "content")

    assert store.is_object_sync("file.txt")
    assert not store.is_object_sync("subdir")
    assert not store.is_path_sync("file.txt")
    assert store.is_path_sync("subdir")


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_write_and_read_arrow(tmp_path: Path) -> None:
    """Test write and read Arrow table operations."""
    import pyarrow as pa

    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    # Create test Arrow table
    data: dict[str, Any] = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "score": [95.5, 87.0, 92.3]}
    table = pa.table(data)

    store.write_arrow_sync("test_data.parquet", table)
    result = store.read_arrow_sync("test_data.parquet")

    assert result.equals(table)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_read_arrow_sync_resolves_cloud_base_path_once(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.storage.backends import obstore as obstore_module
    from sqlspec.storage.backends.obstore import ObStoreBackend

    expected_table = object()
    store = ObStoreBackend("memory://", base_path="mybase")
    fake_store = _FakeStore()
    store.store = fake_store
    monkeypatch.setattr(obstore_module, "import_pyarrow_parquet", lambda: _FakeParquet(expected_table))

    result = store.read_arrow_sync("key")

    assert result is expected_table
    assert fake_store.get_paths == ["mybase/key"]


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_write_arrow_sync_resolves_cloud_base_path_once() -> None:
    import pyarrow as pa

    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend("memory://", base_path="mybase")
    table = pa.table({"id": [1, 2, 3]})

    store.write_arrow_sync("key", table)

    assert store.list_objects_sync() == ["mybase/key"]
    assert store.read_arrow_sync("key").equals(table)


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_stream_arrow(tmp_path: Path) -> None:
    """Test stream Arrow record batches."""
    import pyarrow as pa

    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    # Create test Arrow table
    data: dict[str, Any] = {"id": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "d", "e"]}
    table = pa.table(data)

    store.write_arrow_sync("stream_test.parquet", table)

    # Stream record batches
    batches = list(store.stream_arrow_sync("stream_test.parquet"))
    assert len(batches) > 0

    # Verify we can read the data
    reconstructed = pa.Table.from_batches(batches)
    assert reconstructed.equals(table)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_sign_raises_not_implemented_for_local_files(tmp_path: Path) -> None:
    """Test sign_sync raises NotImplementedError for local file protocol."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    store.write_text_sync("test.txt", "content")

    # Local file protocol does not support URL signing
    assert store.supports_signing is False

    with pytest.raises(NotImplementedError, match="URL signing is not supported for protocol 'file'"):
        store.sign_sync("test.txt")


def test_obstore_not_installed() -> None:
    """Test error when obstore is not installed."""
    if OBSTORE_INSTALLED:
        pytest.skip("obstore is installed")

    with pytest.raises(MissingDependencyError, match="obstore"):
        ObStoreBackend("file:///tmp")  # type: ignore


# Async tests


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_async_write_and_read_bytes(tmp_path: Path) -> None:
    """Test async write and read bytes operations."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    test_data = b"async test data content"

    await store.write_bytes_async("async_test_file.bin", test_data)
    result = await store.read_bytes_async("async_test_file.bin")

    assert result == test_data


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_async_write_and_read_text(tmp_path: Path) -> None:
    """Test async write and read text operations."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    test_text = "async test text content\nwith multiple lines"

    await store.write_text_async("async_test_file.txt", test_text)
    result = await store.read_text_async("async_test_file.txt")

    assert result == test_text


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_async_exists(tmp_path: Path) -> None:
    """Test async exists operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    assert not await store.exists_async("async_nonexistent.txt")

    await store.write_text_async("async_existing.txt", "content")
    assert await store.exists_async("async_existing.txt")


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_async_delete(tmp_path: Path) -> None:
    """Test async delete operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    await store.write_text_async("async_to_delete.txt", "content")
    assert await store.exists_async("async_to_delete.txt")

    await store.delete_async("async_to_delete.txt")
    assert not await store.exists_async("async_to_delete.txt")


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_async_copy(tmp_path: Path) -> None:
    """Test async copy operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    original_content = "async original content"

    await store.write_text_async("async_original.txt", original_content)
    await store.copy_async("async_original.txt", "async_copied.txt")

    assert await store.exists_async("async_copied.txt")
    assert await store.read_text_async("async_copied.txt") == original_content


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_async_move(tmp_path: Path) -> None:
    """Test async move operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    original_content = "async content to move"

    await store.write_text_async("async_original.txt", original_content)
    await store.move_async("async_original.txt", "async_moved.txt")

    assert not await store.exists_async("async_original.txt")
    assert await store.exists_async("async_moved.txt")
    assert await store.read_text_async("async_moved.txt") == original_content


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_async_list_objects(tmp_path: Path) -> None:
    """Test async list_objects operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    # Create test files
    await store.write_text_async("async_file1.txt", "content1")
    await store.write_text_async("async_file2.txt", "content2")
    await store.write_text_async("async_subdir/file3.txt", "content3")

    # List all objects
    all_objects = await store.list_objects_async()
    assert any("file1.txt" in obj for obj in all_objects)
    assert any("file2.txt" in obj for obj in all_objects)
    assert any("file3.txt" in obj for obj in all_objects)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_async_get_metadata(tmp_path: Path) -> None:
    """Test async get_metadata operation."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    test_content = "async test content for metadata"

    await store.write_text_async("async_test_file.txt", test_content)
    metadata = await store.get_metadata_async("async_test_file.txt")

    assert "exists" in metadata
    assert metadata["exists"] is True


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_get_metadata_async_uses_local_store_path_resolution(tmp_path: Path) -> None:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}", base_path="data")
    fake_store = _FakeHeadStore()
    store.store = fake_store

    metadata = await store.get_metadata_async("nested/object.txt")

    assert fake_store.head_async_paths == ["nested/object.txt"]
    assert metadata["path"] == "nested/object.txt"
    assert metadata["exists"] is True


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_get_metadata_async_returns_typed_dict_keys(tmp_path: Path) -> None:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    store.store = _FakeHeadStore()

    metadata = await store.get_metadata_async("object.txt")

    assert metadata == {
        "path": "object.txt",
        "exists": True,
        "size": 12,
        "last_modified": None,
        "e_tag": "etag",
        "version": "1",
        "custom_metadata": {"owner": "sqlspec"},
    }


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_get_metadata_async_returns_absent_result_for_missing_object(tmp_path: Path) -> None:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    store.store = _FakeHeadStore(missing=True)

    metadata = await store.get_metadata_async("missing.txt")

    assert metadata == {"path": "missing.txt", "exists": False}


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_get_metadata_sync_and_async_result_keys_match(tmp_path: Path) -> None:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    metadata = {"path": "object.txt", "size": 42, "last_modified": None, "e_tag": "same", "version": "7"}
    store = ObStoreBackend(f"file://{tmp_path}")
    store.store = _FakeHeadStore(metadata)

    sync_metadata = store.get_metadata_sync("object.txt")
    async_metadata = await store.get_metadata_async("object.txt")

    assert sync_metadata == async_metadata


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
async def test_async_write_and_read_arrow(tmp_path: Path) -> None:
    """Test async write and read Arrow table operations."""
    import pyarrow as pa

    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    # Create test Arrow table
    data: dict[str, Any] = {
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "David"],
        "score": [95.5, 87.0, 92.3, 89.7],
    }
    table = pa.table(data)

    await store.write_arrow_async("async_test_data.parquet", table)
    result = await store.read_arrow_async("async_test_data.parquet")

    assert result.equals(table)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_read_arrow_async_resolves_cloud_base_path_once(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.storage.backends import obstore as obstore_module
    from sqlspec.storage.backends.obstore import ObStoreBackend

    expected_table = object()
    store = ObStoreBackend("memory://", base_path="mybase")
    fake_store = _FakeStore()
    store.store = fake_store
    monkeypatch.setattr(obstore_module, "import_pyarrow_parquet", lambda: _FakeParquet(expected_table))

    result = await store.read_arrow_async("key")

    assert result is expected_table
    assert fake_store.get_paths == ["mybase/key"]


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
async def test_write_arrow_async_resolves_cloud_base_path_once() -> None:
    import pyarrow as pa

    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend("memory://", base_path="mybase")
    table = pa.table({"id": [1, 2, 3]})

    await store.write_arrow_async("key", table)

    assert store.list_objects_sync() == ["mybase/key"]
    assert store.read_arrow_sync("key").equals(table)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_stream_arrow_async_resolves_cloud_base_path_once(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend("memory://", base_path="mybase")
    seen_patterns: list[str] = []

    def fake_stream_arrow_sync(self: ObStoreBackend, pattern: str, **kwargs: Any) -> Iterator[Any]:
        _ = self
        _ = kwargs
        seen_patterns.append(pattern)
        return iter(())

    monkeypatch.setattr(ObStoreBackend, "stream_arrow_sync", fake_stream_arrow_sync)

    _ = [batch async for batch in store.stream_arrow_async("*.parquet")]

    assert seen_patterns == ["*.parquet"]


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
async def test_async_stream_arrow(tmp_path: Path) -> None:
    """Test async stream Arrow record batches."""
    import pyarrow as pa

    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    # Create test Arrow table
    data: dict[str, Any] = {"id": [1, 2, 3, 4, 5, 6], "value": ["a", "b", "c", "d", "e", "f"]}
    table = pa.table(data)

    await store.write_arrow_async("async_stream_test.parquet", table)

    # Stream record batches
    batches = [batch async for batch in store.stream_arrow_async("async_stream_test.parquet")]

    assert len(batches) > 0

    # Verify we can read the data
    reconstructed = pa.Table.from_batches(batches)
    assert reconstructed.equals(table)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_async_sign_raises_not_implemented_for_local_files(tmp_path: Path) -> None:
    """Test sign_async raises NotImplementedError for local file protocol."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    await store.write_text_async("async_test.txt", "content")

    # Local file protocol does not support URL signing
    with pytest.raises(NotImplementedError, match="URL signing is not supported for protocol 'file'"):
        await store.sign_async("async_test.txt")


def test_obstore_operations_without_obstore() -> None:
    """Test operations raise proper error without obstore."""
    if OBSTORE_INSTALLED:
        pytest.skip("obstore is installed")

    with pytest.raises(MissingDependencyError, match="obstore"):
        ObStoreBackend("file:///tmp")  # type: ignore


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_arrow_operations_without_pyarrow(tmp_path: Path) -> None:
    """Test Arrow operations raise proper error without PyArrow."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    if PYARROW_INSTALLED:
        pytest.skip("PyArrow is installed")

    store = ObStoreBackend(f"file://{tmp_path}")

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        store.read_arrow_sync("test.parquet")

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        store.write_arrow_sync("test.parquet", None)  # type: ignore

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        list(store.stream_arrow_sync("*.parquet"))


# Tests for base_path fixes (Issue #336)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_file_uri_with_relative_base_path(tmp_path: Path) -> None:
    """Test that file:// URI + relative base_path combines correctly."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    subdir = "data/uploads"
    store = ObStoreBackend(f"file://{tmp_path}", base_path=subdir)

    # Verify internal state - paths should be combined
    expected_root = str(tmp_path / subdir)
    assert store._local_store_root == expected_root

    # Write and read back
    store.write_bytes_sync("test.txt", b"hello")
    assert store.read_bytes_sync("test.txt") == b"hello"

    # Verify file is in correct physical location
    assert (tmp_path / subdir / "test.txt").exists()
    assert (tmp_path / subdir / "test.txt").read_bytes() == b"hello"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_file_uri_with_absolute_base_path_override(tmp_path: Path) -> None:
    """Test that absolute base_path overrides URI path (backward compatible)."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    override_path = tmp_path / "override"
    override_path.mkdir()

    # Use an ignored URI path but absolute base_path
    store = ObStoreBackend(f"file://{tmp_path}/ignored", base_path=str(override_path))

    # Should use the absolute base_path directly (Path division behavior)
    assert store._local_store_root == str(override_path)

    # Operations should work in override location
    store.write_bytes_sync("test.txt", b"override content")
    assert (override_path / "test.txt").read_bytes() == b"override content"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_file_uri_with_nested_base_path(tmp_path: Path) -> None:
    """Test that deeply nested base_path works correctly."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    nested_path = "a/b/c/d/e"
    store = ObStoreBackend(f"file://{tmp_path}", base_path=nested_path)

    # Verify paths are combined correctly
    expected_root = str(tmp_path / nested_path)
    assert store._local_store_root == expected_root

    # Write and verify
    store.write_bytes_sync("deep.txt", b"deep content")
    assert (tmp_path / nested_path / "deep.txt").exists()
    assert store.read_bytes_sync("deep.txt") == b"deep content"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_stream_read_async_with_base_path(tmp_path: Path) -> None:
    """Test that stream_read_async works after write with base_path."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}", base_path="workspaces")

    # Write data
    test_data = b"streaming test data" * 100
    await store.write_bytes_async("stream.bin", test_data)

    # Read back via streaming
    chunks = [chunk async for chunk in await store.stream_read_async("stream.bin")]

    assert b"".join(chunks) == test_data


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_full_workflow_with_base_path(tmp_path: Path) -> None:
    """Test complete workflow: write, list, read, delete with base_path."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}", base_path="app/data")

    # Write files
    await store.write_bytes_async("file1.txt", b"content1")
    await store.write_bytes_async("subdir/file2.txt", b"content2")

    # List files
    files = await store.list_objects_async()
    assert any("file1.txt" in f for f in files)
    assert any("file2.txt" in f for f in files)

    # Read files
    assert await store.read_bytes_async("file1.txt") == b"content1"
    assert await store.read_bytes_async("subdir/file2.txt") == b"content2"

    # Verify physical location
    assert (tmp_path / "app/data/file1.txt").exists()
    assert (tmp_path / "app/data/subdir/file2.txt").exists()

    # Delete and verify
    await store.delete_async("file1.txt")
    assert not await store.exists_async("file1.txt")
    assert not (tmp_path / "app/data/file1.txt").exists()


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_stream_read_async_does_not_block_event_loop(tmp_path: Path) -> None:
    """Test that stream_read_async doesn't block other async tasks."""

    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    # Write a reasonably sized file
    test_data = b"x" * (1024 * 1024)  # 1MB
    await store.write_bytes_async("large_file.bin", test_data)

    # Track if concurrent task executed
    concurrent_task_executed = False

    async def concurrent_task() -> None:
        nonlocal concurrent_task_executed
        await asyncio.sleep(0.001)  # Small delay
        concurrent_task_executed = True

    async def stream_and_check() -> bytes:
        # Start concurrent task
        task = asyncio.create_task(concurrent_task())

        # Stream the file
        chunks = [chunk async for chunk in await store.stream_read_async("large_file.bin", chunk_size=8192)]

        await task
        return b"".join(chunks)

    result = await stream_and_check()

    assert result == test_data
    assert concurrent_task_executed, "Concurrent task should have executed during streaming"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_stream_read_async_respects_chunk_size(tmp_path: Path) -> None:
    """Test that stream_read_async respects the chunk_size parameter."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    # Write test data
    test_data = b"a" * 1000
    await store.write_bytes_async("chunked.bin", test_data)

    # Stream with specific chunk size
    chunk_size = 100
    chunks = [chunk async for chunk in await store.stream_read_async("chunked.bin", chunk_size=chunk_size)]

    # All chunks except possibly the last should be exactly chunk_size
    for chunk in chunks[:-1]:
        assert len(chunk) == chunk_size

    # Total data should match
    assert b"".join(chunks) == test_data


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_write_arrow_sync_forwards_row_group_size() -> None:
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend("memory://")
    store.write_arrow_sync("key", pa.table({"id": list(range(20))}), row_group_size=5)

    assert pq.ParquetFile(io.BytesIO(store.read_bytes_sync("key"))).num_row_groups == 4


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
def test_write_arrow_sync_failure_publishes_nothing() -> None:
    import pyarrow as pa

    from sqlspec.exceptions import StorageOperationFailedError
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend("memory://")
    unwritable = pa.table({
        "u": pa.UnionArray.from_sparse(pa.array([0, 1], pa.int8()), [pa.array([1, 2]), pa.array(["a", "b"])])
    })

    with pytest.raises(StorageOperationFailedError):
        store.write_arrow_sync("partial", unwritable)

    assert store.list_objects_sync() == []


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
@pytest.mark.parametrize("use_file_uri", [False, True])
def test_write_sync_overwrite_false_creates_and_rejects_existing(tmp_path: Path, use_file_uri: bool) -> None:
    """Verify write_bytes_sync and write_text_sync honor overwrite=False on memory and file stores."""
    uri = f"file://{tmp_path}" if use_file_uri else "memory://"
    store = ObStoreBackend(uri)

    store.write_bytes_sync("exclusive.bin", b"first", overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        store.write_bytes_sync("exclusive.bin", b"second", overwrite=False)
    assert store.read_bytes_sync("exclusive.bin") == b"first"

    store.write_text_sync("exclusive.txt", "first", overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        store.write_text_sync("exclusive.txt", "second", overwrite=False)
    assert store.read_text_sync("exclusive.txt") == "first"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
@pytest.mark.parametrize("use_file_uri", [False, True])
async def test_write_async_overwrite_false_creates_and_rejects_existing(tmp_path: Path, use_file_uri: bool) -> None:
    """Verify write_bytes_async and write_text_async honor overwrite=False on memory and file stores."""
    uri = f"file://{tmp_path}" if use_file_uri else "memory://"
    store = ObStoreBackend(uri)

    await store.write_bytes_async("exclusive.bin", b"first", overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        await store.write_bytes_async("exclusive.bin", b"second", overwrite=False)
    assert await store.read_bytes_async("exclusive.bin") == b"first"

    await store.write_text_async("exclusive.txt", "first", overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        await store.write_text_async("exclusive.txt", "second", overwrite=False)
    assert await store.read_text_async("exclusive.txt") == "first"


class _FakePutStore:
    def __init__(self) -> None:
        self.sync_calls: list[tuple[str, bytes, dict[str, Any]]] = []
        self.async_calls: list[tuple[str, bytes, dict[str, Any]]] = []

    def put(self, path: str, data: bytes, **kwargs: Any) -> None:
        self.sync_calls.append((path, data, kwargs))

    async def put_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        self.async_calls.append((path, data, kwargs))


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_write_bytes_forwards_valid_options_and_validates_kwargs() -> None:
    """Verify supported obstore put options are forwarded and invalid/conflicting kwargs fail."""
    store = ObStoreBackend("memory://")
    fake_store = _FakePutStore()
    store.store = fake_store

    store.write_bytes_sync("obj1.bin", b"a", overwrite=False, attributes={"Cache-Control": "no-cache"})
    await store.write_bytes_async("obj2.bin", b"b", tags={"env": "test"}, use_multipart=False)

    assert fake_store.sync_calls == [
        ("obj1.bin", b"a", {"attributes": {"Cache-Control": "no-cache"}, "mode": "create"})
    ]
    assert fake_store.async_calls == [("obj2.bin", b"b", {"tags": {"env": "test"}, "use_multipart": False})]

    with pytest.raises(TypeError, match="Unsupported write keyword argument"):
        store.write_bytes_sync("obj.bin", b"a", invalid_kwarg=True)

    with pytest.raises(TypeError, match="Unsupported write keyword argument"):
        await store.write_bytes_async("obj.bin", b"a", invalid_kwarg=True)

    with pytest.raises(ValueError, match="Conflicting write options"):
        store.write_bytes_sync("obj.bin", b"a", overwrite=False, mode="overwrite")

    with pytest.raises(ValueError, match="Conflicting write options"):
        await store.write_bytes_async("obj.bin", b"a", overwrite=False, mode="overwrite")


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_direct_stream_write_protocol_and_capabilities() -> None:
    """ObStoreBackend satisfies DirectStreamWriteProtocol and reports format support."""
    store = ObStoreBackend("memory://")
    assert supports_direct_stream_write(store) is True

    supported_formats: tuple[Any, ...] = ("parquet", "arrow-ipc", "csv")
    for fmt in supported_formats:
        assert store.supports_direct_arrow_write(fmt, overwrite=True) is True
        assert store.supports_direct_arrow_write(fmt, overwrite=False) is False

    unsupported_formats: tuple[Any, ...] = ("json", "jsonl", "unsupported")
    for fmt in unsupported_formats:
        assert store.supports_direct_arrow_write(fmt, overwrite=True) is False
        assert store.supports_direct_arrow_write(fmt, overwrite=False) is False


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
@pytest.mark.parametrize("use_file_uri", [False, True])
async def test_open_write_stream_sync_and_async_round_trip(tmp_path: Path, use_file_uri: bool) -> None:
    """Sync and async write streams round trip bytes and manage closed state."""
    uri = f"file://{tmp_path}" if use_file_uri else "memory://"
    store = ObStoreBackend(uri)
    extra_kwargs = {"attributes": {"Content-Type": "application/octet-stream"}} if not use_file_uri else {}

    with store.open_write_stream_sync(
        "sync_stream.bin",
        chunk_size=1024,
        **extra_kwargs,
    ) as sync_sink:
        assert sync_sink.closed is False
        sync_sink.write(bytearray(b"part-1"))
        sync_sink.write(memoryview(b"-part-2"))

    assert sync_sink.closed is True
    assert store.read_bytes_sync("sync_stream.bin") == b"part-1-part-2"

    async with store.open_write_stream_async(
        "async_stream.bin",
        chunk_size=1024,
    ) as async_sink:
        assert async_sink.closed is False
        assert isinstance(async_sink.closed, bool)
        await async_sink.write(bytearray(b"async-1"))
        await async_sink.write(memoryview(b"-async-2"))

    assert async_sink.closed is True
    assert isinstance(async_sink.closed, bool)
    assert await store.read_bytes_async("async_stream.bin") == b"async-1-async-2"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
@pytest.mark.parametrize("use_file_uri", [False, True])
async def test_open_write_stream_mid_write_failure_preserves_existing_object(
    tmp_path: Path, use_file_uri: bool
) -> None:
    """Failures inside stream blocks abort uploads and preserve existing objects."""
    uri = f"file://{tmp_path}" if use_file_uri else "memory://"
    store = ObStoreBackend(uri)

    store.write_bytes_sync("existing.parquet", b"v1-intact")

    with pytest.raises(RuntimeError, match="producer boom sync"):
        with store.open_write_stream_sync("existing.parquet") as sink:
            sink.write(b"corrupted-partial")
            raise RuntimeError("producer boom sync")

    assert store.read_bytes_sync("existing.parquet") == b"v1-intact"

    with pytest.raises(RuntimeError, match="producer boom sync fresh"):
        with store.open_write_stream_sync("never_created_sync.parquet") as sink:
            sink.write(b"corrupted-partial")
            raise RuntimeError("producer boom sync fresh")

    assert store.exists_sync("never_created_sync.parquet") is False

    with pytest.raises(RuntimeError, match="producer boom async"):
        async with store.open_write_stream_async("existing.parquet") as async_sink:
            await async_sink.write(b"corrupted-partial")
            raise RuntimeError("producer boom async")

    assert await store.read_bytes_async("existing.parquet") == b"v1-intact"

    with pytest.raises(RuntimeError, match="producer boom async fresh"):
        async with store.open_write_stream_async("never_created_async.parquet") as async_sink:
            await async_sink.write(b"corrupted-partial")
            raise RuntimeError("producer boom async fresh")

    assert await store.exists_async("never_created_async.parquet") is False


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
@pytest.mark.parametrize("use_file_uri", [False, True])
async def test_write_arrow_failure_preserves_existing_object(tmp_path: Path, use_file_uri: bool) -> None:
    """Arrow write encoding failures abort without mutating or deleting existing objects."""
    import pyarrow as pa

    uri = f"file://{tmp_path}" if use_file_uri else "memory://"
    store = ObStoreBackend(uri)

    store.write_bytes_sync("existing.parquet", b"v1-intact")
    unwritable = pa.table({
        "u": pa.UnionArray.from_sparse(pa.array([0, 1], pa.int8()), [pa.array([1, 2]), pa.array(["a", "b"])])
    })

    with pytest.raises(StorageOperationFailedError):
        store.write_arrow_sync("existing.parquet", unwritable)
    assert store.read_bytes_sync("existing.parquet") == b"v1-intact"

    with pytest.raises(StorageOperationFailedError):
        await store.write_arrow_async("existing.parquet", unwritable)
    assert await store.read_bytes_async("existing.parquet") == b"v1-intact"


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
@pytest.mark.parametrize("use_file_uri", [False, True])
async def test_open_write_stream_async_cancelled_preserves_existing_object(tmp_path: Path, use_file_uri: bool) -> None:
    """Task cancellation mid-stream aborts upload and preserves existing objects."""
    uri = f"file://{tmp_path}" if use_file_uri else "memory://"
    store = ObStoreBackend(uri)

    store.write_bytes_sync("existing.parquet", b"v1-intact")

    started = asyncio.Event()

    async def write_existing() -> None:
        async with store.open_write_stream_async("existing.parquet") as sink:
            await sink.write(b"partial-data")
            started.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(write_existing())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await store.read_bytes_async("existing.parquet") == b"v1-intact"

    fresh_started = asyncio.Event()

    async def write_fresh() -> None:
        async with store.open_write_stream_async("never_created_cancelled.parquet") as sink:
            await sink.write(b"partial-data")
            fresh_started.set()
            await asyncio.sleep(10)

    fresh_task = asyncio.create_task(write_fresh())
    await fresh_started.wait()
    fresh_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await fresh_task

    assert await store.exists_async("never_created_cancelled.parquet") is False


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_async_obstore_write_stream_cancel_during_commit() -> None:
    """Cancellation during commit completes upload before propagating CancelledError."""
    loop = asyncio.get_running_loop()
    commit_entered = asyncio.Event()
    commit_gate = loop.create_future()
    commit_finished = False

    class _SpyAsyncWriter:
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            nonlocal commit_finished
            if exc_type is None:
                commit_entered.set()
                await commit_gate
                commit_finished = True

        def write(self, data: bytes) -> int:
            return len(data)

    stream = _AsyncObstoreWriteStream(
        None,
        "dummy.parquet",
        {},
        backend_type="obstore",
        log_fields=None,
    )
    stream._entered = _SpyAsyncWriter()

    async def do_exit() -> None:
        await stream.__aexit__(None, None, None)

    task = asyncio.create_task(do_exit())
    await commit_entered.wait()
    task.cancel()
    await asyncio.sleep(0.01)
    task.cancel()
    assert commit_finished is False

    commit_gate.set_result(None)
    with pytest.raises(asyncio.CancelledError):
        await task

    assert commit_finished is True


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
@pytest.mark.parametrize("use_file_uri", [False, True])
async def test_write_arrow_multi_format_and_snappy_and_single_log(
    tmp_path: Path, use_file_uri: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """write_arrow_* supports parquet/ipc/csv, defaults to snappy, logs once, and supports overwrite=False."""
    import io
    from decimal import Decimal

    import pyarrow as pa
    import pyarrow.parquet as pq

    import sqlspec.storage.backends.obstore as obstore_module

    uri = f"file://{tmp_path}" if use_file_uri else "memory://"
    store = ObStoreBackend(uri)

    table = pa.table({
        "id": [1, 2],
        "amt": pa.array([Decimal("12.34"), Decimal("56.78")], type=pa.decimal64(10, 2)),
    })

    log_events: list[dict[str, Any]] = []

    def fake_log_storage_event(event_name: str, **kwargs: Any) -> None:
        if event_name == "storage.write":
            log_events.append(dict(kwargs))

    monkeypatch.setattr(obstore_module, "log_storage_event", fake_log_storage_event)

    log_events.clear()
    store.write_arrow_sync("test_snappy.parquet", table)
    assert len(log_events) == 1
    assert log_events[0]["operation"] == "write_arrow"
    parquet_bytes = store.read_bytes_sync("test_snappy.parquet")
    meta = pq.ParquetFile(io.BytesIO(parquet_bytes)).metadata
    assert meta.row_group(0).column(0).compression == "SNAPPY"

    log_events.clear()
    await store.write_arrow_async("test_snappy_async.parquet", table)
    assert len(log_events) == 1
    assert log_events[0]["operation"] == "write_arrow"

    store.write_arrow_sync("test.ipc", table, format_hint="arrow-ipc")
    from sqlspec.storage._arrow_payload import decode_arrow_payload

    assert decode_arrow_payload(store.read_bytes_sync("test.ipc"), "arrow-ipc").num_rows == 2

    store.write_arrow_sync("test.csv", table, format_hint="csv")
    csv_bytes = store.read_bytes_sync("test.csv")
    assert b"12.34" in csv_bytes

    store.write_arrow_sync("once_sync.parquet", table, overwrite=False, buffer_size=65536)
    with pytest.raises(FileExistsInStorageError):
        store.write_arrow_sync("once_sync.parquet", table, overwrite=False, buffer_size=65536)

    await store.write_arrow_async("once_async.parquet", table, overwrite=False, buffer_size=65536)
    with pytest.raises(FileExistsInStorageError):
        await store.write_arrow_async("once_async.parquet", table, overwrite=False, buffer_size=65536)


@pytest.mark.skipif(not OBSTORE_INSTALLED or not PYARROW_INSTALLED, reason="obstore or PyArrow missing")
async def test_pre_execution_validation_errors(tmp_path: Path) -> None:
    """Pre-execution validation errors propagate without StorageOperationFailedError wrapping."""
    import pyarrow as pa

    store = ObStoreBackend(f"file://{tmp_path}")
    table = pa.table({"a": [1]})

    with pytest.raises(StorageCapabilityError):
        store.open_write_stream_sync("k", overwrite=False)

    with pytest.raises(StorageCapabilityError):
        store.open_write_stream_async("k", mode="create")

    with pytest.raises(ValueError, match="Conflicting write options"):
        store.open_write_stream_sync("k", overwrite=False, mode="overwrite")

    with pytest.raises(ValueError, match="Conflicting buffer sizes"):
        store.open_write_stream_sync("k", buffer_size=1024, chunk_size=2048)

    with pytest.raises(ValueError, match="buffer_size must be positive"):
        store.open_write_stream_sync("k", buffer_size=0)

    with pytest.raises(ValueError, match="chunk_size must be positive"):
        store.open_write_stream_sync("k", chunk_size=0)

    with pytest.raises(TypeError, match="Unsupported write stream keyword argument"):
        store.open_write_stream_sync("k", bogus=1)

    with pytest.raises(StoragePathTraversalError):
        store.open_write_stream_sync("../outside.bin")

    json_format: Any = "json"
    with pytest.raises(StorageCapabilityError):
        store.write_arrow_sync("k", table, format_hint=json_format)

    with pytest.raises(TypeError, match="Unsupported write option"):
        await store.write_arrow_async("k", table, bogus=1)

    with pytest.raises(ValueError, match="Conflicting buffer sizes"):
        store.write_arrow_sync("k", table, overwrite=False, buffer_size=1024, chunk_size=2048)

    with pytest.raises(ValueError, match="buffer_size must be positive"):
        store.write_arrow_sync("k", table, overwrite=False, buffer_size=0)

    with pytest.raises(ValueError, match="chunk_size must be positive"):
        store.write_arrow_sync("k", table, overwrite=False, chunk_size=0)

