"""Unit tests for FSSpecBackend."""

import gzip
from pathlib import Path
from typing import Any, Self

import pytest

from sqlspec.exceptions import FileExistsInStorageError, MissingDependencyError
from sqlspec.typing import FSSPEC_INSTALLED, PYARROW_INSTALLED

if FSSPEC_INSTALLED:
    from sqlspec.storage.backends.fsspec import FSSpecBackend


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_init_with_filesystem_string() -> None:
    """Test initialization with filesystem string."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file")
    assert store.protocol == "file"


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_init_with_uri() -> None:
    """Test initialization with URI."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file:///tmp")
    assert store.protocol == "file"


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_from_config() -> None:
    """Test from_config class method."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    config = {"protocol": "file", "base_path": "/tmp/test", "fs_config": {}}
    store = FSSpecBackend.from_config(config)
    assert store.protocol == "file"
    assert store.base_path == "/tmp/test"


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_write_and_read_bytes(tmp_path: Path) -> None:
    """Test write and read bytes operations."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    test_data = b"test data content"

    store.write_bytes_sync("test_file.bin", test_data)
    result = store.read_bytes_sync("test_file.bin")

    assert result == test_data


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_write_and_read_text(tmp_path: Path) -> None:
    """Test write and read text operations."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    test_text = "test text content\nwith multiple lines"

    store.write_text_sync("test_file.txt", test_text)
    result = store.read_text_sync("test_file.txt")

    assert result == test_text


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_exists(tmp_path: Path) -> None:
    """Test exists operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    assert not store.exists_sync("nonexistent.txt")

    store.write_text_sync("existing.txt", "content")
    assert store.exists_sync("existing.txt")


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_delete(tmp_path: Path) -> None:
    """Test delete operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    store.write_text_sync("to_delete.txt", "content")
    assert store.exists_sync("to_delete.txt")

    store.delete_sync("to_delete.txt")
    assert not store.exists_sync("to_delete.txt")


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_copy(tmp_path: Path) -> None:
    """Test copy operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    original_content = "original content"

    store.write_text_sync("original.txt", original_content)
    store.copy_sync("original.txt", "copied.txt")

    assert store.exists_sync("copied.txt")
    assert store.read_text_sync("copied.txt") == original_content


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_move(tmp_path: Path) -> None:
    """Test move operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    original_content = "content to move"

    store.write_text_sync("original.txt", original_content)
    store.move_sync("original.txt", "moved.txt")

    assert not store.exists_sync("original.txt")
    assert store.exists_sync("moved.txt")
    assert store.read_text_sync("moved.txt") == original_content


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_list_objects(tmp_path: Path) -> None:
    """Test list_objects operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    # Create test files
    store.write_text_sync("file1.txt", "content1")
    store.write_text_sync("file2.txt", "content2")
    store.write_text_sync("subdir/file3.txt", "content3")

    # List all objects
    all_objects = store.list_objects_sync()
    assert any("file1.txt" in obj for obj in all_objects)
    assert any("file2.txt" in obj for obj in all_objects)
    assert any("file3.txt" in obj for obj in all_objects)


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_glob(tmp_path: Path) -> None:
    """Test glob pattern matching."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    # Create test files
    store.write_text_sync("test1.sql", "SELECT 1")
    store.write_text_sync("test2.sql", "SELECT 2")
    store.write_text_sync("config.json", "{}")

    # Test glob patterns
    sql_files = store.glob_sync("*.sql")
    assert any("test1.sql" in obj for obj in sql_files)
    assert any("test2.sql" in obj for obj in sql_files)
    assert not any("config.json" in obj for obj in sql_files)


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_get_metadata(tmp_path: Path) -> None:
    """Test get_metadata operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    test_content = "test content for metadata"

    store.write_text_sync("test_file.txt", test_content)
    metadata = store.get_metadata_sync("test_file.txt")

    assert "size" in metadata
    assert "exists" in metadata
    assert metadata["exists"] is True


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_is_object_and_is_path(tmp_path: Path) -> None:
    """Test is_object and is_path operations."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    store.write_text_sync("file.txt", "content")
    (tmp_path / "subdir").mkdir()

    assert store.is_object_sync("file.txt")
    assert not store.is_object_sync("subdir")
    assert not store.is_path_sync("file.txt")
    assert store.is_path_sync("subdir")


@pytest.mark.skipif(not FSSPEC_INSTALLED or not PYARROW_INSTALLED, reason="fsspec or PyArrow missing")
def test_write_and_read_arrow(tmp_path: Path) -> None:
    """Test write and read Arrow table operations."""
    import pyarrow as pa

    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    # Create test Arrow table
    data: dict[str, Any] = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "score": [95.5, 87.0, 92.3]}
    table = pa.table(data)

    store.write_arrow_sync("test_data.parquet", table)
    result = store.read_arrow_sync("test_data.parquet")

    assert result.equals(table)


@pytest.mark.skipif(not FSSPEC_INSTALLED or not PYARROW_INSTALLED, reason="fsspec or PyArrow missing")
def test_stream_arrow(tmp_path: Path) -> None:
    """Test stream Arrow record batches."""
    import pyarrow as pa

    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

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


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_sign_returns_uri(tmp_path: Path) -> None:
    """Test sign_sync raises NotImplementedError for fsspec backends."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    store.write_text_sync("test.txt", "content")

    # FSSpec backends do not support URL signing
    assert store.supports_signing is False

    with pytest.raises(NotImplementedError, match="URL signing is not supported for fsspec backend"):
        store.sign_sync("test.txt")


def test_fsspec_not_installed() -> None:
    """Test error when fsspec is not installed."""
    if FSSPEC_INSTALLED:
        pytest.skip("fsspec is installed")

    with pytest.raises(MissingDependencyError, match="fsspec"):
        from sqlspec.storage.backends.fsspec import FSSpecBackend

        FSSpecBackend("file")


# Async tests


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_async_write_and_read_bytes(tmp_path: Path) -> None:
    """Test async write and read bytes operations."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    test_data = b"async test data content"

    await store.write_bytes_async("async_test_file.bin", test_data)
    result = await store.read_bytes_async("async_test_file.bin")

    assert result == test_data


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_async_write_and_read_text(tmp_path: Path) -> None:
    """Test async write and read text operations."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    test_text = "async test text content\nwith multiple lines"

    await store.write_text_async("async_test_file.txt", test_text)
    result = await store.read_text_async("async_test_file.txt")

    assert result == test_text


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_async_exists(tmp_path: Path) -> None:
    """Test async exists operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    assert not await store.exists_async("async_nonexistent.txt")

    await store.write_text_async("async_existing.txt", "content")
    assert await store.exists_async("async_existing.txt")


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_async_delete(tmp_path: Path) -> None:
    """Test async delete operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    await store.write_text_async("async_to_delete.txt", "content")
    assert await store.exists_async("async_to_delete.txt")

    await store.delete_async("async_to_delete.txt")
    assert not await store.exists_async("async_to_delete.txt")


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_async_copy(tmp_path: Path) -> None:
    """Test async copy operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    original_content = "async original content"

    await store.write_text_async("async_original.txt", original_content)
    await store.copy_async("async_original.txt", "async_copied.txt")

    assert await store.exists_async("async_copied.txt")
    assert await store.read_text_async("async_copied.txt") == original_content


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_async_move(tmp_path: Path) -> None:
    """Test async move operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    original_content = "async content to move"

    await store.write_text_async("async_original.txt", original_content)
    await store.move_async("async_original.txt", "async_moved.txt")

    assert not await store.exists_async("async_original.txt")
    assert await store.exists_async("async_moved.txt")
    assert await store.read_text_async("async_moved.txt") == original_content


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_async_list_objects(tmp_path: Path) -> None:
    """Test async list_objects operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    # Create test files
    await store.write_text_async("async_file1.txt", "content1")
    await store.write_text_async("async_file2.txt", "content2")
    await store.write_text_async("async_subdir/file3.txt", "content3")

    # List all objects
    all_objects = await store.list_objects_async()
    assert any("file1.txt" in obj for obj in all_objects)
    assert any("file2.txt" in obj for obj in all_objects)
    assert any("file3.txt" in obj for obj in all_objects)


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_async_get_metadata(tmp_path: Path) -> None:
    """Test async get_metadata operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    test_content = "async test content for metadata"

    await store.write_text_async("async_test_file.txt", test_content)
    metadata = await store.get_metadata_async("async_test_file.txt")

    assert "size" in metadata
    assert "exists" in metadata
    assert metadata["exists"] is True


@pytest.mark.skipif(not FSSPEC_INSTALLED or not PYARROW_INSTALLED, reason="fsspec or PyArrow missing")
async def test_async_write_and_read_arrow(tmp_path: Path) -> None:
    """Test async write and read Arrow table operations."""
    import pyarrow as pa

    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

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


@pytest.mark.skipif(not FSSPEC_INSTALLED or not PYARROW_INSTALLED, reason="fsspec or PyArrow missing")
async def test_async_stream_arrow(tmp_path: Path) -> None:
    """Test async stream Arrow record batches."""
    import pyarrow as pa

    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

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


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_async_sign_raises_not_implemented(tmp_path: Path) -> None:
    """Test async sign_async raises NotImplementedError for fsspec backends."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    await store.write_text_async("async_test.txt", "content")

    with pytest.raises(NotImplementedError, match="URL signing is not supported for fsspec backend"):
        await store.sign_async("async_test.txt")


def test_fsspec_operations_without_fsspec() -> None:
    """Test operations raise proper error without fsspec."""
    if FSSPEC_INSTALLED:
        pytest.skip("fsspec is installed")

    with pytest.raises(MissingDependencyError, match="fsspec"):
        FSSpecBackend("file")  # type: ignore


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_arrow_operations_without_pyarrow(tmp_path: Path) -> None:
    """Test Arrow operations raise proper error without PyArrow."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    if PYARROW_INSTALLED:
        pytest.skip("PyArrow is installed")

    store = FSSpecBackend("file", base_path=str(tmp_path))

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        store.read_arrow_sync("test.parquet")

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        store.write_arrow_sync("test.parquet", None)  # type: ignore

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        list(store.stream_arrow_sync("*.parquet"))


# Tests for file:// URI auto-derive base_path fix


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_file_uri_auto_derives_base_path(tmp_path: Path) -> None:
    """Test that file:// URI automatically derives base_path."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend(f"file://{tmp_path}")

    # base_path should be derived from URI (absolute path preserved)
    assert store.base_path == str(tmp_path)


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_file_uri_with_explicit_base_path_combination(tmp_path: Path) -> None:
    """Test that file:// URI + explicit base_path are combined."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend(f"file://{tmp_path}", base_path="subdir")

    # base_path should combine URI path with explicit base_path (absolute path preserved)
    expected_base_path = f"{tmp_path}/subdir"
    assert store.base_path == expected_base_path


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_file_uri_base_path_full_workflow(tmp_path: Path) -> None:
    """Test full read/write workflow with file:// URI auto-derived base_path."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    # Create subdirectory for the test
    subdir = tmp_path / "data"
    subdir.mkdir()

    store = FSSpecBackend(f"file://{tmp_path}", base_path="data")

    # Write and read should work correctly
    test_data = b"test content"
    store.write_bytes_sync("test.bin", test_data)
    result = store.read_bytes_sync("test.bin")

    assert result == test_data
    # Verify file is in the correct location
    assert (subdir / "test.bin").exists()


# Tests for async streaming non-blocking fix


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_stream_read_async_does_not_block_event_loop(tmp_path: Path) -> None:
    """Test that stream_read_async doesn't block the event loop."""
    import asyncio

    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    # Write a reasonably sized file
    test_data = b"x" * 100_000
    store.write_bytes_sync("large_file.bin", test_data)

    # Track if concurrent task runs during streaming
    concurrent_task_ran = False

    async def concurrent_task() -> None:
        nonlocal concurrent_task_ran
        await asyncio.sleep(0)
        concurrent_task_ran = True

    async def stream_file() -> bytes:
        chunks = [chunk async for chunk in await store.stream_read_async("large_file.bin", chunk_size=1000)]
        return b"".join(chunks)

    # Run streaming and concurrent task together
    result, _ = await asyncio.gather(stream_file(), concurrent_task())

    assert result == test_data
    assert concurrent_task_ran, "Concurrent task should have run during streaming"


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_stream_read_async_respects_chunk_size(tmp_path: Path) -> None:
    """Test that stream_read_async respects the chunk_size parameter."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    test_data = b"x" * 10_000
    store.write_bytes_sync("chunked_file.bin", test_data)

    chunk_size = 1000
    chunks = [chunk async for chunk in await store.stream_read_async("chunked_file.bin", chunk_size=chunk_size)]

    # All chunks except possibly the last should be exactly chunk_size
    for chunk in chunks[:-1]:
        assert len(chunk) == chunk_size

    # Reassemble and verify
    assert b"".join(chunks) == test_data


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_stream_read_async_with_file_uri_base_path(tmp_path: Path) -> None:
    """Test async streaming works correctly with file:// URI base_path."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    subdir = tmp_path / "data"
    subdir.mkdir()

    store = FSSpecBackend(f"file://{tmp_path}", base_path="data")

    test_data = b"streaming test data"
    store.write_bytes_sync("stream_test.bin", test_data)

    chunks = [chunk async for chunk in await store.stream_read_async("stream_test.bin")]

    assert b"".join(chunks) == test_data


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
@pytest.mark.parametrize("protocol_kind", ["file", "memory"])
def test_write_sync_overwrite_false_creates_and_rejects_existing(tmp_path: Path, protocol_kind: str) -> None:
    """Verify write_bytes_sync and write_text_sync honor overwrite=False on file and memory protocols."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    base = str(tmp_path) if protocol_kind == "file" else f"/fsspec_sync_{tmp_path.name}"
    store = FSSpecBackend(protocol_kind, base_path=base)

    store.write_bytes_sync("exclusive.bin", b"first", overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        store.write_bytes_sync("exclusive.bin", b"second", overwrite=False)
    assert store.read_bytes_sync("exclusive.bin") == b"first"

    store.write_text_sync("exclusive.txt", "first", overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        store.write_text_sync("exclusive.txt", "second", overwrite=False)
    assert store.read_text_sync("exclusive.txt") == "first"


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
@pytest.mark.parametrize("protocol_kind", ["file", "memory"])
async def test_write_async_overwrite_false_creates_and_rejects_existing(tmp_path: Path, protocol_kind: str) -> None:
    """Verify write_bytes_async and write_text_async honor overwrite=False on file and memory protocols."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    base = str(tmp_path) if protocol_kind == "file" else f"/fsspec_async_{tmp_path.name}"
    store = FSSpecBackend(protocol_kind, base_path=base)

    await store.write_bytes_async("exclusive.bin", b"first", overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        await store.write_bytes_async("exclusive.bin", b"second", overwrite=False)
    assert await store.read_bytes_async("exclusive.bin") == b"first"

    await store.write_text_async("exclusive.txt", "first", overwrite=False)
    with pytest.raises(FileExistsInStorageError):
        await store.write_text_async("exclusive.txt", "second", overwrite=False)
    assert await store.read_text_async("exclusive.txt") == "first"


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_write_bytes_sync_forwards_supported_open_options(tmp_path: Path) -> None:
    """Verify supported fsspec open options such as compression are forwarded."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    store.write_bytes_sync("compressed.bin.gz", b"hello compressed", compression="gzip")

    raw_bytes = (tmp_path / "compressed.bin.gz").read_bytes()
    assert gzip.decompress(raw_bytes) == b"hello compressed"


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
@pytest.mark.parametrize("protocol_kind", ["file", "memory"])
def test_write_rejects_mode_and_unknown_kwargs_on_local_and_memory(tmp_path: Path, protocol_kind: str) -> None:
    """Verify mode and unknown kwargs raise TypeError on file and memory protocols."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    base = str(tmp_path) if protocol_kind == "file" else f"/fsspec_invalid_{tmp_path.name}"
    store = FSSpecBackend(protocol_kind, base_path=base)

    with pytest.raises(TypeError, match="Unsupported keyword argument 'mode'"):
        store.write_bytes_sync("item.bin", b"data", mode="wb")

    with pytest.raises(TypeError, match="Unsupported write keyword argument"):
        store.write_bytes_sync("item.bin", b"data", bogus_option=123)


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_write_async_rejects_invalid_kwargs_and_forwards_remote_options(tmp_path: Path) -> None:
    """Verify async writes reject invalid kwargs and remote protocols forward custom open options."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    with pytest.raises(TypeError, match="Unsupported write keyword argument"):
        await store.write_bytes_async("item.bin", b"data", bogus_option=123)

    captured: list[tuple[str, str, dict[str, Any]]] = []

    class _FakeFile:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_: Any) -> None:
            return None

        def write(self, data: bytes) -> int:
            return len(data)

    class _FakeRemoteFS:
        def open(self, path: str, mode: str = "rb", **kwargs: Any) -> _FakeFile:
            captured.append((path, mode, kwargs))
            return _FakeFile()

    remote_store = FSSpecBackend.__new__(FSSpecBackend)
    remote_store.protocol = "s3"
    remote_store.base_path = "my-bucket"
    remote_store._fs_uri = "s3://my-bucket"
    remote_store.fs = _FakeRemoteFS()

    remote_store.write_bytes_sync("obj.bin", b"payload", overwrite=False, acl="private")
    assert captured == [("my-bucket/obj.bin", "xb", {"acl": "private"})]


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_write_strategy_and_initial_capabilities(tmp_path: Path) -> None:
    """_fsspec_write_strategy maps protocols cleanly and supports_direct_arrow_write matches strategy capabilities."""
    from sqlspec.exceptions import StorageCapabilityError
    from sqlspec.storage._arrow_payload import StorageFormat
    from sqlspec.storage.backends.fsspec import FSSpecBackend, _fsspec_write_strategy
    from sqlspec.utils.type_guards import supports_direct_stream_write

    expected_map = {
        "file": "local",
        "local": "local",
        "s3": "commit",
        "s3a": "commit",
        "gs": "commit",
        "gcs": "commit",
        "az": "block",
        "abfs": "block",
        "abfss": "block",
        "memory": "staged",
        "dropbox": "staged",
    }
    for proto, expected_strategy in expected_map.items():
        assert _fsspec_write_strategy(proto) == expected_strategy

    mem_store = FSSpecBackend("memory", base_path=f"/cap_{tmp_path.name}")
    file_store = FSSpecBackend(f"file://{tmp_path}")
    assert supports_direct_stream_write(mem_store) is True
    assert supports_direct_stream_write(file_store) is True

    arrow_formats: tuple[StorageFormat, ...] = ("parquet", "arrow-ipc", "csv")
    for fmt in arrow_formats:
        assert mem_store.supports_direct_arrow_write(fmt, overwrite=True) is True
        assert mem_store.supports_direct_arrow_write(fmt, overwrite=False) is False
        assert file_store.supports_direct_arrow_write(fmt, overwrite=True) is True
        assert file_store.supports_direct_arrow_write(fmt, overwrite=False) is True

    assert mem_store.supports_direct_arrow_write("json", overwrite=True) is False
    assert file_store.supports_direct_arrow_write("json", overwrite=True) is False

    with pytest.raises(StorageCapabilityError):
        mem_store.open_write_stream_sync("x.bin", overwrite=False)
    with pytest.raises(StorageCapabilityError):
        mem_store.open_write_stream_async("x.bin", overwrite=False)
    with pytest.raises(TypeError, match="autocommit"):
        file_store.open_write_stream_sync("x.bin", autocommit=True)
    with pytest.raises(TypeError, match="autocommit"):
        file_store.open_write_stream_async("x.bin", autocommit=False)


def _fsspec_stage_entries(store: "FSSpecBackend", directory_or_prefix: str = "") -> list[str]:
    items = store.list_objects_sync(prefix=directory_or_prefix)
    return [item for item in items if ".sqlspec-stage-" in item]


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
@pytest.mark.parametrize("protocol_kind", ["file", "memory"])
async def test_fsspec_local_and_staged_streams_round_trip_abort_and_cancel(tmp_path: Path, protocol_kind: str) -> None:
    """FSSpec local and staged write streams publish atomically and preserve existing objects on abort or cancel."""
    import asyncio
    from typing import cast

    from sqlspec.storage.backends.fsspec import FSSpecBackend, _AsyncFsspecSink, _FsspecSink

    base = str(tmp_path) if protocol_kind == "file" else f"/stream_{tmp_path.name}"
    store = FSSpecBackend(protocol_kind, base_path=base)

    with store.open_write_stream_sync("nested/sync.bin") as sink:
        assert isinstance(sink, _FsspecSink)
        assert sink.write(b"a") == 1
        assert sink.write(cast("Any", memoryview(b"b"))) == 1
        sink.flush()

    assert store.read_bytes_sync("nested/sync.bin") == b"ab"
    assert _fsspec_stage_entries(store) == []

    async with store.open_write_stream_async("nested/async.bin") as async_sink:
        assert isinstance(async_sink, _AsyncFsspecSink)
        assert await async_sink.write(cast("Any", bytearray(b"a"))) == 1
        assert await async_sink.write(b"b") == 1
        await async_sink.flush()

    assert await store.read_bytes_async("nested/async.bin") == b"ab"
    assert _fsspec_stage_entries(store) == []

    store.write_bytes_sync("existing.bin", b"v1")

    with pytest.raises(RuntimeError, match="abort-sync"), store.open_write_stream_sync("existing.bin") as sink:
        sink.write(b"partial")
        raise RuntimeError("abort-sync")

    assert store.read_bytes_sync("existing.bin") == b"v1"
    assert _fsspec_stage_entries(store) == []

    with pytest.raises(RuntimeError, match="abort-async"):
        async with store.open_write_stream_async("existing.bin") as async_sink:
            await async_sink.write(b"partial-async")
            raise RuntimeError("abort-async")

    assert store.read_bytes_sync("existing.bin") == b"v1"
    assert _fsspec_stage_entries(store) == []

    started = asyncio.Event()
    park = asyncio.Event()

    async def _parked_write() -> None:
        async with store.open_write_stream_async("existing.bin") as async_sink:
            await async_sink.write(b"partial-cancel")
            started.set()
            await park.wait()

    task = asyncio.create_task(_parked_write())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert store.read_bytes_sync("existing.bin") == b"v1"
    assert _fsspec_stage_entries(store) == []


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_fsspec_sink_errors_open_errors_and_cancelled_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_FsspecSink/_AsyncFsspecSink and open errors normalize to StorageOperationFailedError; cancelled open removes stage."""
    import asyncio
    import errno
    import time

    from sqlspec.exceptions import StorageOperationFailedError
    from sqlspec.storage.backends import fsspec as fsspec_mod
    from sqlspec.storage.backends.fsspec import FSSpecBackend, _AsyncFsspecSink, _FsspecSink

    class _FailingDouble:
        closed = False

        def write(self, _data: Any) -> int:
            raise OSError(errno.ENOSPC, "full")

        def flush(self) -> None:
            raise OSError(errno.EIO, "io")

    sync_sink = _FsspecSink(_FailingDouble(), backend="fsspec", path="x")
    with pytest.raises(StorageOperationFailedError):
        sync_sink.write(b"data")
    with pytest.raises(StorageOperationFailedError):
        sync_sink.flush()

    async_sink = _AsyncFsspecSink(_FailingDouble(), backend="fsspec", path="x")
    with pytest.raises(StorageOperationFailedError):
        await async_sink.write(b"data")
    with pytest.raises(StorageOperationFailedError):
        await async_sink.flush()

    file_store = FSSpecBackend(f"file://{tmp_path}")
    file_store.write_bytes_sync("blocked", b"regular-file")
    with pytest.raises(StorageOperationFailedError), file_store.open_write_stream_sync("blocked/x.bin"):
        pass
    with pytest.raises(StorageOperationFailedError):
        async with file_store.open_write_stream_async("blocked/x.bin"):
            pass

    orig_open_stage = fsspec_mod._open_local_stage
    entered_open = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _delayed_open(destination: Path) -> Any:
        result = orig_open_stage(destination)
        loop.call_soon_threadsafe(entered_open.set)
        time.sleep(0.05)
        return result

    monkeypatch.setattr(fsspec_mod, "_open_local_stage", _delayed_open)

    async def _open_then_cancel() -> None:
        async with file_store.open_write_stream_async("cancel_open.bin") as s:
            await s.write(b"x")

    cancel_task = asyncio.create_task(_open_then_cancel())
    await entered_open.wait()
    cancel_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancel_task

    assert not file_store.exists_sync("cancel_open.bin")
    assert _fsspec_stage_entries(file_store) == []


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_fsspec_commit_and_block_strategies_and_conflict_mapping() -> None:
    """FSSpec commit (s3/gs) and block (az) strategies commit, discard on abort/error, map conflicts, and clear fs.transaction.files."""
    from collections import deque
    from collections.abc import Callable

    from sqlspec.exceptions import StorageOperationFailedError
    from sqlspec.storage._arrow_payload import StorageFormat
    from sqlspec.storage.backends.fsspec import FSSpecBackend, _forget_transaction_file

    class _TxHolder:
        def __init__(self) -> None:
            self.files: deque[Any] = deque()

    class _CommitFileDouble:
        def __init__(self, *, commit_exc: Exception | None = None) -> None:
            self.calls: list[str] = []
            self.closed = False
            self._commit_exc = commit_exc

        def write(self, data: bytes) -> int:
            self.calls.append(f"write:{len(data)}")
            return len(data)

        def close(self) -> None:
            self.calls.append("close")
            self.closed = True

        def commit(self) -> None:
            self.calls.append("commit")
            if self._commit_exc is not None:
                raise self._commit_exc

        def discard(self) -> None:
            self.calls.append("discard")

    class _CommitFSDouble:
        def __init__(self, factory: Callable[[], _CommitFileDouble]) -> None:
            self.transaction = _TxHolder()
            self._factory = factory
            self.opened: list[tuple[str, str, dict[str, Any], _CommitFileDouble]] = []

        def open(self, path: str, mode: str = "rb", **kwargs: Any) -> _CommitFileDouble:
            f = self._factory()
            if kwargs.get("autocommit") is False:
                self.transaction.files.append(f)
            self.opened.append((path, mode, kwargs, f))
            return f

    def _make_remote_store(protocol: str, fs_double: Any) -> FSSpecBackend:
        st = FSSpecBackend.__new__(FSSpecBackend)
        st.protocol = protocol
        st.base_path = "bucket"
        st._fs_uri = f"{protocol}://bucket"
        st.fs = fs_double
        return st

    s3_fs = _CommitFSDouble(_CommitFileDouble)
    s3_store = _make_remote_store("s3", s3_fs)
    formats: tuple[StorageFormat, ...] = ("parquet", "arrow-ipc", "csv")
    for fmt in formats:
        assert s3_store.supports_direct_arrow_write(fmt, overwrite=True) is True
        assert s3_store.supports_direct_arrow_write(fmt, overwrite=False) is True

    with s3_store.open_write_stream_sync("ok.parquet", overwrite=True) as sink:
        assert len(s3_fs.transaction.files) == 0
        sink.write(b"abc")
    path_0, mode_0, kw_0, file_0 = s3_fs.opened[0]
    assert path_0 == "bucket/ok.parquet"
    assert mode_0 == "wb"
    assert kw_0 == {"autocommit": False}
    assert file_0.calls == ["write:3", "close", "commit"]
    assert len(s3_fs.transaction.files) == 0

    with pytest.raises(RuntimeError, match="caller-abort"), s3_store.open_write_stream_sync("abort.parquet") as sink:
        sink.write(b"xyz")
        raise RuntimeError("caller-abort")
    file_1 = s3_fs.opened[1][3]
    assert file_1.calls == ["write:3", "discard"]
    assert file_1.closed is True
    assert len(s3_fs.transaction.files) == 0

    with pytest.raises(RuntimeError, match="async-abort"):
        async with s3_store.open_write_stream_async("abort_async.parquet", overwrite=False) as async_sink:
            await async_sink.write(b"xy")
            raise RuntimeError("async-abort")
    _, mode_2, _, file_2 = s3_fs.opened[2]
    assert mode_2 == "xb"
    assert file_2.calls == ["write:2", "discard"]
    assert file_2.closed is True
    assert len(s3_fs.transaction.files) == 0

    fail_commit_fs = _CommitFSDouble(lambda: _CommitFileDouble(commit_exc=OSError(5, "io")))
    fail_commit_store = _make_remote_store("s3", fail_commit_fs)
    with pytest.raises(StorageOperationFailedError), fail_commit_store.open_write_stream_sync("fail.parquet") as sink:
        sink.write(b"123")
    fail_file = fail_commit_fs.opened[0][3]
    assert fail_file.calls == ["write:3", "close", "commit", "discard"]
    assert fail_file.closed is True

    class _ClientErrorLike(Exception):
        def __init__(self) -> None:
            super().__init__("PreconditionFailed")
            self.response = {"Error": {"Code": "PreconditionFailed"}}

    def _s3_precondition_file() -> _CommitFileDouble:
        outer = OSError(22, "PreconditionFailed")
        outer.__cause__ = _ClientErrorLike()
        return _CommitFileDouble(commit_exc=outer)

    conflict_s3_fs = _CommitFSDouble(_s3_precondition_file)
    conflict_s3_store = _make_remote_store("s3", conflict_s3_fs)
    with (
        pytest.raises(FileExistsInStorageError),
        conflict_s3_store.open_write_stream_sync("dup.parquet", overwrite=False) as sink,
    ):
        sink.write(b"dup")
    assert "discard" in conflict_s3_fs.opened[0][3].calls
    assert conflict_s3_fs.opened[0][3].closed is True

    gcs_conflict_fs = _CommitFSDouble(lambda: _CommitFileDouble(commit_exc=FileExistsError("bucket/dup_gcs.parquet")))
    gcs_store = _make_remote_store("gs", gcs_conflict_fs)
    with pytest.raises(FileExistsInStorageError):
        async with gcs_store.open_write_stream_async("dup_gcs.parquet", overwrite=False) as async_sink:
            await async_sink.write(b"dup")
    assert "discard" in gcs_conflict_fs.opened[0][3].calls
    assert gcs_conflict_fs.opened[0][3].closed is True

    class _BlockFileDouble:
        def __init__(self) -> None:
            self.forced = False
            self.closed = False
            self.close_forced_snapshots: list[bool] = []

        def write(self, data: bytes) -> int:
            return len(data)

        def close(self) -> None:
            self.close_forced_snapshots.append(self.forced)
            self.closed = True

    class _BlockFSDouble:
        def __init__(self) -> None:
            self.opened: list[tuple[str, str, _BlockFileDouble]] = []

        def open(self, path: str, mode: str = "rb", **_kwargs: Any) -> _BlockFileDouble:
            f = _BlockFileDouble()
            self.opened.append((path, mode, f))
            return f

    az_fs = _BlockFSDouble()
    az_store = _make_remote_store("az", az_fs)
    assert az_store.supports_direct_arrow_write("parquet", overwrite=True) is True
    assert az_store.supports_direct_arrow_write("parquet", overwrite=False) is True

    with az_store.open_write_stream_sync("ok_az.parquet", overwrite=False) as sink:
        sink.write(b"123")
    assert az_fs.opened[0][1] == "xb"
    assert az_fs.opened[0][2].close_forced_snapshots == [False]

    with pytest.raises(RuntimeError, match="az-abort"), az_store.open_write_stream_sync("bad_az.parquet") as sink:
        sink.write(b"123")
        raise RuntimeError("az-abort")
    assert az_fs.opened[1][2].forced is True
    assert az_fs.opened[1][2].close_forced_snapshots == [True]

    class _FlakyTxFiles:
        def __init__(self) -> None:
            self.attempts = 0
            self.items = ["sentinel"]

        def remove(self, item: Any) -> None:
            self.attempts += 1
            if self.attempts == 1:
                raise IndexError("concurrent deque mutation")
            self.items.remove(item)

    class _FlakyTxHolder:
        def __init__(self) -> None:
            self.files = _FlakyTxFiles()

    class _FlakyTxFS:
        def __init__(self) -> None:
            self.transaction = _FlakyTxHolder()

    flaky_fs = _FlakyTxFS()
    _forget_transaction_file(flaky_fs, "sentinel")
    assert flaky_fs.transaction.files.items == []
    assert flaky_fs.transaction.files.attempts == 2
    _forget_transaction_file(flaky_fs, "missing")
