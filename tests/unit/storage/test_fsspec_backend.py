"""Unit tests for FSSpecBackend."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.exceptions import MissingDependencyError
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
