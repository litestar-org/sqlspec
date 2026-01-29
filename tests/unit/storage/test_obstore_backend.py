# pyright: reportPrivateUsage=false
"""Unit tests for ObStoreBackend."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import OBSTORE_INSTALLED, PYARROW_INSTALLED

if OBSTORE_INSTALLED:
    from sqlspec.storage.backends.obstore import ObStoreBackend


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
    import asyncio

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
