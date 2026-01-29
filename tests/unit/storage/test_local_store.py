# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for LocalStore backend."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.storage.backends.local import LocalStore
from sqlspec.typing import PYARROW_INSTALLED


def test_init_with_file_uri(tmp_path: Path) -> None:
    """Test initialization with file:// URI."""
    store = LocalStore(f"file://{tmp_path}")
    assert store.base_path == tmp_path.resolve()


def test_init_with_path_string(tmp_path: Path) -> None:
    """Test initialization with plain path string."""
    store = LocalStore(str(tmp_path))
    assert store.base_path == tmp_path.resolve()


def test_init_empty_defaults_to_cwd() -> None:
    """Test initialization with empty string defaults to current directory."""
    store = LocalStore("")
    assert store.base_path == Path.cwd()


def test_write_and_read_bytes(tmp_path: Path) -> None:
    """Test write and read bytes operations."""
    store = LocalStore(str(tmp_path))
    test_data = b"test data content"

    store.write_bytes_sync("test_file.bin", test_data)
    result = store.read_bytes_sync("test_file.bin")

    assert result == test_data


def test_write_and_read_text(tmp_path: Path) -> None:
    """Test write and read text operations."""
    store = LocalStore(str(tmp_path))
    test_text = "test text content\nwith multiple lines"

    store.write_text_sync("test_file.txt", test_text)
    result = store.read_text_sync("test_file.txt")

    assert result == test_text


def test_write_and_read_text_custom_encoding(tmp_path: Path) -> None:
    """Test write and read text with custom encoding."""
    store = LocalStore(str(tmp_path))
    test_text = "test with ünicode"

    store.write_text_sync("test_file.txt", test_text, encoding="latin-1")
    result = store.read_text_sync("test_file.txt", encoding="latin-1")

    assert result == test_text


def test_exists(tmp_path: Path) -> None:
    """Test exists operation."""
    store = LocalStore(str(tmp_path))

    assert not store.exists_sync("nonexistent.txt")

    store.write_text_sync("existing.txt", "content")
    assert store.exists_sync("existing.txt")


def test_delete(tmp_path: Path) -> None:
    """Test delete operation."""
    store = LocalStore(str(tmp_path))

    store.write_text_sync("to_delete.txt", "content")
    assert store.exists_sync("to_delete.txt")

    store.delete_sync("to_delete.txt")
    assert not store.exists_sync("to_delete.txt")


def test_copy(tmp_path: Path) -> None:
    """Test copy operation."""
    store = LocalStore(str(tmp_path))
    original_content = "original content"

    store.write_text_sync("original.txt", original_content)
    store.copy_sync("original.txt", "copied.txt")

    assert store.exists_sync("copied.txt")
    assert store.read_text_sync("copied.txt") == original_content


def test_move(tmp_path: Path) -> None:
    """Test move operation."""
    store = LocalStore(str(tmp_path))
    original_content = "content to move"

    store.write_text_sync("original.txt", original_content)
    store.move_sync("original.txt", "moved.txt")

    assert not store.exists_sync("original.txt")
    assert store.exists_sync("moved.txt")
    assert store.read_text_sync("moved.txt") == original_content


def test_list_objects(tmp_path: Path) -> None:
    """Test list_objects operation."""
    store = LocalStore(str(tmp_path))

    # Create test files
    store.write_text_sync("file1.txt", "content1")
    store.write_text_sync("file2.txt", "content2")
    store.write_text_sync("subdir/file3.txt", "content3")

    # List all objects
    all_objects = store.list_objects_sync()
    assert "file1.txt" in all_objects
    assert "file2.txt" in all_objects
    assert "subdir/file3.txt" in all_objects


def test_list_objects_with_prefix(tmp_path: Path) -> None:
    """Test list_objects with prefix filtering."""
    store = LocalStore(str(tmp_path))

    # Create test files
    store.write_text_sync("prefix_file1.txt", "content1")
    store.write_text_sync("prefix_file2.txt", "content2")
    store.write_text_sync("other_file.txt", "content3")

    # List with prefix
    prefixed_objects = store.list_objects_sync(prefix="prefix_")
    assert "prefix_file1.txt" in prefixed_objects
    assert "prefix_file2.txt" in prefixed_objects
    assert "other_file.txt" not in prefixed_objects


def test_glob(tmp_path: Path) -> None:
    """Test glob pattern matching."""
    store = LocalStore(str(tmp_path))

    # Create test files
    store.write_text_sync("test1.sql", "SELECT 1")
    store.write_text_sync("test2.sql", "SELECT 2")
    store.write_text_sync("config.json", "{}")
    store.write_text_sync("subdir/test3.sql", "SELECT 3")

    # Test glob patterns
    sql_files = store.glob_sync("*.sql")
    assert "test1.sql" in sql_files
    assert "test2.sql" in sql_files
    assert "config.json" not in sql_files


def test_get_metadata(tmp_path: Path) -> None:
    """Test get_metadata operation."""
    store = LocalStore(str(tmp_path))
    test_content = "test content for metadata"

    store.write_text_sync("test_file.txt", test_content)
    metadata = store.get_metadata_sync("test_file.txt")

    assert "size" in metadata
    assert "modified" in metadata
    assert metadata["size"] == len(test_content.encode())


def test_is_object_and_is_path(tmp_path: Path) -> None:
    """Test is_object and is_path operations."""
    store = LocalStore(str(tmp_path))

    store.write_text_sync("file.txt", "content")
    (tmp_path / "subdir").mkdir()

    assert store.is_object_sync("file.txt")
    assert not store.is_object_sync("subdir")
    assert not store.is_path_sync("file.txt")
    assert store.is_path_sync("subdir")


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="PyArrow not installed")
def test_write_and_read_arrow(tmp_path: Path) -> None:
    """Test write and read Arrow table operations."""
    store = LocalStore(str(tmp_path))

    # Create test Arrow table
    data: dict[str, Any] = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "score": [95.5, 87.0, 92.3]}
    table = pa.table(data)

    store.write_arrow_sync("test_data.parquet", table)
    result = store.read_arrow_sync("test_data.parquet")

    assert result.equals(table)


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="PyArrow not installed")
def test_stream_arrow(tmp_path: Path) -> None:
    """Test stream Arrow record batches."""
    store = LocalStore(str(tmp_path))

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


def test_sign_sync_raises_not_implemented(tmp_path: Path) -> None:
    """Test sign_sync raises NotImplementedError for local files."""
    store = LocalStore(str(tmp_path))

    store.write_text_sync("test.txt", "content")

    # Local storage does not support URL signing
    assert store.supports_signing is False

    with pytest.raises(NotImplementedError, match="URL signing is not applicable"):
        store.sign_sync("test.txt")


def test_resolve_path_absolute(tmp_path: Path) -> None:
    """Test path resolution with absolute paths."""
    store = LocalStore(str(tmp_path))

    # Absolute path should be returned as-is
    test_path = tmp_path / "test.txt"
    store.write_text_sync("test.txt", "content")

    resolved = store._resolve_path(str(test_path))
    assert resolved == test_path


def test_resolve_path_relative(tmp_path: Path) -> None:
    """Test path resolution with relative paths."""
    store = LocalStore(str(tmp_path))

    resolved = store._resolve_path("subdir/file.txt")
    expected = tmp_path.resolve() / "subdir" / "file.txt"
    assert resolved == expected


def test_nested_directory_operations(tmp_path: Path) -> None:
    """Test operations with nested directories."""
    store = LocalStore(str(tmp_path))

    # Write to nested path
    store.write_text_sync("level1/level2/file.txt", "nested content")
    assert store.exists_sync("level1/level2/file.txt")
    assert store.read_text_sync("level1/level2/file.txt") == "nested content"

    # List should include nested files
    objects = store.list_objects_sync()
    assert "level1/level2/file.txt" in objects


def test_file_not_found_errors(tmp_path: Path) -> None:
    """Test operations on non-existent files raise appropriate errors."""
    store = LocalStore(str(tmp_path))

    with pytest.raises(FileNotFoundError):
        store.read_bytes_sync("nonexistent.bin")

    with pytest.raises(FileNotFoundError):
        store.read_text_sync("nonexistent.txt")


# Async tests


async def test_async_write_and_read_bytes(tmp_path: Path) -> None:
    """Test async write and read bytes operations."""
    store = LocalStore(str(tmp_path))
    test_data = b"async test data content"

    await store.write_bytes_async("async_test_file.bin", test_data)
    result = await store.read_bytes_async("async_test_file.bin")

    assert result == test_data


async def test_async_write_and_read_text(tmp_path: Path) -> None:
    """Test async write and read text operations."""
    store = LocalStore(str(tmp_path))
    test_text = "async test text content\nwith multiple lines"

    await store.write_text_async("async_test_file.txt", test_text)
    result = await store.read_text_async("async_test_file.txt")

    assert result == test_text


async def test_async_exists(tmp_path: Path) -> None:
    """Test async exists operation."""
    store = LocalStore(str(tmp_path))

    assert not await store.exists_async("async_nonexistent.txt")

    await store.write_text_async("async_existing.txt", "content")
    assert await store.exists_async("async_existing.txt")


async def test_async_delete(tmp_path: Path) -> None:
    """Test async delete operation."""
    store = LocalStore(str(tmp_path))

    await store.write_text_async("async_to_delete.txt", "content")
    assert await store.exists_async("async_to_delete.txt")

    await store.delete_async("async_to_delete.txt")
    assert not await store.exists_async("async_to_delete.txt")


async def test_async_copy(tmp_path: Path) -> None:
    """Test async copy operation."""
    store = LocalStore(str(tmp_path))
    original_content = "async original content"

    await store.write_text_async("async_original.txt", original_content)
    await store.copy_async("async_original.txt", "async_copied.txt")

    assert await store.exists_async("async_copied.txt")
    assert await store.read_text_async("async_copied.txt") == original_content


async def test_async_move(tmp_path: Path) -> None:
    """Test async move operation."""
    store = LocalStore(str(tmp_path))
    original_content = "async content to move"

    await store.write_text_async("async_original.txt", original_content)
    await store.move_async("async_original.txt", "async_moved.txt")

    assert not await store.exists_async("async_original.txt")
    assert await store.exists_async("async_moved.txt")
    assert await store.read_text_async("async_moved.txt") == original_content


async def test_async_list_objects(tmp_path: Path) -> None:
    """Test async list_objects operation."""
    store = LocalStore(str(tmp_path))

    # Create test files
    await store.write_text_async("async_file1.txt", "content1")
    await store.write_text_async("async_file2.txt", "content2")
    await store.write_text_async("async_subdir/file3.txt", "content3")

    # List all objects
    all_objects = await store.list_objects_async()
    assert "async_file1.txt" in all_objects
    assert "async_file2.txt" in all_objects
    assert "async_subdir/file3.txt" in all_objects


async def test_async_get_metadata(tmp_path: Path) -> None:
    """Test async get_metadata operation."""
    store = LocalStore(str(tmp_path))
    test_content = "async test content for metadata"

    await store.write_text_async("async_test_file.txt", test_content)
    metadata = await store.get_metadata_async("async_test_file.txt")

    assert "size" in metadata
    assert "modified" in metadata
    assert metadata["size"] == len(test_content.encode())


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="PyArrow not installed")
async def test_async_write_and_read_arrow(tmp_path: Path) -> None:
    """Test async write and read Arrow table operations."""
    store = LocalStore(str(tmp_path))

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


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="PyArrow not installed")
async def test_async_stream_arrow(tmp_path: Path) -> None:
    """Test async stream Arrow record batches."""
    store = LocalStore(str(tmp_path))

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


async def test_async_sign_raises_not_implemented(tmp_path: Path) -> None:
    """Test sign_async raises NotImplementedError for local files."""
    store = LocalStore(str(tmp_path))

    await store.write_text_async("async_test.txt", "content")

    with pytest.raises(NotImplementedError, match="URL signing is not applicable"):
        await store.sign_async("async_test.txt")


def test_arrow_operations_without_pyarrow(tmp_path: Path) -> None:
    """Test Arrow operations raise proper error without PyArrow."""
    if PYARROW_INSTALLED:
        pytest.skip("PyArrow is installed")

    store = LocalStore(str(tmp_path))

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        store.read_arrow_sync("test.parquet")

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        store.write_arrow_sync("test.parquet", None)  # type: ignore

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        list(store.stream_arrow_sync("*.parquet"))


# Tests for base_path combination fix


def test_file_uri_with_relative_base_path(tmp_path: Path) -> None:
    """Test that file:// URI + relative base_path are combined correctly."""
    # Create the subdirectory
    subdir = tmp_path / "subdir"
    subdir.mkdir()

    store = LocalStore(f"file://{tmp_path}", base_path="subdir")

    # base_path should be combined with URI path
    assert store.base_path == subdir.resolve()


def test_file_uri_with_absolute_base_path_override(tmp_path: Path) -> None:
    """Test that absolute base_path takes precedence (backward compatible)."""
    other_path = tmp_path / "other"
    other_path.mkdir()

    store = LocalStore(f"file://{tmp_path}", base_path=str(other_path))

    # Absolute base_path should override
    assert store.base_path == other_path.resolve()


def test_file_uri_with_nested_base_path(tmp_path: Path) -> None:
    """Test that nested relative base_path is combined correctly."""
    # Create nested directory
    nested = tmp_path / "level1" / "level2"
    nested.mkdir(parents=True)

    store = LocalStore(f"file://{tmp_path}", base_path="level1/level2")

    assert store.base_path == nested.resolve()


def test_base_path_combination_full_workflow(tmp_path: Path) -> None:
    """Test full read/write workflow with base_path combination."""
    subdir = tmp_path / "storage" / "data"
    subdir.mkdir(parents=True)

    store = LocalStore(f"file://{tmp_path}/storage", base_path="data")

    # Write and read should work correctly
    test_data = b"test content"
    store.write_bytes_sync("test.bin", test_data)
    result = store.read_bytes_sync("test.bin")

    assert result == test_data
    # Verify file is in the correct location
    assert (subdir / "test.bin").exists()


# Tests for async streaming non-blocking fix


async def test_stream_read_async_does_not_block_event_loop(tmp_path: Path) -> None:
    """Test that stream_read_async doesn't block the event loop."""
    import asyncio

    store = LocalStore(str(tmp_path))

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


async def test_stream_read_async_respects_chunk_size(tmp_path: Path) -> None:
    """Test that stream_read_async respects the chunk_size parameter."""
    store = LocalStore(str(tmp_path))

    test_data = b"x" * 10_000
    store.write_bytes_sync("chunked_file.bin", test_data)

    chunk_size = 1000
    chunks = [chunk async for chunk in await store.stream_read_async("chunked_file.bin", chunk_size=chunk_size)]

    # All chunks except possibly the last should be exactly chunk_size
    for chunk in chunks[:-1]:
        assert len(chunk) == chunk_size

    # Reassemble and verify
    assert b"".join(chunks) == test_data


async def test_stream_read_async_with_base_path(tmp_path: Path) -> None:
    """Test async streaming works correctly with base_path combination."""
    subdir = tmp_path / "data"
    subdir.mkdir()

    store = LocalStore(f"file://{tmp_path}", base_path="data")

    test_data = b"streaming test data"
    store.write_bytes_sync("stream_test.bin", test_data)

    chunks = [chunk async for chunk in await store.stream_read_async("stream_test.bin")]

    assert b"".join(chunks) == test_data
