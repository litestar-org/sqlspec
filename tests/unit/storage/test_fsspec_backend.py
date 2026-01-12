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

    store.write_bytes("test_file.bin", test_data)
    result = store.read_bytes("test_file.bin")

    assert result == test_data


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_write_and_read_text(tmp_path: Path) -> None:
    """Test write and read text operations."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    test_text = "test text content\nwith multiple lines"

    store.write_text("test_file.txt", test_text)
    result = store.read_text("test_file.txt")

    assert result == test_text


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_exists(tmp_path: Path) -> None:
    """Test exists operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    assert not store.exists("nonexistent.txt")

    store.write_text("existing.txt", "content")
    assert store.exists("existing.txt")


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_delete(tmp_path: Path) -> None:
    """Test delete operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    store.write_text("to_delete.txt", "content")
    assert store.exists("to_delete.txt")

    store.delete("to_delete.txt")
    assert not store.exists("to_delete.txt")


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_copy(tmp_path: Path) -> None:
    """Test copy operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    original_content = "original content"

    store.write_text("original.txt", original_content)
    store.copy("original.txt", "copied.txt")

    assert store.exists("copied.txt")
    assert store.read_text("copied.txt") == original_content


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_move(tmp_path: Path) -> None:
    """Test move operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    original_content = "content to move"

    store.write_text("original.txt", original_content)
    store.move("original.txt", "moved.txt")

    assert not store.exists("original.txt")
    assert store.exists("moved.txt")
    assert store.read_text("moved.txt") == original_content


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_list_objects(tmp_path: Path) -> None:
    """Test list_objects operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    # Create test files
    store.write_text("file1.txt", "content1")
    store.write_text("file2.txt", "content2")
    store.write_text("subdir/file3.txt", "content3")

    # List all objects
    all_objects = store.list_objects()
    assert any("file1.txt" in obj for obj in all_objects)
    assert any("file2.txt" in obj for obj in all_objects)
    assert any("file3.txt" in obj for obj in all_objects)


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_glob(tmp_path: Path) -> None:
    """Test glob pattern matching."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    # Create test files
    store.write_text("test1.sql", "SELECT 1")
    store.write_text("test2.sql", "SELECT 2")
    store.write_text("config.json", "{}")

    # Test glob patterns
    sql_files = store.glob("*.sql")
    assert any("test1.sql" in obj for obj in sql_files)
    assert any("test2.sql" in obj for obj in sql_files)
    assert not any("config.json" in obj for obj in sql_files)


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_get_metadata(tmp_path: Path) -> None:
    """Test get_metadata operation."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    test_content = "test content for metadata"

    store.write_text("test_file.txt", test_content)
    metadata = store.get_metadata("test_file.txt")

    assert "size" in metadata
    assert "exists" in metadata
    assert metadata["exists"] is True


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_is_object_and_is_path(tmp_path: Path) -> None:
    """Test is_object and is_path operations."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    store.write_text("file.txt", "content")
    (tmp_path / "subdir").mkdir()

    assert store.is_object("file.txt")
    assert not store.is_object("subdir")
    assert not store.is_path("file.txt")
    assert store.is_path("subdir")


@pytest.mark.skipif(not FSSPEC_INSTALLED or not PYARROW_INSTALLED, reason="fsspec or PyArrow missing")
def test_write_and_read_arrow(tmp_path: Path) -> None:
    """Test write and read Arrow table operations."""
    import pyarrow as pa

    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    # Create test Arrow table
    data: dict[str, Any] = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "score": [95.5, 87.0, 92.3]}
    table = pa.table(data)

    store.write_arrow("test_data.parquet", table)
    result = store.read_arrow("test_data.parquet")

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

    store.write_arrow("stream_test.parquet", table)

    # Stream record batches
    batches = list(store.stream_arrow("stream_test.parquet"))
    assert len(batches) > 0

    # Verify we can read the data
    reconstructed = pa.Table.from_batches(batches)
    assert reconstructed.equals(table)


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_sign_returns_uri(tmp_path: Path) -> None:
    """Test sign_sync raises NotImplementedError for fsspec backends."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    store.write_text("test.txt", "content")

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
        store.read_arrow("test.parquet")

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        store.write_arrow("test.parquet", None)  # type: ignore

    with pytest.raises(MissingDependencyError, match="pyarrow"):
        list(store.stream_arrow("*.parquet"))
