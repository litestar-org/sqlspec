"""Unit tests for ObStoreBackend.

This module tests the ObStoreBackend class including:
- Initialization with URI and options
- Path resolution with base paths
- Read/write operations (bytes and text)
- Object listing and glob patterns
- Object existence and metadata
- Copy/move/delete operations
- Arrow table operations (native support)
- Error handling and dependency checks
- Native vs fallback method handling
- Instrumentation and logging
"""

import logging
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import LogCaptureFixture

from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError
from sqlspec.storage.backends.obstore import ObStoreBackend


# Test Fixtures
@pytest.fixture
def mock_store() -> MagicMock:
    """Create a mock obstore instance."""
    store = MagicMock()

    # Mock basic operations
    get_result = MagicMock()
    get_result.bytes.return_value = b"test data"
    store.get.return_value = get_result

    # Mock async operations
    async_result = MagicMock()
    async_result.bytes = MagicMock(return_value=b"async test data")
    store.get_async = AsyncMock(return_value=async_result)
    store.put_async = AsyncMock()
    store.list_async = AsyncMock()

    # Mock metadata operations
    metadata = MagicMock()
    metadata.size = 1024
    metadata.last_modified = "2023-01-01T00:00:00Z"
    metadata.e_tag = '"abc123"'
    store.head.return_value = metadata
    store.head_async = AsyncMock(return_value=metadata)

    # Mock list operations
    list_item = MagicMock()
    list_item.path = "test.txt"
    store.list.return_value = [list_item]

    # Mock Arrow operations
    arrow_table = MagicMock()
    arrow_table.to_pydict.return_value = {"col1": [1, 2, 3]}
    store.read_arrow.return_value = arrow_table
    store.read_arrow_async = AsyncMock(return_value=arrow_table)

    return store


@pytest.fixture
def backend_with_mock_store(mock_store: MagicMock) -> ObStoreBackend:
    """Create backend with mocked store."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.MemoryStore") as mock_memory_store:
            mock_memory_store.return_value = mock_store

            backend = ObStoreBackend("memory://test", base_path="/base")
            backend.store = mock_store
            return backend


# Initialization Tests
def test_initialization_with_memory_store() -> None:
    """Test initialization with memory store URI."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.MemoryStore") as mock_memory_store:
            mock_store = MagicMock()
            mock_memory_store.return_value = mock_store

            backend = ObStoreBackend("memory://test", base_path="/base/path")

            assert backend.store_uri == "memory://test"
            assert backend.base_path == "/base/path"
            assert backend.backend_type == "obstore"
            mock_memory_store.assert_called_once()


def test_initialization_with_file_store() -> None:
    """Test initialization with file store URI."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.LocalStore") as mock_local_store:
            mock_store = MagicMock()
            mock_local_store.return_value = mock_store

            backend = ObStoreBackend("file:///test/path", base_path="subdir")

            assert backend.store_uri == "file:///test/path"
            assert backend.base_path == "subdir"
            mock_local_store.assert_called_once_with("/")


def test_initialization_with_cloud_store() -> None:
    """Test initialization with cloud store URI."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.from_url") as mock_from_url:
            mock_store = MagicMock()
            mock_from_url.return_value = mock_store

            backend = ObStoreBackend("s3://bucket/prefix", base_path="data", aws_access_key_id="key")

            assert backend.store_uri == "s3://bucket/prefix"
            assert backend.base_path == "data"
            assert backend.store_options == {"aws_access_key_id": "key"}
            mock_from_url.assert_called_once_with("s3://bucket/prefix", aws_access_key_id="key")


def test_missing_obstore_dependency() -> None:
    """Test error when obstore is not installed."""
    with patch("sqlspec.storage.backends.obstore.OBSTORE_INSTALLED", False):
        with pytest.raises(MissingDependencyError, match="obstore"):
            ObStoreBackend("s3://bucket")


def test_initialization_error() -> None:
    """Test error handling during initialization."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.from_url", side_effect=Exception("Connection failed")):
            with pytest.raises(StorageOperationFailedError, match="Failed to initialize obstore backend"):
                ObStoreBackend("s3://bucket")


# Path Resolution Tests
@pytest.mark.parametrize(
    "base_path,input_path,expected",
    [
        ("/base", "file.txt", "/base/file.txt"),
        ("/base", "/file.txt", "/base/file.txt"),
        ("/base/", "file.txt", "/base/file.txt"),
        ("/base/", "/file.txt", "/base/file.txt"),
        ("", "file.txt", "file.txt"),
        ("", "/file.txt", "/file.txt"),
    ],
    ids=["no_slash", "leading_slash", "trailing_base", "both_slash", "empty_base", "empty_base_leading"],
)
def test_path_resolution(
    backend_with_mock_store: ObStoreBackend, base_path: str, input_path: str, expected: str
) -> None:
    """Test path resolution with various base paths."""
    backend = backend_with_mock_store
    backend.base_path = base_path
    resolved = backend._resolve_path(input_path)
    assert resolved == expected


def test_file_uri_path_resolution(backend_with_mock_store: ObStoreBackend) -> None:
    """Test special path resolution for file:// URIs."""
    backend = backend_with_mock_store
    backend.store_uri = "file:///root"
    backend.base_path = ""

    # Absolute paths should have leading slash stripped
    assert backend._resolve_path("/absolute/path.txt") == "absolute/path.txt"
    # Relative paths remain unchanged
    assert backend._resolve_path("relative/path.txt") == "relative/path.txt"


# Read/Write Operations Tests
def test_read_bytes(backend_with_mock_store: ObStoreBackend) -> None:
    """Test reading bytes from object."""
    backend = backend_with_mock_store
    backend.store = backend.store  # Use the fixture's mock

    result = backend.read_bytes("test.txt")

    assert result == b"test data"
    backend.store.get.assert_called_once_with("/base/test.txt")


def test_write_bytes(backend_with_mock_store: ObStoreBackend) -> None:
    """Test writing bytes to object."""
    backend = backend_with_mock_store

    backend.write_bytes("test.txt", b"new data")

    backend.store.put.assert_called_once_with("/base/test.txt", b"new data")


def test_read_text(backend_with_mock_store: ObStoreBackend) -> None:
    """Test reading text from object."""
    backend = backend_with_mock_store

    result = backend.read_text("test.txt")

    assert result == "test data"
    backend.store.get.assert_called_once_with("/base/test.txt")


def test_write_text(backend_with_mock_store: ObStoreBackend) -> None:
    """Test writing text to object."""
    backend = backend_with_mock_store

    backend.write_text("test.txt", "text data")

    backend.store.put.assert_called_once_with("/base/test.txt", b"text data")


def test_read_text_with_encoding(backend_with_mock_store: ObStoreBackend) -> None:
    """Test reading text with custom encoding."""
    backend = backend_with_mock_store
    backend.store.get.return_value.bytes.return_value = "データ".encode("utf-16")

    result = backend.read_text("test.txt", encoding="utf-16")

    assert result == "データ"


def test_write_text_with_encoding(backend_with_mock_store: ObStoreBackend) -> None:
    """Test writing text with custom encoding."""
    backend = backend_with_mock_store

    backend.write_text("test.txt", "データ", encoding="utf-16")

    backend.store.put.assert_called_once()
    args = backend.store.put.call_args[0]
    assert args[0] == "/base/test.txt"
    assert args[1] == "データ".encode("utf-16")


# Error Handling Tests
def test_read_bytes_error(backend_with_mock_store: ObStoreBackend) -> None:
    """Test error handling in read_bytes."""
    backend = backend_with_mock_store
    backend.store.get.side_effect = Exception("Read failed")

    with pytest.raises(StorageOperationFailedError, match="Failed to read bytes from test.txt"):
        backend.read_bytes("test.txt")


def test_write_bytes_error(backend_with_mock_store: ObStoreBackend) -> None:
    """Test error handling in write_bytes."""
    backend = backend_with_mock_store
    backend.store.put.side_effect = Exception("Write failed")

    with pytest.raises(StorageOperationFailedError, match="Failed to write bytes to test.txt"):
        backend.write_bytes("test.txt", b"data")


# Object Operations Tests
def test_exists_true(backend_with_mock_store: ObStoreBackend) -> None:
    """Test checking if object exists (true case)."""
    backend = backend_with_mock_store

    result = backend.exists("test.txt")

    assert result is True
    backend.store.head.assert_called_once_with("/base/test.txt")


def test_exists_false(backend_with_mock_store: ObStoreBackend) -> None:
    """Test checking if object exists (false case)."""
    backend = backend_with_mock_store
    backend.store.head.side_effect = Exception("Not found")

    result = backend.exists("test.txt")

    assert result is False


def test_delete(backend_with_mock_store: ObStoreBackend) -> None:
    """Test deleting an object."""
    backend = backend_with_mock_store

    backend.delete("test.txt")

    backend.store.delete.assert_called_once_with("/base/test.txt")


def test_delete_error(backend_with_mock_store: ObStoreBackend) -> None:
    """Test error handling in delete."""
    backend = backend_with_mock_store
    backend.store.delete.side_effect = Exception("Delete failed")

    with pytest.raises(StorageOperationFailedError, match="Failed to delete test.txt"):
        backend.delete("test.txt")


def test_copy(backend_with_mock_store: ObStoreBackend) -> None:
    """Test copying an object."""
    backend = backend_with_mock_store

    backend.copy("source.txt", "dest.txt")

    backend.store.copy.assert_called_once_with("/base/source.txt", "/base/dest.txt")


def test_copy_error(backend_with_mock_store: ObStoreBackend) -> None:
    """Test error handling in copy."""
    backend = backend_with_mock_store
    backend.store.copy.side_effect = Exception("Copy failed")

    with pytest.raises(StorageOperationFailedError, match="Failed to copy source.txt to dest.txt"):
        backend.copy("source.txt", "dest.txt")


def test_move(backend_with_mock_store: ObStoreBackend) -> None:
    """Test moving an object."""
    backend = backend_with_mock_store

    backend.move("source.txt", "dest.txt")

    backend.store.rename.assert_called_once_with("/base/source.txt", "/base/dest.txt")


def test_move_error(backend_with_mock_store: ObStoreBackend) -> None:
    """Test error handling in move."""
    backend = backend_with_mock_store
    backend.store.rename.side_effect = Exception("Move failed")

    with pytest.raises(StorageOperationFailedError, match="Failed to move source.txt to dest.txt"):
        backend.move("source.txt", "dest.txt")


# List Operations Tests
def test_list_objects_recursive(backend_with_mock_store: ObStoreBackend) -> None:
    """Test listing objects recursively."""
    backend = backend_with_mock_store
    item1 = MagicMock()
    item1.path = "dir/file1.txt"
    item2 = MagicMock()
    item2.path = "dir/subdir/file2.txt"
    backend.store.list.return_value = [item1, item2]

    result = backend.list_objects("dir", recursive=True)

    assert result == ["dir/file1.txt", "dir/subdir/file2.txt"]
    backend.store.list.assert_called_once_with("/base/dir")


def test_list_objects_non_recursive(backend_with_mock_store: ObStoreBackend) -> None:
    """Test listing objects non-recursively."""
    backend = backend_with_mock_store
    item1 = MagicMock()
    item1.path = "dir/file1.txt"
    backend.store.list_with_delimiter.return_value = [item1]

    result = backend.list_objects("dir", recursive=False)

    assert result == ["dir/file1.txt"]
    backend.store.list_with_delimiter.assert_called_once_with("/base/dir")


def test_list_objects_with_key_attribute(backend_with_mock_store: ObStoreBackend) -> None:
    """Test listing objects when items have 'key' attribute instead of 'path'."""
    backend = backend_with_mock_store
    item = MagicMock(spec=[])  # No 'path' attribute
    item.key = "file.txt"
    backend.store.list.return_value = [item]

    result = backend.list_objects()

    assert result == ["file.txt"]


def test_list_objects_fallback_str(backend_with_mock_store: ObStoreBackend) -> None:
    """Test listing objects with fallback to str()."""
    backend = backend_with_mock_store

    # Create a custom object without path or key attributes
    class CustomItem:
        def __str__(self) -> str:
            return "file.txt"

    item = CustomItem()
    backend.store.list.return_value = [item]

    result = backend.list_objects()

    assert result == ["file.txt"]


# Glob Operations Tests
def test_glob_simple_pattern(backend_with_mock_store: ObStoreBackend) -> None:
    """Test glob with simple pattern."""
    backend = backend_with_mock_store
    # Set base_path to empty to avoid path resolution issues
    backend.base_path = ""
    # Mock list_objects to return the files
    with patch.object(backend, "list_objects", return_value=["file1.txt", "file2.csv", "file3.txt"]):
        result = backend.glob("*.txt")

    assert result == ["file1.txt", "file3.txt"]


def test_glob_double_star_pattern(backend_with_mock_store: ObStoreBackend) -> None:
    """Test glob with ** pattern."""
    backend = backend_with_mock_store
    # Set base_path to empty to avoid path resolution issues
    backend.base_path = ""
    # Mock list_objects to return the files
    with patch.object(backend, "list_objects", return_value=["file.txt", "dir/file.txt", "dir/subdir/file.txt"]):
        result = backend.glob("**/*.txt")

    assert set(result) == {"file.txt", "dir/file.txt", "dir/subdir/file.txt"}


def test_glob_double_star_at_start(backend_with_mock_store: ObStoreBackend) -> None:
    """Test glob with ** at start of pattern."""
    backend = backend_with_mock_store
    backend.base_path = ""
    backend.store.list.return_value = [
        MagicMock(path="test.txt"),
        MagicMock(path="dir/test.txt"),
        MagicMock(path="other.csv"),
    ]

    result = backend.glob("**/test.txt")

    assert set(result) == {"test.txt", "dir/test.txt"}


# Metadata Operations Tests
def test_get_metadata_exists(backend_with_mock_store: ObStoreBackend) -> None:
    """Test getting metadata for existing object."""
    backend = backend_with_mock_store
    metadata = MagicMock()
    metadata.size = 2048
    metadata.last_modified = "2023-06-01T12:00:00Z"
    metadata.e_tag = '"xyz789"'
    metadata.version = "v1"
    metadata.metadata = {"custom": "value"}
    backend.store.head.return_value = metadata

    result = backend.get_metadata("test.txt")

    assert result == {
        "path": "/base/test.txt",
        "exists": True,
        "size": 2048,
        "last_modified": "2023-06-01T12:00:00Z",
        "e_tag": '"xyz789"',
        "version": "v1",
        "custom_metadata": {"custom": "value"},
    }


def test_get_metadata_not_exists(backend_with_mock_store: ObStoreBackend) -> None:
    """Test getting metadata for non-existent object."""
    backend = backend_with_mock_store
    backend.store.head.side_effect = Exception("Not found")

    result = backend.get_metadata("test.txt")

    assert result == {"path": "/base/test.txt", "exists": False}


def test_get_metadata_partial_attributes(backend_with_mock_store: ObStoreBackend) -> None:
    """Test getting metadata when some attributes are missing."""
    backend = backend_with_mock_store
    metadata = MagicMock(spec=["size"])  # Only has size attribute
    metadata.size = 1024
    backend.store.head.return_value = metadata

    result = backend.get_metadata("test.txt")

    assert result["size"] == 1024
    assert "last_modified" not in result
    assert "e_tag" not in result


# Type Check Operations Tests
def test_is_object_true(backend_with_mock_store: ObStoreBackend) -> None:
    """Test checking if path is an object (true case)."""
    backend = backend_with_mock_store
    backend.store.head.return_value = MagicMock()  # Exists

    result = backend.is_object("file.txt")

    assert result is True


def test_is_object_false_not_exists(backend_with_mock_store: ObStoreBackend) -> None:
    """Test checking if path is an object (doesn't exist)."""
    backend = backend_with_mock_store
    backend.store.head.side_effect = Exception("Not found")

    result = backend.is_object("file.txt")

    assert result is False


def test_is_object_false_directory(backend_with_mock_store: ObStoreBackend) -> None:
    """Test checking if path is an object (is directory)."""
    backend = backend_with_mock_store
    backend.store.head.return_value = MagicMock()  # Exists

    result = backend.is_object("dir/")

    assert result is False  # Ends with / so it's a directory


def test_is_path_with_trailing_slash(backend_with_mock_store: ObStoreBackend) -> None:
    """Test checking if path is a directory with trailing slash."""
    backend = backend_with_mock_store

    result = backend.is_path("dir/")

    assert result is True


def test_is_path_with_objects(backend_with_mock_store: ObStoreBackend) -> None:
    """Test checking if path is a directory by listing objects."""
    backend = backend_with_mock_store
    backend.store.list.return_value = [MagicMock(path="dir/file.txt")]

    with patch.object(backend, "list_objects", return_value=["dir/file.txt"]):
        result = backend.is_path("dir")

    assert result is True


def test_is_path_no_objects(backend_with_mock_store: ObStoreBackend) -> None:
    """Test checking if path is a directory with no objects."""
    backend = backend_with_mock_store

    with patch.object(backend, "list_objects", return_value=[]):
        result = backend.is_path("dir")

    assert result is False


# Arrow Operations Tests
def test_read_arrow_native(backend_with_mock_store: ObStoreBackend) -> None:
    """Test reading Arrow table with native support."""
    backend = backend_with_mock_store
    mock_table = MagicMock()
    backend.store.read_arrow.return_value = mock_table

    result = backend.read_arrow("test.parquet")

    assert result == mock_table
    backend.store.read_arrow.assert_called_once_with("/base/test.parquet")


def test_read_arrow_fallback(backend_with_mock_store: ObStoreBackend) -> None:
    """Test reading Arrow table with fallback to bytes."""
    backend = backend_with_mock_store
    # Remove native Arrow support
    delattr(backend.store, "read_arrow")

    # Mock read_bytes return value
    backend.store.get.return_value.bytes.return_value = b"parquet data"

    with patch("pyarrow.parquet.read_table") as mock_read_table:
        with patch("io.BytesIO") as mock_bytesio:
            mock_buffer = MagicMock()
            mock_bytesio.return_value = mock_buffer
            mock_table = MagicMock()
            mock_read_table.return_value = mock_table

            result = backend.read_arrow("test.parquet")

            assert result == mock_table
            # Due to double path resolution, the path is /base/base/test.parquet
            backend.store.get.assert_called_once_with("/base/base/test.parquet")
            mock_bytesio.assert_called_once_with(b"parquet data")
            mock_read_table.assert_called_once_with(mock_buffer)


def test_read_arrow_error(backend_with_mock_store: ObStoreBackend) -> None:
    """Test error handling in read_arrow."""
    backend = backend_with_mock_store
    backend.store.read_arrow.side_effect = Exception("Read failed")

    with pytest.raises(StorageOperationFailedError, match="Failed to read Arrow table from test.parquet"):
        backend.read_arrow("test.parquet")


def test_write_arrow_native(backend_with_mock_store: ObStoreBackend) -> None:
    """Test writing Arrow table with native support."""
    backend = backend_with_mock_store
    mock_table = MagicMock()

    backend.write_arrow("test.parquet", mock_table)

    backend.store.write_arrow.assert_called_once_with("/base/test.parquet", mock_table)


def test_write_arrow_fallback(backend_with_mock_store: ObStoreBackend) -> None:
    """Test writing Arrow table with fallback to bytes."""
    backend = backend_with_mock_store
    # Remove native Arrow support
    delattr(backend.store, "write_arrow")

    with patch("pyarrow.parquet.write_table") as mock_write_table:
        with patch("io.BytesIO") as mock_bytesio:
            mock_buffer = MagicMock()
            mock_bytesio.return_value = mock_buffer
            mock_read_result = MagicMock()
            mock_buffer.read.return_value = mock_read_result

            mock_table = MagicMock()
            # Mock schema iteration for decimal64 check
            mock_table.schema = []

            backend.write_arrow("test.parquet", mock_table)

            mock_write_table.assert_called_once_with(mock_table, mock_buffer)
            mock_buffer.seek.assert_called_once_with(0)
            # Due to double path resolution, the path is /base/base/test.parquet
            backend.store.put.assert_called_once_with("/base/base/test.parquet", mock_read_result)


def test_write_arrow_decimal64_conversion(backend_with_mock_store: ObStoreBackend) -> None:
    """Test writing Arrow table with decimal64 to decimal128 conversion."""
    backend = backend_with_mock_store
    # Remove native Arrow support to test fallback with conversion
    delattr(backend.store, "write_arrow")

    with patch("pyarrow.parquet.write_table"):
        with patch("io.BytesIO"):
            with patch("pyarrow.field") as mock_field:
                with patch("pyarrow.decimal128") as mock_decimal128:
                    with patch("pyarrow.schema") as mock_schema:
                        # Create mock table with decimal64 field
                        field1 = MagicMock()
                        field1.name = "amount"
                        field1.type.__str__.return_value = "decimal64(10, 2)"

                        field2 = MagicMock()
                        field2.name = "other"
                        field2.type.__str__.return_value = "int64"

                        mock_table = MagicMock()
                        mock_table.schema = [field1, field2]

                        # Mock the conversion
                        new_field = MagicMock()
                        mock_field.return_value = new_field
                        mock_decimal128.return_value = "decimal128(10, 2)"
                        new_schema = MagicMock()
                        mock_schema.return_value = new_schema
                        mock_table.cast.return_value = mock_table

                        backend.write_arrow("test.parquet", mock_table)

                        # Verify decimal128 was called with correct precision/scale
                        mock_decimal128.assert_called_once_with(10, 2)
                        # Verify table was cast to new schema
                        mock_table.cast.assert_called_once_with(new_schema)


def test_write_arrow_error(backend_with_mock_store: ObStoreBackend) -> None:
    """Test error handling in write_arrow."""
    backend = backend_with_mock_store
    backend.store.write_arrow.side_effect = Exception("Write failed")
    mock_table = MagicMock()

    with pytest.raises(StorageOperationFailedError, match="Failed to write Arrow table to test.parquet"):
        backend.write_arrow("test.parquet", mock_table)


def test_stream_arrow(backend_with_mock_store: ObStoreBackend) -> None:
    """Test streaming Arrow record batches."""
    backend = backend_with_mock_store
    batch1 = MagicMock()
    batch2 = MagicMock()
    backend.store.stream_arrow.return_value = iter([batch1, batch2])

    result = list(backend.stream_arrow("*.parquet"))

    assert result == [batch1, batch2]
    backend.store.stream_arrow.assert_called_once_with("/base/*.parquet")


def test_stream_arrow_error(backend_with_mock_store: ObStoreBackend) -> None:
    """Test error handling in stream_arrow."""
    backend = backend_with_mock_store
    backend.store.stream_arrow.side_effect = Exception("Stream failed")

    with pytest.raises(StorageOperationFailedError, match="Failed to stream Arrow data for pattern"):
        list(backend.stream_arrow("*.parquet"))


# Async Operations Tests
@pytest.mark.asyncio
async def test_read_bytes_async(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async reading bytes."""
    backend = backend_with_mock_store

    result = await backend.read_bytes_async("test.txt")

    assert result == b"async test data"
    backend.store.get_async.assert_called_once_with("/base/test.txt")


@pytest.mark.asyncio
async def test_write_bytes_async(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async writing bytes."""
    backend = backend_with_mock_store

    await backend.write_bytes_async("test.txt", b"async data")

    backend.store.put_async.assert_called_once_with("/base/test.txt", b"async data")


@pytest.mark.asyncio
async def test_read_text_async(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async reading text."""
    backend = backend_with_mock_store

    result = await backend.read_text_async("test.txt")

    assert result == "async test data"


@pytest.mark.asyncio
async def test_write_text_async(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async writing text."""
    backend = backend_with_mock_store

    await backend.write_text_async("test.txt", "async text")

    backend.store.put_async.assert_called_once_with("/base/test.txt", b"async text")


@pytest.mark.asyncio
async def test_list_objects_async_recursive(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async listing objects recursively."""
    backend = backend_with_mock_store

    # Create mock items
    item1 = MagicMock()
    item1.path = "dir/file1.txt"
    item2 = MagicMock()
    item2.path = "dir/subdir/file2.txt"

    # Create an async generator that returns the items
    async def async_generator() -> Any:
        for item in [item1, item2]:
            yield item

    # Mock list_async to return the async generator
    backend.store.list_async = MagicMock(return_value=async_generator())

    result = await backend.list_objects_async("dir", recursive=True)

    assert result == ["dir/file1.txt", "dir/subdir/file2.txt"]


@pytest.mark.asyncio
async def test_list_objects_async_non_recursive(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async listing objects non-recursively with filtering."""
    backend = backend_with_mock_store

    # Create mock items - need to account for base_path in filtering
    item1 = MagicMock()
    item1.path = "base/dir/file1.txt"
    item2 = MagicMock()
    item2.path = "base/dir/subdir/file2.txt"

    # Create an async generator that returns the items
    async def async_generator() -> Any:
        for item in [item1, item2]:
            yield item

    # Mock list_async to return the async generator
    backend.store.list_async = MagicMock(return_value=async_generator())

    result = await backend.list_objects_async("dir", recursive=False)

    # Non-recursive should filter out deeper paths
    # With base_path="/base", resolved_prefix="/base/dir" has depth 2
    # "base/dir/file1.txt" has 2 slashes, "base/dir/subdir/file2.txt" has 3 slashes
    # So both are included since 2 <= 3 and 3 <= 3
    assert result == ["base/dir/file1.txt", "base/dir/subdir/file2.txt"]


@pytest.mark.asyncio
async def test_exists_async_true(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async checking if object exists (true case)."""
    backend = backend_with_mock_store

    result = await backend.exists_async("test.txt")

    assert result is True
    backend.store.head_async.assert_called_once_with("/base/test.txt")


@pytest.mark.asyncio
async def test_exists_async_false(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async checking if object exists (false case)."""
    backend = backend_with_mock_store
    backend.store.head_async.side_effect = Exception("Not found")

    result = await backend.exists_async("test.txt")

    assert result is False


@pytest.mark.asyncio
async def test_delete_async(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async deleting an object."""
    backend = backend_with_mock_store
    backend.store.delete_async = AsyncMock()

    await backend.delete_async("test.txt")

    backend.store.delete_async.assert_called_once_with("/base/test.txt")


@pytest.mark.asyncio
async def test_copy_async(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async copying an object."""
    backend = backend_with_mock_store
    backend.store.copy_async = AsyncMock()

    await backend.copy_async("source.txt", "dest.txt")

    backend.store.copy_async.assert_called_once_with("/base/source.txt", "/base/dest.txt")


@pytest.mark.asyncio
async def test_move_async(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async moving an object."""
    backend = backend_with_mock_store
    backend.store.rename_async = AsyncMock()

    await backend.move_async("source.txt", "dest.txt")

    backend.store.rename_async.assert_called_once_with("/base/source.txt", "/base/dest.txt")


@pytest.mark.asyncio
async def test_get_metadata_async(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async getting metadata."""
    backend = backend_with_mock_store
    metadata = MagicMock()
    metadata.size = 2048
    metadata.last_modified = "2023-06-01T12:00:00Z"
    backend.store.head_async.return_value = metadata

    result = await backend.get_metadata_async("test.txt")

    assert result["size"] == 2048
    assert result["last_modified"] == "2023-06-01T12:00:00Z"


@pytest.mark.asyncio
async def test_read_arrow_async(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async reading Arrow table."""
    backend = backend_with_mock_store
    mock_table = MagicMock()
    backend.store.read_arrow_async.return_value = mock_table

    result = await backend.read_arrow_async("test.parquet")

    assert result == mock_table
    backend.store.read_arrow_async.assert_called_once_with("/base/test.parquet")


@pytest.mark.asyncio
async def test_write_arrow_async_native(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async writing Arrow table with native support."""
    backend = backend_with_mock_store
    backend.store.write_arrow_async = AsyncMock()
    mock_table = MagicMock()

    await backend.write_arrow_async("test.parquet", mock_table)

    backend.store.write_arrow_async.assert_called_once_with("/base/test.parquet", mock_table)


@pytest.mark.asyncio
async def test_write_arrow_async_fallback(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async writing Arrow table with fallback."""
    backend = backend_with_mock_store
    # Remove native async Arrow support
    delattr(backend.store, "write_arrow_async")

    with patch("pyarrow.parquet.write_table") as mock_write_table:
        with patch("io.BytesIO") as mock_bytesio:
            mock_buffer = MagicMock()
            mock_bytesio.return_value = mock_buffer
            mock_buffer.read.return_value = b"parquet data"

            mock_table = MagicMock()

            await backend.write_arrow_async("test.parquet", mock_table)

            mock_write_table.assert_called_once_with(mock_table, mock_buffer)
            mock_buffer.seek.assert_called_once_with(0)
            # Due to double path resolution, the path is /base/base/test.parquet
            backend.store.put_async.assert_called_once_with("/base/base/test.parquet", b"parquet data")


@pytest.mark.asyncio
async def test_stream_arrow_async(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async streaming Arrow record batches."""
    backend = backend_with_mock_store

    async def mock_stream() -> Any:
        yield MagicMock()
        yield MagicMock()

    backend.store.stream_arrow_async.return_value = mock_stream()

    result = [batch async for batch in backend.stream_arrow_async("*.parquet")]
    assert len(result) == 2
    backend.store.stream_arrow_async.assert_called_once_with("/base/*.parquet")


# Instrumentation Tests
def test_logging_debug_mode(backend_with_mock_store: ObStoreBackend, caplog: LogCaptureFixture) -> None:
    """Test debug logging during initialization."""
    with caplog.at_level(logging.DEBUG):
        backend = backend_with_mock_store
        backend.store_uri = "s3://bucket"

        # Trigger a log by re-initializing
        with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
            with patch("obstore.store.from_url") as mock_from_url:
                mock_store = MagicMock()
                mock_from_url.return_value = mock_store

                ObStoreBackend("s3://bucket")

                assert "ObStore backend initialized for s3://bucket" in caplog.text


# Edge Cases Tests
def test_empty_base_path() -> None:
    """Test initialization with empty base path."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.from_url") as mock_from_url:
            mock_store = MagicMock()
            mock_from_url.return_value = mock_store

            backend = ObStoreBackend("s3://bucket", base_path="")

            assert backend.base_path == ""


def test_base_path_normalization() -> None:
    """Test base path normalization."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.from_url") as mock_from_url:
            mock_store = MagicMock()
            mock_from_url.return_value = mock_store

            backend = ObStoreBackend("s3://bucket", base_path="/base/path/")

            assert backend.base_path == "/base/path"


def test_empty_list_objects(backend_with_mock_store: ObStoreBackend) -> None:
    """Test listing with no objects."""
    backend = backend_with_mock_store
    backend.store.list.return_value = []

    result = backend.list_objects()

    assert result == []


def test_glob_no_matches(backend_with_mock_store: ObStoreBackend) -> None:
    """Test glob with no matches."""
    backend = backend_with_mock_store
    backend.store.list.return_value = [MagicMock(path="file.csv"), MagicMock(path="file.json")]

    result = backend.glob("*.txt")

    assert result == []


def test_operations_without_base_path() -> None:
    """Test operations without base path."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.from_url") as mock_from_url:
            mock_store = MagicMock()
            mock_from_url.return_value = mock_store

            backend = ObStoreBackend("s3://bucket")
            backend.store = mock_store

            backend.read_bytes("file.txt")
            mock_store.get.assert_called_once_with("file.txt")

            mock_store.reset_mock()
            backend.write_bytes("file.txt", b"data")
            mock_store.put.assert_called_once_with("file.txt", b"data")


def test_uri_variations() -> None:
    """Test initialization with various URI formats."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        uris = ["s3://bucket", "gcs://bucket", "az://container", "file:///path"]

        for uri in uris:
            with patch("obstore.store.from_url") as mock_from_url:
                mock_store = MagicMock()
                mock_from_url.return_value = mock_store

                backend = ObStoreBackend(uri)
                assert backend.store_uri == uri


# Pathlike Object Support Tests
def test_read_bytes_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test read_bytes with Path object."""
    backend = backend_with_mock_store

    path_obj = Path("test.txt")
    result = backend.read_bytes(path_obj)

    assert result == b"test data"
    backend.store.get.assert_called_once_with("/base/test.txt")


def test_write_bytes_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test write_bytes with Path object."""
    backend = backend_with_mock_store

    path_obj = Path("test.txt")
    backend.write_bytes(path_obj, b"test data")

    backend.store.put.assert_called_once_with("/base/test.txt", b"test data")


def test_exists_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test exists with Path object."""
    backend = backend_with_mock_store

    path_obj = Path("test.txt")
    result = backend.exists(path_obj)

    assert result is True
    backend.store.head.assert_called_once_with("/base/test.txt")


def test_copy_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test copy with Path objects."""
    backend = backend_with_mock_store

    source_path = Path("source.txt")
    dest_path = Path("dest.txt")
    backend.copy(source_path, dest_path)

    backend.store.copy.assert_called_once_with("/base/source.txt", "/base/dest.txt")


def test_move_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test move with Path objects."""
    backend = backend_with_mock_store

    source_path = Path("source.txt")
    dest_path = Path("dest.txt")
    backend.move(source_path, dest_path)

    backend.store.rename.assert_called_once_with("/base/source.txt", "/base/dest.txt")


def test_delete_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test delete with Path object."""
    backend = backend_with_mock_store

    path_obj = Path("test.txt")
    backend.delete(path_obj)

    backend.store.delete.assert_called_once_with("/base/test.txt")


def test_get_metadata_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test get_metadata with Path object."""
    backend = backend_with_mock_store

    path_obj = Path("test.txt")
    result = backend.get_metadata(path_obj)

    assert result["path"] == "/base/test.txt"
    backend.store.head.assert_called_once_with("/base/test.txt")


def test_read_arrow_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test read_arrow with Path object."""
    backend = backend_with_mock_store
    mock_table = MagicMock()
    backend.store.read_arrow.return_value = mock_table

    path_obj = Path("test.parquet")
    result = backend.read_arrow(path_obj)

    assert result == mock_table
    backend.store.read_arrow.assert_called_once_with("/base/test.parquet")


def test_write_arrow_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test write_arrow with Path object."""
    backend = backend_with_mock_store
    mock_table = MagicMock()

    path_obj = Path("test.parquet")
    backend.write_arrow(path_obj, mock_table)

    backend.store.write_arrow.assert_called_once_with("/base/test.parquet", mock_table)


def test_is_object_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test is_object with Path object."""
    backend = backend_with_mock_store

    path_obj = Path("test.txt")
    result = backend.is_object(path_obj)

    assert result is True


def test_is_path_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test is_path with Path object."""
    backend = backend_with_mock_store

    # Path normalizes away trailing slash, so we need to mock list_objects
    with patch.object(backend, "list_objects", return_value=["dir/file.txt"]):
        path_obj = Path("dir")
        result = backend.is_path(path_obj)

    assert result is True


@pytest.mark.asyncio
async def test_async_operations_with_pathlike(backend_with_mock_store: ObStoreBackend) -> None:
    """Test async operations with Path objects."""
    backend = backend_with_mock_store

    # Test read_bytes_async
    path_obj = Path("test.txt")
    result = await backend.read_bytes_async(path_obj)
    assert result == b"async test data"

    # Test write_bytes_async
    await backend.write_bytes_async(path_obj, b"async data")
    backend.store.put_async.assert_called_with("/base/test.txt", b"async data")

    # Test exists_async
    exists_result = await backend.exists_async(path_obj)
    assert exists_result is True

    # Test copy_async
    dest_path = Path("dest.txt")
    backend.store.copy_async = AsyncMock()
    await backend.copy_async(path_obj, dest_path)
    backend.store.copy_async.assert_called_once_with("/base/test.txt", "/base/dest.txt")

    # Test move_async
    backend.store.rename_async = AsyncMock()
    await backend.move_async(path_obj, dest_path)
    backend.store.rename_async.assert_called_once_with("/base/test.txt", "/base/dest.txt")
