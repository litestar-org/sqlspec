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
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import LogCaptureFixture

from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError
from sqlspec.storage.backends.obstore import ObStoreBackend

if TYPE_CHECKING:
    pass


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
    metadata.last_modified = "2024-01-01T00:00:00Z"
    metadata.e_tag = "abc123"
    store.head.return_value = metadata
    store.head_async = AsyncMock(return_value=metadata)

    # Mock list operations
    list_item = MagicMock()
    list_item.path = "test/file.txt"
    store.list.return_value = [list_item]
    store.list_with_delimiter.return_value = [list_item]

    # Mock native arrow support
    store.read_arrow = MagicMock()
    store.write_arrow = MagicMock()

    return store


@pytest.fixture
def backend_with_mock_store(mock_store: MagicMock) -> ObStoreBackend:
    """Create backend with mocked obstore."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.from_url", return_value=mock_store):
            backend = ObStoreBackend("s3://test-bucket", base_path="/data")
            backend.store = mock_store  # Ensure our mock is used
            return backend


# Initialization Tests
def test_initialization_success(mock_store: MagicMock) -> None:
    """Test successful initialization."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.from_url", return_value=mock_store):
            backend = ObStoreBackend("s3://test-bucket", base_path="/data", region="us-east-1")

            assert backend.store_uri == "s3://test-bucket"
            assert backend.base_path == "/data"
            assert backend.store_options == {"region": "us-east-1"}
            assert backend.backend_type == "obstore"


def test_initialization_without_obstore() -> None:
    """Test error when obstore is not installed."""
    with patch("sqlspec.storage.backends.obstore.OBSTORE_INSTALLED", False):
        with pytest.raises(MissingDependencyError, match="obstore"):
            ObStoreBackend("s3://test-bucket")


def test_initialization_error() -> None:
    """Test error during initialization."""
    with patch("sqlspec.storage.backends.obstore.OBSTORE_INSTALLED", True):
        with patch("obstore.store.from_url", side_effect=Exception("Connection failed")):
            with pytest.raises(StorageOperationFailedError, match="Failed to initialize obstore backend"):
                ObStoreBackend("s3://test-bucket")


@pytest.mark.parametrize(
    "store_uri,base_path,options",
    [
        ("s3://bucket", "/data", {}),
        ("gcs://bucket/path", "", {"project": "test"}),
        ("az://container", "prefix", {"account": "test", "key": "secret"}),
    ],
    ids=["s3_with_base", "gcs_no_base", "azure_with_options"],
)
def test_initialization_variations(
    mock_store: MagicMock, store_uri: str, base_path: str, options: dict[str, Any]
) -> None:
    """Test initialization with various configurations."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.from_url", return_value=mock_store) as mock_from_url:
            backend = ObStoreBackend(store_uri, base_path=base_path, **options)

            assert backend.store_uri == store_uri
            assert backend.base_path == base_path
            assert backend.store_options == options
            mock_from_url.assert_called_once_with(store_uri, **options)


# Path Resolution Tests
@pytest.mark.parametrize(
    "base_path,input_path,expected",
    [
        ("/data", "file.txt", "/data/file.txt"),
        ("/data", "/file.txt", "/data/file.txt"),
        ("/data/", "file.txt", "/data/file.txt"),
        ("", "file.txt", "file.txt"),
        ("", "/file.txt", "/file.txt"),
    ],
    ids=["with_base", "with_base_leading_slash", "trailing_base_slash", "empty_base", "empty_base_leading"],
)
def test_path_resolution(
    backend_with_mock_store: ObStoreBackend, base_path: str, input_path: str, expected: str
) -> None:
    """Test path resolution with various base paths."""
    backend = backend_with_mock_store
    backend.base_path = base_path
    assert backend._resolve_path(input_path) == expected


# Read Operations Tests
def test_read_bytes(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test reading bytes from storage."""
    backend = backend_with_mock_store

    result = backend.read_bytes("test.txt")

    assert result == b"test data"
    mock_store.get.assert_called_once_with("/data/test.txt")


def test_read_bytes_error(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test error handling in read_bytes."""
    backend = backend_with_mock_store
    mock_store.get.side_effect = Exception("Read failed")

    with pytest.raises(StorageOperationFailedError, match="Failed to read bytes from test.txt"):
        backend.read_bytes("test.txt")


def test_read_text(backend_with_mock_store: ObStoreBackend) -> None:
    """Test reading text from storage."""
    backend = backend_with_mock_store

    with patch.object(backend, "read_bytes", return_value=b"test text"):
        result = backend.read_text("test.txt", encoding="utf-8")

    assert result == "test text"


@pytest.mark.parametrize(
    "encoding,data,expected",
    [("utf-8", b"test text", "test text"), ("latin-1", b"test \xe9", "test é"), ("ascii", b"test", "test")],
    ids=["utf8", "latin1", "ascii"],
)
def test_read_text_encodings(
    backend_with_mock_store: ObStoreBackend, encoding: str, data: bytes, expected: str
) -> None:
    """Test reading text with different encodings."""
    backend = backend_with_mock_store

    with patch.object(backend, "read_bytes", return_value=data):
        result = backend.read_text("test.txt", encoding=encoding)

    assert result == expected


# Write Operations Tests
def test_write_bytes(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test writing bytes to storage."""
    backend = backend_with_mock_store

    backend.write_bytes("output.txt", b"test data")

    mock_store.put.assert_called_once_with("/data/output.txt", b"test data")


def test_write_bytes_error(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test error handling in write_bytes."""
    backend = backend_with_mock_store
    mock_store.put.side_effect = Exception("Write failed")

    with pytest.raises(StorageOperationFailedError, match="Failed to write bytes to output.txt"):
        backend.write_bytes("output.txt", b"test data")


def test_write_text(backend_with_mock_store: ObStoreBackend) -> None:
    """Test writing text to storage."""
    backend = backend_with_mock_store

    with patch.object(backend, "write_bytes") as mock_write:
        backend.write_text("output.txt", "test text", encoding="utf-8")

    mock_write.assert_called_once_with("output.txt", b"test text")


# List Operations Tests
def test_list_objects_recursive(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test listing objects recursively."""
    backend = backend_with_mock_store

    # Mock list items
    item1 = MagicMock()
    item1.path = "/data/file1.txt"
    item2 = MagicMock()
    item2.path = "/data/dir/file2.txt"
    mock_store.list.return_value = [item1, item2]

    result = backend.list_objects("", recursive=True)

    assert result == ["/data/dir/file2.txt", "/data/file1.txt"]  # Sorted
    mock_store.list.assert_called_once_with("/data")


def test_list_objects_non_recursive(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test listing objects non-recursively."""
    backend = backend_with_mock_store

    # Mock list items
    item1 = MagicMock()
    item1.path = "/data/file1.txt"
    item2 = MagicMock(spec=[])  # spec=[] prevents auto-creating attributes
    item2.key = "/data/file2.txt"  # Test key attribute fallback
    mock_store.list_with_delimiter.return_value = [item1, item2]

    result = backend.list_objects("", recursive=False)

    assert result == ["/data/file1.txt", "/data/file2.txt"]  # Sorted
    mock_store.list_with_delimiter.assert_called_once_with("/data")


def test_list_objects_with_prefix(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test listing objects with a prefix."""
    backend = backend_with_mock_store

    item = MagicMock()
    item.path = "/data/subdir/file.txt"
    mock_store.list.return_value = [item]

    result = backend.list_objects("subdir", recursive=True)

    assert result == ["/data/subdir/file.txt"]
    mock_store.list.assert_called_once_with("/data/subdir")


# Existence and Metadata Tests
def test_exists_true(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test checking if object exists."""
    backend = backend_with_mock_store

    assert backend.exists("test.txt") is True
    mock_store.head.assert_called_once_with("/data/test.txt")


def test_exists_false(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test checking if object doesn't exist."""
    backend = backend_with_mock_store
    mock_store.head.side_effect = Exception("Not found")

    assert backend.exists("missing.txt") is False
    mock_store.head.assert_called_once_with("/data/missing.txt")


def test_get_metadata(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test getting object metadata."""
    backend = backend_with_mock_store

    metadata = backend.get_metadata("file.txt")

    assert metadata["path"] == "/data/file.txt"
    assert metadata["exists"] is True
    assert metadata["size"] == 1024
    assert metadata["last_modified"] == "2024-01-01T00:00:00Z"
    assert metadata["e_tag"] == "abc123"
    mock_store.head.assert_called_once_with("/data/file.txt")


def test_get_metadata_with_custom_metadata(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test getting object metadata with custom metadata."""
    backend = backend_with_mock_store

    # Add custom metadata to mock
    mock_metadata = mock_store.head.return_value
    mock_metadata.metadata = {"custom": "value"}

    metadata = backend.get_metadata("file.txt")

    assert metadata["custom_metadata"] == {"custom": "value"}


def test_get_metadata_not_found(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test getting metadata for non-existent object."""
    backend = backend_with_mock_store
    mock_store.head.side_effect = Exception("Not found")

    metadata = backend.get_metadata("missing.txt")

    assert metadata["path"] == "/data/missing.txt"
    assert metadata["exists"] is False


# File Operations Tests
def test_delete(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test deleting an object."""
    backend = backend_with_mock_store

    backend.delete("unwanted.txt")
    mock_store.delete.assert_called_once_with("/data/unwanted.txt")


def test_copy_native(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test copying with native support."""
    backend = backend_with_mock_store
    mock_store.copy = MagicMock()

    backend.copy("source.txt", "dest.txt")
    mock_store.copy.assert_called_once_with("/data/source.txt", "/data/dest.txt")


def test_copy_fallback(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test copying when method is missing."""
    backend = backend_with_mock_store
    # Remove copy method to simulate missing functionality
    delattr(mock_store, "copy")

    # Without copy method, it should raise AttributeError wrapped in StorageOperationFailedError
    with pytest.raises(StorageOperationFailedError, match="Failed to copy"):
        backend.copy("source.txt", "dest.txt")


def test_move_native(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test moving with native support."""
    backend = backend_with_mock_store
    mock_store.rename = MagicMock()

    backend.move("old.txt", "new.txt")
    mock_store.rename.assert_called_once_with("/data/old.txt", "/data/new.txt")


def test_move_fallback(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test moving when method is missing."""
    backend = backend_with_mock_store
    # Remove rename method to simulate missing functionality
    delattr(mock_store, "rename")

    # Without rename method, it should raise AttributeError wrapped in StorageOperationFailedError
    with pytest.raises(StorageOperationFailedError, match="Failed to move"):
        backend.move("old.txt", "new.txt")


# Glob Operations Tests
def test_glob(backend_with_mock_store: ObStoreBackend) -> None:
    """Test glob pattern matching."""
    backend = backend_with_mock_store

    with patch.object(
        backend, "list_objects", return_value=["/data/file1.txt", "/data/file2.csv", "/data/dir/file3.txt"]
    ):
        result = backend.glob("*.txt")

        assert result == ["/data/file1.txt", "/data/dir/file3.txt"]


@pytest.mark.parametrize(
    "pattern,files,expected",
    [
        ("*.txt", ["file1.txt", "file2.csv"], ["file1.txt"]),
        ("dir/*.csv", ["dir/data.csv", "dir/file.txt", "other.csv"], ["dir/data.csv"]),
        (
            "**/*.parquet",
            ["a.parquet", "dir/b.parquet", "dir/sub/c.parquet"],
            ["a.parquet", "dir/b.parquet", "dir/sub/c.parquet"],
        ),
    ],
    ids=["simple_glob", "dir_glob", "recursive_glob"],
)
def test_glob_patterns(
    backend_with_mock_store: ObStoreBackend, pattern: str, files: list[str], expected: list[str]
) -> None:
    """Test various glob patterns."""
    backend = backend_with_mock_store
    # Add base path to files
    full_files = [f"/data/{f}" for f in files]
    expected_full = [f"/data/{f}" for f in expected]

    with patch.object(backend, "list_objects", return_value=full_files):
        result = backend.glob(pattern)
        assert sorted(result) == sorted(expected_full)


# Object Type Tests
def test_is_object(backend_with_mock_store: ObStoreBackend) -> None:
    """Test checking if path is an object."""
    backend = backend_with_mock_store

    with patch.object(backend, "exists", return_value=True):
        assert backend.is_object("file.txt") is True
        assert backend.is_object("directory/") is False

    with patch.object(backend, "exists", return_value=False):
        assert backend.is_object("missing.txt") is False


def test_is_path(backend_with_mock_store: ObStoreBackend) -> None:
    """Test checking if path is a directory."""
    backend = backend_with_mock_store

    # Test with trailing slash
    assert backend.is_path("directory/") is True

    # Test without trailing slash but has objects
    with patch.object(backend, "list_objects", return_value=["file1.txt"]):
        assert backend.is_path("directory") is True

    # Test without trailing slash and no objects
    with patch.object(backend, "list_objects", return_value=[]):
        assert backend.is_path("directory") is False


# Arrow Operations Tests
def test_read_arrow_native(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test reading Arrow table with native support."""
    backend = backend_with_mock_store
    mock_table = MagicMock()
    mock_store.read_arrow.return_value = mock_table

    result = backend.read_arrow("data.parquet")

    assert result == mock_table
    mock_store.read_arrow.assert_called_once_with("/data/data.parquet")


def test_read_arrow_fallback(backend_with_mock_store: ObStoreBackend) -> None:
    """Test reading Arrow table when method is missing."""
    backend = backend_with_mock_store
    # Remove the method to simulate missing functionality
    delattr(backend.store, "read_arrow")

    # Without read_arrow method, it should raise StorageOperationFailedError
    with pytest.raises(StorageOperationFailedError, match="Failed to read Arrow table"):
        backend.read_arrow("data.parquet")


def test_write_arrow_native(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test writing Arrow table with native support."""
    backend = backend_with_mock_store
    mock_table = MagicMock()

    backend.write_arrow("output.parquet", mock_table, compression="snappy")

    mock_store.write_arrow.assert_called_once_with("/data/output.parquet", mock_table, compression="snappy")


def test_write_arrow_fallback(backend_with_mock_store: ObStoreBackend) -> None:
    """Test writing Arrow table when method is missing."""
    backend = backend_with_mock_store
    # Remove the method to simulate missing functionality
    delattr(backend.store, "write_arrow")

    mock_table = MagicMock()
    # Without write_arrow method, it should raise StorageOperationFailedError
    with pytest.raises(StorageOperationFailedError, match="Failed to write Arrow table"):
        backend.write_arrow("output.parquet", mock_table)


def test_stream_arrow_native(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test streaming Arrow with native support."""
    backend = backend_with_mock_store
    mock_batch1 = MagicMock()
    mock_batch2 = MagicMock()
    mock_store.stream_arrow = MagicMock(return_value=[mock_batch1, mock_batch2])

    batches = list(backend.stream_arrow("*.parquet"))

    assert len(batches) == 2
    assert batches[0] == mock_batch1
    assert batches[1] == mock_batch2
    mock_store.stream_arrow.assert_called_once_with("/data/*.parquet")


def test_stream_arrow_fallback(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test streaming Arrow without native support."""
    backend = backend_with_mock_store
    delattr(mock_store, "stream_arrow")

    # Without stream_arrow method, it should raise StorageOperationFailedError
    with pytest.raises(StorageOperationFailedError, match="Failed to stream Arrow data"):
        list(backend.stream_arrow("*.parquet"))


# Async Operations Tests
@pytest.mark.asyncio
async def test_async_read_bytes(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test async read bytes operation."""
    backend = backend_with_mock_store
    async_result = MagicMock()
    async_result.bytes = MagicMock(return_value=b"async test data")
    mock_store.get_async = AsyncMock(return_value=async_result)

    result = await backend.read_bytes_async("test.txt")

    assert result == b"async test data"
    mock_store.get_async.assert_called_once_with("/data/test.txt")


@pytest.mark.asyncio
async def test_async_write_bytes(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test async write bytes operation."""
    backend = backend_with_mock_store

    await backend.write_bytes_async("output.txt", b"async data")

    mock_store.put_async.assert_called_once_with("/data/output.txt", b"async data")


@pytest.mark.asyncio
async def test_async_list_objects(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test async list objects operation."""
    backend = backend_with_mock_store
    item = MagicMock()
    item.path = "/data/file.txt"

    # Create an async generator for the mock
    async def async_list_items() -> Any:
        yield item

    mock_store.list_async = MagicMock(return_value=async_list_items())

    result = await backend.list_objects_async("", recursive=True)

    assert result == ["/data/file.txt"]
    mock_store.list_async.assert_called_once()


@pytest.mark.asyncio
async def test_async_exists(backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
    """Test async exists operation."""
    backend = backend_with_mock_store

    result = await backend.exists_async("test.txt")

    assert result is True
    mock_store.head_async.assert_called_once_with("/data/test.txt")


# Instrumentation Tests
def test_instrumentation_with_logging(caplog: LogCaptureFixture) -> None:
    """Test instrumentation with logging enabled."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        mock_store = MagicMock()
        mock_store.read_arrow = MagicMock()
        mock_store.write_arrow = MagicMock()

        with patch("obstore.store.from_url", return_value=mock_store):
            with caplog.at_level(logging.DEBUG):
                # Initialize backend with debug mode to trigger debug log
                ObStoreBackend("s3://test")

            # Should log about initialization
            assert any("ObStore backend initialized" in record.message for record in caplog.records)


def test_instrumentation_read_logging(backend_with_mock_store: ObStoreBackend, caplog: LogCaptureFixture) -> None:
    """Test read operation doesn't fail with logging enabled."""
    backend = backend_with_mock_store

    with caplog.at_level(logging.DEBUG):
        # Just ensure the operation doesn't fail
        result = backend.read_bytes("test.txt")

    # Verify the operation succeeded
    assert result == b"test data"


# Error Handling Tests
@pytest.mark.parametrize(
    "method_name,args,error_msg",
    [
        ("read_bytes", ("test.txt",), "Access denied"),
        ("write_bytes", ("test.txt", b"data"), "Quota exceeded"),
        ("delete", ("test.txt",), "Not found"),
        ("copy", ("src.txt", "dst.txt"), "Permission denied"),
        ("move", ("old.txt", "new.txt"), "Resource busy"),
    ],
    ids=["read_error", "write_error", "delete_error", "copy_error", "move_error"],
)
def test_error_propagation(
    backend_with_mock_store: ObStoreBackend, mock_store: MagicMock, method_name: str, args: tuple, error_msg: str
) -> None:
    """Test that errors are properly wrapped in StorageOperationFailedError."""
    backend = backend_with_mock_store

    # Get the underlying store method
    if method_name == "read_bytes":
        mock_method = mock_store.get
    elif method_name == "write_bytes":
        mock_method = mock_store.put
    elif method_name == "delete":
        mock_method = mock_store.delete
    elif method_name == "copy":
        mock_method = mock_store.copy
    elif method_name == "move":
        mock_method = mock_store.rename

    mock_method.side_effect = Exception(error_msg)  # pyright: ignore

    with pytest.raises(StorageOperationFailedError) as exc_info:
        getattr(backend, method_name)(*args)

    # The error message should describe the failed operation
    assert "Failed to" in str(exc_info.value)


# Edge Cases
def test_empty_base_path_operations(mock_store: MagicMock) -> None:
    """Test operations with empty base path."""
    with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
        with patch("obstore.store.from_url", return_value=mock_store):
            backend = ObStoreBackend("s3://bucket", base_path="")
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
