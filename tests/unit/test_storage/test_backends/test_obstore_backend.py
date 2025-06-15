"""Comprehensive tests for ObStoreBackend."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError
from sqlspec.storage.backends.obstore import ObStoreBackend


class TestObStoreBackend:
    """Test the ObStoreBackend class."""

    @pytest.fixture
    def mock_store(self) -> MagicMock:
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
    def backend_with_mock_store(self, mock_store: MagicMock) -> ObStoreBackend:
        """Create backend with mocked obstore."""
        with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
            with patch("obstore.store.from_url", return_value=mock_store):
                backend = ObStoreBackend("s3://test-bucket", base_path="/data")
                backend.store = mock_store  # Ensure our mock is used
                return backend

    def test_initialization_success(self, mock_store: MagicMock) -> None:
        """Test successful initialization."""
        with patch("sqlspec.typing.OBSTORE_INSTALLED", True):
            with patch("obstore.store.from_url", return_value=mock_store):
                backend = ObStoreBackend("s3://test-bucket", base_path="/data", region="us-east-1")

                assert backend.store_uri == "s3://test-bucket"
                assert backend.base_path == "/data"
                assert backend.store_options == {"region": "us-east-1"}
                assert backend.backend_type == "obstore"

    def test_initialization_without_obstore(self) -> None:
        """Test error when obstore is not installed."""
        with patch("sqlspec.storage.backends.obstore.OBSTORE_INSTALLED", False):
            with pytest.raises(MissingDependencyError, match="obstore"):
                ObStoreBackend("s3://test-bucket")

    def test_initialization_error(self) -> None:
        """Test error during initialization."""
        with patch("sqlspec.storage.backends.obstore.OBSTORE_INSTALLED", True):
            with patch("obstore.store.from_url", side_effect=Exception("Connection failed")):
                with pytest.raises(StorageOperationFailedError, match="Failed to initialize obstore backend"):
                    ObStoreBackend("s3://test-bucket")

    def test_path_resolution(self, backend_with_mock_store: ObStoreBackend) -> None:
        """Test path resolution with base path."""
        backend = backend_with_mock_store

        assert backend._resolve_path("file.txt") == "/data/file.txt"
        assert backend._resolve_path("/file.txt") == "/data/file.txt"

        # Test with empty base path
        backend.base_path = ""
        assert backend._resolve_path("file.txt") == "file.txt"

    def test_read_bytes(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test reading bytes from storage."""
        backend = backend_with_mock_store

        result = backend.read_bytes("test.txt")

        assert result == b"test data"
        mock_store.get.assert_called_once_with("/data/test.txt")

    def test_read_bytes_error(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test error handling in read_bytes."""
        backend = backend_with_mock_store
        mock_store.get.side_effect = Exception("Read failed")

        with pytest.raises(StorageOperationFailedError, match="Failed to read bytes from test.txt"):
            backend.read_bytes("test.txt")

    def test_write_bytes(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test writing bytes to storage."""
        backend = backend_with_mock_store

        backend.write_bytes("output.txt", b"test data")

        mock_store.put.assert_called_once_with("/data/output.txt", b"test data")

    def test_write_bytes_error(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test error handling in write_bytes."""
        backend = backend_with_mock_store
        mock_store.put.side_effect = Exception("Write failed")

        with pytest.raises(StorageOperationFailedError, match="Failed to write bytes to output.txt"):
            backend.write_bytes("output.txt", b"test data")

    def test_read_text(self, backend_with_mock_store: ObStoreBackend) -> None:
        """Test reading text from storage."""
        backend = backend_with_mock_store

        with patch.object(backend, "_read_bytes", return_value=b"test text"):
            result = backend.read_text("test.txt", encoding="utf-8")

        assert result == "test text"

    def test_write_text(self, backend_with_mock_store: ObStoreBackend) -> None:
        """Test writing text to storage."""
        backend = backend_with_mock_store

        with patch.object(backend, "_write_bytes") as mock_write:
            backend.write_text("output.txt", "test text", encoding="utf-8")

        mock_write.assert_called_once_with("output.txt", b"test text")

    def test_list_objects_recursive(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
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

    def test_list_objects_non_recursive(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
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

    def test_exists_true(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test checking if object exists."""
        backend = backend_with_mock_store

        assert backend.exists("test.txt") is True
        mock_store.head.assert_called_once_with("/data/test.txt")

    def test_exists_false(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test checking if object doesn't exist."""
        backend = backend_with_mock_store
        mock_store.head.side_effect = Exception("Not found")

        assert backend.exists("missing.txt") is False
        mock_store.head.assert_called_once_with("/data/missing.txt")

    def test_delete(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test deleting an object."""
        backend = backend_with_mock_store

        backend.delete("unwanted.txt")
        mock_store.delete.assert_called_once_with("/data/unwanted.txt")

    def test_copy_native(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test copying with native support."""
        backend = backend_with_mock_store
        mock_store.copy = MagicMock()

        backend.copy("source.txt", "dest.txt")
        mock_store.copy.assert_called_once_with("/data/source.txt", "/data/dest.txt")

    def test_copy_fallback(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test copying when method is missing."""
        backend = backend_with_mock_store
        # Remove copy method to simulate missing functionality
        delattr(mock_store, "copy")

        # Without copy method, it should raise AttributeError wrapped in StorageOperationFailedError
        with pytest.raises(StorageOperationFailedError, match="Failed to copy"):
            backend.copy("source.txt", "dest.txt")

    def test_move_native(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test moving with native support."""
        backend = backend_with_mock_store
        mock_store.rename = MagicMock()

        backend.move("old.txt", "new.txt")
        mock_store.rename.assert_called_once_with("/data/old.txt", "/data/new.txt")

    def test_move_fallback(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test moving when method is missing."""
        backend = backend_with_mock_store
        # Remove rename method to simulate missing functionality
        delattr(mock_store, "rename")

        # Without rename method, it should raise AttributeError wrapped in StorageOperationFailedError
        with pytest.raises(StorageOperationFailedError, match="Failed to move"):
            backend.move("old.txt", "new.txt")

    def test_glob(self, backend_with_mock_store: ObStoreBackend) -> None:
        """Test glob pattern matching."""
        backend = backend_with_mock_store

        with patch.object(
            backend, "_list_objects", return_value=["/data/file1.txt", "/data/file2.csv", "/data/dir/file3.txt"]
        ):
            result = backend.glob("*.txt")

            assert result == ["/data/file1.txt", "/data/dir/file3.txt"]

    def test_get_metadata(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test getting object metadata."""
        backend = backend_with_mock_store

        metadata = backend.get_metadata("file.txt")

        assert metadata["path"] == "/data/file.txt"
        assert metadata["exists"] is True
        assert metadata["size"] == 1024
        assert metadata["last_modified"] == "2024-01-01T00:00:00Z"
        assert metadata["e_tag"] == "abc123"
        mock_store.head.assert_called_once_with("/data/file.txt")

    def test_get_metadata_with_custom_metadata(
        self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock
    ) -> None:
        """Test getting object metadata with custom metadata."""
        backend = backend_with_mock_store

        # Add custom metadata to mock
        mock_metadata = mock_store.head.return_value
        mock_metadata.metadata = {"custom": "value"}

        metadata = backend.get_metadata("file.txt")

        assert metadata["custom_metadata"] == {"custom": "value"}

    def test_is_object(self, backend_with_mock_store: ObStoreBackend) -> None:
        """Test checking if path is an object."""
        backend = backend_with_mock_store

        with patch.object(backend, "_exists", return_value=True):
            assert backend.is_object("file.txt") is True
            assert backend.is_object("directory/") is False

        with patch.object(backend, "_exists", return_value=False):
            assert backend.is_object("missing.txt") is False

    def test_is_path(self, backend_with_mock_store: ObStoreBackend) -> None:
        """Test checking if path is a directory."""
        backend = backend_with_mock_store

        # Test with trailing slash
        assert backend.is_path("directory/") is True

        # Test without trailing slash but has objects
        with patch.object(backend, "_list_objects", return_value=["file1.txt"]):
            assert backend.is_path("directory") is True

        # Test without trailing slash and no objects
        with patch.object(backend, "_list_objects", return_value=[]):
            assert backend.is_path("directory") is False

    def test_read_arrow_native(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test reading Arrow table with native support."""
        backend = backend_with_mock_store
        mock_table = MagicMock()
        mock_store.read_arrow.return_value = mock_table

        result = backend.read_arrow("data.parquet")

        assert result == mock_table
        mock_store.read_arrow.assert_called_once_with("/data/data.parquet")

    def test_read_arrow_fallback(self, backend_with_mock_store: ObStoreBackend) -> None:
        """Test reading Arrow table when method is missing."""
        backend = backend_with_mock_store
        # Remove the method to simulate missing functionality
        delattr(backend.store, "read_arrow")

        # Without read_arrow method, it should raise StorageOperationFailedError
        with pytest.raises(StorageOperationFailedError, match="Failed to read Arrow table"):
            backend.read_arrow("data.parquet")

    def test_write_arrow_native(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test writing Arrow table with native support."""
        backend = backend_with_mock_store
        mock_table = MagicMock()

        backend.write_arrow("output.parquet", mock_table, compression="snappy")

        mock_store.write_arrow.assert_called_once_with("/data/output.parquet", mock_table, compression="snappy")

    def test_write_arrow_fallback(self, backend_with_mock_store: ObStoreBackend) -> None:
        """Test writing Arrow table when method is missing."""
        backend = backend_with_mock_store
        # Remove the method to simulate missing functionality
        delattr(backend.store, "write_arrow")

        mock_table = MagicMock()
        # Without write_arrow method, it should raise StorageOperationFailedError
        with pytest.raises(StorageOperationFailedError, match="Failed to write Arrow table"):
            backend.write_arrow("output.parquet", mock_table)

    def test_stream_arrow_native(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
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

    def test_stream_arrow_fallback(self, backend_with_mock_store: ObStoreBackend, mock_store: MagicMock) -> None:
        """Test streaming Arrow without native support."""
        backend = backend_with_mock_store
        delattr(mock_store, "stream_arrow")

        # Without stream_arrow method, it should raise StorageOperationFailedError
        with pytest.raises(StorageOperationFailedError, match="Failed to stream Arrow data"):
            list(backend.stream_arrow("*.parquet"))

    def test_instrumentation_with_logging(self, caplog: pytest.LogCaptureFixture) -> None:
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
