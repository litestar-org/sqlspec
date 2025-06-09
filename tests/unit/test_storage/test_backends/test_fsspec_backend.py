"""Comprehensive tests for FSSpecBackend."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.config import InstrumentationConfig
from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError
from sqlspec.storage.backends.fsspec import FSSpecBackend


class TestFSSpecBackend:
    """Test the FSSpecBackend class."""

    @pytest.fixture
    def mock_fs(self) -> MagicMock:
        """Create a mock filesystem."""
        fs = MagicMock()
        fs.protocol = "file"
        fs.cat.return_value = b"test data"
        fs.exists.return_value = True
        fs.info.return_value = {"size": 1024, "type": "file"}
        fs.glob.return_value = ["file1.txt", "file2.csv"]
        fs.isdir.return_value = False
        return fs

    @pytest.fixture
    def backend_with_mock_fs(self, mock_fs: MagicMock) -> FSSpecBackend:
        """Create backend with mocked filesystem."""
        with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
            return FSSpecBackend(mock_fs, base_path="/base")

    def test_initialization_with_filesystem_instance(self, mock_fs: MagicMock) -> None:
        """Test initialization with filesystem instance."""
        with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
            backend = FSSpecBackend(mock_fs, base_path="/base/path")

            assert backend.fs == mock_fs
            assert backend.base_path == "/base/path"
            assert backend.protocol == "file"
            assert backend.backend_type == "fsspec"

    def test_initialization_with_uri_string(self) -> None:
        """Test initialization with URI string."""
        with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
            with patch("fsspec.filesystem") as mock_filesystem:
                mock_fs = MagicMock()
                mock_filesystem.return_value = mock_fs

                backend = FSSpecBackend("s3://bucket", base_path="/data")

                mock_filesystem.assert_called_once_with("s3")
                assert backend.fs == mock_fs
                assert backend.protocol == "s3"
                assert backend.base_path == "/data"

    def test_missing_fsspec_dependency(self) -> None:
        """Test error when fsspec is not installed."""
        with patch("sqlspec.storage.backends.fsspec.FSSPEC_INSTALLED", False):
            with pytest.raises(MissingDependencyError, match="fsspec"):
                FSSpecBackend("file:///tmp")

    def test_path_resolution(self, backend_with_mock_fs: FSSpecBackend) -> None:
        """Test path resolution with base path."""
        backend = backend_with_mock_fs

        # Test with leading slash
        assert backend._resolve_path("/file.txt") == "/base/file.txt"

        # Test without leading slash
        assert backend._resolve_path("file.txt") == "/base/file.txt"

        # Test with empty base path
        backend.base_path = ""
        assert backend._resolve_path("file.txt") == "file.txt"

    def test_read_bytes(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test reading bytes from storage."""
        backend = backend_with_mock_fs

        result = backend.read_bytes("test.txt")

        assert result == b"test data"
        mock_fs.cat.assert_called_once_with("/base/test.txt")

    def test_read_bytes_error(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test error handling in read_bytes."""
        backend = backend_with_mock_fs
        mock_fs.cat.side_effect = Exception("Read failed")

        with pytest.raises(StorageOperationFailedError, match="Failed to read bytes from test.txt"):
            backend.read_bytes("test.txt")

    def test_write_bytes(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test writing bytes to storage."""
        backend = backend_with_mock_fs
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__.return_value = mock_file

        backend.write_bytes("output.txt", b"test data")

        mock_fs.open.assert_called_once_with("/base/output.txt", mode="wb")
        mock_file.write.assert_called_once_with(b"test data")

    def test_write_bytes_error(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test error handling in write_bytes."""
        backend = backend_with_mock_fs
        mock_fs.open.side_effect = Exception("Write failed")

        with pytest.raises(StorageOperationFailedError, match="Failed to write bytes to output.txt"):
            backend.write_bytes("output.txt", b"test data")

    def test_read_text(self, backend_with_mock_fs: FSSpecBackend) -> None:
        """Test reading text from storage."""
        backend = backend_with_mock_fs

        with patch.object(backend, "read_bytes", return_value=b"test text"):
            result = backend.read_text("test.txt", encoding="utf-8")

        assert result == "test text"

    def test_write_text(self, backend_with_mock_fs: FSSpecBackend) -> None:
        """Test writing text to storage."""
        backend = backend_with_mock_fs

        with patch.object(backend, "write_bytes") as mock_write:
            backend.write_text("output.txt", "test text", encoding="utf-8")

        mock_write.assert_called_once_with("output.txt", b"test text")

    def test_list_objects_recursive(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test listing objects recursively."""
        backend = backend_with_mock_fs
        mock_fs.glob.return_value = ["/base/dir/file1.txt", "/base/file2.csv"]
        mock_fs.isdir.return_value = False

        result = backend.list_objects("dir", recursive=True)

        assert result == ["/base/dir/file1.txt", "/base/file2.csv"]
        mock_fs.glob.assert_called_once_with("/base/dir/**")

    def test_list_objects_non_recursive(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test listing objects non-recursively."""
        backend = backend_with_mock_fs
        mock_fs.glob.return_value = ["/base/dir/file1.txt", "/base/dir/subdir/"]

        # Mock isdir to return True for directories
        def is_dir(path: str) -> bool:
            return path.endswith("/")

        mock_fs.isdir.side_effect = is_dir

        result = backend.list_objects("dir", recursive=False)

        assert result == ["/base/dir/file1.txt"]  # Directories filtered out
        mock_fs.glob.assert_called_once_with("/base/dir/*")

    def test_exists(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test checking if object exists."""
        backend = backend_with_mock_fs

        assert backend.exists("test.txt") is True
        mock_fs.exists.assert_called_once_with("/base/test.txt")

        mock_fs.exists.return_value = False
        assert backend.exists("missing.txt") is False

    def test_delete(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test deleting an object."""
        backend = backend_with_mock_fs

        backend.delete("unwanted.txt")
        mock_fs.rm.assert_called_once_with("/base/unwanted.txt")

    def test_copy(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test copying an object."""
        backend = backend_with_mock_fs

        backend.copy("source.txt", "dest.txt")
        mock_fs.copy.assert_called_once_with("/base/source.txt", "/base/dest.txt")

    def test_move(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test moving an object."""
        backend = backend_with_mock_fs

        backend.move("old.txt", "new.txt")
        mock_fs.mv.assert_called_once_with("/base/old.txt", "/base/new.txt")

    def test_glob(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test glob pattern matching."""
        backend = backend_with_mock_fs
        mock_fs.glob.return_value = ["/base/file1.txt", "/base/file2.txt", "/base/dir/"]
        mock_fs.isdir.side_effect = lambda p: p.endswith("/")

        result = backend.glob("*.txt")

        assert result == ["/base/file1.txt", "/base/file2.txt"]  # Directory filtered out
        mock_fs.glob.assert_called_once_with("/base/*.txt")

    def test_is_object(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test checking if path is an object."""
        backend = backend_with_mock_fs
        mock_fs.exists.return_value = True
        mock_fs.isdir.return_value = False

        assert backend.is_object("file.txt") is True

        # Test directory
        mock_fs.isdir.return_value = True
        assert backend.is_object("directory/") is False

        # Test non-existent
        mock_fs.exists.return_value = False
        assert backend.is_object("missing.txt") is False

    def test_is_path(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test checking if path is a directory."""
        backend = backend_with_mock_fs
        mock_fs.isdir.return_value = True

        assert backend.is_path("directory/") is True

        mock_fs.isdir.return_value = False
        assert backend.is_path("file.txt") is False

    def test_get_metadata(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test getting object metadata."""
        backend = backend_with_mock_fs

        # Test with dict info
        mock_fs.info.return_value = {"size": 2048, "type": "file", "mtime": 1234567890}
        metadata = backend.get_metadata("file.txt")

        assert metadata["size"] == 2048
        assert metadata["type"] == "file"
        assert metadata["mtime"] == 1234567890

        # Test with object info
        mock_info = MagicMock()
        mock_info.size = 4096
        mock_info.type = "directory"
        mock_fs.info.return_value = mock_info

        metadata = backend.get_metadata("dir/")
        assert metadata["size"] == 4096
        assert metadata["type"] == "directory"

    def test_read_arrow(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test reading Arrow table."""
        backend = backend_with_mock_fs

        with patch("sqlspec.typing.PYARROW_INSTALLED", True):
            mock_table = MagicMock()
            mock_file = MagicMock()
            mock_fs.open.return_value.__enter__.return_value = mock_file

            with patch("pyarrow.parquet.read_table", return_value=mock_table) as mock_read:
                result = backend.read_arrow("data.parquet")

                assert result == mock_table
                mock_fs.open.assert_called_once_with("/base/data.parquet", mode="rb")
                mock_read.assert_called_once_with(mock_file)

    def test_read_arrow_missing_pyarrow(self, backend_with_mock_fs: FSSpecBackend) -> None:
        """Test error when pyarrow is not installed."""
        backend = backend_with_mock_fs

        with patch("pyarrow.parquet.read_table", side_effect=ImportError):
            with pytest.raises(MissingDependencyError, match="pyarrow"):
                backend.read_arrow("data.parquet")

    def test_write_arrow(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test writing Arrow table."""
        backend = backend_with_mock_fs

        with patch("sqlspec.typing.PYARROW_INSTALLED", True):
            mock_table = MagicMock()
            mock_file = MagicMock()
            mock_fs.open.return_value.__enter__.return_value = mock_file

            with patch("pyarrow.parquet.write_table") as mock_write:
                backend.write_arrow("output.parquet", mock_table, compression="snappy")

                mock_fs.open.assert_called_once_with("/base/output.parquet", mode="wb")
                mock_write.assert_called_once_with(mock_table, mock_file, compression="snappy")

    def test_stream_arrow(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test streaming Arrow record batches."""
        backend = backend_with_mock_fs

        with patch("sqlspec.typing.PYARROW_INSTALLED", True):
            # Mock glob to return matching files
            with patch.object(backend, "glob", return_value=["data1.parquet", "data2.parquet"]):
                # Mock file reading
                mock_batch1 = MagicMock()
                mock_batch2 = MagicMock()

                with patch.object(backend, "_stream_file_batches") as mock_stream:
                    mock_stream.side_effect = [[mock_batch1], [mock_batch2]]

                    batches = list(backend.stream_arrow("*.parquet"))

                    assert len(batches) == 2
                    assert batches[0] == mock_batch1
                    assert batches[1] == mock_batch2

    def test_get_signed_url_not_supported(self, backend_with_mock_fs: FSSpecBackend) -> None:
        """Test that signed URL generation is not supported."""
        backend = backend_with_mock_fs

        with pytest.raises(NotImplementedError, match="Signed URL generation not supported"):
            backend.get_signed_url("file.txt")

    @pytest.mark.asyncio
    async def test_async_read_bytes_with_native_support(
        self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock
    ) -> None:
        """Test async read with native fsspec async support."""
        backend = backend_with_mock_fs
        mock_fs._cat = AsyncMock(return_value=b"async data")

        result = await backend.read_bytes_async("test.txt")

        assert result == b"async data"
        mock_fs._cat.assert_called_once_with("/base/test.txt")

    @pytest.mark.asyncio
    async def test_async_read_bytes_fallback(self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
        """Test async read fallback to sync."""
        backend = backend_with_mock_fs
        # Remove _cat to trigger fallback
        delattr(mock_fs, "_cat")

        with patch.object(backend, "_read_bytes", return_value=b"sync data"):
            result = await backend.read_bytes_async("test.txt")

        assert result == b"sync data"

    @pytest.mark.asyncio
    async def test_async_write_bytes_with_native_support(
        self, backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock
    ) -> None:
        """Test async write with native fsspec async support."""
        backend = backend_with_mock_fs
        mock_fs._pipe = AsyncMock()

        await backend.write_bytes_async("test.txt", b"async data")

        mock_fs._pipe.assert_called_once_with("/base/test.txt", b"async data")

    @pytest.mark.asyncio
    async def test_async_operations_with_wrap(self, backend_with_mock_fs: FSSpecBackend) -> None:
        """Test async operations that wrap sync methods."""
        backend = backend_with_mock_fs

        # Test read_text_async
        with patch.object(backend, "_read_text", return_value="test text"):
            result = await backend.read_text_async("test.txt")
            assert result == "test text"

        # Test write_text_async
        with patch.object(backend, "_write_text") as mock_write:
            await backend.write_text_async("test.txt", "test text")
            mock_write.assert_called_once_with("test.txt", "test text", "utf-8")

        # Test list_objects_async
        with patch.object(backend, "_list_objects", return_value=["file1.txt", "file2.txt"]):
            result = await backend.list_objects_async()
            assert result == ["file1.txt", "file2.txt"]

    def test_from_config(self) -> None:
        """Test creating backend from config dict."""
        with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
            with patch("fsspec.filesystem") as mock_filesystem:
                mock_fs = MagicMock()
                mock_filesystem.return_value = mock_fs

                config = {"protocol": "s3", "fs_config": {"key": "test", "secret": "test"}, "base_path": "/data"}

                backend = FSSpecBackend.from_config(config)

                mock_filesystem.assert_called_once_with("s3", key="test", secret="test")
                assert backend.fs == mock_fs
                assert backend.base_path == "/data"

    def test_instrumentation_logging(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that operations are properly instrumented."""
        with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
            config = InstrumentationConfig(log_service_operations=True, debug_mode=True)
            mock_fs = MagicMock()
            mock_fs.protocol = "s3"
            mock_fs.cat.return_value = b"test data"

            backend = FSSpecBackend(mock_fs, instrumentation_config=config)

            with caplog.at_level(logging.DEBUG):
                result = backend.read_bytes("test.txt")

            assert result == b"test data"
            # Should have debug and info logs
            assert any("Reading bytes from test.txt" in record.message for record in caplog.records)
            assert any("Read 9 bytes from test.txt" in record.message for record in caplog.records)
