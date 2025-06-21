"""Unit tests for FSSpecBackend.

This module tests the FSSpecBackend class including:
- Initialization with filesystem instance and URI string
- Path resolution with base paths
- Read/write operations (bytes and text)
- Object listing and glob patterns
- Object existence and metadata
- Copy/move/delete operations
- Arrow table operations
- Async operation wrappers
- Error handling and dependency checks
- Instrumentation and logging
"""

import logging
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest
from pytest import LogCaptureFixture

from sqlspec.exceptions import MissingDependencyError
from sqlspec.storage.backends.fsspec import FSSpecBackend

if TYPE_CHECKING:
    pass


# Test Fixtures
@pytest.fixture
def mock_fs() -> MagicMock:
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
def backend_with_mock_fs(mock_fs: MagicMock) -> FSSpecBackend:
    """Create backend with mocked filesystem."""
    with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
        return FSSpecBackend(mock_fs, base_path="/base")


# Initialization Tests
def test_initialization_with_filesystem_instance(mock_fs: MagicMock) -> None:
    """Test initialization with filesystem instance."""
    with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
        backend = FSSpecBackend(mock_fs, base_path="/base/path")

        assert backend.fs == mock_fs
        assert backend.base_path == "/base/path"
        assert backend.protocol == "file"
        assert backend.backend_type == "fsspec"


def test_initialization_with_uri_string() -> None:
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


def test_missing_fsspec_dependency() -> None:
    """Test error when fsspec is not installed."""
    with patch("sqlspec.storage.backends.fsspec.FSSPEC_INSTALLED", False):
        with pytest.raises(MissingDependencyError, match="fsspec"):
            FSSpecBackend("file:///tmp")


# Path Resolution Tests
@pytest.mark.parametrize(
    "base_path,input_path,expected",
    [
        ("/base", "/file.txt", "/base/file.txt"),
        ("/base", "file.txt", "/base/file.txt"),
        ("/base/", "/file.txt", "/base/file.txt"),
        ("/base/", "file.txt", "/base/file.txt"),
        ("", "file.txt", "file.txt"),
        ("", "/file.txt", "/file.txt"),
    ],
    ids=[
        "leading_slash",
        "no_leading_slash",
        "trailing_base_slash",
        "trailing_base_no_leading",
        "empty_base",
        "empty_base_leading",
    ],
)
def test_path_resolution(backend_with_mock_fs: FSSpecBackend, base_path: str, input_path: str, expected: str) -> None:
    """Test path resolution with various base paths."""
    backend = backend_with_mock_fs
    backend.base_path = base_path
    assert backend._resolve_path(input_path) == expected


# Read Operations Tests
def test_read_bytes(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test reading bytes from storage."""
    backend = backend_with_mock_fs

    result = backend.read_bytes("test.txt")

    assert result == b"test data"
    mock_fs.cat.assert_called_once_with("/base/test.txt")


def test_read_bytes_error(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test error handling in read_bytes."""
    backend = backend_with_mock_fs
    mock_fs.cat.side_effect = Exception("Read failed")

    with pytest.raises(Exception, match="Read failed"):
        backend.read_bytes("test.txt")


def test_read_text(backend_with_mock_fs: FSSpecBackend) -> None:
    """Test reading text from storage."""
    backend = backend_with_mock_fs

    with patch.object(backend, "read_bytes", return_value=b"test text"):
        result = backend.read_text("test.txt", encoding="utf-8")

    assert result == "test text"


@pytest.mark.parametrize(
    "encoding,data,expected",
    [("utf-8", b"test text", "test text"), ("latin-1", b"test \xe9", "test é"), ("ascii", b"test", "test")],
    ids=["utf8", "latin1", "ascii"],
)
def test_read_text_encodings(backend_with_mock_fs: FSSpecBackend, encoding: str, data: bytes, expected: str) -> None:
    """Test reading text with different encodings."""
    backend = backend_with_mock_fs

    with patch.object(backend, "read_bytes", return_value=data):
        result = backend.read_text("test.txt", encoding=encoding)

    assert result == expected


# Write Operations Tests
def test_write_bytes(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test writing bytes to storage."""
    backend = backend_with_mock_fs
    mock_file = MagicMock()
    mock_fs.open.return_value.__enter__.return_value = mock_file

    backend.write_bytes("output.txt", b"test data")

    mock_fs.open.assert_called_once_with("/base/output.txt", mode="wb")
    mock_file.write.assert_called_once_with(b"test data")


def test_write_bytes_error(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test error handling in write_bytes."""
    backend = backend_with_mock_fs
    mock_fs.open.side_effect = Exception("Write failed")

    with pytest.raises(Exception, match="Write failed"):
        backend.write_bytes("output.txt", b"test data")


def test_write_text(backend_with_mock_fs: FSSpecBackend) -> None:
    """Test writing text to storage."""
    backend = backend_with_mock_fs

    with patch.object(backend, "write_bytes") as mock_write:
        backend.write_text("output.txt", "test text", encoding="utf-8")

    mock_write.assert_called_once_with("output.txt", b"test text")


# List Operations Tests
def test_list_objects_recursive(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test listing objects recursively."""
    backend = backend_with_mock_fs
    mock_fs.glob.return_value = ["/base/dir/file1.txt", "/base/file2.csv"]
    mock_fs.isdir.return_value = False

    result = backend.list_objects("dir", recursive=True)

    assert result == ["/base/dir/file1.txt", "/base/file2.csv"]
    mock_fs.glob.assert_called_once_with("/base/dir/**")


def test_list_objects_non_recursive(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
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


@pytest.mark.parametrize(
    "pattern,expected_glob",
    [("*.txt", "/base/*.txt"), ("dir/*.csv", "/base/dir/*.csv"), ("**/*.parquet", "/base/**/*.parquet")],
    ids=["simple_glob", "dir_glob", "recursive_glob"],
)
def test_glob_patterns(
    backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock, pattern: str, expected_glob: str
) -> None:
    """Test glob pattern matching."""
    backend = backend_with_mock_fs
    mock_fs.glob.return_value = ["/base/file1.txt", "/base/file2.txt"]
    mock_fs.isdir.return_value = False

    result = backend.glob(pattern)

    mock_fs.glob.assert_called_once_with(expected_glob)
    assert result == ["/base/file1.txt", "/base/file2.txt"]


# Existence and Metadata Tests
def test_exists(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test checking if object exists."""
    backend = backend_with_mock_fs

    assert backend.exists("test.txt") is True
    mock_fs.exists.assert_called_once_with("/base/test.txt")

    mock_fs.exists.return_value = False
    assert backend.exists("missing.txt") is False


def test_is_object(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
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


def test_is_path(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test checking if path is a directory."""
    backend = backend_with_mock_fs
    mock_fs.isdir.return_value = True

    assert backend.is_path("directory/") is True

    mock_fs.isdir.return_value = False
    assert backend.is_path("file.txt") is False


def test_get_metadata_dict(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test getting object metadata from dict info."""
    backend = backend_with_mock_fs

    mock_fs.info.return_value = {"size": 2048, "type": "file", "mtime": 1234567890}
    metadata = backend.get_metadata("file.txt")

    assert metadata["size"] == 2048
    assert metadata["type"] == "file"
    assert metadata["mtime"] == 1234567890
    mock_fs.info.assert_called_once_with("/base/file.txt")


def test_get_metadata_object(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test getting object metadata from object info."""
    backend = backend_with_mock_fs

    mock_info = MagicMock()
    mock_info.size = 4096
    mock_info.type = "directory"
    mock_fs.info.return_value = mock_info

    metadata = backend.get_metadata("dir/")
    assert metadata["size"] == 4096
    assert metadata["type"] == "directory"


# File Operations Tests
def test_delete(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test deleting an object."""
    backend = backend_with_mock_fs

    backend.delete("unwanted.txt")
    mock_fs.rm.assert_called_once_with("/base/unwanted.txt")


def test_copy(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test copying an object."""
    backend = backend_with_mock_fs

    backend.copy("source.txt", "dest.txt")
    mock_fs.copy.assert_called_once_with("/base/source.txt", "/base/dest.txt")


def test_move(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test moving an object."""
    backend = backend_with_mock_fs

    backend.move("old.txt", "new.txt")
    mock_fs.mv.assert_called_once_with("/base/old.txt", "/base/new.txt")


# Arrow Operations Tests
def test_read_arrow(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
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


def test_read_arrow_missing_pyarrow(backend_with_mock_fs: FSSpecBackend) -> None:
    """Test error when pyarrow is not installed."""
    backend = backend_with_mock_fs

    with patch("sqlspec.storage.backends.fsspec.PYARROW_INSTALLED", False):
        with pytest.raises(MissingDependencyError, match="pyarrow"):
            backend.read_arrow("data.parquet")


def test_write_arrow(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
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


def test_stream_arrow(backend_with_mock_fs: FSSpecBackend) -> None:
    """Test streaming Arrow record batches."""
    backend = backend_with_mock_fs

    with patch("sqlspec.typing.PYARROW_INSTALLED", True):
        # Mock glob to return matching files
        with patch.object(backend, "glob", return_value=["data1.parquet", "data2.parquet"]):
            # Mock file operations
            mock_file1 = MagicMock()
            mock_file2 = MagicMock()
            backend.fs.open = MagicMock(side_effect=[mock_file1, mock_file2])

            # Mock ParquetFile
            with patch("pyarrow.parquet.ParquetFile") as mock_pq_file:
                mock_pq1 = MagicMock()
                mock_pq2 = MagicMock()
                mock_batch1 = MagicMock()
                mock_batch2 = MagicMock()

                mock_pq1.iter_batches.return_value = [mock_batch1]
                mock_pq2.iter_batches.return_value = [mock_batch2]

                mock_pq_file.side_effect = [mock_pq1, mock_pq2]

                batches = list(backend.stream_arrow("*.parquet"))

                assert len(batches) == 2
                assert batches[0] == mock_batch1
                assert batches[1] == mock_batch2


# Async Operations Tests
@pytest.mark.asyncio
async def test_async_read_bytes_with_sync_wrapper(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test async read using reliable sync-to-async wrapper."""
    backend = backend_with_mock_fs

    result = await backend.read_bytes_async("test.txt")

    assert result == b"test data"  # Uses sync implementation
    mock_fs.cat.assert_called_once_with("/base/test.txt")


@pytest.mark.asyncio
async def test_async_write_bytes_with_sync_wrapper(backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock) -> None:
    """Test async write using reliable sync-to-async wrapper."""
    backend = backend_with_mock_fs
    mock_file = MagicMock()
    mock_fs.open.return_value.__enter__.return_value = mock_file

    await backend.write_bytes_async("test.txt", b"async data")

    mock_fs.open.assert_called_once_with("/base/test.txt", mode="wb")
    mock_file.write.assert_called_once_with(b"async data")


@pytest.mark.asyncio
async def test_async_operations_with_wrap(backend_with_mock_fs: FSSpecBackend) -> None:
    """Test async operations that wrap sync methods."""
    backend = backend_with_mock_fs

    # Test read_text_async
    with patch.object(backend, "read_text", return_value="test text"):
        result = await backend.read_text_async("test.txt")
        assert result == "test text"

    # Test write_text_async
    with patch.object(backend, "write_text") as mock_write:
        await backend.write_text_async("test.txt", "test text")
        mock_write.assert_called_once_with("test.txt", "test text", "utf-8")

    # Test list_objects_async
    with patch.object(backend, "list_objects", return_value=["file1.txt", "file2.txt"]):
        result = await backend.list_objects_async()
        assert result == ["file1.txt", "file2.txt"]  # type: ignore[comparison-overlap]


@pytest.mark.asyncio
async def test_async_exists(backend_with_mock_fs: FSSpecBackend) -> None:
    """Test async exists operation."""
    backend = backend_with_mock_fs

    with patch.object(backend, "exists", return_value=True):
        result = await backend.exists_async("test.txt")
        assert result is True


@pytest.mark.asyncio
async def test_async_delete(backend_with_mock_fs: FSSpecBackend) -> None:
    """Test async delete operation."""
    backend = backend_with_mock_fs

    with patch.object(backend, "delete") as mock_delete:
        await backend.delete_async("test.txt")
        mock_delete.assert_called_once_with("test.txt")


# Configuration Tests
def test_from_config() -> None:
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


@pytest.mark.parametrize(
    "config,expected_protocol,expected_base",
    [
        ({"protocol": "s3", "base_path": "/data"}, "s3", "/data"),
        ({"protocol": "gcs", "base_path": ""}, "gcs", ""),
        ({"protocol": "az"}, "az", ""),
    ],
    ids=["s3_with_base", "gcs_empty_base", "az_no_base"],
)
def test_from_config_variations(config: dict[str, Any], expected_protocol: str, expected_base: str) -> None:
    """Test creating backend from various config variations."""
    with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
        with patch("fsspec.filesystem") as mock_filesystem:
            mock_fs = MagicMock()
            mock_filesystem.return_value = mock_fs

            backend = FSSpecBackend.from_config(config)

            mock_filesystem.assert_called_once_with(expected_protocol)
            assert backend.base_path == expected_base


# Instrumentation Tests
def test_instrumentation_logging(caplog: LogCaptureFixture) -> None:
    """Test that operations complete without errors when logging is enabled."""
    with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
        mock_fs = MagicMock()
        mock_fs.protocol = "s3"
        mock_fs.cat.return_value = b"test data"

        backend = FSSpecBackend(mock_fs)

        with caplog.at_level(logging.DEBUG):
            result = backend.read_bytes("test.txt")

        # Just verify the operation succeeded
        assert result == b"test data"
        mock_fs.cat.assert_called_once_with("test.txt")


def test_instrumentation_write_logging(caplog: LogCaptureFixture) -> None:
    """Test write operation logging."""
    with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
        mock_fs = MagicMock()
        mock_fs.protocol = "s3"
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__.return_value = mock_file

        backend = FSSpecBackend(mock_fs)

        with caplog.at_level(logging.DEBUG):
            backend.write_bytes("output.txt", b"test data")

        # Just verify the operation succeeded
        mock_fs.open.assert_called_once_with("output.txt", mode="wb")
        mock_file.write.assert_called_once_with(b"test data")


# Error Handling Tests
@pytest.mark.parametrize(
    "method_name,args,error_msg",
    [
        ("read_bytes", ("test.txt",), "Permission denied"),
        ("write_bytes", ("test.txt", b"data"), "Disk full"),
        ("delete", ("test.txt",), "File not found"),
        ("copy", ("src.txt", "dst.txt"), "Source not found"),
        ("move", ("old.txt", "new.txt"), "Access denied"),
    ],
    ids=["read_error", "write_error", "delete_error", "copy_error", "move_error"],
)
def test_error_propagation(
    backend_with_mock_fs: FSSpecBackend, mock_fs: MagicMock, method_name: str, args: tuple, error_msg: str
) -> None:
    """Test that errors are properly propagated."""
    backend = backend_with_mock_fs

    # Get the underlying fs method
    if method_name == "read_bytes":
        mock_method = mock_fs.cat
    elif method_name == "write_bytes":
        mock_method = mock_fs.open
    elif method_name == "delete":
        mock_method = mock_fs.rm
    elif method_name == "copy":
        mock_method = mock_fs.copy
    elif method_name == "move":
        mock_method = mock_fs.mv

    mock_method.side_effect = Exception(error_msg)  # pyright: ignore

    with pytest.raises(Exception, match=error_msg):
        getattr(backend, method_name)(*args)


# Stream Operations Tests
def test_stream_arrow_error_handling(backend_with_mock_fs: FSSpecBackend) -> None:
    """Test error handling in arrow streaming."""
    backend = backend_with_mock_fs

    with patch("sqlspec.typing.PYARROW_INSTALLED", True):
        with patch.object(backend, "glob", return_value=["data.parquet"]):
            # Mock file open to raise error
            backend.fs.open = MagicMock(side_effect=Exception("Read error"))

            # Should handle errors gracefully
            with pytest.raises(Exception, match="Read error"):
                list(backend.stream_arrow("*.parquet"))


# Edge Cases
def test_empty_base_path_operations(mock_fs: MagicMock) -> None:
    """Test operations with empty base path."""
    with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
        backend = FSSpecBackend(mock_fs, base_path="")

        backend.read_bytes("file.txt")
        mock_fs.cat.assert_called_once_with("file.txt")

        mock_fs.reset_mock()
        backend.write_bytes("file.txt", b"data")
        mock_fs.open.assert_called_once_with("file.txt", mode="wb")


def test_protocol_specific_initialization() -> None:
    """Test protocol-specific initialization."""
    with patch("sqlspec.typing.FSSPEC_INSTALLED", True):
        # Test various protocols
        protocols = ["s3", "gcs", "az", "http", "ftp"]

        for protocol in protocols:
            with patch("fsspec.filesystem") as mock_filesystem:
                mock_fs = MagicMock()
                mock_filesystem.return_value = mock_fs

                backend = FSSpecBackend(f"{protocol}://test")

                mock_filesystem.assert_called_once_with(protocol)
                assert backend.protocol == protocol
