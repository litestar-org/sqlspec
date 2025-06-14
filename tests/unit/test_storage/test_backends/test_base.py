"""Tests for InstrumentedObjectStore base class."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from sqlspec.config import InstrumentationConfig
from sqlspec.storage.backends.base import InstrumentedObjectStore
from sqlspec.typing import ArrowRecordBatch, ArrowTable


class MockBackend(InstrumentedObjectStore):
    """Mock backend for testing base instrumentation."""

    def __init__(self, instrumentation_config: InstrumentationConfig | None = None) -> None:
        super().__init__(instrumentation_config, "MockBackend")
        self.call_count = 0

    # Implement all abstract sync methods
    def _read_bytes(self, path: str, **kwargs: Any) -> bytes:
        self.call_count += 1
        return b"test data"

    def _write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        self.call_count += 1

    def _read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        self.call_count += 1
        return "test text"

    def _write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        self.call_count += 1

    def _list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        self.call_count += 1
        return ["file1.txt", "file2.csv"]

    def _exists(self, path: str, **kwargs: Any) -> bool:
        self.call_count += 1
        return True

    def _delete(self, path: str, **kwargs: Any) -> None:
        self.call_count += 1

    def _copy(self, source: str, destination: str, **kwargs: Any) -> None:
        self.call_count += 1

    def _move(self, source: str, destination: str, **kwargs: Any) -> None:
        self.call_count += 1

    def _glob(self, pattern: str, **kwargs: Any) -> list[str]:
        self.call_count += 1
        return ["match1.txt", "match2.txt"]

    def _get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        self.call_count += 1
        return {"size": 1024, "exists": True}

    def _is_object(self, path: str) -> bool:
        self.call_count += 1
        return True

    def _is_path(self, path: str) -> bool:
        self.call_count += 1
        return False

    def _read_arrow(self, path: str, **kwargs: Any) -> ArrowTable:
        self.call_count += 1
        # Return a mock Arrow table
        mock_table = MagicMock()
        mock_table.num_rows = 10
        mock_table.num_columns = 2
        mock_table.__len__ = MagicMock(return_value=10)
        mock_table.columns = ["col1", "col2"]
        return mock_table

    def _write_arrow(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        self.call_count += 1

    def _stream_arrow(self, pattern: str, **kwargs: Any) -> Iterator[ArrowRecordBatch]:
        self.call_count += 1
        # Yield mock batches
        for _ in range(2):
            yield MagicMock()

    # Implement all abstract async methods
    async def _read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        self.call_count += 1
        return b"async test data"

    async def _write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        self.call_count += 1

    async def _read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        self.call_count += 1
        return "async test text"

    async def _write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        self.call_count += 1

    async def _list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        self.call_count += 1
        return ["async_file1.txt", "async_file2.csv"]

    async def _exists_async(self, path: str, **kwargs: Any) -> bool:
        self.call_count += 1
        return True

    async def _delete_async(self, path: str, **kwargs: Any) -> None:
        self.call_count += 1

    async def _copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        self.call_count += 1

    async def _move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        self.call_count += 1

    async def _get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        self.call_count += 1
        return {"size": 2048, "exists": True}

    async def _read_arrow_async(self, path: str, **kwargs: Any) -> ArrowTable:
        self.call_count += 1
        mock_table = MagicMock()
        mock_table.num_rows = 20
        mock_table.num_columns = 3
        mock_table.__len__ = MagicMock(return_value=20)
        mock_table.columns = ["col1", "col2", "col3"]
        return mock_table

    async def _write_arrow_async(self, path: str, table: ArrowTable, **kwargs: Any) -> None:
        self.call_count += 1

    async def _stream_arrow_async(self, pattern: str, **kwargs: Any) -> AsyncIterator[ArrowRecordBatch]:
        self.call_count += 1
        # Yield mock batches
        for _ in range(2):
            yield MagicMock()


class TestInstrumentedObjectStore:
    """Test the base InstrumentedObjectStore class."""

    def test_initialization(self) -> None:
        """Test backend initialization with and without config."""
        # Without config
        backend = MockBackend()
        assert backend.backend_name == "MockBackend"
        assert backend.backend_type == "mock"
        assert backend.instrumentation_config is not None

        # With config
        config = InstrumentationConfig(log_service_operations=True)
        backend = MockBackend(config)
        assert backend.instrumentation_config == config

    def test_sync_read_bytes_instrumentation(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that read_bytes properly instruments the operation."""
        config = InstrumentationConfig(log_service_operations=True)
        backend = MockBackend(config)

        with caplog.at_level(logging.DEBUG):
            result = backend.read_bytes("test.txt")

        assert result == b"test data"
        assert backend.call_count == 1

        # Check debug log
        assert any("Reading bytes from test.txt" in record.message for record in caplog.records)
        # Check info log
        assert any("Read 9 bytes from test.txt" in record.message for record in caplog.records)

    def test_sync_write_bytes_instrumentation(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that write_bytes properly instruments the operation."""
        config = InstrumentationConfig(log_service_operations=True)
        backend = MockBackend(config)

        with caplog.at_level(logging.DEBUG):
            backend.write_bytes("test.txt", b"test data")

        assert backend.call_count == 1

        # Check debug log
        assert any("Writing 9 bytes to test.txt" in record.message for record in caplog.records)
        # Check info log
        assert any("Wrote 9 bytes to test.txt" in record.message for record in caplog.records)

    def test_sync_list_objects_instrumentation(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that list_objects properly instruments the operation."""
        config = InstrumentationConfig(log_service_operations=True)
        backend = MockBackend(config)

        with caplog.at_level(logging.INFO):
            result = backend.list_objects("prefix", recursive=True)

        assert result == ["file1.txt", "file2.csv"]
        assert backend.call_count == 1
        assert any("Listed 2 objects with prefix prefix" in record.message for record in caplog.records)

    def test_sync_error_handling(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that errors are properly logged and re-raised."""
        config = InstrumentationConfig(log_service_operations=True)
        backend = MockBackend(config)

        # Mock to raise error
        backend._read_bytes = Mock(side_effect=ValueError("Test error"))

        with pytest.raises(ValueError, match="Test error"):
            with caplog.at_level(logging.ERROR):
                backend.read_bytes("error.txt")

        assert any("Failed to read from error.txt" in record.message for record in caplog.records)

    def test_sync_operations_without_logging(self) -> None:
        """Test that operations work without logging enabled."""
        config = InstrumentationConfig(log_service_operations=False)
        backend = MockBackend(config)

        # Should work without any logging
        result = backend.read_bytes("test.txt")
        assert result == b"test data"
        assert backend.call_count == 1

    @pytest.mark.asyncio
    async def test_async_read_bytes_instrumentation(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that async read_bytes properly instruments the operation."""
        config = InstrumentationConfig(log_service_operations=True)
        backend = MockBackend(config)

        with caplog.at_level(logging.DEBUG):
            result = await backend.read_bytes_async("async_test.txt")

        assert result == b"async test data"
        assert backend.call_count == 1

        # Check debug log
        assert any("Async reading bytes from async_test.txt" in record.message for record in caplog.records)
        # Check info log
        assert any("Async read 15 bytes from async_test.txt" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_async_write_bytes_instrumentation(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that async write_bytes properly instruments the operation."""
        config = InstrumentationConfig(log_service_operations=True)
        backend = MockBackend(config)

        with caplog.at_level(logging.DEBUG):
            await backend.write_bytes_async("async_test.txt", b"async test data")

        assert backend.call_count == 1

        # Check debug log
        assert any("Async writing 15 bytes to async_test.txt" in record.message for record in caplog.records)
        # Check info log
        assert any("Async wrote 15 bytes to async_test.txt" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_async_list_objects_instrumentation(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that async list_objects properly instruments the operation."""
        config = InstrumentationConfig(log_service_operations=True)
        backend = MockBackend(config)

        with caplog.at_level(logging.INFO):
            result = await backend.list_objects_async("async_prefix", recursive=False)

        assert result == ["async_file1.txt", "async_file2.csv"]
        assert backend.call_count == 1
        assert any("Async listed 2 objects with prefix 'async_prefix'" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_async_error_handling(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that async errors are properly logged and re-raised."""
        config = InstrumentationConfig(log_service_operations=True)
        backend = MockBackend(config)

        # Mock to raise error
        backend._read_bytes_async = AsyncMock(side_effect=ValueError("Async test error"))

        with pytest.raises(ValueError, match="Async test error"):
            with caplog.at_level(logging.ERROR):
                await backend.read_bytes_async("async_error.txt")

        assert any("Failed to async read from async_error.txt" in record.message for record in caplog.records)

    def test_arrow_operations(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test Arrow-specific operations with instrumentation."""
        config = InstrumentationConfig(log_service_operations=True)
        backend = MockBackend(config)

        # Test read_arrow
        with caplog.at_level(logging.INFO):
            table = backend.read_arrow("data.parquet")

        assert table.num_rows == 10
        assert table.num_columns == 2
        assert any("Read Arrow table from data.parquet (10 rows)" in record.message for record in caplog.records)

        # Test write_arrow
        mock_table = MagicMock()
        mock_table.num_rows = 100
        mock_table.num_columns = 5
        mock_table.__len__ = MagicMock(return_value=100)
        mock_table.columns = ["col1", "col2", "col3", "col4", "col5"]

        with caplog.at_level(logging.INFO):
            backend.write_arrow("output.parquet", mock_table)

        assert any("Wrote Arrow table to output.parquet (100 rows)" in record.message for record in caplog.records)

    def test_stream_arrow_operations(self) -> None:
        """Test Arrow streaming operations."""
        backend = MockBackend()

        # Test sync streaming
        batches = list(backend.stream_arrow("*.parquet"))
        assert len(batches) == 2
        assert backend.call_count == 1

    @pytest.mark.asyncio
    async def test_async_stream_arrow_operations(self) -> None:
        """Test async Arrow streaming operations."""
        backend = MockBackend()

        # Test async streaming
        batches = [batch async for batch in backend.stream_arrow_async("*.parquet")]

        assert len(batches) == 2
        assert backend.call_count == 1

    def test_metadata_operations(self) -> None:
        """Test metadata operations."""
        backend = MockBackend()

        # Test get_metadata
        metadata = backend.get_metadata("file.txt")
        assert metadata["size"] == 1024
        assert metadata["exists"] is True

        # Test exists
        assert backend.exists("file.txt") is True

        # Test is_object and is_path
        assert backend.is_object("file.txt") is True
        assert backend.is_path("directory/") is False

    def test_file_operations(self) -> None:
        """Test file operations like copy, move, delete."""
        backend = MockBackend()

        # Test copy
        backend.copy("source.txt", "dest.txt")
        assert backend.call_count == 1

        # Test move
        backend.move("old.txt", "new.txt")
        assert backend.call_count == 2

        # Test delete
        backend.delete("unwanted.txt")
        assert backend.call_count == 3

    def test_glob_operations(self) -> None:
        """Test glob pattern matching."""
        backend = MockBackend()

        matches = backend.glob("*.txt")
        assert matches == ["match1.txt", "match2.txt"]
        assert backend.call_count == 1

    def test_telemetry_context(self) -> None:
        """Test that operations include telemetry context."""
        backend = MockBackend()

        # Mock instrument_operation to verify it's called
        with patch("sqlspec.storage.backends.base.instrument_operation") as mock_instrument:
            backend.read_bytes("test.txt")

            # Verify instrument_operation was called with correct parameters
            mock_instrument.assert_called_once()
            args = mock_instrument.call_args[0]
            assert args[0] == backend  # self
            assert args[1] == "storage.read_bytes"  # operation name
            assert args[2] == "storage"  # operation type

            kwargs = mock_instrument.call_args[1]
            assert kwargs["path"] == "test.txt"
            assert kwargs["backend"] == "mock"

    @pytest.mark.asyncio
    async def test_async_telemetry_context(self) -> None:
        """Test that async operations include telemetry context."""
        backend = MockBackend()

        # Mock instrument_operation_async to verify it's called (async methods use async instrumentation)
        with patch("sqlspec.storage.backends.base.instrument_operation_async") as mock_instrument:
            # Mock the async context manager
            mock_context = AsyncMock()
            mock_instrument.return_value = mock_context

            await backend.read_bytes_async("async_test.txt")

            # Verify instrument_operation_async was called with correct parameters
            mock_instrument.assert_called_once()
            args = mock_instrument.call_args[0]
            assert args[0] == backend  # self
            assert args[1] == "storage.read_bytes_async"  # operation name
            assert args[2] == "storage"  # operation type

            kwargs = mock_instrument.call_args[1]
            assert kwargs["path"] == "async_test.txt"
            assert kwargs["backend"] == "mock"

    def test_backend_type_property(self) -> None:
        """Test that backend_type is correctly derived from class name."""
        backend = MockBackend()
        assert backend.backend_type == "mock"

        # Test with custom backend name
        backend.backend_name = "CustomStorage"
        # backend_type still uses class name
        assert backend.backend_type == "mock"
