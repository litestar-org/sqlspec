"""Tests for async instrumentation patterns.

This module tests that async methods properly use async instrumentation
context managers instead of blocking sync ones. This prevents event loop
blocking bugs that were systematically fixed across the codebase.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.config import InstrumentationConfig
from sqlspec.storage.backends.base import InstrumentedObjectStore
from sqlspec.utils.correlation import CorrelationContext


class MockAsyncBackend(InstrumentedObjectStore):
    """Mock async storage backend for testing instrumentation."""

    def __init__(self, instrumentation_config: InstrumentationConfig | None = None) -> None:
        super().__init__(instrumentation_config=instrumentation_config)
        # Track method calls for verification
        self.method_calls: list[str] = []

    # Implement required abstract methods
    def _read_bytes(self, path: str, **kwargs: Any) -> bytes:
        return b"sync_data"

    def _write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        pass

    def _read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        return "sync_text"

    def _write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        pass

    def _list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        return ["file1.txt", "file2.txt"]

    def _exists(self, path: str, **kwargs: Any) -> bool:
        return True

    def _delete(self, path: str, **kwargs: Any) -> None:
        pass

    def _copy(self, source: str, destination: str, **kwargs: Any) -> None:
        pass

    def _move(self, source: str, destination: str, **kwargs: Any) -> None:
        pass

    def _glob(self, pattern: str, **kwargs: Any) -> list[str]:
        return ["match1.txt"]

    def _get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return {"size": 100}

    def _get_signed_url(self, path: str, operation: str = "read", expires_in: int = 3600, **kwargs: Any) -> str:
        return f"https://example.com/{path}?expires={expires_in}"

    def _is_object(self, path: str) -> bool:
        return True

    def _is_path(self, path: str) -> bool:
        return False

    def _read_arrow(self, path: str, **kwargs: Any) -> Any:
        # Mock arrow table
        mock_table = MagicMock()
        mock_table.num_rows = 10
        mock_table.num_columns = 3
        return mock_table

    def _write_arrow(self, path: str, table: Any, **kwargs: Any) -> None:
        pass

    def _stream_arrow(self, pattern: str, **kwargs: Any) -> Any:
        # Mock iterator
        return iter([MagicMock(), MagicMock()])

    # Async implementations
    async def _read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        self.method_calls.append("_read_bytes_async")
        await asyncio.sleep(0.01)  # Simulate async work
        return b"async_data"

    async def _write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        self.method_calls.append("_write_bytes_async")
        await asyncio.sleep(0.01)

    async def _read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        self.method_calls.append("_read_text_async")
        await asyncio.sleep(0.01)
        return "async_text"

    async def _write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        self.method_calls.append("_write_text_async")
        await asyncio.sleep(0.01)

    async def _list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        self.method_calls.append("_list_objects_async")
        await asyncio.sleep(0.01)
        return ["async_file1.txt", "async_file2.txt"]

    async def _exists_async(self, path: str, **kwargs: Any) -> bool:
        self.method_calls.append("_exists_async")
        await asyncio.sleep(0.01)
        return True

    async def _delete_async(self, path: str, **kwargs: Any) -> None:
        self.method_calls.append("_delete_async")
        await asyncio.sleep(0.01)

    async def _copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        self.method_calls.append("_copy_async")
        await asyncio.sleep(0.01)

    async def _move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        self.method_calls.append("_move_async")
        await asyncio.sleep(0.01)

    async def _get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        self.method_calls.append("_get_metadata_async")
        await asyncio.sleep(0.01)
        return {"size": 200}

    async def _read_arrow_async(self, path: str, **kwargs: Any) -> Any:
        self.method_calls.append("_read_arrow_async")
        await asyncio.sleep(0.01)
        mock_table = MagicMock()
        mock_table.num_rows = 20
        mock_table.num_columns = 5
        return mock_table

    async def _write_arrow_async(self, path: str, table: Any, **kwargs: Any) -> None:
        self.method_calls.append("_write_arrow_async")
        await asyncio.sleep(0.01)

    async def _stream_arrow_async(self, pattern: str, **kwargs: Any) -> Any:
        self.method_calls.append("_stream_arrow_async")
        for i in range(3):
            await asyncio.sleep(0.01)
            yield MagicMock()


@pytest.fixture
def mock_backend() -> MockAsyncBackend:
    """Create a mock async backend with instrumentation enabled."""
    config = InstrumentationConfig(
        debug_mode=True,
        log_service_operations=True,
        log_queries=True,
        log_parameters=True,
        log_results_count=True,
        log_runtime=True,
    )
    return MockAsyncBackend(instrumentation_config=config)


@pytest.fixture
def correlation_context() -> str:
    """Set up correlation context for testing."""
    correlation_id = "test-correlation-123"
    CorrelationContext.set(correlation_id)
    yield correlation_id
    CorrelationContext.clear()


class TestAsyncInstrumentationPatterns:
    """Test that async methods use proper async instrumentation."""

    @patch("sqlspec.storage.backends.base.instrument_operation_async")
    async def test_read_bytes_async_uses_async_instrumentation(
        self, mock_instrument_async: AsyncMock, mock_backend: MockAsyncBackend
    ) -> None:
        """Test that read_bytes_async uses async instrumentation."""
        # Mock the async context manager
        mock_context = AsyncMock()
        mock_instrument_async.return_value = mock_context

        # Call the async method
        result = await mock_backend.read_bytes_async("test/path.txt")

        # Verify async instrumentation was called
        mock_instrument_async.assert_called_once_with(
            mock_backend,
            "storage.read_bytes_async",
            "storage",
            path="test/path.txt",
            backend="mockasync",  # backend_type removes "Backend" and lowercases
        )

        # Verify the underlying method was called
        assert "_read_bytes_async" in mock_backend.method_calls
        assert result == b"async_data"

    @patch("sqlspec.storage.backends.base.instrument_operation_async")
    async def test_write_text_async_uses_async_instrumentation(
        self, mock_instrument_async: AsyncMock, mock_backend: MockAsyncBackend
    ) -> None:
        """Test that write_text_async uses async instrumentation."""
        # Mock the async context manager
        mock_context = AsyncMock()
        mock_instrument_async.return_value = mock_context

        # Call the async method
        await mock_backend.write_text_async("test/file.txt", "async content", encoding="utf-8")

        # Verify async instrumentation was called with correct parameters
        mock_instrument_async.assert_called_once_with(
            mock_backend,
            "storage.write_text_async",
            "storage",
            path="test/file.txt",
            char_count=13,  # len("async content")
            encoding="utf-8",
            backend="mockasync",  # backend_type removes "Backend" and lowercases
        )

        # Verify the underlying method was called
        assert "_write_text_async" in mock_backend.method_calls

    @patch("sqlspec.storage.backends.base.instrument_operation_async")
    async def test_stream_arrow_async_uses_async_instrumentation(
        self, mock_instrument_async: AsyncMock, mock_backend: MockAsyncBackend
    ) -> None:
        """Test that stream_arrow_async uses async instrumentation."""
        # Mock the async context manager
        mock_context = AsyncMock()
        mock_instrument_async.return_value = mock_context

        # Call the async streaming method
        batches = [batch async for batch in mock_backend.stream_arrow_async("*.parquet")]

        # Verify async instrumentation was called
        mock_instrument_async.assert_called_once_with(
            mock_backend,
            "storage.stream_arrow_async",
            "storage",
            pattern="*.parquet",
            backend="mockasync",  # backend_type removes "Backend" and lowercases
        )

        # Verify we got the expected batches
        assert len(batches) == 3
        assert "_stream_arrow_async" in mock_backend.method_calls

    @patch("sqlspec.utils.telemetry.instrument_operation")
    async def test_async_methods_do_not_use_sync_instrumentation(
        self, mock_instrument_sync: MagicMock, mock_backend: MockAsyncBackend
    ) -> None:
        """Test that async methods DO NOT use sync instrumentation."""
        # Call several async methods
        await mock_backend.read_bytes_async("test.txt")
        await mock_backend.exists_async("test.txt")
        await mock_backend.delete_async("test.txt")

        # Verify sync instrumentation was NEVER called
        mock_instrument_sync.assert_not_called()

    @pytest.mark.parametrize(
        "method_name,args,kwargs",
        [
            ("read_bytes_async", ("test.txt",), {}),
            ("write_bytes_async", ("test.txt", b"data"), {}),
            ("read_text_async", ("test.txt",), {"encoding": "utf-8"}),
            ("write_text_async", ("test.txt", "content"), {"encoding": "utf-8"}),
            ("exists_async", ("test.txt",), {}),
            ("delete_async", ("test.txt",), {}),
            ("list_objects_async", (), {"prefix": "test/", "recursive": True}),
            ("copy_async", ("src.txt", "dst.txt"), {}),
            ("move_async", ("old.txt", "new.txt"), {}),
            ("get_metadata_async", ("test.txt",), {}),
            ("read_arrow_async", ("data.parquet",), {}),
            ("write_arrow_async", ("data.parquet", MagicMock()), {}),
        ],
    )
    @patch("sqlspec.storage.backends.base.instrument_operation_async")
    async def test_all_async_methods_use_async_instrumentation(
        self,
        mock_instrument_async: AsyncMock,
        mock_backend: MockAsyncBackend,
        method_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        """Test that all async methods use async instrumentation."""
        # Mock the async context manager
        mock_context = AsyncMock()
        mock_instrument_async.return_value = mock_context

        # Get the method and call it
        method = getattr(mock_backend, method_name)
        await method(*args, **kwargs)

        # Verify async instrumentation was called
        mock_instrument_async.assert_called_once()
        call_args = mock_instrument_async.call_args

        # Verify the first three arguments are correct
        assert call_args[0][0] is mock_backend
        assert call_args[0][1] == f"storage.{method_name}"
        assert call_args[0][2] == "storage"

    async def test_async_methods_work_without_instrumentation(self, mock_backend: MockAsyncBackend) -> None:
        """Test that async methods work properly even with instrumentation disabled."""
        # Disable instrumentation
        mock_backend.instrumentation_config.debug_mode = False
        mock_backend.instrumentation_config.log_service_operations = False

        # Call async methods and verify they still work
        data = await mock_backend.read_bytes_async("test.txt")
        assert data == b"async_data"

        text = await mock_backend.read_text_async("test.txt")
        assert text == "async_text"

        exists = await mock_backend.exists_async("test.txt")
        assert exists is True

        # Verify underlying methods were called
        assert "_read_bytes_async" in mock_backend.method_calls
        assert "_read_text_async" in mock_backend.method_calls
        assert "_exists_async" in mock_backend.method_calls

    async def test_correlation_context_in_async_methods(
        self, mock_backend: MockAsyncBackend, correlation_context: str
    ) -> None:
        """Test that correlation context is properly handled in async methods."""
        # Mock the logger to capture log messages
        with patch.object(mock_backend.logger, "debug") as mock_debug:
            await mock_backend.read_text_async("test.txt", encoding="utf-8")

            # Verify debug logging was called with correlation ID
            mock_debug.assert_called()
            call_args = mock_debug.call_args
            extra_data = call_args[1]["extra"]
            assert extra_data["correlation_id"] == correlation_context

    @patch("sqlspec.storage.backends.base.instrument_operation_async")
    async def test_async_instrumentation_exception_handling(
        self, mock_instrument_async: AsyncMock, mock_backend: MockAsyncBackend
    ) -> None:
        """Test that exceptions in async methods are properly handled and instrumented."""
        # Mock the async context manager
        mock_context = AsyncMock()
        mock_instrument_async.return_value = mock_context

        # Make the underlying method raise an exception
        async def failing_method(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("Test async failure")

        mock_backend._read_bytes_async = failing_method

        # Call the method and expect the exception
        with pytest.raises(RuntimeError, match="Test async failure"):
            await mock_backend.read_bytes_async("test.txt")

        # Verify async instrumentation was still called
        mock_instrument_async.assert_called_once()

    async def test_no_hasattr_checks_in_async_methods(self, mock_backend: MockAsyncBackend) -> None:
        """Test that async methods don't use hasattr checks (cleaned up fallback pattern)."""
        # Patch hasattr to track if it's called
        original_hasattr = hasattr
        hasattr_calls = []

        def tracking_hasattr(obj: Any, name: str) -> bool:
            hasattr_calls.append((obj, name))
            return original_hasattr(obj, name)

        with patch("builtins.hasattr", side_effect=tracking_hasattr):
            # Call async methods
            await mock_backend.read_text_async("test.txt")
            await mock_backend.write_text_async("test.txt", "content")
            await mock_backend.exists_async("test.txt")

        # Verify no hasattr calls were made for async methods
        async_method_checks = [
            call for call in hasattr_calls if isinstance(call[1], str) and call[1].endswith("_async")
        ]
        assert len(async_method_checks) == 0, f"Unexpected hasattr checks for async methods: {async_method_checks}"

    @patch("sqlspec.utils.sync_tools.async_")
    async def test_no_sync_to_async_fallbacks(
        self, mock_async_wrapper: MagicMock, mock_backend: MockAsyncBackend
    ) -> None:
        """Test that async methods don't use sync-to-async fallback wrappers."""
        # Call async methods
        await mock_backend.read_text_async("test.txt")
        await mock_backend.write_text_async("test.txt", "content")
        await mock_backend.exists_async("test.txt")
        await mock_backend.delete_async("test.txt")

        # Verify sync-to-async wrapper was never called
        mock_async_wrapper.assert_not_called()


class TestEventLoopBlockingPrevention:
    """Test that async methods don't block the event loop."""

    async def test_async_methods_are_truly_async(self, mock_backend: MockAsyncBackend) -> None:
        """Test that async methods are actually asynchronous and don't block."""
        import time

        # Record start time
        start_time = time.monotonic()

        # Run multiple async operations concurrently
        tasks = [mock_backend.read_bytes_async(f"file_{i}.txt") for i in range(10)]

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)

        # Check that it took roughly the time of one operation (since they run concurrently)
        # Each mock operation sleeps for 0.01s, so 10 concurrent operations should take ~0.01s, not 0.1s
        elapsed = time.monotonic() - start_time
        assert elapsed < 0.05, f"Operations took too long ({elapsed}s), suggesting they weren't truly concurrent"

        # Verify all operations completed successfully
        assert len(results) == 10
        assert all(result == b"async_data" for result in results)

    async def test_sync_and_async_methods_coexist(self, mock_backend: MockAsyncBackend) -> None:
        """Test that sync and async methods can coexist without interference."""
        # Run sync method
        sync_result = mock_backend.read_text("sync_file.txt")
        assert sync_result == "sync_text"

        # Run async method
        async_result = await mock_backend.read_text_async("async_file.txt")
        assert async_result == "async_text"

        # Verify both methods used their respective implementations
        assert "_read_text_async" in mock_backend.method_calls
