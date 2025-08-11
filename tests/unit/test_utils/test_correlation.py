"""Comprehensive tests for sqlspec.utils.correlation module.

Tests correlation ID tracking for distributed tracing across database operations.
Covers context management, thread safety, and logging integration.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid
from typing import Any

import pytest

from sqlspec.utils.correlation import CorrelationContext, correlation_context, get_correlation_adapter


# Test setup/teardown
def setup_function() -> None:
    """Clean up correlation context before each test."""
    CorrelationContext.clear()


def test_get_initial_state() -> None:
    """Test that initial correlation ID is None."""
    assert CorrelationContext.get() is None


def test_set_and_get() -> None:
    """Test setting and getting correlation ID."""
    test_id = "test-correlation-id"
    CorrelationContext.set(test_id)
    assert CorrelationContext.get() == test_id


def test_set_none_clears_id() -> None:
    """Test that setting None clears the correlation ID."""
    CorrelationContext.set("test-id")
    assert CorrelationContext.get() == "test-id"

    CorrelationContext.set(None)
    assert CorrelationContext.get() is None


def test_clear() -> None:
    """Test clear method."""
    CorrelationContext.set("test-id")
    assert CorrelationContext.get() == "test-id"

    CorrelationContext.clear()
    assert CorrelationContext.get() is None


def test_generate() -> None:
    """Test generate method creates valid UUID."""
    correlation_id = CorrelationContext.generate()
    assert isinstance(correlation_id, str)
    assert len(correlation_id) > 0

    # Should be a valid UUID
    uuid.UUID(correlation_id)  # Will raise ValueError if invalid


def test_generate_unique_ids() -> None:
    """Test that generate creates unique IDs."""
    id1 = CorrelationContext.generate()
    id2 = CorrelationContext.generate()
    assert id1 != id2


def test_context_manager_with_provided_id() -> None:
    """Test context manager with provided correlation ID."""
    test_id = "provided-id"

    with CorrelationContext.context(test_id) as context_id:
        assert context_id == test_id
        assert CorrelationContext.get() == test_id

    # Should be cleared after context
    assert CorrelationContext.get() is None


def test_context_manager_with_generated_id() -> None:
    """Test context manager with auto-generated correlation ID."""
    with CorrelationContext.context() as context_id:
        assert isinstance(context_id, str)
        assert len(context_id) > 0
        assert CorrelationContext.get() == context_id

        # Should be a valid UUID
        uuid.UUID(context_id)

    # Should be cleared after context
    assert CorrelationContext.get() is None


def test_context_manager_restores_previous_id() -> None:
    """Test that context manager restores previous correlation ID."""
    original_id = "original-id"
    CorrelationContext.set(original_id)

    with CorrelationContext.context("temporary-id") as temp_id:
        assert temp_id == "temporary-id"
        assert CorrelationContext.get() == "temporary-id"

    # Should restore original ID
    assert CorrelationContext.get() == original_id


def test_nested_context_managers() -> None:
    """Test nested context managers work correctly."""
    with CorrelationContext.context("outer") as outer_id:
        assert outer_id == "outer"
        assert CorrelationContext.get() == "outer"

        with CorrelationContext.context("inner") as inner_id:
            assert inner_id == "inner"
            assert CorrelationContext.get() == "inner"

        # Should restore outer context
        assert CorrelationContext.get() == "outer"

    # Should be cleared after all contexts
    assert CorrelationContext.get() is None


def test_context_manager_exception_handling() -> None:
    """Test that context manager restores state even on exceptions."""
    original_id = "original"
    CorrelationContext.set(original_id)

    try:
        with CorrelationContext.context("temp"):
            assert CorrelationContext.get() == "temp"
            raise ValueError("Test exception")
    except ValueError:
        pass

    # Should restore original ID even after exception
    assert CorrelationContext.get() == original_id


def test_to_dict_with_id() -> None:
    """Test to_dict method when correlation ID is set."""
    test_id = "test-id"
    CorrelationContext.set(test_id)

    result = CorrelationContext.to_dict()
    assert result == {"correlation_id": test_id}


def test_to_dict_without_id() -> None:
    """Test to_dict method when no correlation ID is set."""
    CorrelationContext.clear()
    result = CorrelationContext.to_dict()
    assert result == {}


def test_thread_safety() -> None:
    """Test that CorrelationContext is thread-safe."""
    results = {}

    def worker(thread_id: int) -> None:
        correlation_id = f"thread-{thread_id}"
        CorrelationContext.set(correlation_id)
        # Simulate some work
        import time

        time.sleep(0.01)
        results[thread_id] = CorrelationContext.get()

    # Start multiple threads
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    # Each thread should have maintained its own correlation ID
    for i in range(10):
        assert results[i] == f"thread-{i}"


# Test correlation_context convenience function


def test_with_provided_id() -> None:
    """Test correlation_context with provided ID."""
    test_id = "function-test"

    with correlation_context(test_id) as context_id:
        assert context_id == test_id
        assert CorrelationContext.get() == test_id


def test_with_generated_id() -> None:
    """Test correlation_context with auto-generated ID."""
    with correlation_context() as context_id:
        assert isinstance(context_id, str)
        assert len(context_id) > 0
        assert CorrelationContext.get() == context_id
        uuid.UUID(context_id)  # Validate UUID format


def test_compatibility_with_class_method() -> None:
    """Test that function and class method are compatible."""
    with CorrelationContext.context("class-method"):
        with correlation_context("function") as func_id:
            assert func_id == "function"
            assert CorrelationContext.get() == "function"

        # Should restore class method context
        assert CorrelationContext.get() == "class-method"


# Test get_correlation_adapter function


def test_creates_logger_adapter() -> None:
    """Test that get_correlation_adapter creates a LoggerAdapter."""
    from logging import LoggerAdapter

    base_logger = logging.getLogger("test")
    adapter = get_correlation_adapter(base_logger)

    assert isinstance(adapter, LoggerAdapter)
    assert adapter.logger is base_logger


def test_adapter_adds_correlation_id() -> None:
    """Test that adapter adds correlation ID to log messages."""
    base_logger = logging.getLogger("test")
    adapter = get_correlation_adapter(base_logger)

    # Set a correlation ID
    CorrelationContext.set("test-correlation")

    # Test the process method
    msg, kwargs = adapter.process("Test message", {})

    assert msg == "Test message"
    assert "extra" in kwargs
    assert kwargs["extra"]["correlation_id"] == "test-correlation"


def test_adapter_without_correlation_id() -> None:
    """Test adapter behavior when no correlation ID is set."""
    base_logger = logging.getLogger("test")
    adapter = get_correlation_adapter(base_logger)

    # Clear correlation ID
    CorrelationContext.clear()

    # Test the process method
    msg, kwargs = adapter.process("Test message", {})

    assert msg == "Test message"
    # Should not add correlation_id to extra when none is set
    extra = kwargs.get("extra", {})
    assert "correlation_id" not in extra


def test_adapter_preserves_existing_extra() -> None:
    """Test that adapter preserves existing extra fields."""
    base_logger = logging.getLogger("test")
    adapter = get_correlation_adapter(base_logger)

    CorrelationContext.set("test-id")

    existing_extra = {"user_id": 123, "action": "login"}
    msg, kwargs = adapter.process("Test message", {"extra": existing_extra})

    assert msg == "Test message"
    assert "extra" in kwargs

    extra = kwargs["extra"]
    assert extra["user_id"] == 123
    assert extra["action"] == "login"
    assert extra["correlation_id"] == "test-id"


def test_adapter_with_empty_extra() -> None:
    """Test adapter when extra is initially empty dict."""
    base_logger = logging.getLogger("test")
    adapter = get_correlation_adapter(base_logger)

    CorrelationContext.set("test-id")

    msg, kwargs = adapter.process("Test message", {"extra": {}})

    assert kwargs["extra"]["correlation_id"] == "test-id"


def test_database_operation_simulation() -> None:
    """Test correlation tracking in simulated database operations."""

    def simulate_db_query(query: str) -> dict[str, Any]:
        """Simulate a database query with correlation logging."""
        correlation_id = CorrelationContext.get()
        return {"query": query, "correlation_id": correlation_id, "result": "success"}

    with correlation_context() as correlation_id:
        # Simulate multiple database operations within same context
        result1 = simulate_db_query("SELECT * FROM users")
        result2 = simulate_db_query("SELECT * FROM orders")

        assert result1["correlation_id"] == correlation_id
        assert result2["correlation_id"] == correlation_id
        assert result1["correlation_id"] == result2["correlation_id"]


def test_nested_operation_contexts() -> None:
    """Test nested operations maintain proper correlation context."""
    results = []

    def operation(name: str) -> None:
        results.append({"operation": name, "correlation_id": CorrelationContext.get()})

    with correlation_context("request-123") as outer_id:
        operation("outer-start")

        with correlation_context("sub-operation-456") as inner_id:
            operation("inner")
            assert inner_id != outer_id

        operation("outer-end")

    assert len(results) == 3
    assert results[0]["correlation_id"] == "request-123"
    assert results[1]["correlation_id"] == "sub-operation-456"
    assert results[2]["correlation_id"] == "request-123"


@pytest.mark.asyncio
async def test_async_context_preservation() -> None:
    """Test that correlation context is preserved across async operations."""

    async def async_operation(delay: float) -> str:
        await asyncio.sleep(delay)
        return CorrelationContext.get() or "no-id"

    with correlation_context("async-test") as correlation_id:
        # Start multiple async operations
        tasks = [async_operation(0.01), async_operation(0.02), async_operation(0.005)]

        results = await asyncio.gather(*tasks)

        # All should have maintained the same correlation ID
        assert all(result == correlation_id for result in results)


def test_logging_integration() -> None:
    """Test integration with actual logging system."""
    import io
    import logging

    # Create a logger with string stream handler
    logger = logging.getLogger("test_correlation")
    logger.setLevel(logging.INFO)

    # Capture log output
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    logger.addHandler(handler)

    # Use correlation adapter
    correlation_adapter = get_correlation_adapter(logger)

    try:
        with correlation_context("integration-test"):
            correlation_adapter.info("Test message with correlation")

        # Check that log was written
        log_output = log_stream.getvalue()
        assert len(log_output) > 0

    finally:
        # Cleanup
        logger.removeHandler(handler)


def test_concurrent_contexts() -> None:
    """Test concurrent correlation contexts don't interfere."""
    import concurrent.futures

    def worker_with_context(worker_id: int) -> tuple[int, str | None]:
        correlation_id = f"worker-{worker_id}"
        with correlation_context(correlation_id):
            # Simulate some work
            import time

            time.sleep(0.01)
            return worker_id, CorrelationContext.get()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker_with_context, i) for i in range(10)]
        results = [future.result() for future in futures]

    # Each worker should have maintained its own correlation ID
    for worker_id, correlation_id in results:
        assert correlation_id == f"worker-{worker_id}"


def test_context_with_none_id_generates_uuid() -> None:
    """Test that passing None to context generates a UUID."""
    with CorrelationContext.context(None) as correlation_id:
        assert correlation_id is not None
        uuid.UUID(correlation_id)  # Should be valid UUID


def test_multiple_clear_calls() -> None:
    """Test that multiple clear calls don't cause issues."""
    CorrelationContext.set("test")
    CorrelationContext.clear()
    CorrelationContext.clear()  # Should not raise
    CorrelationContext.clear()  # Should not raise

    assert CorrelationContext.get() is None


def test_context_manager_with_empty_string() -> None:
    """Test context manager with empty string ID."""
    with CorrelationContext.context("") as correlation_id:
        assert correlation_id == ""
        assert CorrelationContext.get() == ""


def test_to_dict_consistency() -> None:
    """Test that to_dict is consistent with get."""
    # When no ID is set
    CorrelationContext.clear()
    assert CorrelationContext.get() is None
    assert CorrelationContext.to_dict() == {}

    # When ID is set
    test_id = "consistency-test"
    CorrelationContext.set(test_id)
    assert CorrelationContext.get() == test_id
    assert CorrelationContext.to_dict() == {"correlation_id": test_id}


def test_context_var_isolation() -> None:
    """Test that context variables provide proper isolation."""

    # This test ensures that the ContextVar behavior is working correctly
    def check_isolation() -> str | None:
        return CorrelationContext.get()

    # Set in main thread
    CorrelationContext.set("main-thread")
    assert CorrelationContext.get() == "main-thread"

    # Check in separate thread - should not see main thread's value
    result_container = []

    def thread_worker() -> None:
        result_container.append(check_isolation())

    thread = threading.Thread(target=thread_worker)
    thread.start()
    thread.join()

    # Thread should not see main thread's correlation ID
    assert result_container[0] is None

    # Main thread should still have its ID
    assert CorrelationContext.get() == "main-thread"


@pytest.mark.parametrize(
    "test_id",
    [
        "simple-id",
        "complex-id-with-many-parts",
        "id_with_underscores",
        "123-numeric-id",
        "special!@#$%^&*()chars",
        "unicode-café-test",
    ],
)
def test_various_correlation_id_formats(test_id: str) -> None:
    """Test that various correlation ID formats are handled correctly."""
    CorrelationContext.clear()

    with CorrelationContext.context(test_id) as context_id:
        assert context_id == test_id
        assert CorrelationContext.get() == test_id

        # Should work with to_dict as well
        assert CorrelationContext.to_dict() == {"correlation_id": test_id}

    # Should be cleared after context
    assert CorrelationContext.get() is None


def test_generate_produces_valid_uuids() -> None:
    """Test that generate method consistently produces valid UUIDs."""
    for _ in range(100):  # Generate many UUIDs to test consistency
        correlation_id = CorrelationContext.generate()
        assert isinstance(correlation_id, str)

        # Should be parseable as UUID
        parsed_uuid = uuid.UUID(correlation_id)
        assert str(parsed_uuid) == correlation_id
