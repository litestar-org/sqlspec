"""Comprehensive tests for sqlspec.utils.logging module.

Tests for structured logging utilities with correlation ID support.
Covers logger configuration, structured formatting, and context tracking.
"""

from __future__ import annotations

import json
import logging
import threading
from contextvars import copy_context
from io import StringIO
from unittest.mock import Mock

import pytest

from sqlspec.utils.logging import (
    CorrelationIDFilter,
    StructuredFormatter,
    correlation_id_var,
    get_correlation_id,
    get_logger,
    log_with_context,
    set_correlation_id,
)


def setup_function() -> None:
    """Clear correlation ID before each test."""
    correlation_id_var.set(None)


def test_correlation_id_initial_state() -> None:
    """Test that initial correlation ID is None."""
    assert get_correlation_id() is None


def test_correlation_id_set_and_get() -> None:
    """Test setting and getting correlation ID."""
    test_id = "test-correlation-123"
    set_correlation_id(test_id)
    assert get_correlation_id() == test_id


def test_correlation_id_set_none_clears() -> None:
    """Test that setting None clears the correlation ID."""
    set_correlation_id("test-id")
    assert get_correlation_id() == "test-id"

    set_correlation_id(None)
    assert get_correlation_id() is None


def test_correlation_id_context_isolation_between_threads() -> None:
    """Test that correlation IDs are isolated between threads."""
    main_id = "main-thread-id"
    set_correlation_id(main_id)

    thread_results = {}

    def thread_worker(thread_id: int) -> None:
        # Each thread should start with None
        thread_results[f"initial_{thread_id}"] = get_correlation_id()

        # Set thread-specific ID
        thread_specific_id = f"thread-{thread_id}"
        set_correlation_id(thread_specific_id)
        thread_results[f"after_set_{thread_id}"] = get_correlation_id()

    threads = [threading.Thread(target=thread_worker, args=(i,)) for i in range(3)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    # Main thread should still have its ID
    assert get_correlation_id() == main_id

    # Each thread should have started with None and maintained its own ID
    for i in range(3):
        assert thread_results[f"initial_{i}"] is None
        assert thread_results[f"after_set_{i}"] == f"thread-{i}"


def test_correlation_id_context_variable_copy() -> None:
    """Test that context variables work correctly with copy_context."""
    set_correlation_id("original")

    def task_in_copied_context() -> str | None:
        return get_correlation_id()

    # Create a copy of the current context
    ctx = copy_context()

    # The copied context should have the same correlation ID
    result = ctx.run(task_in_copied_context)
    assert result == "original"

    # Changes in copied context shouldn't affect original
    def modify_in_copy() -> None:
        set_correlation_id("modified")

    ctx.run(modify_in_copy)

    # Original context should be unchanged
    assert get_correlation_id() == "original"


def test_structured_formatter_basic_log_formatting() -> None:
    """Test basic log record formatting."""
    formatter = StructuredFormatter()
    correlation_id_var.set(None)

    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname="/path/to/file.py",
        lineno=42,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    record.module = "test_module"
    record.funcName = "test_function"

    result = formatter.format(record)

    # Should be valid JSON
    parsed = json.loads(result)

    # Check required fields
    assert "timestamp" in parsed
    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "test.logger"
    assert parsed["message"] == "Test message"
    assert parsed["module"] == "test_module"
    assert parsed["function"] == "test_function"
    assert parsed["line"] == 42


def test_structured_formatter_with_correlation_id() -> None:
    """Test log formatting includes correlation ID when set."""
    formatter = StructuredFormatter()
    set_correlation_id("test-correlation")

    record = logging.LogRecord(
        name="test.logger",
        level=logging.WARNING,
        pathname="/path/to/file.py",
        lineno=100,
        msg="Warning message",
        args=(),
        exc_info=None,
    )
    record.module = "test_module"
    record.funcName = "test_function"

    result = formatter.format(record)
    parsed = json.loads(result)

    assert parsed["correlation_id"] == "test-correlation"
    assert parsed["message"] == "Warning message"
    assert parsed["level"] == "WARNING"


def test_structured_formatter_without_correlation_id() -> None:
    """Test log formatting when no correlation ID is set."""
    formatter = StructuredFormatter()
    correlation_id_var.set(None)

    record = logging.LogRecord(
        name="test.logger",
        level=logging.ERROR,
        pathname="/path/to/file.py",
        lineno=200,
        msg="Error message",
        args=(),
        exc_info=None,
    )
    record.module = "test_module"
    record.funcName = "test_function"

    result = formatter.format(record)
    parsed = json.loads(result)

    assert "correlation_id" not in parsed
    assert parsed["message"] == "Error message"


def test_structured_formatter_with_extra_fields() -> None:
    """Test that extra fields are included in formatted output."""
    formatter = StructuredFormatter()
    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname="/path/to/file.py",
        lineno=50,
        msg="Message with extra",
        args=(),
        exc_info=None,
    )
    record.module = "test_module"
    record.funcName = "test_function"
    record.extra_fields = {"user_id": 123, "action": "login", "duration": 1.5}

    result = formatter.format(record)
    parsed = json.loads(result)

    assert parsed["user_id"] == 123
    assert parsed["action"] == "login"
    assert parsed["duration"] == 1.5
    assert parsed["message"] == "Message with extra"


def test_structured_formatter_with_exception() -> None:
    """Test log formatting includes exception information."""
    formatter = StructuredFormatter()
    try:
        raise ValueError("Test exception")
    except ValueError:
        import sys

        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="test.logger",
        level=logging.ERROR,
        pathname="/path/to/file.py",
        lineno=75,
        msg="Error with exception",
        args=(),
        exc_info=exc_info,
    )
    record.module = "test_module"
    record.funcName = "test_function"

    result = formatter.format(record)
    parsed = json.loads(result)

    assert "exception" in parsed
    assert "ValueError: Test exception" in parsed["exception"]
    assert "Traceback" in parsed["exception"]


def test_structured_formatter_custom_date_format() -> None:
    """Test formatter with custom date format."""
    formatter = StructuredFormatter(datefmt="%Y-%m-%d")

    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname="/path/to/file.py",
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    record.module = "test_module"
    record.funcName = "test_function"

    result = formatter.format(record)
    parsed = json.loads(result)

    # Should use custom date format (YYYY-MM-DD)
    import re

    assert re.match(r"\d{4}-\d{2}-\d{2}", parsed["timestamp"])


def test_correlation_id_filter_adds_correlation_id() -> None:
    """Test that filter adds correlation ID to record when set."""
    filter_obj = CorrelationIDFilter()
    correlation_id_var.set(None)
    set_correlation_id("filter-test-id")

    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname="/path/to/file.py",
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    # Filter should return True and add correlation_id
    result = filter_obj.filter(record)

    assert result is True
    assert hasattr(record, "correlation_id")
    assert getattr(record, "correlation_id") == "filter-test-id"


def test_correlation_id_filter_without_correlation_id() -> None:
    """Test filter behavior when no correlation ID is set."""
    filter_obj = CorrelationIDFilter()
    correlation_id_var.set(None)

    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname="/path/to/file.py",
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    # Filter should still return True but not add correlation_id
    result = filter_obj.filter(record)

    assert result is True
    assert not hasattr(record, "correlation_id")


def test_correlation_id_filter_always_returns_true() -> None:
    """Test that filter always passes records through."""
    filter_obj = CorrelationIDFilter()

    # Test with correlation ID
    set_correlation_id("test-id")
    record1 = logging.LogRecord("test", logging.INFO, "/path", 1, "msg", (), None)
    assert filter_obj.filter(record1) is True

    # Test without correlation ID
    correlation_id_var.set(None)
    record2 = logging.LogRecord("test", logging.ERROR, "/path", 2, "msg", (), None)
    assert filter_obj.filter(record2) is True


def test_get_logger_default() -> None:
    """Test getting default sqlspec logger."""
    logger = get_logger()
    assert logger.name == "sqlspec"
    assert isinstance(logger, logging.Logger)


def test_get_logger_named() -> None:
    """Test getting named logger with sqlspec prefix."""
    logger = get_logger("database.postgres")
    assert logger.name == "sqlspec.database.postgres"
    assert isinstance(logger, logging.Logger)


def test_get_logger_already_prefixed() -> None:
    """Test that sqlspec prefix isn't double-added."""
    logger = get_logger("sqlspec.already.prefixed")
    assert logger.name == "sqlspec.already.prefixed"


def test_get_logger_has_correlation_filter() -> None:
    """Test that returned logger has CorrelationIDFilter added."""
    logger = get_logger("test.filtered")

    # Should have at least one CorrelationIDFilter
    correlation_filters = [f for f in logger.filters if isinstance(f, CorrelationIDFilter)]
    assert len(correlation_filters) >= 1


def test_get_logger_filter_not_duplicated() -> None:
    """Test that multiple calls don't duplicate the correlation filter."""
    logger1 = get_logger("test.no.duplicate")
    initial_filter_count = len([f for f in logger1.filters if isinstance(f, CorrelationIDFilter)])

    logger2 = get_logger("test.no.duplicate")  # Same logger
    final_filter_count = len([f for f in logger2.filters if isinstance(f, CorrelationIDFilter)])

    assert logger1 is logger2  # Should be same logger instance
    assert initial_filter_count == final_filter_count  # Filter count shouldn't increase


def test_get_logger_different_loggers_independent() -> None:
    """Test that different loggers are independent."""
    logger1 = get_logger("test.independent1")
    logger2 = get_logger("test.independent2")

    assert logger1 is not logger2
    assert logger1.name != logger2.name


class TestLogWithContext:
    """Test cases for log_with_context function."""

    def setup_method(self) -> None:
        """Set up test logger and capture output."""
        self.logger = logging.getLogger("test_context_logger")
        self.logger.setLevel(logging.DEBUG)

        # Clear existing handlers
        self.logger.handlers.clear()

        # Add string stream handler to capture output
        self.log_stream = StringIO()
        self.handler = logging.StreamHandler(self.log_stream)
        self.handler.setFormatter(StructuredFormatter())
        self.logger.addHandler(self.handler)

    def teardown_method(self) -> None:
        """Clean up after each test."""
        self.logger.removeHandler(self.handler)

    def test_log_with_extra_fields(self) -> None:
        """Test logging with extra context fields."""
        log_with_context(self.logger, logging.INFO, "Test message", user_id=123, action="login", duration=1.5)

        log_output = self.log_stream.getvalue()
        parsed = json.loads(log_output.strip())

        assert parsed["message"] == "Test message"
        assert parsed["level"] == "INFO"
        assert parsed["user_id"] == 123
        assert parsed["action"] == "login"
        assert parsed["duration"] == 1.5

    def test_log_with_correlation_id_and_extra(self) -> None:
        """Test logging with both correlation ID and extra fields."""
        set_correlation_id("context-test")

        log_with_context(
            self.logger, logging.WARNING, "Warning with context", request_id="req-123", endpoint="/api/users"
        )

        log_output = self.log_stream.getvalue()
        parsed = json.loads(log_output.strip())

        assert parsed["message"] == "Warning with context"
        assert parsed["level"] == "WARNING"
        assert parsed["correlation_id"] == "context-test"
        assert parsed["request_id"] == "req-123"
        assert parsed["endpoint"] == "/api/users"

    def test_log_without_extra_fields(self) -> None:
        """Test logging without extra fields."""
        log_with_context(self.logger, logging.ERROR, "Simple error message")

        log_output = self.log_stream.getvalue()
        parsed = json.loads(log_output.strip())

        assert parsed["message"] == "Simple error message"
        assert parsed["level"] == "ERROR"

    @pytest.mark.parametrize(
        "log_level", [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]
    )
    def test_log_with_different_levels(self, log_level: int) -> None:
        """Test log_with_context with different log levels."""
        level_name = logging.getLevelName(log_level)

        log_with_context(self.logger, log_level, f"Message at {level_name} level", level_test=True)

        log_output = self.log_stream.getvalue()
        parsed = json.loads(log_output.strip())

        assert parsed["level"] == level_name
        assert parsed["message"] == f"Message at {level_name} level"
        assert parsed["level_test"] is True


class TestIntegrationScenarios:
    """Integration test scenarios combining multiple logging features."""

    def setup_method(self) -> None:
        """Set up integration test environment."""
        correlation_id_var.set(None)

    def test_complete_structured_logging_flow(self) -> None:
        """Test complete structured logging workflow."""
        # Set up logger with structured formatter
        logger = logging.getLogger("integration_test")
        logger.setLevel(logging.INFO)

        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)

        try:
            # Set correlation ID
            set_correlation_id("integration-flow-123")

            # Log various messages
            logger.info("Starting operation")

            log_with_context(logger, logging.INFO, "Processing user data", user_id=456, operation="update")

            # Simulate error
            try:
                raise ValueError("Simulated error")
            except ValueError:
                logger.error("Operation failed", exc_info=True)

            # Get all log output
            log_output = log_stream.getvalue()
            log_lines = [line.strip() for line in log_output.strip().split("\n") if line.strip()]

            # Should have 3 log entries
            assert len(log_lines) == 3

            # Parse and verify each log entry
            logs = [json.loads(line) for line in log_lines]

            # First log: basic info with correlation ID
            assert logs[0]["message"] == "Starting operation"
            assert logs[0]["correlation_id"] == "integration-flow-123"

            # Second log: with extra context
            assert logs[1]["message"] == "Processing user data"
            assert logs[1]["correlation_id"] == "integration-flow-123"
            assert logs[1]["user_id"] == 456
            assert logs[1]["operation"] == "update"

            # Third log: error with exception
            assert logs[2]["message"] == "Operation failed"
            assert logs[2]["correlation_id"] == "integration-flow-123"
            assert "exception" in logs[2]
            assert "ValueError: Simulated error" in logs[2]["exception"]

        finally:
            logger.removeHandler(handler)

    def test_logger_hierarchy_and_filtering(self) -> None:
        """Test logger hierarchy with correlation filtering."""
        # Create parent and child loggers
        parent_logger = get_logger("parent")
        child_logger = get_logger("parent.child")

        # Both should have correlation filters
        parent_filters = [f for f in parent_logger.filters if isinstance(f, CorrelationIDFilter)]
        child_filters = [f for f in child_logger.filters if isinstance(f, CorrelationIDFilter)]

        assert len(parent_filters) >= 1
        assert len(child_filters) >= 1

    def test_concurrent_logging_with_correlation_ids(self) -> None:
        """Test concurrent logging maintains proper correlation context."""
        import concurrent.futures

        log_stream = StringIO()
        logger = logging.getLogger("concurrent_test")
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)

        def worker_task(worker_id: int) -> None:
            # Each worker sets its own correlation ID
            set_correlation_id(f"worker-{worker_id}")

            # Log a message
            log_with_context(
                logger, logging.INFO, f"Worker {worker_id} processing", worker_id=worker_id, task="concurrent_test"
            )

        try:
            # Run multiple workers concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(worker_task, i) for i in range(10)]

                # Wait for all to complete
                for future in concurrent.futures.as_completed(futures):
                    future.result()

            # Parse all log output
            log_output = log_stream.getvalue()
            log_lines = [line.strip() for line in log_output.strip().split("\n") if line.strip()]
            logs = [json.loads(line) for line in log_lines if line]

            # Should have 10 log entries, each with correct correlation ID
            assert len(logs) == 10

            for log_entry in logs:
                worker_id = log_entry["worker_id"]
                expected_correlation_id = f"worker-{worker_id}"
                assert log_entry["correlation_id"] == expected_correlation_id
                assert log_entry["message"] == f"Worker {worker_id} processing"

        finally:
            logger.removeHandler(handler)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_formatter_with_none_values(self) -> None:
        """Test formatter handles None values correctly."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.name = None  # pyright: ignore
        record.module = None  # pyright: ignore
        record.funcName = None  # pyright: ignore

        # Should not raise exception
        result = formatter.format(record)
        parsed = json.loads(result)

        assert parsed["logger"] is None
        assert parsed["module"] is None
        assert parsed["function"] is None

    def test_filter_with_malformed_record(self) -> None:
        """Test filter handles malformed log records."""
        filter_obj = CorrelationIDFilter()

        # Create minimal record
        record = Mock()

        # Should not raise exception and should return True
        result = filter_obj.filter(record)
        assert result is True

    def test_get_logger_with_empty_name(self) -> None:
        """Test get_logger with empty string name."""
        logger = get_logger("")
        assert logger.name == "sqlspec."  # Should still add prefix

    def test_correlation_id_with_special_characters(self) -> None:
        """Test correlation ID with special characters."""
        special_id = "test-id-with-special!@#$%^&*()chars"
        set_correlation_id(special_id)

        formatter = StructuredFormatter()
        record = logging.LogRecord("test", logging.INFO, "/path", 1, "msg", (), None)
        record.module = "test"
        record.funcName = "test"

        result = formatter.format(record)
        parsed = json.loads(result)

        assert parsed["correlation_id"] == special_id

    def test_unicode_in_log_messages(self) -> None:
        """Test logging with Unicode characters."""
        unicode_message = "Test message with unicode: 你好世界 🌍 café"

        formatter = StructuredFormatter()
        record = logging.LogRecord("test", logging.INFO, "/path", 1, unicode_message, (), None)
        record.module = "test"
        record.funcName = "test"

        result = formatter.format(record)
        parsed = json.loads(result)

        assert parsed["message"] == unicode_message


def test_module_exports() -> None:
    """Test that all expected functions and classes are exported."""
    from sqlspec.utils.logging import __all__

    expected_exports = {
        "StructuredFormatter",
        "correlation_id_var",
        "get_correlation_id",
        "get_logger",
        "set_correlation_id",
    }

    for export in expected_exports:
        assert export in __all__


@pytest.mark.parametrize(
    "correlation_id",
    [
        "simple-id",
        "complex-id-with-many-parts-123",
        "special!@#$%^&*()chars",
        "unicode-café-test",
        "",  # Empty string
        "a" * 1000,  # Very long string
    ],
)
def test_correlation_id_formats(correlation_id: str) -> None:
    """Test various correlation ID formats are handled correctly."""
    set_correlation_id(correlation_id)
    assert get_correlation_id() == correlation_id

    # Test with formatter
    formatter = StructuredFormatter()
    record = logging.LogRecord("test", logging.INFO, "/path", 1, "msg", (), None)
    record.module = "test"
    record.funcName = "test"

    result = formatter.format(record)
    parsed = json.loads(result)

    if correlation_id:  # Non-empty
        assert parsed["correlation_id"] == correlation_id
    else:  # Empty string case
        assert parsed.get("correlation_id") is None
