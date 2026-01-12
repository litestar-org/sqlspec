"""Tests for sqlspec.utils.logging module.

Tests for structured logging utilities with correlation ID support.
Covers logger configuration, structured formatting, and context tracking.
"""

import json
import logging
import threading
from collections.abc import Iterator
from contextvars import copy_context
from io import StringIO
from unittest.mock import Mock

import pytest

from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import (
    CorrelationIDFilter,
    StructuredFormatter,
    __all__,
    correlation_id_var,
    get_correlation_id,
    get_logger,
    log_with_context,
    set_correlation_id,
)

pytestmark = pytest.mark.xdist_group("utils")


def setup_function() -> None:
    """Clear correlation ID before each test."""
    correlation_id_var.set(None)


def teardown_function() -> None:
    """Clear correlation ID after each test to prevent pollution."""
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
        thread_results[f"initial_{thread_id}"] = get_correlation_id()

        thread_specific_id = f"thread-{thread_id}"
        set_correlation_id(thread_specific_id)
        thread_results[f"after_set_{thread_id}"] = get_correlation_id()

    threads = [threading.Thread(target=thread_worker, args=(i,)) for i in range(3)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert get_correlation_id() == main_id

    for i in range(3):
        assert thread_results[f"initial_{i}"] is None
        assert thread_results[f"after_set_{i}"] == f"thread-{i}"


def test_correlation_id_context_variable_copy() -> None:
    """Test that context variables work correctly with copy_context."""
    set_correlation_id("original")

    def task_in_copied_context() -> str | None:
        return get_correlation_id()

    ctx = copy_context()

    result = ctx.run(task_in_copied_context)
    assert result == "original"

    def modify_in_copy() -> None:
        set_correlation_id("modified")

    ctx.run(modify_in_copy)

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

    parsed = json.loads(result)

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

    result = filter_obj.filter(record)

    assert result is True
    assert not hasattr(record, "correlation_id")


def test_correlation_id_filter_always_returns_true() -> None:
    """Test that filter always passes records through."""
    filter_obj = CorrelationIDFilter()

    set_correlation_id("test-id")
    record1 = logging.LogRecord("test", logging.INFO, "/path", 1, "msg", (), None)
    assert filter_obj.filter(record1) is True

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

    correlation_filters = [f for f in logger.filters if isinstance(f, CorrelationIDFilter)]
    assert len(correlation_filters) >= 1


def test_get_logger_filter_not_duplicated() -> None:
    """Test that multiple calls don't duplicate the correlation filter."""
    logger1 = get_logger("test.no.duplicate")
    initial_filter_count = len([f for f in logger1.filters if isinstance(f, CorrelationIDFilter)])

    logger2 = get_logger("test.no.duplicate")
    final_filter_count = len([f for f in logger2.filters if isinstance(f, CorrelationIDFilter)])

    assert logger1 is logger2
    assert initial_filter_count == final_filter_count


def test_get_logger_different_loggers_independent() -> None:
    """Test that different loggers are independent."""
    logger1 = get_logger("test.independent1")
    logger2 = get_logger("test.independent2")

    assert logger1 is not logger2
    assert logger1.name != logger2.name


@pytest.fixture
def log_context_logger() -> "Iterator[tuple[logging.Logger, StringIO]]":
    """Set up a test logger and capture output."""
    logger = logging.getLogger("test_context_logger")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(StructuredFormatter())
    logger.addHandler(handler)

    try:
        yield logger, log_stream
    finally:
        logger.removeHandler(handler)


def test_log_with_extra_fields(log_context_logger: tuple[logging.Logger, StringIO]) -> None:
    """Test logging with extra context fields."""
    logger, log_stream = log_context_logger
    log_with_context(logger, logging.INFO, "Test message", user_id=123, action="login", duration=1.5)

    log_output = log_stream.getvalue()
    parsed = json.loads(log_output.strip())

    assert parsed["message"] == "Test message"
    assert parsed["level"] == "INFO"
    assert parsed["user_id"] == 123
    assert parsed["action"] == "login"
    assert parsed["duration"] == 1.5


def test_log_with_correlation_id_and_extra(log_context_logger: tuple[logging.Logger, StringIO]) -> None:
    """Test logging with both correlation ID and extra fields."""
    logger, log_stream = log_context_logger
    set_correlation_id("context-test")

    log_with_context(logger, logging.WARNING, "Warning with context", request_id="req-123", endpoint="/api/users")

    log_output = log_stream.getvalue()
    parsed = json.loads(log_output.strip())

    assert parsed["message"] == "Warning with context"
    assert parsed["level"] == "WARNING"
    assert parsed["correlation_id"] == "context-test"
    assert parsed["request_id"] == "req-123"
    assert parsed["endpoint"] == "/api/users"


def test_log_without_extra_fields(log_context_logger: tuple[logging.Logger, StringIO]) -> None:
    """Test logging without extra fields."""
    logger, log_stream = log_context_logger
    log_with_context(logger, logging.ERROR, "Simple error message")

    log_output = log_stream.getvalue()
    parsed = json.loads(log_output.strip())

    assert parsed["message"] == "Simple error message"
    assert parsed["level"] == "ERROR"


@pytest.mark.parametrize("log_level", [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL])
def test_log_with_different_levels(log_context_logger: tuple[logging.Logger, StringIO], log_level: int) -> None:
    """Test log_with_context with different log levels."""
    logger, log_stream = log_context_logger
    level_name = logging.getLevelName(log_level)

    log_with_context(logger, log_level, f"Message at {level_name} level", level_test=True)

    log_output = log_stream.getvalue()
    parsed = json.loads(log_output.strip())

    assert parsed["level"] == level_name
    assert parsed["message"] == f"Message at {level_name} level"
    assert parsed["level_test"] is True


def test_complete_structured_logging_flow() -> None:
    """Test complete structured logging workflow."""
    correlation_id_var.set(None)

    logger = logging.getLogger("integration_test")
    logger.setLevel(logging.INFO)

    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(StructuredFormatter())
    logger.addHandler(handler)

    try:
        set_correlation_id("integration-flow-123")

        logger.info("Starting operation")

        log_with_context(logger, logging.INFO, "Processing user data", user_id=456, operation="update")

        try:
            raise ValueError("Simulated error")
        except ValueError:
            logger.error("Operation failed", exc_info=True)

        log_output = log_stream.getvalue()
        log_lines = [line.strip() for line in log_output.strip().split("\n") if line.strip()]

        assert len(log_lines) == 3

        logs = [json.loads(line) for line in log_lines]

        assert logs[0]["message"] == "Starting operation"
        assert logs[0]["correlation_id"] == "integration-flow-123"

        assert logs[1]["message"] == "Processing user data"
        assert logs[1]["correlation_id"] == "integration-flow-123"
        assert logs[1]["user_id"] == 456
        assert logs[1]["operation"] == "update"

        assert logs[2]["message"] == "Operation failed"
        assert logs[2]["correlation_id"] == "integration-flow-123"
        assert "exception" in logs[2]
        assert "ValueError: Simulated error" in logs[2]["exception"]

    finally:
        logger.removeHandler(handler)


def test_logger_hierarchy_and_filtering() -> None:
    """Test logger hierarchy with correlation filtering."""
    parent_logger = get_logger("parent")
    child_logger = get_logger("parent.child")

    parent_filters = [f for f in parent_logger.filters if isinstance(f, CorrelationIDFilter)]
    child_filters = [f for f in child_logger.filters if isinstance(f, CorrelationIDFilter)]

    assert len(parent_filters) >= 1
    assert len(child_filters) >= 1


def test_concurrent_logging_with_correlation_ids() -> None:
    """Test concurrent logging maintains proper correlation context."""
    import concurrent.futures

    log_stream = StringIO()
    logger = logging.getLogger("concurrent_test")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(StructuredFormatter())
    logger.addHandler(handler)

    def worker_task(worker_id: int) -> None:
        set_correlation_id(f"worker-{worker_id}")

        log_with_context(
            logger, logging.INFO, f"Worker {worker_id} processing", worker_id=worker_id, task="concurrent_test"
        )

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker_task, i) for i in range(10)]

            for future in concurrent.futures.as_completed(futures):
                future.result()

        log_output = log_stream.getvalue()
        log_lines = [line.strip() for line in log_output.strip().split("\n") if line.strip()]
        logs = [json.loads(line) for line in log_lines if line]

        assert len(logs) == 10

        for log_entry in logs:
            worker_id = log_entry["worker_id"]
            expected_correlation_id = f"worker-{worker_id}"
            assert log_entry["correlation_id"] == expected_correlation_id
            assert log_entry["message"] == f"Worker {worker_id} processing"

    finally:
        logger.removeHandler(handler)


def test_formatter_with_none_values() -> None:
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
    setattr(record, "name", None)
    setattr(record, "module", None)
    setattr(record, "funcName", None)

    result = formatter.format(record)
    parsed = json.loads(result)

    assert parsed["logger"] is None
    assert parsed["module"] is None
    assert parsed["function"] is None


def test_filter_with_malformed_record() -> None:
    """Test filter handles malformed log records."""
    filter_obj = CorrelationIDFilter()

    record = Mock()

    result = filter_obj.filter(record)
    assert result is True


def test_get_logger_with_empty_name() -> None:
    """Test get_logger with empty string name."""
    logger = get_logger("")
    assert logger.name == "sqlspec."


def test_correlation_id_with_special_characters() -> None:
    """Test correlation ID with special characters."""
    special_id = "test-id-with-special!@#$%"
    set_correlation_id(special_id)

    formatter = StructuredFormatter()
    record = logging.LogRecord("test", logging.INFO, "/path", 1, "msg", (), None)
    record.module = "test"
    record.funcName = "test"

    result = formatter.format(record)
    parsed = json.loads(result)

    assert parsed["correlation_id"] == special_id


def test_unicode_in_log_messages() -> None:
    """Test logging with Unicode characters."""
    unicode_message = "Test message with unicode: cafe"

    formatter = StructuredFormatter()
    record = logging.LogRecord("test", logging.INFO, "/path", 1, unicode_message, (), None)
    record.module = "test"
    record.funcName = "test"

    result = formatter.format(record)
    parsed = json.loads(result)

    assert parsed["message"] == unicode_message


def test_module_exports() -> None:
    """Test that all expected functions and classes are exported."""

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
    ["simple-id", "complex-id-with-many-parts-123", "special!@#$%^&*", "unicode-café-test", "", "a" * 1000],
)
def test_correlation_id_formats(correlation_id: str) -> None:
    """Test various correlation ID formats are handled correctly."""
    set_correlation_id(correlation_id)
    assert get_correlation_id() == correlation_id

    formatter = StructuredFormatter()
    record = logging.LogRecord("test", logging.INFO, "/path", 1, "msg", (), None)
    record.module = "test"
    record.funcName = "test"

    result = formatter.format(record)
    parsed = json.loads(result)

    if correlation_id:
        assert parsed["correlation_id"] == correlation_id
    else:
        assert parsed.get("correlation_id") is None


def test_structured_formatter_includes_logging_extra_fields() -> None:
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(StructuredFormatter())

    logger = get_logger("tests.logging.extra")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)

    try:
        with CorrelationContext.context("cid-123"):
            logger.info("hello", extra={"foo": "bar"})
    finally:
        logger.removeHandler(handler)

    payload = json.loads(stream.getvalue().strip())
    assert payload["message"] == "hello"
    assert payload["foo"] == "bar"
    assert payload["correlation_id"] == "cid-123"


def test_log_with_context_preserves_source_location_and_fields() -> None:
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(StructuredFormatter())

    logger = get_logger("tests.logging.context")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)

    try:
        log_with_context(logger, logging.INFO, "event.test", driver="Dummy")
    finally:
        logger.removeHandler(handler)

    payload = json.loads(stream.getvalue().strip())
    assert payload["message"] == "event.test"
    assert payload["driver"] == "Dummy"
    assert payload["line"] != 0
