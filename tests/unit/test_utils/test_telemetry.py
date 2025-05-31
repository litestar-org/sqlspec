"""Tests for sqlspec.utils.telemetry module."""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async


@pytest.fixture
def mock_driver() -> Mock:
    """Create a mock driver object with instrumentation capabilities."""
    driver = Mock()
    driver.dialect = "postgresql"

    # Mock instrumentation config
    driver.instrumentation_config = Mock()
    driver.instrumentation_config.log_queries = True
    driver.instrumentation_config.log_runtime = True
    driver.instrumentation_config.service_name = "test_service"
    driver.instrumentation_config.custom_tags = {"env": "test"}

    # Mock tracer
    driver._tracer = Mock()
    span = Mock()
    driver._tracer.start_span.return_value = span

    # Mock metrics
    driver._query_counter = Mock()
    driver._latency_histogram = Mock()
    driver._error_counter = Mock()

    return driver


@pytest.fixture
def driver_without_instrumentation() -> Mock:
    """Create a mock driver without instrumentation config."""
    return Mock(spec=[])  # Empty spec means no attributes


def test_instrument_operation_without_instrumentation_config(driver_without_instrumentation: Mock) -> None:
    """Test that instrument_operation works when driver has no instrumentation."""
    executed = False

    with instrument_operation(driver_without_instrumentation, "test_operation"):
        executed = True

    assert executed


def test_instrument_operation_basic_success(mock_driver: Mock) -> None:
    """Test basic successful operation instrumentation."""
    executed = False

    with instrument_operation(mock_driver, "select_query"):
        executed = True

    assert executed

    # Verify tracer was called
    mock_driver._tracer.start_span.assert_called_once_with("select_query")
    span = mock_driver._tracer.start_span.return_value
    span.set_attribute.assert_any_call("operation.type", "database")
    span.set_attribute.assert_any_call("db.system", "postgresql")
    span.set_attribute.assert_any_call("service.name", "test_service")
    span.set_attribute.assert_any_call("env", "test")
    span.end.assert_called_once()

    # Verify metrics were updated
    mock_driver._query_counter.labels.assert_called_once()
    mock_driver._latency_histogram.labels.assert_called_once()


def test_instrument_operation_custom_operation_type(mock_driver: Mock) -> None:
    """Test instrument_operation with custom operation type."""
    with instrument_operation(mock_driver, "backup_operation", operation_type="maintenance"):
        pass

    span = mock_driver._tracer.start_span.return_value
    span.set_attribute.assert_any_call("operation.type", "maintenance")


def test_instrument_operation_with_custom_tags(mock_driver: Mock) -> None:
    """Test instrument_operation with additional custom tags."""
    custom_tags = {"table": "users", "action": "insert"}

    with instrument_operation(mock_driver, "insert_query", **custom_tags):
        pass

    span = mock_driver._tracer.start_span.return_value
    span.set_attribute.assert_any_call("table", "users")
    span.set_attribute.assert_any_call("action", "insert")
    span.set_attribute.assert_any_call("env", "test")  # From config


def test_instrument_operation_exception_handling(mock_driver: Mock) -> None:
    """Test that exceptions are properly handled and metrics updated."""
    test_exception = ValueError("Database error")

    with pytest.raises(ValueError, match="Database error"):
        with instrument_operation(mock_driver, "failing_query"):
            raise test_exception

    span = mock_driver._tracer.start_span.return_value
    span.record_exception.assert_called_once_with(test_exception)
    span.end.assert_called_once()

    # Verify error counter was updated
    mock_driver._error_counter.labels.assert_called_once()


def test_instrument_operation_no_tracer(mock_driver: Mock) -> None:
    """Test instrument_operation when driver has no tracer."""
    mock_driver._tracer = None

    executed = False
    with instrument_operation(mock_driver, "test_operation"):
        executed = True

    assert executed


def test_instrument_operation_no_metrics(mock_driver: Mock) -> None:
    """Test instrument_operation when driver has no metrics objects."""
    mock_driver._query_counter = None
    mock_driver._latency_histogram = None
    mock_driver._error_counter = None

    executed = False
    with instrument_operation(mock_driver, "test_operation"):
        executed = True

    assert executed


def test_instrument_operation_logging_disabled(mock_driver: Mock) -> None:
    """Test instrument_operation when logging is disabled."""
    mock_driver.instrumentation_config.log_queries = False
    mock_driver.instrumentation_config.log_runtime = False

    with patch("sqlspec.utils.telemetry.logger") as mock_logger:
        with instrument_operation(mock_driver, "test_operation"):
            pass

        # Should not log when disabled
        mock_logger.info.assert_not_called()


@patch("sqlspec.utils.telemetry.logger")
def test_instrument_operation_logging_enabled(mock_logger: Mock, mock_driver: Mock) -> None:
    """Test instrument_operation logging when enabled."""
    with instrument_operation(mock_driver, "test_operation"):
        pass

    # Should log start and completion
    assert mock_logger.info.call_count >= 2


@patch("sqlspec.utils.telemetry.logger")
def test_instrument_operation_exception_logging(mock_logger: Mock, mock_driver: Mock) -> None:
    """Test that exceptions are logged properly."""
    test_exception = RuntimeError("Test error")

    with pytest.raises(RuntimeError):
        with instrument_operation(mock_driver, "failing_operation"):
            raise test_exception

    # Should log exception
    mock_logger.exception.assert_called_once()


def test_instrument_operation_timing_measurement(mock_driver: Mock) -> None:
    """Test that operation timing is measured and recorded."""
    with patch("sqlspec.utils.telemetry.time.monotonic", side_effect=[0.0, 0.1]):
        with instrument_operation(mock_driver, "timed_operation"):
            pass

    span = mock_driver._tracer.start_span.return_value
    span.set_attribute.assert_any_call("duration_ms", 100.0)  # 0.1 seconds = 100ms


@pytest.mark.asyncio
async def test_instrument_operation_async_without_instrumentation_config(
    driver_without_instrumentation: Mock,
) -> None:
    """Test that async instrument_operation works when driver has no instrumentation."""
    executed = False

    async with instrument_operation_async(driver_without_instrumentation, "test_operation"):
        executed = True

    assert executed


@pytest.mark.asyncio
async def test_instrument_operation_async_basic_success(mock_driver: Mock) -> None:
    """Test basic successful async operation instrumentation."""
    executed = False

    async with instrument_operation_async(mock_driver, "async_select_query"):
        executed = True

    assert executed

    # Verify tracer was called
    mock_driver._tracer.start_span.assert_called_once_with("async_select_query")
    span = mock_driver._tracer.start_span.return_value
    span.set_attribute.assert_any_call("operation.type", "database")
    span.set_attribute.assert_any_call("db.system", "postgresql")
    span.end.assert_called_once()


@pytest.mark.asyncio
async def test_instrument_operation_async_exception_handling(mock_driver: Mock) -> None:
    """Test that async exceptions are properly handled."""
    test_exception = ValueError("Async database error")

    with pytest.raises(ValueError, match="Async database error"):
        async with instrument_operation_async(mock_driver, "failing_async_query"):
            raise test_exception

    span = mock_driver._tracer.start_span.return_value
    span.record_exception.assert_called_once_with(test_exception)
    span.end.assert_called_once()


@pytest.mark.asyncio
async def test_instrument_operation_async_custom_tags(mock_driver: Mock) -> None:
    """Test async instrument_operation with custom tags."""
    custom_tags = {"async": "true", "pool": "main"}

    async with instrument_operation_async(mock_driver, "async_query", **custom_tags):
        pass

    span = mock_driver._tracer.start_span.return_value
    span.set_attribute.assert_any_call("async", "true")
    span.set_attribute.assert_any_call("pool", "main")


@pytest.mark.asyncio
@patch("sqlspec.utils.telemetry.logger")
async def test_instrument_operation_async_logging(mock_logger: Mock, mock_driver: Mock) -> None:
    """Test async operation logging."""
    async with instrument_operation_async(mock_driver, "async_operation"):
        pass

    # Should log start and completion
    assert mock_logger.info.call_count >= 2


@pytest.mark.asyncio
async def test_instrument_operation_async_timing_measurement(mock_driver: Mock) -> None:
    """Test that async operation timing is measured correctly."""
    with patch("sqlspec.utils.telemetry.time.monotonic", side_effect=[0.0, 0.05]):
        async with instrument_operation_async(mock_driver, "timed_async_operation"):
            pass

    span = mock_driver._tracer.start_span.return_value
    span.set_attribute.assert_any_call("duration_ms", 50.0)  # 0.05 seconds = 50ms


def test_instrument_operation_custom_tags_override_config_tags(mock_driver: Mock) -> None:
    """Test that custom tags override config tags when they have the same key."""
    mock_driver.instrumentation_config.custom_tags = {"env": "prod", "region": "us-east"}
    custom_tags = {"env": "test", "table": "orders"}  # Override env

    with instrument_operation(mock_driver, "test_query", **custom_tags):
        pass

    span = mock_driver._tracer.start_span.return_value
    span.set_attribute.assert_any_call("env", "test")  # Should use override
    span.set_attribute.assert_any_call("region", "us-east")  # Should use config
    span.set_attribute.assert_any_call("table", "orders")  # Should use custom


def test_instrument_operation_no_custom_tags_in_config(mock_driver: Mock) -> None:
    """Test instrument_operation when config has no custom_tags attribute."""
    del mock_driver.instrumentation_config.custom_tags

    with instrument_operation(mock_driver, "test_operation", custom="tag"):
        pass

    span = mock_driver._tracer.start_span.return_value
    span.set_attribute.assert_any_call("custom", "tag")


@pytest.mark.parametrize(
    ("operation_name", "operation_type", "expected_span_name"),
    [
        ("select_users", "query", "select_users"),
        ("backup_db", "maintenance", "backup_db"),
        ("health_check", "monitoring", "health_check"),
        ("", "database", ""),  # Edge case: empty operation name
    ],
    ids=["query_operation", "maintenance_operation", "monitoring_operation", "empty_name"],
)
def test_instrument_operation_various_operation_types(
    mock_driver: Mock, operation_name: str, operation_type: str, expected_span_name: str
) -> None:
    """Test instrument_operation with various operation names and types."""
    with instrument_operation(mock_driver, operation_name, operation_type=operation_type):
        pass

    mock_driver._tracer.start_span.assert_called_once_with(expected_span_name)
    span = mock_driver._tracer.start_span.return_value
    span.set_attribute.assert_any_call("operation.type", operation_type)


def test_instrument_operation_metrics_labels_include_custom_tags(mock_driver: Mock) -> None:
    """Test that metrics labels include custom tags."""
    custom_tags = {"table": "users", "action": "select"}

    with instrument_operation(mock_driver, "select_query", **custom_tags):
        pass

    # Verify metrics were called with custom tags
    expected_labels_call = mock_driver._query_counter.labels
    call_args = expected_labels_call.call_args
    assert "table" in str(call_args) or any("table" in str(arg) for arg in call_args.kwargs.values())


def test_instrument_operation_exception_metrics_include_error_type(mock_driver: Mock) -> None:
    """Test that error metrics include the exception type."""
    with pytest.raises(KeyError):
        with instrument_operation(mock_driver, "failing_query"):
            raise KeyError("Key not found")

    # Verify error counter was called with error type
    expected_call = mock_driver._error_counter.labels
    call_args = expected_call.call_args
    # Should include error_type=KeyError
    assert "KeyError" in str(call_args) or "error_type" in str(call_args)
