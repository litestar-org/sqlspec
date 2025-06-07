"""Unit tests for the InstrumentedService base class."""

from unittest.mock import MagicMock, patch

import pytest

from sqlspec.config import InstrumentationConfig
from sqlspec.service.base import InstrumentedService


class TestInstrumentedService:
    """Test the InstrumentedService base class."""

    @pytest.fixture
    def config(self):
        """Create instrumentation config."""
        return InstrumentationConfig(
            log_service_operations=True,
            log_queries=True,
            structured_logging=True,
            generate_correlation_id=True
        )

    @pytest.fixture
    def service(self, config):
        """Create instrumented service instance."""
        return InstrumentedService(config, "TestService")

    def test_init_default(self) -> None:
        """Test initialization with defaults."""
        service = InstrumentedService()
        assert service.instrumentation_config is not None
        assert service.service_name == "InstrumentedService"
        assert service.logger is not None

    def test_init_with_config(self, config) -> None:
        """Test initialization with custom config."""
        service = InstrumentedService(config, "CustomService")
        assert service.instrumentation_config is config
        assert service.service_name == "CustomService"

    @patch("sqlspec.service.base.instrument_operation")
    def test_instrument(self, mock_instrument, service) -> None:
        """Test _instrument method creates proper context."""
        # Mock the context manager
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=None)
        mock_cm.__exit__ = MagicMock(return_value=None)
        mock_instrument.return_value = mock_cm

        # Use the context manager
        with service._instrument("test_op", key1="value1", key2="value2"):
            pass

        # This should call instrument_operation
        mock_instrument.assert_called_once_with(
            service,
            "test_op",
            "service.testservice",
            service="TestService",
            key1="value1",
            key2="value2"
        )

    @patch("sqlspec.utils.correlation.CorrelationContext.get")
    def test_log_operation_start(self, mock_correlation, service) -> None:
        """Test logging operation start."""
        mock_correlation.return_value = "test-correlation-123"

        with patch.object(service.logger, "info") as mock_info:
            service._log_operation_start("test_op", user_id=123, action="read")

            mock_info.assert_called_once_with(
                "Starting %s operation",
                "test_op",
                extra={
                    "operation": "test_op",
                    "service": "TestService",
                    "correlation_id": "test-correlation-123",
                    "user_id": 123,
                    "action": "read"
                }
            )

    def test_log_operation_start_disabled(self) -> None:
        """Test logging disabled when config says so."""
        config = InstrumentationConfig(log_service_operations=False)
        service = InstrumentedService(config)

        with patch.object(service.logger, "info") as mock_info:
            service._log_operation_start("test_op")
            mock_info.assert_not_called()

    @patch("sqlspec.utils.correlation.CorrelationContext.get")
    def test_log_operation_complete(self, mock_correlation, service) -> None:
        """Test logging operation completion."""
        mock_correlation.return_value = "test-correlation-123"

        with patch.object(service.logger, "info") as mock_info:
            service._log_operation_complete(
                "test_op",
                duration_ms=123.45,
                result_count=10,
                status="success"
            )

            mock_info.assert_called_once()
            args, kwargs = mock_info.call_args
            assert "Completed %s operation in %.3fms" in args[0]
            assert args[1] == "test_op"
            assert args[2] == 123.45

            extra = kwargs["extra"]
            assert extra["operation"] == "test_op"
            assert extra["service"] == "TestService"
            assert extra["duration_ms"] == 123.45
            assert extra["correlation_id"] == "test-correlation-123"
            assert extra["result_count"] == 10
            assert extra["status"] == "success"

    def test_log_operation_complete_no_result_count(self, service) -> None:
        """Test logging completion without result count."""
        with patch.object(service.logger, "info") as mock_info:
            service._log_operation_complete("test_op", duration_ms=100.0)

            extra = mock_info.call_args[1]["extra"]
            assert "result_count" not in extra

    @patch("sqlspec.utils.correlation.CorrelationContext.get")
    def test_log_operation_error(self, mock_correlation, service) -> None:
        """Test logging operation errors."""
        mock_correlation.return_value = "error-correlation-123"
        error = ValueError("Test error message")

        with patch.object(service.logger, "error") as mock_error:
            service._log_operation_error("failed_op", error, request_id="req-123")

            mock_error.assert_called_once_with(
                "Error in %s operation: %s",
                "failed_op",
                "Test error message",
                extra={
                    "operation": "failed_op",
                    "service": "TestService",
                    "error_type": "ValueError",
                    "correlation_id": "error-correlation-123",
                    "request_id": "req-123"
                }
            )

    def test_inherited_service(self, config) -> None:
        """Test that services can inherit from InstrumentedService."""

        class MyCustomService(InstrumentedService):
            def custom_operation(self) -> str:
                with self._instrument("custom_op", custom_tag="value"):
                    self._log_operation_start("custom_op")
                    # Do work
                    self._log_operation_complete("custom_op", duration_ms=50.0)
                    return "result"

        service = MyCustomService(config, "MyCustom")
        assert service.service_name == "MyCustom"

        with patch.object(service, "_log_operation_start") as mock_start, \
             patch.object(service, "_log_operation_complete") as mock_complete:
            result = service.custom_operation()

            assert result == "result"
            mock_start.assert_called_once_with("custom_op")
            mock_complete.assert_called_once_with("custom_op", duration_ms=50.0)

    @patch("sqlspec.service.base.get_logger")
    def test_logger_name_format(self, mock_get_logger) -> None:
        """Test that logger name follows correct format."""
        InstrumentedService(service_name="MyService")
        mock_get_logger.assert_called_with("service.MyService")

    def test_instrumentation_config_defaults(self) -> None:
        """Test that default instrumentation config is created if none provided."""
        service = InstrumentedService(instrumentation_config=None)
        assert isinstance(service.instrumentation_config, InstrumentationConfig)
        assert service.instrumentation_config is not None
