"""Base class for instrumented services.

This module provides a base class that adds instrumentation to service operations,
including correlation tracking, performance monitoring, and structured logging.
"""

from typing import TYPE_CHECKING, Any, Optional

from sqlspec.config import InstrumentationConfig
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger
from sqlspec.utils.telemetry import instrument_operation

if TYPE_CHECKING:
    from contextlib import AbstractContextManager


__all__ = ("InstrumentedService",)


class InstrumentedService:
    """Base class for instrumented services.

    This class provides instrumentation for all service operations,
    including logging, telemetry, and performance tracking.
    """

    def __init__(
        self, instrumentation_config: Optional["InstrumentationConfig"] = None, service_name: Optional[str] = None
    ) -> None:
        """Initialize the instrumented service.

        Args:
            instrumentation_config: Instrumentation configuration
            service_name: Name of the service for logging
        """

        self.instrumentation_config = instrumentation_config or InstrumentationConfig()
        self.service_name = service_name or self.__class__.__name__
        self.logger = get_logger(f"service.{self.service_name}")

    def _instrument(self, operation_name: str, **extra_attrs: Any) -> "AbstractContextManager[None]":
        """Create instrumentation context for service operations.

        Args:
            operation_name: Name of the operation
            **extra_attrs: Additional attributes to include in telemetry

        Returns:
            Context manager for the instrumented operation
        """
        return instrument_operation(
            self, operation_name, f"service.{self.service_name.lower()}", service=self.service_name, **extra_attrs
        )

    def _log_operation_start(self, operation: str, **context: Any) -> None:
        """Log the start of a service operation.

        Args:
            operation: Operation name
            **context: Additional context to log
        """
        if self.instrumentation_config.log_service_operations:
            correlation_id = CorrelationContext.get()
            self.logger.info(
                "Starting %s operation",
                operation,
                extra={
                    "operation": operation,
                    "service": self.service_name,
                    "correlation_id": correlation_id,
                    **context,
                },
            )

    def _log_operation_complete(
        self, operation: str, duration_ms: float, result_count: Optional[int] = None, **context: Any
    ) -> None:
        """Log the completion of a service operation.

        Args:
            operation: Operation name
            duration_ms: Duration in milliseconds
            result_count: Optional count of results
            **context: Additional context to log
        """
        if self.instrumentation_config.log_service_operations:
            correlation_id = CorrelationContext.get()
            extra = {
                "operation": operation,
                "service": self.service_name,
                "duration_ms": duration_ms,
                "correlation_id": correlation_id,
                **context,
            }

            if result_count is not None:
                extra["result_count"] = result_count

            self.logger.info("Completed %s operation in %.3fms", operation, duration_ms, extra=extra)

    def _log_operation_error(self, operation: str, error: Exception, **context: Any) -> None:
        """Log an error during a service operation.

        Args:
            operation: Operation name
            error: The exception that occurred
            **context: Additional context to log
        """
        correlation_id = CorrelationContext.get()
        self.logger.error(
            "Error in %s operation: %s",
            operation,
            str(error),
            extra={
                "operation": operation,
                "service": self.service_name,
                "error_type": type(error).__name__,
                "correlation_id": correlation_id,
                **context,
            },
        )
