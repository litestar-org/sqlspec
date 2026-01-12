"""Base protocol for cloud log formatters."""

from typing import Any, Protocol

__all__ = ("CloudLogFormatter",)


class CloudLogFormatter(Protocol):
    """Protocol for cloud-specific log formatting.

    Implementations format log entries according to cloud provider
    structured logging requirements (GCP, AWS, Azure).

    Example:
        ```python
        class GCPLogFormatter:
            def format(
                self,
                level: str,
                message: str,
                *,
                correlation_id: str | None = None,
                trace_id: str | None = None,
                span_id: str | None = None,
                duration_ms: float | None = None,
                extra: dict[str, Any] | None = None,
            ) -> dict[str, Any]:
                return {"severity": level, "message": message, ...}
        ```
    """

    def format(
        self,
        level: str,
        message: str,
        *,
        correlation_id: str | None = None,
        trace_id: str | None = None,
        span_id: str | None = None,
        duration_ms: float | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Format log entry for cloud provider.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
            message: Log message.
            correlation_id: Request correlation ID.
            trace_id: Distributed trace ID.
            span_id: Span ID within the trace.
            duration_ms: Operation duration in milliseconds.
            extra: Additional fields to include in the log entry.

        Returns:
            Dictionary formatted for the cloud provider's structured logging.
        """
        ...
