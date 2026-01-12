"""Amazon Web Services log formatter."""

from datetime import datetime, timezone
from typing import Any, ClassVar

__all__ = ("AWSLogFormatter",)


class AWSLogFormatter:
    """Formatter for AWS CloudWatch Logs structured format.

    Produces JSON-compatible dictionaries that conform to AWS CloudWatch
    structured logging conventions, including:
    - level field with AWS log level conventions
    - requestId for correlation ID
    - xray_trace_id for X-Ray integration
    - ISO 8601 timestamp

    Example:
        ```python
        formatter = AWSLogFormatter()
        entry = formatter.format(
            "INFO",
            "Query executed",
            correlation_id="abc-123",
            trace_id="1-5f84c7a1-sample",
            duration_ms=15.5,
        )
        ```

    Reference:
        https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/AnalyzingLogData.html
    """

    __slots__ = ()

    LEVEL_MAP: ClassVar[dict[str, str]] = {
        "DEBUG": "DEBUG",
        "INFO": "INFO",
        "WARNING": "WARN",
        "ERROR": "ERROR",
        "CRITICAL": "FATAL",
    }

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
        """Format log entry for AWS CloudWatch.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
            message: Log message.
            correlation_id: Request correlation ID (maps to requestId).
            trace_id: X-Ray trace ID.
            span_id: X-Ray segment ID.
            duration_ms: Operation duration in milliseconds.
            extra: Additional fields to include in the log entry.

        Returns:
            Dictionary formatted for AWS CloudWatch structured logging.
        """
        entry: dict[str, Any] = {
            "level": self.LEVEL_MAP.get(level.upper(), "INFO"),
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if correlation_id:
            entry["requestId"] = correlation_id

        if trace_id:
            entry["xray_trace_id"] = trace_id

        if span_id:
            entry["xray_segment_id"] = span_id

        if duration_ms is not None:
            entry["duration_ms"] = duration_ms

        if extra:
            entry.update(extra)

        return entry

    def __hash__(self) -> int:
        return hash(self.__class__.__name__)

    def __repr__(self) -> str:
        return "AWSLogFormatter()"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AWSLogFormatter):
            return NotImplemented
        return True
