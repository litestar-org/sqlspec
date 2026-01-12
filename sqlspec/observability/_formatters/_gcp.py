"""Google Cloud Platform log formatter."""

import os
from typing import Any, ClassVar

__all__ = ("GCPLogFormatter",)


class GCPLogFormatter:
    """Formatter for Google Cloud Logging structured format.

    Produces JSON-compatible dictionaries that conform to GCP's
    structured logging format, including:
    - severity field with GCP severity levels
    - logging.googleapis.com/trace for trace correlation
    - logging.googleapis.com/spanId for span tracking
    - logging.googleapis.com/labels for custom labels
    - logging.googleapis.com/sourceLocation for code location

    Example:
        ```python
        formatter = GCPLogFormatter(project_id="my-project")
        entry = formatter.format(
            "INFO",
            "Query executed",
            correlation_id="abc-123",
            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
            duration_ms=15.5,
        )
        ```

    Reference:
        https://cloud.google.com/logging/docs/structured-logging
    """

    __slots__ = ("_project_id",)

    SEVERITY_MAP: ClassVar[dict[str, str]] = {
        "DEBUG": "DEBUG",
        "INFO": "INFO",
        "WARNING": "WARNING",
        "ERROR": "ERROR",
        "CRITICAL": "CRITICAL",
    }

    def __init__(self, project_id: str | None = None) -> None:
        """Initialize GCP log formatter.

        Args:
            project_id: GCP project ID for trace URL construction.
                If not provided, attempts to read from GOOGLE_CLOUD_PROJECT
                environment variable.
        """
        self._project_id = project_id or os.environ.get("GOOGLE_CLOUD_PROJECT")

    @property
    def project_id(self) -> str | None:
        """Get the configured GCP project ID."""
        return self._project_id

    def format(
        self,
        level: str,
        message: str,
        *,
        correlation_id: str | None = None,
        trace_id: str | None = None,
        span_id: str | None = None,
        duration_ms: float | None = None,
        source_file: str | None = None,
        source_line: int | None = None,
        source_function: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Format log entry for Google Cloud Logging.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
            message: Log message.
            correlation_id: Request correlation ID.
            trace_id: Distributed trace ID.
            span_id: Span ID within the trace.
            duration_ms: Operation duration in milliseconds.
            source_file: Source file path.
            source_line: Source line number.
            source_function: Source function name.
            extra: Additional fields to include in the log entry.

        Returns:
            Dictionary formatted for GCP structured logging.
        """
        entry: dict[str, Any] = {"severity": self.SEVERITY_MAP.get(level.upper(), "DEFAULT"), "message": message}

        if trace_id and self._project_id:
            entry["logging.googleapis.com/trace"] = f"projects/{self._project_id}/traces/{trace_id}"

        if span_id:
            entry["logging.googleapis.com/spanId"] = span_id

        if correlation_id:
            entry.setdefault("logging.googleapis.com/labels", {})
            entry["logging.googleapis.com/labels"]["correlation_id"] = correlation_id

        if source_file or source_line is not None or source_function:
            source_location: dict[str, str] = {}
            if source_file:
                source_location["file"] = source_file
            if source_line is not None:
                source_location["line"] = str(source_line)
            if source_function:
                source_location["function"] = source_function
            entry["logging.googleapis.com/sourceLocation"] = source_location

        if duration_ms is not None:
            entry["duration_ms"] = duration_ms

        if extra:
            entry.update(extra)

        return entry

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self._project_id))

    def __repr__(self) -> str:
        return f"GCPLogFormatter(project_id={self._project_id!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GCPLogFormatter):
            return NotImplemented
        return self._project_id == other._project_id
