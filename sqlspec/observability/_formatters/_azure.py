"""Microsoft Azure log formatter."""

from typing import Any, ClassVar

__all__ = ("AzureLogFormatter",)


class AzureLogFormatter:
    """Formatter for Azure Monitor / Application Insights structured format.

    Produces JSON-compatible dictionaries that conform to Azure Monitor
    structured logging conventions, including:
    - severityLevel with numeric severity (0-4)
    - operation_Id for trace correlation
    - operation_ParentId for span tracking
    - properties dict for custom fields

    Example:
        ```python
        formatter = AzureLogFormatter()
        entry = formatter.format(
            "INFO",
            "Query executed",
            correlation_id="abc-123",
            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
            duration_ms=15.5,
        )
        ```

    Reference:
        https://docs.microsoft.com/en-us/azure/azure-monitor/app/data-model
    """

    __slots__ = ()

    SEVERITY_MAP: ClassVar[dict[str, int]] = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}

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
        """Format log entry for Azure Monitor.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
            message: Log message.
            correlation_id: Request correlation ID.
            trace_id: Operation ID for distributed tracing.
            span_id: Parent operation ID.
            duration_ms: Operation duration in milliseconds.
            extra: Additional fields to include in properties.

        Returns:
            Dictionary formatted for Azure Monitor structured logging.
        """
        entry: dict[str, Any] = {"message": message, "severityLevel": self.SEVERITY_MAP.get(level.upper(), 1)}

        if trace_id:
            entry["operation_Id"] = trace_id

        if span_id:
            entry["operation_ParentId"] = span_id

        properties: dict[str, Any] = {}

        if correlation_id:
            properties["correlationId"] = correlation_id

        if duration_ms is not None:
            properties["durationMs"] = duration_ms

        if extra:
            properties.update(extra)

        if properties:
            entry["properties"] = properties

        return entry

    def __hash__(self) -> int:
        return hash(self.__class__.__name__)

    def __repr__(self) -> str:
        return "AzureLogFormatter()"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AzureLogFormatter):
            return NotImplemented
        return True
