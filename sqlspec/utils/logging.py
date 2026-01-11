"""Logging utilities for SQLSpec.

This module provides utilities for structured logging with correlation IDs.
Users should configure their own logging handlers and levels as needed.
SQLSpec provides StructuredFormatter for JSON-formatted logs if desired.
"""

import logging
from logging import LogRecord
from typing import TYPE_CHECKING, Any, cast

from sqlspec._serialization import encode_json
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.correlation import correlation_id_var as _correlation_id_var

if TYPE_CHECKING:
    from contextvars import ContextVar

__all__ = (
    "SqlglotCommandFallbackFilter",
    "StructuredFormatter",
    "correlation_id_var",
    "get_correlation_id",
    "get_logger",
    "set_correlation_id",
    "suppress_erroneous_sqlglot_log_messages",
)

_BASE_RECORD_KEYS: "set[str] | None" = None


def _get_base_record_keys() -> "set[str]":
    """Get base LogRecord keys lazily to avoid mypyc module-level dict issues."""
    global _BASE_RECORD_KEYS
    if _BASE_RECORD_KEYS is None:
        _BASE_RECORD_KEYS = set(
            logging.LogRecord(
                name="sqlspec", level=logging.INFO, pathname="(unknown file)", lineno=0, msg="", args=(), exc_info=None
            ).__dict__.keys()
        )
        _BASE_RECORD_KEYS.update({"message", "asctime"})
    return _BASE_RECORD_KEYS


correlation_id_var: "ContextVar[str | None]" = _correlation_id_var


def _get_trace_context() -> "tuple[str | None, str | None]":
    """Resolve trace context lazily to avoid import cycles.

    Returns:
        Tuple of (trace_id, span_id) or (None, None) if unavailable.
    """
    try:
        from sqlspec.observability import get_trace_context
    except Exception:
        return (None, None)
    return get_trace_context()


def set_correlation_id(correlation_id: "str | None") -> None:
    """Set the correlation ID for the current context.

    Args:
        correlation_id: The correlation ID to set, or None to clear
    """
    CorrelationContext.set(correlation_id)


def get_correlation_id() -> "str | None":
    """Get the current correlation ID.

    Returns:
        The current correlation ID or None if not set
    """
    return CorrelationContext.get()


class StructuredFormatter(logging.Formatter):
    """Structured JSON formatter with correlation ID support."""

    def format(self, record: LogRecord) -> str:
        """Format log record as structured JSON.

        Args:
            record: The log record to format

        Returns:
            JSON formatted log entry
        """
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        record_dict = record.__dict__
        correlation_id = cast("str | None", record_dict.get("correlation_id")) or get_correlation_id()
        if correlation_id:
            log_entry["correlation_id"] = correlation_id
        trace_id = cast("str | None", record_dict.get("trace_id"))
        span_id = cast("str | None", record_dict.get("span_id"))
        if trace_id is None or span_id is None:
            trace_id, span_id = _get_trace_context()
        if trace_id:
            log_entry["trace_id"] = trace_id
        if span_id:
            log_entry["span_id"] = span_id

        extra_fields = record_dict.get("extra_fields")
        if isinstance(extra_fields, dict):
            log_entry.update(extra_fields)

        extras = {
            key: value
            for key, value in record_dict.items()
            if key not in _get_base_record_keys() and key not in {"extra_fields", "correlation_id"}
        }
        if extras:
            log_entry.update(extras)

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return encode_json(log_entry)


class CorrelationIDFilter(logging.Filter):
    """Filter that adds correlation ID to log records."""

    def filter(self, record: LogRecord) -> bool:
        """Add correlation ID to record if available.

        Args:
            record: The log record to filter

        Returns:
            Always True to pass the record through
        """
        if correlation_id := get_correlation_id():
            record.correlation_id = correlation_id
        return True


class SqlglotCommandFallbackFilter(logging.Filter):
    """Filter to suppress sqlglot warnings we consider benign.

    - "Falling back to parsing as a 'Command'": emitted when sqlglot hits syntax it
      intentionally downgrades; expected in SQLSpec usage.
    - "Cannot traverse scope …": emitted by sqlglot's scope analysis in cases where
      SQLSpec feeds partially constructed expressions; harmless for our flows.
    """

    _suppressed_substrings = (
        "falling back to parsing as a 'command'",
        "cannot traverse scope",
        "locking reads using 'for update/share' are not supported",
    )

    def filter(self, record: LogRecord) -> bool:
        """Suppress known-safe sqlglot warnings.

        Args:
            record: The log record to evaluate

        Returns:
            False if the record message matches a suppressed pattern, True otherwise.
        """
        message = record.getMessage().lower()
        return not any(substr in message for substr in self._suppressed_substrings)


def get_logger(name: "str | None" = None) -> logging.Logger:
    """Get a logger instance with standardized configuration.

    Args:
        name: Logger name. If not provided, returns the root sqlspec logger.

    Returns:
        Configured logger instance
    """
    if name is None:
        return logging.getLogger("sqlspec")

    if not name.startswith("sqlspec"):
        name = f"sqlspec.{name}"

    logger = logging.getLogger(name)

    if not any(isinstance(f, CorrelationIDFilter) for f in logger.filters):
        logger.addFilter(CorrelationIDFilter())

    return logger


def log_with_context(logger: logging.Logger, level: int, message: str, **extra_fields: Any) -> None:
    """Log a message with structured extra fields.

    Args:
        logger: The logger to use
        level: Log level
        message: Log message
        **extra_fields: Additional fields to include in structured logs
    """
    logger.log(level, message, extra={"extra_fields": extra_fields}, stacklevel=2)


def suppress_erroneous_sqlglot_log_messages() -> None:
    """Suppress confusing sqlglot warning messages.

    Adds a filter to the sqlglot logger to suppress the warning message
    about falling back to parsing as a Command. This is expected behavior
    in SQLSpec and the warning is confusing to users.
    """
    for logger_name in ("sqlglot", "sqlglot.scope", "sqlglot.generator"):
        sqlglot_logger = logging.getLogger(logger_name)
        if not any(isinstance(f, SqlglotCommandFallbackFilter) for f in sqlglot_logger.filters):
            sqlglot_logger.addFilter(SqlglotCommandFallbackFilter())
