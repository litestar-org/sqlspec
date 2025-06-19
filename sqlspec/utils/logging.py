"""Logging utilities for SQLSpec.

This module provides utilities for structured logging with correlation IDs.
Users should configure their own logging handlers and levels as needed.
SQLSpec provides StructuredFormatter for JSON-formatted logs if desired.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from sqlspec._serialization import encode_json

if TYPE_CHECKING:
    from logging import LogRecord

__all__ = ("StructuredFormatter", "correlation_id_var", "get_correlation_id", "get_logger", "set_correlation_id")

# Context variable for correlation ID tracking
correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def set_correlation_id(correlation_id: str | None) -> None:
    """Set the correlation ID for the current context.

    Args:
        correlation_id: The correlation ID to set, or None to clear
    """
    correlation_id_var.set(correlation_id)


def get_correlation_id() -> str | None:
    """Get the current correlation ID.

    Returns:
        The current correlation ID or None if not set
    """
    return correlation_id_var.get()


class StructuredFormatter(logging.Formatter):
    """Structured JSON formatter with correlation ID support."""

    def format(self, record: LogRecord) -> str:
        """Format log record as structured JSON.

        Args:
            record: The log record to format

        Returns:
            JSON formatted log entry
        """
        # Base log entry
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add correlation ID if available
        if correlation_id := get_correlation_id():
            log_entry["correlation_id"] = correlation_id

        # Add any extra fields from the record
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)  # pyright: ignore

        # Add exception info if present
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


def get_logger(name: str | None = None) -> logging.Logger:
    """Get a logger instance with standardized configuration.

    Args:
        name: Logger name. If not provided, returns the root sqlspec logger.

    Returns:
        Configured logger instance
    """
    if name is None:
        return logging.getLogger("sqlspec")

    # Ensure all loggers are under the sqlspec namespace
    if not name.startswith("sqlspec"):
        name = f"sqlspec.{name}"

    logger = logging.getLogger(name)

    # Add correlation ID filter if not already present
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
    # Create a LogRecord with extra fields
    record = logger.makeRecord(logger.name, level, "(unknown file)", 0, message, (), None)
    record.extra_fields = extra_fields
    logger.handle(record)
