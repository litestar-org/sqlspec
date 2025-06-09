# ruff: noqa: PLR6301
"""Centralized logging configuration for SQLSpec.

This module provides a standardized logging setup for the entire SQLSpec library,
including structured logging with correlation IDs and performance metrics.
"""

from __future__ import annotations

import logging
import sys
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from sqlspec._serialization import encode_json

if TYPE_CHECKING:
    from logging import LogRecord

__all__ = (
    "StructuredFormatter",
    "configure_logging",
    "correlation_id_var",
    "get_correlation_id",
    "get_logger",
    "set_correlation_id",
)

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
            record.correlation_id = correlation_id  # type: ignore[attr-defined]
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


def configure_logging(
    level: str = "INFO",
    format_style: str = "structured",
    enable_colors: bool = True,
    log_to_file: str | None = None,
    extra_handlers: list[logging.Handler] | None = None,
) -> None:
    """Configure logging for the entire SQLSpec library.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_style: Log format style ("structured" for JSON, "simple" for text)
        enable_colors: Enable colored output for simple format (if supported)
        log_to_file: Optional file path to log to
        extra_handlers: Additional handlers to add
    """
    # Get the root sqlspec logger
    root_logger = logging.getLogger("sqlspec")
    root_logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)

    if format_style == "structured":
        formatter = StructuredFormatter()
    else:
        # Simple text format with optional colors
        if enable_colors and sys.stdout.isatty():
            try:
                import colorama

                colorama.init(autoreset=True)
                format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            except ImportError:
                format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        else:
            format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        formatter = logging.Formatter(format_string)  # type: ignore

    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler if requested
    if log_to_file:
        file_handler = logging.FileHandler(log_to_file)
        file_handler.setFormatter(StructuredFormatter())  # Always use structured for files
        root_logger.addHandler(file_handler)

    # Add any extra handlers
    if extra_handlers:
        for handler in extra_handlers:
            root_logger.addHandler(handler)

    # Don't propagate to the root Python logger
    root_logger.propagate = False

    # Log initial configuration
    root_logger.info(
        "SQLSpec logging configured",
        extra={
            "extra_fields": {
                "level": level,
                "format_style": format_style,
                "handlers_count": len(root_logger.handlers),
            }
        },
    )


# Convenience function for structured logging with extra fields
def log_with_context(
    logger: logging.Logger,
    level: int,
    message: str,
    **extra_fields: Any,
) -> None:
    """Log a message with structured extra fields.

    Args:
        logger: The logger to use
        level: Log level
        message: Log message
        **extra_fields: Additional fields to include in structured logs
    """
    # Create a LogRecord with extra fields
    record = logger.makeRecord(
        logger.name,
        level,
        "(unknown file)",
        0,
        message,
        (),
        None,
    )
    record.extra_fields = extra_fields  # type: ignore[attr-defined]
    logger.handle(record)
