"""Correlation ID tracking for distributed tracing.

This module provides utilities for tracking correlation IDs across
database operations, enabling distributed tracing and debugging.
"""

import uuid
from collections.abc import Generator, MutableMapping
from contextlib import contextmanager
from contextvars import ContextVar
from logging import Logger, LoggerAdapter
from typing import Any, ClassVar

__all__ = ("CorrelationContext", "correlation_context", "get_correlation_adapter")

correlation_id_var: "ContextVar[str | None]" = ContextVar("sqlspec_correlation_id", default=None)


class CorrelationContext:
    """Context manager for correlation ID tracking.

    This class provides a context-aware way to track correlation IDs
    across async and sync operations.
    """

    _correlation_id: ClassVar["ContextVar[str | None]"] = correlation_id_var

    @classmethod
    def get(cls) -> str | None:
        """Get the current correlation ID.

        Returns:
            The current correlation ID or None if not set
        """
        return cls._correlation_id.get()

    @classmethod
    def set(cls, correlation_id: str | None) -> None:
        """Set the correlation ID.

        Args:
            correlation_id: The correlation ID to set
        """
        cls._correlation_id.set(correlation_id)

    @classmethod
    def generate(cls) -> str:
        """Generate a new correlation ID.

        Returns:
            A new UUID-based correlation ID
        """
        return str(uuid.uuid4())

    @classmethod
    @contextmanager
    def context(cls, correlation_id: str | None = None) -> Generator[str, None, None]:
        """Context manager for correlation ID scope.

        Args:
            correlation_id: The correlation ID to use. If None, generates a new one.

        Yields:
            The correlation ID being used
        """
        if correlation_id is None:
            correlation_id = cls.generate()

        previous_id = cls.get()

        try:
            cls.set(correlation_id)
            yield correlation_id
        finally:
            cls.set(previous_id)

    @classmethod
    def clear(cls) -> None:
        """Clear the current correlation ID."""
        cls.set(None)

    @classmethod
    def to_dict(cls) -> "dict[str, Any]":
        """Get correlation context as a dictionary.

        Returns:
            Dictionary with correlation_id key if set
        """
        correlation_id = cls.get()
        return {"correlation_id": correlation_id} if correlation_id else {}


@contextmanager
def correlation_context(correlation_id: str | None = None) -> Generator[str, None, None]:
    """Convenience context manager for correlation ID tracking.

    Args:
        correlation_id: Optional correlation ID. If None, generates a new one.

    Yields:
        The active correlation ID

    Example:
        ```python
        with correlation_context() as correlation_id:
            logger.info(
                "Processing request",
                extra={"correlation_id": correlation_id},
            )
        ```
    """
    with CorrelationContext.context(correlation_id) as cid:
        yield cid


class _CorrelationAdapter(LoggerAdapter):  # pyright: ignore
    """Logger adapter that adds correlation ID to all logs."""

    def process(self, msg: str, kwargs: MutableMapping[str, Any]) -> "tuple[str, dict[str, Any]]":
        """Add correlation ID to the log record."""

        extra = kwargs.get("extra", {})

        if correlation_id := CorrelationContext.get():
            extra["correlation_id"] = correlation_id

        kwargs["extra"] = extra
        return msg, dict(kwargs)


def get_correlation_adapter(logger: Logger | LoggerAdapter) -> LoggerAdapter:  # pyright: ignore
    """Get a logger adapter that automatically includes correlation ID."""

    return _CorrelationAdapter(logger, {})
