"""Correlation ID tracking for distributed tracing.

This module provides utilities for tracking correlation IDs across
database operations, enabling distributed tracing and debugging.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator, MutableMapping
    from logging import LoggerAdapter

__all__ = ("CorrelationContext", "correlation_context", "get_correlation_adapter")


class CorrelationContext:
    """Context manager for correlation ID tracking.

    This class provides a context-aware way to track correlation IDs
    across async and sync operations.
    """

    _correlation_id: ContextVar[str | None] = ContextVar("sqlspec_correlation_id", default=None)

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

        # Save the current correlation ID
        previous_id = cls.get()

        try:
            # Set the new correlation ID
            cls.set(correlation_id)
            yield correlation_id
        finally:
            # Restore the previous correlation ID
            cls.set(previous_id)

    @classmethod
    def clear(cls) -> None:
        """Clear the current correlation ID."""
        cls.set(None)

    @classmethod
    def to_dict(cls) -> dict[str, Any]:
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
            # All operations within this context will have the same correlation ID
        ```
    """
    with CorrelationContext.context(correlation_id) as cid:
        yield cid


def get_correlation_adapter(logger: Any) -> LoggerAdapter:
    """Get a logger adapter that automatically includes correlation ID.

    Args:
        logger: The base logger to wrap

    Returns:
        LoggerAdapter that includes correlation ID in all logs
    """
    from logging import LoggerAdapter

    class CorrelationAdapter(LoggerAdapter):
        """Logger adapter that adds correlation ID to all logs."""

        def process(self, msg: str, kwargs: MutableMapping[str, Any]) -> tuple[str, dict[str, Any]]:
            """Add correlation ID to the log record.

            Args:
                msg: The log message
                kwargs: Keyword arguments for the log record

            Returns:
                The message and updated kwargs
            """
            extra = kwargs.get("extra", {})

            # Add correlation ID if available
            if correlation_id := CorrelationContext.get():
                extra["correlation_id"] = correlation_id

            kwargs["extra"] = extra
            return msg, dict(kwargs)

    return CorrelationAdapter(logger, {})
