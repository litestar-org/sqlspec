"""Protocols for EventChannel handlers."""

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from sqlspec.extensions.events.channel import EventMessage

__all__ = ("AsyncEventHandler", "SyncEventHandler")


class AsyncEventHandler(Protocol):
    """Protocol describing async event handler callables."""

    async def __call__(self, message: "EventMessage") -> Any:  # pragma: no cover - typing only
        """Process a queued event message asynchronously."""


class SyncEventHandler(Protocol):
    """Protocol describing sync event handler callables."""

    def __call__(self, message: "EventMessage") -> Any:  # pragma: no cover - typing only
        """Process a queued event message synchronously."""
