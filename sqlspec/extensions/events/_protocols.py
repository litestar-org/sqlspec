"""Protocols for EventChannel handlers and backends."""

from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable

if TYPE_CHECKING:
    from sqlspec.extensions.events._models import EventMessage

__all__ = ("AsyncEventBackendProtocol", "AsyncEventHandler", "SyncEventBackendProtocol", "SyncEventHandler")


class AsyncEventHandler(Protocol):
    """Protocol describing async event handler callables."""

    async def __call__(self, message: "EventMessage") -> Any:  # pragma: no cover - typing only
        """Process a queued event message asynchronously."""


class SyncEventHandler(Protocol):
    """Protocol describing sync event handler callables."""

    def __call__(self, message: "EventMessage") -> Any:  # pragma: no cover - typing only
        """Process a queued event message synchronously."""


@runtime_checkable
class AsyncEventBackendProtocol(Protocol):
    """Protocol for async event backends.

    All async event backends (native or queue-based) must implement these methods.
    """

    supports_async: ClassVar[bool]
    backend_name: ClassVar[str]

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        """Publish an event to a channel.

        Args:
            channel: Target channel name.
            payload: Event payload (must be JSON-serializable).
            metadata: Optional metadata dict.

        Returns:
            The event ID.
        """
        ...

    async def dequeue(self, channel: str, poll_interval: float) -> "EventMessage | None":
        """Dequeue an event from the channel.

        Args:
            channel: Channel name to listen on.
            poll_interval: Timeout in seconds to wait for a notification.

        Returns:
            EventMessage if a notification was received, None otherwise.
        """
        ...

    async def ack(self, event_id: str) -> None:
        """Acknowledge an event.

        Args:
            event_id: ID of the event to acknowledge.
        """
        ...

    async def nack(self, event_id: str) -> None:
        """Return an event to the queue for redelivery.

        Args:
            event_id: ID of the event to return.
        """
        ...

    async def shutdown(self) -> None:
        """Shutdown the backend and release resources."""
        ...


@runtime_checkable
class SyncEventBackendProtocol(Protocol):
    """Protocol for sync event backends.

    All sync event backends (native or queue-based) must implement these methods.
    """

    supports_sync: ClassVar[bool]
    backend_name: ClassVar[str]

    def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        """Publish an event to a channel.

        Args:
            channel: Target channel name.
            payload: Event payload (must be JSON-serializable).
            metadata: Optional metadata dict.

        Returns:
            The event ID.
        """
        ...

    def dequeue(self, channel: str, poll_interval: float) -> "EventMessage | None":
        """Dequeue an event from the channel.

        Args:
            channel: Channel name to listen on.
            poll_interval: Timeout in seconds to wait for a notification.

        Returns:
            EventMessage if a notification was received, None otherwise.
        """
        ...

    def ack(self, event_id: str) -> None:
        """Acknowledge an event.

        Args:
            event_id: ID of the event to acknowledge.
        """
        ...

    def nack(self, event_id: str) -> None:
        """Return an event to the queue for redelivery.

        Args:
            event_id: ID of the event to return.
        """
        ...

    def shutdown(self) -> None:
        """Shutdown the backend and release resources."""
        ...
