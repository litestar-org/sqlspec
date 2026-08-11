"""Bounded event buffering helpers."""

from typing import Any

from sqlspec.exceptions import ImproperConfigurationError

__all__ = ("enqueue_with_capacity", "resolve_listener_queue_capacity", "validate_queue_capacity")


def validate_queue_capacity(
    value: object, *, name: str, error_type: "type[Exception]" = ImproperConfigurationError
) -> "int | None":
    """Validate an optional positive queue capacity."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        msg = f"{name} must be a positive integer or None"
        raise error_type(msg)
    return value


def resolve_listener_queue_capacity(config: Any) -> "int | None":
    """Resolve and validate the PostgreSQL listener queue capacity."""
    settings = getattr(config, "extension_config", {}).get("events", {})
    return validate_queue_capacity(settings.get("listener_queue_capacity"), name="listener_queue_capacity")


def enqueue_with_capacity(queue: Any, item: object, capacity: "int | None", *, empty_error: "type[Exception]") -> bool:
    """Enqueue an item, evicting the oldest item when capacity is reached."""
    dropped = False
    if capacity is not None and queue.qsize() >= capacity:
        try:
            queue.get_nowait()
            dropped = True
        except empty_error:
            pass
    queue.put_nowait(item)
    return dropped
