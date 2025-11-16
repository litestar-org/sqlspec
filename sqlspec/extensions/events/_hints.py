"""Runtime hint registry for EventChannel defaults."""

from dataclasses import dataclass
from typing import Any, Final

__all__ = ("EventRuntimeHints", "get_runtime_hints")


@dataclass(frozen=True)
class EventRuntimeHints:
    """Adapter-specific defaults for event polling and leases."""

    poll_interval: float = 1.0
    lease_seconds: int = 30
    retention_seconds: int = 86_400
    select_for_update: bool = False
    skip_locked: bool = False
    json_passthrough: bool = False


_DEFAULT_HINTS: Final[EventRuntimeHints] = EventRuntimeHints()


def get_runtime_hints(adapter: "str | None", config: "Any" = None) -> "EventRuntimeHints":
    """Return runtime hints provided by the adapter configuration."""

    if config is None:
        return _DEFAULT_HINTS
    provider = getattr(config, "get_event_runtime_hints", None)
    if provider is None:
        return _DEFAULT_HINTS
    hints = provider()
    if isinstance(hints, EventRuntimeHints):
        return hints
    return _DEFAULT_HINTS
