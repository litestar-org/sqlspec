"""Runtime hint registry for EventChannel defaults."""

from dataclasses import dataclass
from typing import Any, Final, final

from sqlspec.utils.adapters import resolve_adapter_name

__all__ = ("EventRuntimeHints", "get_runtime_hints", "resolve_adapter_name")


@final
@dataclass(frozen=True)
class EventRuntimeHints:
    """Adapter-specific defaults for event polling and leases."""

    poll_interval: float = 1.0
    lease_seconds: int = 30
    retention_seconds: int = 86_400
    select_for_update: bool = False
    skip_locked: bool = False
    cleanup_on_ack: bool = True
    use_run_in_transaction: bool = False


_DEFAULT_HINTS: Final[EventRuntimeHints] = EventRuntimeHints()


def get_runtime_hints(adapter: "str | None", config: "Any" = None) -> "EventRuntimeHints":
    """Return runtime hints provided by the adapter configuration."""
    if config is None:
        return _DEFAULT_HINTS
    try:
        hints = config.get_event_runtime_hints()
    except AttributeError:
        return _DEFAULT_HINTS
    if isinstance(hints, EventRuntimeHints):
        return hints
    return _DEFAULT_HINTS
