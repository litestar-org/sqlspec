"""Lifecycle dispatcher used by drivers and registry hooks."""

from collections.abc import Callable, Iterable
from typing import Any, Literal

from sqlspec.utils.logging import get_logger

logger = get_logger("sqlspec.observability.lifecycle")

LifecycleContext = dict[str, Any]
LifecycleHook = Callable[[LifecycleContext], None]

LifecycleEvent = Literal[
    "on_pool_create",
    "on_pool_destroy",
    "on_connection_create",
    "on_connection_destroy",
    "on_session_start",
    "on_session_end",
    "on_query_start",
    "on_query_complete",
    "on_error",
]
EVENT_ATTRS: tuple[LifecycleEvent, ...] = (
    "on_pool_create",
    "on_pool_destroy",
    "on_connection_create",
    "on_connection_destroy",
    "on_session_start",
    "on_session_end",
    "on_query_start",
    "on_query_complete",
    "on_error",
)
GUARD_ATTRS = tuple(f"has_{name[3:]}" for name in EVENT_ATTRS)


class LifecycleDispatcher:
    """Dispatches lifecycle hooks with guard flags and diagnostics counters."""

    __slots__ = (
        "_counters",
        "_hooks",
        "has_connection_create",
        "has_connection_destroy",
        "has_error",
        "has_pool_create",
        "has_pool_destroy",
        "has_query_complete",
        "has_query_start",
        "has_session_end",
        "has_session_start",
    )

    def __init__(self, hooks: "dict[str, Iterable[LifecycleHook]] | None" = None) -> None:
        self.has_pool_create = False
        self.has_pool_destroy = False
        self.has_connection_create = False
        self.has_connection_destroy = False
        self.has_session_start = False
        self.has_session_end = False
        self.has_query_start = False
        self.has_query_complete = False
        self.has_error = False

        normalized: dict[LifecycleEvent, tuple[LifecycleHook, ...]] = {}
        for event_name, guard_attr in zip(EVENT_ATTRS, GUARD_ATTRS, strict=False):
            callables = hooks.get(event_name) if hooks else None
            normalized[event_name] = tuple(callables) if callables else ()
            setattr(self, guard_attr, bool(normalized[event_name]))
        self._hooks: dict[LifecycleEvent, tuple[LifecycleHook, ...]] = normalized
        self._counters: dict[LifecycleEvent, int] = dict.fromkeys(EVENT_ATTRS, 0)

    @property
    def is_enabled(self) -> bool:
        """Return True when at least one hook is registered."""

        return any(self._hooks[name] for name in EVENT_ATTRS)

    def emit_pool_create(self, context: "LifecycleContext") -> None:
        """Fire pool creation hooks."""

        self._emit("on_pool_create", context)

    def emit_pool_destroy(self, context: "LifecycleContext") -> None:
        """Fire pool destruction hooks."""

        self._emit("on_pool_destroy", context)

    def emit_connection_create(self, context: "LifecycleContext") -> None:
        """Fire connection creation hooks."""

        self._emit("on_connection_create", context)

    def emit_connection_destroy(self, context: "LifecycleContext") -> None:
        """Fire connection teardown hooks."""

        self._emit("on_connection_destroy", context)

    def emit_session_start(self, context: "LifecycleContext") -> None:
        """Fire session start hooks."""

        self._emit("on_session_start", context)

    def emit_session_end(self, context: "LifecycleContext") -> None:
        """Fire session end hooks."""

        self._emit("on_session_end", context)

    def emit_query_start(self, context: "LifecycleContext") -> None:
        """Fire query start hooks."""

        self._emit("on_query_start", context)

    def emit_query_complete(self, context: "LifecycleContext") -> None:
        """Fire query completion hooks."""

        self._emit("on_query_complete", context)

    def emit_error(self, context: "LifecycleContext") -> None:
        """Fire error hooks with failure context."""

        self._emit("on_error", context)

    def snapshot(self, *, prefix: str | None = None) -> "dict[str, int]":
        """Return counter snapshot keyed for diagnostics export."""

        metrics: dict[str, int] = {}
        for event_name, count in self._counters.items():
            key = event_name.replace("on_", "lifecycle.")
            if prefix:
                key = f"{prefix}.{key}"
            metrics[key] = count
        return metrics

    def _emit(self, event: LifecycleEvent, context: "LifecycleContext") -> None:
        callbacks = self._hooks.get(event)
        if not callbacks:
            return
        self._counters[event] += 1
        for callback in callbacks:
            self._invoke_callback(callback, context, event)

    @staticmethod
    def _invoke_callback(callback: LifecycleHook, context: "LifecycleContext", event: LifecycleEvent) -> None:
        try:
            callback(context)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("Lifecycle hook failed: event=%s error=%s", event, exc)


__all__ = ("LifecycleContext", "LifecycleDispatcher", "LifecycleHook")
