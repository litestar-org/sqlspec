"""Lifecycle dispatcher for drivers and registry hooks."""

import inspect
from collections.abc import Callable, Iterable
from typing import Any, Literal

from sqlspec.utils.logging import get_logger

__all__ = ("LifecycleContext", "LifecycleDispatcher", "LifecycleHook")


logger = get_logger("sqlspec.observability.lifecycle")

LifecycleContext = dict[str, Any]
LifecycleHook = Callable[[LifecycleContext], Any]

LifecycleEvent = Literal[
    "on_pool_create",
    "on_pool_destroying",
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
    "on_pool_destroying",
    "on_pool_destroy",
    "on_connection_create",
    "on_connection_destroy",
    "on_session_start",
    "on_session_end",
    "on_query_start",
    "on_query_complete",
    "on_error",
)


class LifecycleDispatcher:
    """Dispatches lifecycle hooks with guard flags and diagnostics counters."""

    __slots__ = (
        "_counters",
        "_hooks",
        "_is_enabled",
        "has_connection_create",
        "has_connection_destroy",
        "has_error",
        "has_pool_create",
        "has_pool_destroy",
        "has_pool_destroying",
        "has_query_complete",
        "has_query_start",
        "has_session_end",
        "has_session_start",
    )

    def __init__(self, hooks: "dict[str, Iterable[LifecycleHook]] | None" = None) -> None:
        self.has_pool_create = False
        self.has_pool_destroying = False
        self.has_pool_destroy = False
        self.has_connection_create = False
        self.has_connection_destroy = False
        self.has_session_start = False
        self.has_session_end = False
        self.has_query_start = False
        self.has_query_complete = False
        self.has_error = False

        normalized: dict[LifecycleEvent, list[LifecycleHook]] = {}
        for event_name in EVENT_ATTRS:
            callables = hooks.get(event_name) if hooks else None
            normalized[event_name] = list(callables) if callables else []
            if normalized[event_name]:
                self._enable_event_guard(event_name)
        self._hooks: dict[LifecycleEvent, list[LifecycleHook]] = normalized
        self._counters: dict[LifecycleEvent, int] = dict.fromkeys(EVENT_ATTRS, 0)
        self._is_enabled = any(self._hooks.values())

    @property
    def is_enabled(self) -> bool:
        """Return True when at least one hook is registered."""

        return self._is_enabled

    def emit_pool_create_sync(self, context: LifecycleContext) -> None:
        """Fire pool creation hooks synchronously."""

        self._emit_sync("on_pool_create", context)

    async def emit_pool_create_async(self, context: LifecycleContext) -> None:
        """Fire pool creation hooks, awaiting any awaitable return values."""

        await self._emit_async("on_pool_create", context)

    def emit_pool_create(self, context: LifecycleContext) -> None:
        """Fire pool creation hooks synchronously."""

        self.emit_pool_create_sync(context)

    def emit_pool_destroying_sync(self, context: LifecycleContext) -> None:
        """Fire pre-destruction hooks synchronously."""

        self._emit_sync("on_pool_destroying", context)

    async def emit_pool_destroying_async(self, context: LifecycleContext) -> None:
        """Fire pre-destruction hooks, awaiting any awaitable return values."""

        await self._emit_async("on_pool_destroying", context)

    def emit_pool_destroying(self, context: LifecycleContext) -> None:
        """Fire pre-destruction hooks synchronously."""

        self.emit_pool_destroying_sync(context)

    def emit_pool_destroy_sync(self, context: LifecycleContext) -> None:
        """Fire pool destruction hooks synchronously."""

        self._emit_sync("on_pool_destroy", context)

    async def emit_pool_destroy_async(self, context: LifecycleContext) -> None:
        """Fire pool destruction hooks, awaiting any awaitable return values."""

        await self._emit_async("on_pool_destroy", context)

    def emit_pool_destroy(self, context: LifecycleContext) -> None:
        """Fire pool destruction hooks synchronously."""

        self.emit_pool_destroy_sync(context)

    def emit_connection_create_sync(self, context: LifecycleContext) -> None:
        """Fire connection creation hooks synchronously."""

        self._emit_sync("on_connection_create", context)

    async def emit_connection_create_async(self, context: LifecycleContext) -> None:
        """Fire connection creation hooks, awaiting any awaitable return values."""

        await self._emit_async("on_connection_create", context)

    def emit_connection_create(self, context: LifecycleContext) -> None:
        """Fire connection creation hooks synchronously."""

        self.emit_connection_create_sync(context)

    def emit_connection_destroy_sync(self, context: LifecycleContext) -> None:
        """Fire connection teardown hooks synchronously."""

        self._emit_sync("on_connection_destroy", context)

    async def emit_connection_destroy_async(self, context: LifecycleContext) -> None:
        """Fire connection teardown hooks, awaiting any awaitable return values."""

        await self._emit_async("on_connection_destroy", context)

    def emit_connection_destroy(self, context: LifecycleContext) -> None:
        """Fire connection teardown hooks synchronously."""

        self.emit_connection_destroy_sync(context)

    def emit_session_start_sync(self, context: LifecycleContext) -> None:
        """Fire session start hooks synchronously."""

        self._emit_sync("on_session_start", context)

    async def emit_session_start_async(self, context: LifecycleContext) -> None:
        """Fire session start hooks, awaiting any awaitable return values."""

        await self._emit_async("on_session_start", context)

    def emit_session_start(self, context: LifecycleContext) -> None:
        """Fire session start hooks synchronously."""

        self.emit_session_start_sync(context)

    def emit_session_end_sync(self, context: LifecycleContext) -> None:
        """Fire session end hooks synchronously."""

        self._emit_sync("on_session_end", context)

    async def emit_session_end_async(self, context: LifecycleContext) -> None:
        """Fire session end hooks, awaiting any awaitable return values."""

        await self._emit_async("on_session_end", context)

    def emit_session_end(self, context: LifecycleContext) -> None:
        """Fire session end hooks synchronously."""

        self.emit_session_end_sync(context)

    def emit_query_start_sync(self, context: LifecycleContext) -> None:
        """Fire query start hooks synchronously."""

        self._emit_sync("on_query_start", context)

    async def emit_query_start_async(self, context: LifecycleContext) -> None:
        """Fire query start hooks, awaiting any awaitable return values."""

        await self._emit_async("on_query_start", context)

    def emit_query_start(self, context: LifecycleContext) -> None:
        """Fire query start hooks synchronously."""

        self.emit_query_start_sync(context)

    def emit_query_complete_sync(self, context: LifecycleContext) -> None:
        """Fire query completion hooks synchronously."""

        self._emit_sync("on_query_complete", context)

    async def emit_query_complete_async(self, context: LifecycleContext) -> None:
        """Fire query completion hooks, awaiting any awaitable return values."""

        await self._emit_async("on_query_complete", context)

    def emit_query_complete(self, context: LifecycleContext) -> None:
        """Fire query completion hooks synchronously."""

        self.emit_query_complete_sync(context)

    def emit_error_sync(self, context: LifecycleContext) -> None:
        """Fire error hooks synchronously."""

        self._emit_sync("on_error", context)

    async def emit_error_async(self, context: LifecycleContext) -> None:
        """Fire error hooks, awaiting any awaitable return values."""

        await self._emit_async("on_error", context)

    def emit_error(self, context: LifecycleContext) -> None:
        """Fire error hooks synchronously."""

        self.emit_error_sync(context)

    def register_hook(self, event: LifecycleEvent, callback: LifecycleHook) -> None:
        """Append a hook at runtime."""

        callbacks = self._hooks.setdefault(event, [])
        callbacks.append(callback)
        self._enable_event_guard(event)
        self._is_enabled = True

    def snapshot(self, *, prefix: str | None = None) -> "dict[str, int]":
        """Return counter snapshot keyed for diagnostics export."""

        metrics: dict[str, int] = {}
        for event_name, count in self._counters.items():
            key = event_name.replace("on_", "lifecycle.")
            if prefix:
                key = f"{prefix}.{key}"
            metrics[key] = count
        return metrics

    def _enable_event_guard(self, event: LifecycleEvent) -> None:
        match event:
            case "on_pool_create":
                self.has_pool_create = True
            case "on_pool_destroying":
                self.has_pool_destroying = True
            case "on_pool_destroy":
                self.has_pool_destroy = True
            case "on_connection_create":
                self.has_connection_create = True
            case "on_connection_destroy":
                self.has_connection_destroy = True
            case "on_session_start":
                self.has_session_start = True
            case "on_session_end":
                self.has_session_end = True
            case "on_query_start":
                self.has_query_start = True
            case "on_query_complete":
                self.has_query_complete = True
            case "on_error":
                self.has_error = True

    def _emit_sync(self, event: LifecycleEvent, context: LifecycleContext) -> None:
        callbacks = self._hooks.get(event)
        if not callbacks:
            return
        self._counters[event] += 1
        for callback in callbacks:
            self._invoke_callback(callback, context, event)

    async def _emit_async(self, event: LifecycleEvent, context: LifecycleContext) -> None:
        callbacks = self._hooks.get(event)
        if not callbacks:
            return
        self._counters[event] += 1
        for callback in callbacks:
            try:
                result = callback(context)
            except Exception as exc:  # pragma: no cover
                logger.warning("Lifecycle hook failed: event=%s error=%s", event, exc)
                continue
            if inspect.isawaitable(result):
                try:
                    await result
                except Exception as exc:  # pragma: no cover
                    logger.warning("Lifecycle hook failed: event=%s error=%s", event, exc)

    @staticmethod
    def _invoke_callback(callback: LifecycleHook, context: LifecycleContext, event: LifecycleEvent) -> None:
        try:
            callback(context)
        except Exception as exc:  # pragma: no cover
            logger.warning("Lifecycle hook failed: event=%s error=%s", event, exc)
