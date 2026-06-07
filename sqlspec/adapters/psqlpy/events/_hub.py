"""Persistent listener hub for psqlpy event backends.

Owns the psqlpy Listener handle for the backend's lifetime. Each channel
gets a single driver-level callback registered once via ``add_callback``
that dispatches into one queue per consumer task. Subscribe / unsubscribe
are serialized under an asyncio.Lock; ``clear_channel_callbacks`` only
runs on unsubscribe / shutdown so concurrent peers cannot wipe each
other's callbacks mid-iteration.
"""

# pyright: reportPrivateUsage=false

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, cast
from weakref import WeakKeyDictionary

from sqlspec.extensions.events import normalize_event_channel_name
from sqlspec.protocols import NotificationProtocol
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.psqlpy._typing import PsqlpyListener
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig

__all__ = ("PsqlpyListenerHub",)

logger = get_logger("sqlspec.adapters.psqlpy.events.hub")


_PSQLPY_CALLBACK_MIN_ARGS = 3


class _PsqlpyHubCallback:
    """Driver-facing callback that normalizes psqlpy notify args and dispatches."""

    __slots__ = ("_channel", "_dispatch")

    def __init__(self, channel: str, dispatch: "Callable[[str, str], None]") -> None:
        self._channel = channel
        self._dispatch = dispatch

    async def __call__(self, *args: Any) -> None:
        if not args:
            return
        notified_channel: str | None = None
        payload: str | None = None
        if len(args) == 1:
            message = args[0]
            if isinstance(message, NotificationProtocol):
                notified_channel = message.channel
                payload = message.payload
        elif len(args) >= _PSQLPY_CALLBACK_MIN_ARGS:
            value1 = cast("str", args[1])
            value2 = cast("str", args[2])
            if value1 == self._channel:
                notified_channel = value1
                payload = value2
            elif value2 == self._channel:
                notified_channel = value2
                payload = value1
        if notified_channel is None or notified_channel != self._channel or payload is None:
            return
        self._dispatch(self._channel, payload)


class PsqlpyListenerHub:
    """Per-channel persistent listener for psqlpy event backends."""

    __slots__ = (
        "_callbacks",
        "_config",
        "_listener",
        "_listener_started",
        "_lock",
        "_pool_destroying_registered",
        "_queues",
        "_shutting_down",
    )

    def __init__(self, config: "PsqlpyConfig") -> None:
        self._config = config
        self._lock = asyncio.Lock()
        self._queues: dict[str, WeakKeyDictionary[asyncio.Task[Any], asyncio.Queue[str]]] = {}
        self._callbacks: dict[str, _PsqlpyHubCallback] = {}
        self._listener: Any | None = None
        self._listener_started = False
        self._shutting_down = False
        self._pool_destroying_registered = False

    async def subscribe(self, channel: str) -> None:
        async with self._lock:
            if self._shutting_down:
                msg = "PsqlpyListenerHub is shutting down"
                raise RuntimeError(msg)
            if channel in self._queues:
                return
            await self._subscribe_locked(channel)

    async def unsubscribe(self, channel: str) -> None:
        async with self._lock:
            if channel not in self._queues:
                return
            listener = self._listener
            self._queues.pop(channel, None)
            self._callbacks.pop(channel, None)
            if listener is None:
                return
            with contextlib.suppress(Exception):
                await listener.clear_channel_callbacks(channel=channel)

    async def dequeue(self, channel: str, poll_interval: float) -> "str | None":
        task = asyncio.current_task()
        if task is None:  # pragma: no cover
            msg = "PsqlpyListenerHub.dequeue requires an active asyncio task"
            raise RuntimeError(msg)
        async with self._lock:
            if channel not in self._queues:
                queue = await self._subscribe_locked(channel, consumer_task=task)
            else:
                queue = self._get_consumer_queue(channel, task=task)
        if queue is None:
            return None
        try:
            return await asyncio.wait_for(queue.get(), timeout=poll_interval)
        except asyncio.TimeoutError:
            return None

    async def shutdown(self) -> None:
        async with self._lock:
            if self._shutting_down:
                return
            self._shutting_down = True
            listener = self._listener
            channels = list(self._queues.keys())
            was_started = self._listener_started
            self._queues.clear()
            self._callbacks.clear()
            self._listener = None
            self._listener_started = False
        if listener is not None:
            for channel in channels:
                with contextlib.suppress(Exception):
                    await listener.clear_channel_callbacks(channel=channel)
            if was_started:
                with contextlib.suppress(Exception):
                    listener.abort_listen()
            with contextlib.suppress(Exception):
                await listener.shutdown()
        self._shutting_down = False

    def is_subscribed(self, channel: str) -> bool:
        return channel in self._queues

    def _get_consumer_queue(
        self, channel: str, *, task: "asyncio.Task[Any] | None" = None
    ) -> "asyncio.Queue[str] | None":
        task = task or asyncio.current_task()
        if task is None:  # pragma: no cover
            msg = "PsqlpyListenerHub.dequeue requires an active asyncio task"
            raise RuntimeError(msg)
        queues = self._queues.get(channel)
        if queues is None:
            return None
        queue = queues.get(task)
        if queue is None:
            queue = asyncio.Queue()
            queues[task] = queue
        return queue

    async def _subscribe_locked(
        self, channel: str, *, consumer_task: "asyncio.Task[Any] | None" = None
    ) -> "asyncio.Queue[str] | None":
        if self._shutting_down:
            msg = "PsqlpyListenerHub is shutting down"
            raise RuntimeError(msg)
        normalize_event_channel_name(channel)
        listener = await self._ensure_listener_locked()
        callback = _PsqlpyHubCallback(channel, self._dispatch)
        queues: WeakKeyDictionary[asyncio.Task[Any], asyncio.Queue[str]] = WeakKeyDictionary()
        consumer_queue: asyncio.Queue[str] | None = None
        if consumer_task is not None:
            consumer_queue = asyncio.Queue()
            queues[consumer_task] = consumer_queue
        self._queues[channel] = queues
        self._callbacks[channel] = callback
        try:
            await listener.add_callback(channel=channel, callback=callback.__call__)
        except Exception:
            self._queues.pop(channel, None)
            self._callbacks.pop(channel, None)
            raise
        if not self._listener_started:
            listener.listen()
            self._listener_started = True
            # Give psqlpy a brief moment to actually start its consumer
            await asyncio.sleep(0.05)
        return consumer_queue

    async def _ensure_listener_locked(self) -> "PsqlpyListener":
        if self._listener is not None:
            return cast("PsqlpyListener", self._listener)
        pool = await self._config.provide_pool()
        listener = pool.listener()
        await listener.startup()
        self._listener = listener
        self._register_pool_destroying()
        return listener

    def _register_pool_destroying(self) -> None:
        if self._pool_destroying_registered:
            return
        runtime = self._config.get_observability_runtime()
        runtime.register_lifecycle_hook("on_pool_destroying", self._pool_destroying_hook)
        self._pool_destroying_registered = True

    def _pool_destroying_hook(self, _context: "dict[str, Any]") -> "Any":
        return self.shutdown()

    def _dispatch(self, channel: str, payload: str) -> None:
        queues = self._queues.get(channel)
        if queues is None:
            return
        for queue in list(queues.values()):
            queue.put_nowait(payload)
