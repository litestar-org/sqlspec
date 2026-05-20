"""Persistent listener hub used by psqlpy event backends.

Owns the psqlpy Listener handle for the backend's lifetime. Each channel
gets a single driver-level callback registered once via ``add_callback``
that dispatches into a per-channel asyncio.Queue. Subscribe / unsubscribe
are serialized under an asyncio.Lock; ``clear_channel_callbacks`` only
runs on unsubscribe / shutdown so concurrent peers cannot wipe each
other's callbacks mid-iteration.
"""

# pyright: reportPrivateUsage=false

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, cast

from sqlspec.extensions.events import normalize_event_channel_name
from sqlspec.protocols import NotificationProtocol
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.psqlpy._typing import PsqlpyListener
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig

logger = get_logger("sqlspec.adapters.psqlpy.events.hub")

__all__ = ("PsqlpyListenerHub",)

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

    __slots__ = ("_callbacks", "_config", "_listener", "_listener_started", "_lock", "_queues", "_shutting_down")

    def __init__(self, config: "PsqlpyConfig") -> None:
        self._config = config
        self._lock = asyncio.Lock()
        self._queues: dict[str, asyncio.Queue[str]] = {}
        self._callbacks: dict[str, _PsqlpyHubCallback] = {}
        self._listener: Any | None = None
        self._listener_started = False
        self._shutting_down = False

    async def subscribe(self, channel: str) -> None:
        async with self._lock:
            if self._shutting_down:
                msg = "PsqlpyListenerHub is shutting down"
                raise RuntimeError(msg)
            if channel in self._queues:
                return
            normalize_event_channel_name(channel)
            listener = await self._ensure_listener_locked()
            callback = _PsqlpyHubCallback(channel, self._dispatch)
            self._queues[channel] = asyncio.Queue()
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
        if channel not in self._queues:
            await self.subscribe(channel)
        queue = self._queues.get(channel)
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

    async def _ensure_listener_locked(self) -> "PsqlpyListener":
        if self._listener is not None:
            return self._listener
        pool = await self._config.provide_pool()
        listener = pool.listener()
        await listener.startup()
        self._listener = listener
        return listener

    def _dispatch(self, channel: str, payload: str) -> None:
        queue = self._queues.get(channel)
        if queue is None:
            return
        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:  # pragma: no cover - unbounded queues do not raise
            logger.warning("psqlpy listener queue full for channel %s; dropping notification", channel)
