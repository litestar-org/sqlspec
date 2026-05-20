"""Persistent LISTEN/NOTIFY hub used by asyncpg event backends.

Owns a single dedicated asyncpg connection per backend instance. Subscribe /
unsubscribe are serialized under an asyncio.Lock so concurrent callers cannot
race on the shared connection (the asyncpg.InterfaceError "another operation
is in progress" hazard). asyncpg's per-channel add_listener callback fans
notifications into a per-channel asyncio.Queue.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import EventChannelError
from sqlspec.extensions.events import normalize_event_channel_name
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_add_listener

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.asyncpg.config import AsyncpgConfig

logger = get_logger("sqlspec.adapters.asyncpg.events.hub")

__all__ = ("AsyncpgListenerHub",)


class AsyncpgListenerHub:
    """Per-channel persistent listener for asyncpg-based event backends."""

    __slots__ = ("_callbacks", "_config", "_connection", "_connection_cm", "_lock", "_queues", "_shutting_down")

    def __init__(self, config: AsyncpgConfig) -> None:
        self._config = config
        self._lock = asyncio.Lock()
        self._queues: dict[str, asyncio.Queue[str]] = {}
        self._callbacks: dict[str, Callable[..., None]] = {}
        self._connection_cm: Any | None = None
        self._connection: Any | None = None
        self._shutting_down = False

    async def subscribe(self, channel: str) -> None:
        async with self._lock:
            if self._shutting_down:
                msg = "AsyncpgListenerHub is shutting down"
                raise RuntimeError(msg)
            if channel in self._queues:
                return
            await self._ensure_connection_locked()
            validated = normalize_event_channel_name(channel)
            connection = self._connection
            assert connection is not None

            def _callback(_conn: Any, _pid: int, notified_channel: str, payload: str) -> None:
                self._dispatch(notified_channel, payload)

            self._queues[channel] = asyncio.Queue()
            self._callbacks[channel] = _callback
            try:
                await connection.execute(f"LISTEN {validated}")
                await connection.add_listener(channel, _callback)
            except Exception:
                self._queues.pop(channel, None)
                self._callbacks.pop(channel, None)
                raise

    async def unsubscribe(self, channel: str) -> None:
        async with self._lock:
            if channel not in self._queues:
                return
            connection = self._connection
            callback = self._callbacks.pop(channel, None)
            self._queues.pop(channel, None)
            if connection is None:
                return
            if callback is not None:
                with contextlib.suppress(Exception):
                    await connection.remove_listener(channel, callback)
            validated = normalize_event_channel_name(channel)
            with contextlib.suppress(Exception):
                await connection.execute(f"UNLISTEN {validated}")

    async def dequeue(self, channel: str, poll_interval: float) -> str | None:
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
            connection = self._connection
            connection_cm = self._connection_cm
            channels = list(self._queues.keys())
            callbacks = dict(self._callbacks)
            self._queues.clear()
            self._callbacks.clear()
            self._connection = None
            self._connection_cm = None
        if connection is not None:
            for channel in channels:
                callback = callbacks.get(channel)
                if callback is not None:
                    with contextlib.suppress(Exception):
                        await connection.remove_listener(channel, callback)
                validated = normalize_event_channel_name(channel)
                with contextlib.suppress(Exception):
                    await connection.execute(f"UNLISTEN {validated}")
        if connection_cm is not None:
            with contextlib.suppress(Exception):
                await connection_cm.__aexit__(None, None, None)
        self._shutting_down = False

    def is_subscribed(self, channel: str) -> bool:
        return channel in self._queues

    async def _ensure_connection_locked(self) -> None:
        if self._connection is not None:
            return
        connection_cm = self._config.provide_connection()
        connection = await connection_cm.__aenter__()
        if connection is None or not has_add_listener(connection):
            with contextlib.suppress(Exception):
                await connection_cm.__aexit__(None, None, None)
            msg = "PostgreSQL connection does not support LISTEN/NOTIFY callbacks"
            raise EventChannelError(msg)
        self._connection_cm = connection_cm
        self._connection = connection

    def _dispatch(self, channel: str, payload: str) -> None:
        queue = self._queues.get(channel)
        if queue is None:
            return
        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:  # pragma: no cover - unbounded queues do not raise
            logger.warning("asyncpg listener queue full for channel %s; dropping notification", channel)
