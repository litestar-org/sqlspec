"""Persistent LISTEN/NOTIFY hub for asyncpg event backends.

Owns a single dedicated asyncpg connection per backend instance. Subscribe /
unsubscribe are serialized under an asyncio.Lock so concurrent callers cannot
race on the shared connection (the asyncpg.InterfaceError "another operation
is in progress" hazard). asyncpg's per-channel add_listener callback fans
notifications into one queue per consumer task.
"""

# pyright: reportPrivateUsage=false

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any
from weakref import WeakKeyDictionary

from sqlspec.exceptions import EventChannelError
from sqlspec.extensions.events import normalize_event_channel_name
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_add_listener

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.asyncpg.config import AsyncpgConfig

__all__ = ("AsyncpgListenerHub",)

logger = get_logger("sqlspec.adapters.asyncpg.events.hub")


class AsyncpgListenerHub:
    """Per-channel persistent listener for asyncpg-based event backends."""

    __slots__ = (
        "_callbacks",
        "_config",
        "_connection",
        "_connection_cm",
        "_lock",
        "_pool_destroying_registered",
        "_queues",
        "_shutting_down",
    )

    def __init__(self, config: "AsyncpgConfig") -> None:
        self._config = config
        self._lock = asyncio.Lock()
        self._queues: dict[str, WeakKeyDictionary[asyncio.Task[Any], asyncio.Queue[str]]] = {}
        self._callbacks: dict[str, Callable[..., None]] = {}
        self._connection_cm: Any | None = None
        self._connection: Any | None = None
        self._shutting_down = False
        self._pool_destroying_registered = False

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

            self._queues[channel] = WeakKeyDictionary()
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

    async def dequeue(self, channel: str, poll_interval: float) -> "str | None":
        if channel not in self._queues:
            await self.subscribe(channel)
        queue = self._get_consumer_queue(channel)
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

    def _get_consumer_queue(self, channel: str) -> "asyncio.Queue[str] | None":
        task = asyncio.current_task()
        if task is None:  # pragma: no cover
            msg = "AsyncpgListenerHub.dequeue requires an active asyncio task"
            raise RuntimeError(msg)
        queues = self._queues.get(channel)
        if queues is None:
            return None
        queue = queues.get(task)
        if queue is None:
            queue = asyncio.Queue()
            queues[task] = queue
        return queue

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
        self._register_pool_destroying()

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
