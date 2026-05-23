"""Persistent LISTEN/NOTIFY hubs for psycopg async and sync event backends.

Each hub owns a single dedicated autocommit psycopg connection and serializes
LISTEN / UNLISTEN statements against the notification pump. The async hub
runs the pump as an asyncio task and uses asyncio.Lock; the sync hub uses a
single worker thread that serializes all connection access via a command
queue (psycopg sync connections are not thread-safe).
"""

# pyright: reportPrivateUsage=false

import asyncio
import contextlib
import queue as stdlib_queue
import threading
from typing import TYPE_CHECKING, Any
from weakref import WeakKeyDictionary

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import normalize_event_channel_name
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig

logger = get_logger("sqlspec.adapters.psycopg.events.hub")

__all__ = ("PsycopgAsyncListenerHub", "PsycopgSyncListenerHub")

_PUMP_TIMEOUT = 0.05
_PUMP_BATCH = 32


class PsycopgAsyncListenerHub:
    """Async psycopg persistent listener hub."""

    __slots__ = (
        "_config",
        "_connection",
        "_connection_cm",
        "_lock",
        "_pool_destroying_registered",
        "_pump_task",
        "_queues",
        "_shutting_down",
        "_stopping",
    )

    def __init__(self, config: "PsycopgAsyncConfig") -> None:
        self._config = config
        self._lock = asyncio.Lock()
        self._queues: dict[str, WeakKeyDictionary[asyncio.Task[Any], asyncio.Queue[str]]] = {}
        self._connection_cm: Any | None = None
        self._connection: Any | None = None
        self._pump_task: asyncio.Task[None] | None = None
        self._shutting_down = False
        self._stopping = False
        self._pool_destroying_registered = False

    async def subscribe(self, channel: str) -> None:
        async with self._lock:
            if self._shutting_down:
                msg = "PsycopgAsyncListenerHub is shutting down"
                raise RuntimeError(msg)
            if channel in self._queues:
                return
            await self._ensure_connection_locked()
            validated = normalize_event_channel_name(channel)
            connection = self._connection
            assert connection is not None
            self._queues[channel] = WeakKeyDictionary()
            try:
                await connection.execute(f"LISTEN {validated}")
            except Exception:
                self._queues.pop(channel, None)
                raise

    async def unsubscribe(self, channel: str) -> None:
        async with self._lock:
            if channel not in self._queues:
                return
            self._queues.pop(channel, None)
            connection = self._connection
            if connection is None:
                return
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
            self._stopping = True
            connection = self._connection
            connection_cm = self._connection_cm
            pump_task = self._pump_task
            channels = list(self._queues.keys())
            self._queues.clear()
            self._connection = None
            self._connection_cm = None
            self._pump_task = None
        if pump_task is not None:
            pump_task.cancel()
            with contextlib.suppress(BaseException):
                await pump_task
        if connection is not None:
            for channel in channels:
                validated = normalize_event_channel_name(channel)
                with contextlib.suppress(Exception):
                    await connection.execute(f"UNLISTEN {validated}")
        if connection_cm is not None:
            with contextlib.suppress(Exception):
                await connection_cm.__aexit__(None, None, None)
        self._shutting_down = False
        self._stopping = False

    def is_subscribed(self, channel: str) -> bool:
        return channel in self._queues

    def _get_consumer_queue(self, channel: str) -> "asyncio.Queue[str] | None":
        task = asyncio.current_task()
        if task is None:  # pragma: no cover - coroutine dequeue calls always run in a task
            msg = "PsycopgAsyncListenerHub.dequeue requires an active asyncio task"
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
        if connection is None:
            with contextlib.suppress(Exception):
                await connection_cm.__aexit__(None, None, None)
            msg = "Psycopg async adapter did not yield a connection"
            raise ImproperConfigurationError(msg)
        with contextlib.suppress(Exception):
            await connection.set_autocommit(True)
        self._connection_cm = connection_cm
        self._connection = connection
        self._pump_task = asyncio.create_task(self._pump())
        self._register_pool_destroying()

    def _register_pool_destroying(self) -> None:
        if self._pool_destroying_registered:
            return
        runtime = self._config.get_observability_runtime()
        runtime.register_lifecycle_hook("on_pool_destroying", self._pool_destroying_hook)
        self._pool_destroying_registered = True

    def _pool_destroying_hook(self, _context: "dict[str, Any]") -> "Any":
        return self.shutdown()

    async def _pump(self) -> None:
        try:
            while not self._stopping:
                connection = self._connection
                if connection is None:
                    return
                try:
                    async for notify in connection.notifies(timeout=_PUMP_TIMEOUT, stop_after=_PUMP_BATCH):
                        if self._stopping:
                            return
                        self._dispatch(notify.channel, notify.payload)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # pragma: no cover - pump resilience path
                    if self._stopping or getattr(connection, "closed", False):
                        return
                    logger.warning("psycopg async notify pump error: %s", exc)
                    await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            return

    def _dispatch(self, channel: str, payload: str) -> None:
        queues = self._queues.get(channel)
        if queues is None:
            return
        for queue in list(queues.values()):
            queue.put_nowait(payload)


class PsycopgSyncListenerHub:
    """Sync psycopg persistent listener hub backed by a single worker thread.

    psycopg sync connections are not thread-safe. All connection access
    (LISTEN, UNLISTEN, notifies polling) happens on the worker thread.
    Public ``subscribe`` / ``unsubscribe`` calls block until the worker has
    serviced the command.
    """

    __slots__ = (
        "_command_queue",
        "_config",
        "_connection",
        "_connection_cm",
        "_lock",
        "_pool_destroying_registered",
        "_queues",
        "_shutting_down",
        "_stopping",
        "_worker_thread",
    )

    def __init__(self, config: "PsycopgSyncConfig") -> None:
        self._config = config
        self._lock = threading.Lock()
        self._queues: dict[str, WeakKeyDictionary[threading.Thread, stdlib_queue.Queue[str]]] = {}
        self._connection_cm: Any | None = None
        self._connection: Any | None = None
        self._stopping = threading.Event()
        self._shutting_down = False
        self._command_queue: stdlib_queue.Queue[tuple[str, str, threading.Event]] = stdlib_queue.Queue()
        self._worker_thread: threading.Thread | None = None
        self._pool_destroying_registered = False

    def subscribe(self, channel: str) -> None:
        with self._lock:
            if self._shutting_down:
                msg = "PsycopgSyncListenerHub is shutting down"
                raise RuntimeError(msg)
            if channel in self._queues:
                return
            self._ensure_connection_locked()
            self._queues[channel] = WeakKeyDictionary()
            try:
                self._submit("listen", channel)
            except Exception:
                self._queues.pop(channel, None)
                raise

    def unsubscribe(self, channel: str) -> None:
        with self._lock:
            if channel not in self._queues:
                return
            self._queues.pop(channel, None)
            self._submit("unlisten", channel)

    def dequeue(self, channel: str, poll_interval: float) -> "str | None":
        if channel not in self._queues:
            self.subscribe(channel)
        queue = self._get_consumer_queue(channel)
        if queue is None:
            return None
        try:
            return queue.get(timeout=poll_interval)
        except stdlib_queue.Empty:
            return None

    def shutdown(self) -> None:
        with self._lock:
            if self._shutting_down:
                return
            self._shutting_down = True
            connection_cm = self._connection_cm
            channels = list(self._queues.keys())
            worker_thread = self._worker_thread
            self._queues.clear()
            self._connection_cm = None
            self._worker_thread = None
        if worker_thread is not None:
            for channel in channels:
                with contextlib.suppress(Exception):
                    self._submit("unlisten", channel)
            with contextlib.suppress(Exception):
                self._submit("stop", "", timeout=2.0)
            self._stopping.set()
            with contextlib.suppress(Exception):
                worker_thread.join(timeout=3.0)
        if connection_cm is not None:
            with contextlib.suppress(Exception):
                connection_cm.__exit__(None, None, None)
        self._connection = None
        self._shutting_down = False

    def is_subscribed(self, channel: str) -> bool:
        return channel in self._queues

    def _get_consumer_queue(self, channel: str) -> "stdlib_queue.Queue[str] | None":
        with self._lock:
            queues = self._queues.get(channel)
            if queues is None:
                return None
            thread = threading.current_thread()
            queue = queues.get(thread)
            if queue is None:
                queue = stdlib_queue.Queue()
                queues[thread] = queue
            return queue

    def _ensure_connection_locked(self) -> None:
        if self._connection is not None:
            return
        connection_cm = self._config.provide_connection()
        connection = connection_cm.__enter__()
        if connection is None:
            with contextlib.suppress(Exception):
                connection_cm.__exit__(None, None, None)
            msg = "Psycopg sync adapter did not yield a connection"
            raise ImproperConfigurationError(msg)
        connection.autocommit = True
        self._connection_cm = connection_cm
        self._connection = connection
        self._worker_thread = threading.Thread(target=self._worker, name="psycopg-sync-event-worker", daemon=True)
        self._worker_thread.start()
        self._register_pool_destroying()

    def _register_pool_destroying(self) -> None:
        if self._pool_destroying_registered:
            return
        runtime = self._config.get_observability_runtime()
        runtime.register_lifecycle_hook("on_pool_destroying", self._pool_destroying_hook)
        self._pool_destroying_registered = True

    def _pool_destroying_hook(self, _context: "dict[str, Any]") -> None:
        self.shutdown()

    def _submit(self, op: str, channel: str, *, timeout: "float | None" = None) -> None:
        done = threading.Event()
        self._command_queue.put((op, channel, done))
        done.wait(timeout=timeout)

    def _worker(self) -> None:
        connection = self._connection
        if connection is None:
            return
        while not self._stopping.is_set():
            self._drain_commands(connection)
            if self._stopping.is_set():
                return
            try:
                for notify in connection.notifies(timeout=_PUMP_TIMEOUT, stop_after=_PUMP_BATCH):
                    if self._stopping.is_set():
                        return
                    self._dispatch(notify.channel, notify.payload)
            except Exception as exc:  # pragma: no cover - pump resilience path
                if self._stopping.is_set() or getattr(connection, "closed", False):
                    return
                logger.warning("psycopg sync notify worker error: %s", exc)
                self._stopping.wait(timeout=0.25)

    def _drain_commands(self, connection: Any) -> None:
        while True:
            try:
                op, channel, done = self._command_queue.get_nowait()
            except stdlib_queue.Empty:
                return
            try:
                if op == "listen":
                    validated = normalize_event_channel_name(channel)
                    connection.execute(f"LISTEN {validated}")
                elif op == "unlisten":
                    validated = normalize_event_channel_name(channel)
                    with contextlib.suppress(Exception):
                        connection.execute(f"UNLISTEN {validated}")
                elif op == "stop":
                    self._stopping.set()
            finally:
                done.set()

    def _dispatch(self, channel: str, payload: str) -> None:
        with self._lock:
            queues = self._queues.get(channel)
            if queues is None:
                return
            targets = list(queues.values())
        for queue in targets:
            queue.put_nowait(payload)
