"""Unified event channel API with queue fallback."""

import asyncio
import importlib
import inspect
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, AsyncIterator, Iterator

from sqlspec.config import DatabaseConfigProtocol
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.portal import get_global_portal
from ._models import EventMessage
from ._queue import QueueEventBackend, TableEventQueue
from .protocols import AsyncEventHandler, SyncEventHandler

logger = get_logger("events.channel")

__all__ = (
    "EventChannel",
    "EventMessage",
    "AsyncEventListener",
    "SyncEventListener",
)


@dataclass(slots=True)
class AsyncEventListener:
    """Represents a running async listener task."""

    id: str
    channel: str
    task: "asyncio.Task[Any]"
    stop_event: "asyncio.Event"
    poll_interval: float

    async def stop(self) -> None:
        """Signal the listener to stop and await task completion."""

        self.stop_event.set()
        if not self.task.done():
            await self.task


@dataclass(slots=True)
class SyncEventListener:
    """Represents a running sync listener thread."""

    id: str
    channel: str
    thread: threading.Thread
    stop_event: threading.Event
    poll_interval: float

    def stop(self) -> None:
        """Signal the listener to stop and join the thread."""

        self.stop_event.set()
        self.thread.join()


class EventChannel:
    """High-level event API that works across sync and async adapters."""

    __slots__ = (
        "_backend",
        "_config",
        "_is_async",
        "_listeners_async",
        "_listeners_sync",
        "_portal",
        "_portal_bridge",
        "_runtime",
    )

    def __init__(self, config: "DatabaseConfigProtocol[Any, Any, Any]") -> None:
        extension_settings = dict(config.extension_config.get("events", {}))
        if config.is_async:
            extension_settings.setdefault("portal_bridge", True)
        queue_backend = QueueEventBackend(
            TableEventQueue(
                config,
                queue_table=extension_settings.get("queue_table"),
                lease_seconds=extension_settings.get("lease_seconds"),
                retention_seconds=extension_settings.get("retention_seconds"),
            )
        )
        backend_name = config.driver_features.get("events_backend", "queue")
        native_backend = self._load_native_backend(config, backend_name, extension_settings)
        if native_backend is None:
            if backend_name not in (None, "queue"):
                logger.warning("Events backend %s unavailable; defaulting to queue", backend_name)
            self._backend = queue_backend
        else:
            self._backend = native_backend
        self._config = config
        self._is_async = bool(config.is_async)
        self._portal_bridge = bool(extension_settings.get("portal_bridge", False)) and self._is_async
        self._portal = None
        self._runtime = config.get_observability_runtime()
        self._listeners_async: dict[str, AsyncEventListener] = {}
        self._listeners_sync: dict[str, SyncEventListener] = {}

    # Publishing -----------------------------------------------------------------

    async def publish_async(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        """Publish an event using an async driver."""

        if not self._is_async:
            msg = "publish_async requires an async configuration"
            raise ImproperConfigurationError(msg)
        if not getattr(self._backend, "supports_async", False):
            msg = "Current events backend does not support async publishing"
            raise ImproperConfigurationError(msg)
        return await self._backend.publish_async(channel, payload, metadata)

    def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        """Publish an event using a sync driver."""

        if self._is_async:
            if self._should_bridge_sync_calls():
                return self._bridge_sync_call(self.publish_async, channel, payload, metadata)
            msg = "publish requires a sync configuration"
            raise ImproperConfigurationError(msg)
        if not getattr(self._backend, "supports_sync", False):
            msg = "Current events backend does not support sync publishing"
            raise ImproperConfigurationError(msg)
        return self._backend.publish(channel, payload, metadata)

    # Iteration -------------------------------------------------------------------

    async def iter_events_async(
        self, channel: str, *, poll_interval: float = 1.0
    ) -> AsyncIterator[EventMessage]:
        """Yield events as they become available for async adapters."""

        if not self._is_async:
            msg = "iter_events_async requires an async configuration"
            raise ImproperConfigurationError(msg)
        if not getattr(self._backend, "supports_async", False):
            msg = "Current events backend does not support async consumption"
            raise ImproperConfigurationError(msg)
        while True:
            event = await self._backend.dequeue_async(channel, poll_interval)
            if event is None:
                await asyncio.sleep(poll_interval)
                continue
            self._runtime.increment_metric("events.deliver")
            yield event

    def iter_events(self, channel: str, *, poll_interval: float = 1.0) -> Iterator[EventMessage]:
        """Yield events for sync adapters."""

        if self._is_async:
            if self._should_bridge_sync_calls():
                yield from self._iter_events_portal(channel, poll_interval)
                return
            msg = "iter_events requires a sync configuration"
            raise ImproperConfigurationError(msg)
        if not getattr(self._backend, "supports_sync", False):
            msg = "Current events backend does not support sync consumption"
            raise ImproperConfigurationError(msg)
        while True:
            event = self._backend.dequeue(channel, poll_interval)
            if event is None:
                time.sleep(poll_interval)
                continue
            self._runtime.increment_metric("events.deliver")
            yield event

    # Listening -------------------------------------------------------------------

    def listen(
        self,
        channel: str,
        handler: "SyncEventHandler",
        *,
        poll_interval: float = 1.0,
        auto_ack: bool = True,
    ) -> SyncEventListener:
        """Start a background thread that invokes handler for each event."""

        if self._is_async:
            if not self._should_bridge_sync_calls():
                msg = "listen requires a sync configuration"
                raise ImproperConfigurationError(msg)
        if not getattr(self._backend, "supports_sync", False) and not self._should_bridge_sync_calls():
            msg = "Current events backend does not support sync listeners"
            raise ImproperConfigurationError(msg)
        listener_id = uuid.uuid4().hex
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._run_sync_listener,
            args=(listener_id, channel, handler, stop_event, poll_interval, auto_ack),
            daemon=True,
        )
        listener = SyncEventListener(listener_id, channel, thread, stop_event, poll_interval)
        self._listeners_sync[listener_id] = listener
        self._runtime.increment_metric("events.listener.start")
        thread.start()
        return listener

    def stop_listener(self, listener_id: str) -> None:
        """Stop a running sync listener."""

        listener = self._listeners_sync.pop(listener_id, None)
        if listener is None:
            return
        listener.stop()
        self._runtime.increment_metric("events.listener.stop")

    def _run_sync_listener(
        self,
        listener_id: str,
        channel: str,
        handler: "SyncEventHandler",
        stop_event: threading.Event,
        poll_interval: float,
        auto_ack: bool,
    ) -> None:
        try:
            while not stop_event.is_set():
                event = self._dequeue_for_sync(channel, poll_interval)
                if event is None:
                    time.sleep(poll_interval)
                    continue
                message = event
                try:
                    handler(message)
                    if auto_ack:
                        self._ack_for_sync(message.event_id)
                except Exception as error:  # pragma: no cover - logging path
                    logger.warning("sync listener %s handler error: %s", listener_id, error)
        finally:
            self._listeners_sync.pop(listener_id, None)

    def listen_async(
        self,
        channel: str,
        handler: "AsyncEventHandler | SyncEventHandler",
        *,
        poll_interval: float = 1.0,
        auto_ack: bool = True,
    ) -> AsyncEventListener:
        """Start an async task that delivers events to handler."""

        if not self._is_async:
            msg = "listen_async requires an async configuration"
            raise ImproperConfigurationError(msg)
        if not getattr(self._backend, "supports_async", False):
            msg = "Current events backend does not support async listeners"
            raise ImproperConfigurationError(msg)
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        listener_id = uuid.uuid4().hex
        task = loop.create_task(
            self._run_async_listener(listener_id, channel, handler, stop_event, poll_interval, auto_ack)
        )
        listener = AsyncEventListener(listener_id, channel, task, stop_event, poll_interval)
        self._listeners_async[listener_id] = listener
        self._runtime.increment_metric("events.listener.start")
        return listener

    async def stop_listener_async(self, listener_id: str) -> None:
        """Stop a running async listener."""

        listener = self._listeners_async.pop(listener_id, None)
        if listener is None:
            return
        await listener.stop()
        self._runtime.increment_metric("events.listener.stop")

    async def _run_async_listener(
        self,
        listener_id: str,
        channel: str,
        handler: "AsyncEventHandler | SyncEventHandler",
        stop_event: "asyncio.Event",
        poll_interval: float,
        auto_ack: bool,
    ) -> None:
        try:
            while not stop_event.is_set():
                event = await self._backend.dequeue_async(channel, poll_interval)
                if event is None:
                    await asyncio.sleep(poll_interval)
                    continue
                message = event
                try:
                    result = handler(message)
                    if inspect.isawaitable(result):
                        await result
                    if auto_ack:
                        await self._backend.ack_async(message.event_id)
                except Exception as error:  # pragma: no cover - logging path
                    logger.warning("async listener %s handler error: %s", listener_id, error)
        finally:
            self._listeners_async.pop(listener_id, None)

    # Ack helpers -----------------------------------------------------------------

    async def ack_async(self, event_id: str) -> None:
        """Acknowledge an event id asynchronously."""

        if not self._is_async:
            msg = "ack_async requires an async configuration"
            raise ImproperConfigurationError(msg)
        if not getattr(self._backend, "supports_async", False):
            msg = "Current events backend does not support async ack"
            raise ImproperConfigurationError(msg)
        await self._backend.ack_async(event_id)

    def ack(self, event_id: str) -> None:
        """Acknowledge an event id synchronously."""

        if self._is_async:
            if self._should_bridge_sync_calls():
                self._bridge_sync_call(self.ack_async, event_id)
                return
            msg = "ack requires a sync configuration"
            raise ImproperConfigurationError(msg)
        if not getattr(self._backend, "supports_sync", False):
            msg = "Current events backend does not support sync ack"
            raise ImproperConfigurationError(msg)
        self._backend.ack(event_id)

    # Loading helpers -----------------------------------------------------------

    @staticmethod
    def _load_native_backend(
        config: "DatabaseConfigProtocol[Any, Any, Any]",
        backend_name: str | None,
        extension_settings: dict[str, Any],
    ) -> Any | None:
        if backend_name in (None, "queue"):
            return None
        module_name = type(config).__module__
        parts = module_name.split(".")
        if len(parts) < 3 or parts[0] != "sqlspec" or parts[1] != "adapters":
            return None
        adapter_name = parts[2]
        backend_module_name = f"sqlspec.adapters.{adapter_name}.events.backend"
        try:
            backend_module = importlib.import_module(backend_module_name)
        except ModuleNotFoundError:
            logger.debug("Adapter %s has no events backend module", adapter_name)
            return None
        except ImportError as error:
            logger.warning("Failed to import %s: %s", backend_module_name, error)
            return None

        factory = getattr(backend_module, "create_event_backend", None)
        if factory is None:
            logger.debug("Adapter %s missing create_event_backend()", adapter_name)
            return None
        try:
            backend = factory(config, backend_name, extension_settings)
        except MissingDependencyError as error:
            logger.warning("Events backend %s missing dependency: %s", backend_name, error)
            return None
        except ImproperConfigurationError as error:
            logger.warning("Events backend %s rejected configuration: %s", backend_name, error)
            return None
        return backend

    def _iter_events_portal(self, channel: str, poll_interval: float) -> Iterator[EventMessage]:
        while True:
            event = self._bridge_sync_call(self._backend.dequeue_async, channel, poll_interval)
            if event is None:
                time.sleep(poll_interval)
                continue
            self._runtime.increment_metric("events.deliver")
            yield event

    def _dequeue_for_sync(self, channel: str, poll_interval: float) -> "EventMessage | None":
        backend_supports_sync = getattr(self._backend, "supports_sync", False)
        if backend_supports_sync and not self._is_async:
            return self._backend.dequeue(channel, poll_interval)
        if self._should_bridge_sync_calls():
            return self._bridge_sync_call(self._backend.dequeue_async, channel, poll_interval)
        return None

    def _ack_for_sync(self, event_id: str) -> None:
        backend_supports_sync = getattr(self._backend, "supports_sync", False)
        if backend_supports_sync and not self._is_async:
            self._backend.ack(event_id)
            return
        if self._should_bridge_sync_calls():
            self._bridge_sync_call(self._backend.ack_async, event_id)
            return
        msg = "Current events backend does not support sync ack"
        raise ImproperConfigurationError(msg)

    def _should_bridge_sync_calls(self) -> bool:
        return self._portal_bridge and getattr(self._backend, "supports_async", False)

    def _bridge_sync_call(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        portal = self._ensure_portal()
        return portal.call(func, *args, **kwargs)

    def _ensure_portal(self):
        if self._portal is None:
            self._portal = get_global_portal()
        return self._portal
