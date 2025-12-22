"""Event channel API with separate sync and async implementations."""

import asyncio
import importlib
import inspect
import threading
from collections.abc import AsyncIterator, Awaitable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.extensions.events._hints import get_runtime_hints
from sqlspec.extensions.events._models import EventMessage
from sqlspec.extensions.events._queue import build_queue_backend
from sqlspec.extensions.events._store import normalize_event_channel_name
from sqlspec.utils.logging import get_logger
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
    from sqlspec.extensions.events.protocols import AsyncEventHandler, SyncEventHandler
    from sqlspec.observability import ObservabilityRuntime

logger = get_logger("events.channel")

__all__ = ("AsyncEventChannel", "AsyncEventListener", "EventMessage", "SyncEventChannel", "SyncEventListener")

_ADAPTER_MODULE_PARTS = 3


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


def _resolve_adapter_name(config: Any) -> "str | None":
    """Resolve adapter name from config module path."""
    module_name = type(config).__module__
    parts = module_name.split(".")
    if len(parts) >= _ADAPTER_MODULE_PARTS and parts[0] == "sqlspec" and parts[1] == "adapters":
        return parts[2]
    return None


def _normalize_channel_name(channel: str) -> str:
    """Normalize and validate channel name."""
    try:
        return normalize_event_channel_name(channel)
    except ValueError as error:
        raise ImproperConfigurationError(str(error)) from error


def _resolve_poll_interval(poll_interval: "float | None", default: float) -> float:
    """Resolve poll interval with validation."""
    if poll_interval is None:
        return default
    if poll_interval <= 0:
        msg = "poll_interval must be greater than zero"
        raise ImproperConfigurationError(msg)
    return poll_interval


def _load_native_backend(config: Any, backend_name: str | None, extension_settings: "dict[str, Any]") -> Any | None:
    """Load adapter-specific native backend if available."""
    if backend_name in {None, "table_queue"}:
        return None
    module_name = type(config).__module__
    parts = module_name.split(".")
    if len(parts) < _ADAPTER_MODULE_PARTS or parts[0] != "sqlspec" or parts[1] != "adapters":
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


def _start_event_span(
    runtime: "ObservabilityRuntime",
    operation: str,
    backend_name: str,
    adapter_name: "str | None",
    channel: "str | None" = None,
    mode: str = "sync",
) -> Any:
    """Start an observability span for event operations."""
    if not getattr(runtime.span_manager, "is_enabled", False):
        return None
    attributes: dict[str, Any] = {
        "sqlspec.events.operation": operation,
        "sqlspec.events.backend": backend_name,
        "sqlspec.events.mode": mode,
    }
    if adapter_name:
        attributes["sqlspec.events.adapter"] = adapter_name
    if channel:
        attributes["sqlspec.events.channel"] = channel
    return runtime.start_span(f"sqlspec.events.{operation}", attributes=attributes)


def _end_event_span(
    runtime: "ObservabilityRuntime", span: Any, *, error: "Exception | None" = None, result: "str | None" = None
) -> None:
    """End an observability span."""
    if span is None:
        return
    if result is not None:
        setter = getattr(span, "set_attribute", None)
        if setter is not None:
            setter("sqlspec.events.result", result)
    runtime.end_span(span, error=error)


class SyncEventChannel:
    """Event channel for synchronous database configurations.

    Provides publish/subscribe functionality using sync database drivers.
    Use with sync configs like SqliteConfig, DuckdbConfig, PsycopgSyncConfig.
    """

    __slots__ = (
        "_adapter_name",
        "_backend",
        "_backend_name",
        "_config",
        "_listeners",
        "_poll_interval_default",
        "_runtime",
    )

    def __init__(self, config: "SyncDatabaseConfig[Any, Any, Any]") -> None:
        if config.is_async:
            msg = "SyncEventChannel requires a sync configuration"
            raise ImproperConfigurationError(msg)
        extension_settings: dict[str, Any] = dict(config.extension_config.get("events", {}))
        self._adapter_name = _resolve_adapter_name(config)
        hints = get_runtime_hints(self._adapter_name, config)
        self._poll_interval_default = float(extension_settings.get("poll_interval") or hints.poll_interval)
        queue_backend = build_queue_backend(config, extension_settings, adapter_name=self._adapter_name, hints=hints)
        backend_name = config.driver_features.get("events_backend") or "table_queue"
        native_backend = _load_native_backend(config, backend_name, extension_settings)
        if native_backend is None:
            if backend_name not in {None, "table_queue"}:
                logger.warning("Events backend %s unavailable; defaulting to table_queue", backend_name)
            self._backend = queue_backend
            backend_label = "table_queue"
        else:
            self._backend = native_backend
            backend_label = getattr(native_backend, "backend_name", backend_name or "table_queue")
        self._config = config
        self._backend_name = backend_label
        self._runtime = config.get_observability_runtime()
        self._listeners: dict[str, SyncEventListener] = {}

    def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        """Publish an event to a channel.

        Args:
            channel: Target channel name.
            payload: Event payload (must be JSON-serializable).
            metadata: Optional metadata dict.

        Returns:
            The event ID.
        """
        channel = _normalize_channel_name(channel)
        if not getattr(self._backend, "supports_sync", False):
            msg = "Current events backend does not support sync publishing"
            raise ImproperConfigurationError(msg)
        span = _start_event_span(self._runtime, "publish", self._backend_name, self._adapter_name, channel, mode="sync")
        try:
            event_id = self._backend.publish_sync(channel, payload, metadata)
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="published")
        return event_id

    def iter_events(self, channel: str, *, poll_interval: float | None = None) -> Iterator[EventMessage]:
        """Yield events as they become available.

        Args:
            channel: Channel to consume from.
            poll_interval: Override default poll interval.

        Yields:
            EventMessage objects.
        """
        channel = _normalize_channel_name(channel)
        if not getattr(self._backend, "supports_sync", False):
            msg = "Current events backend does not support sync consumption"
            raise ImproperConfigurationError(msg)
        interval = _resolve_poll_interval(poll_interval, self._poll_interval_default)
        while True:
            span = _start_event_span(
                self._runtime, "dequeue", self._backend_name, self._adapter_name, channel, mode="sync"
            )
            try:
                event = self._backend.dequeue_sync(channel, interval)
            except Exception as error:
                _end_event_span(self._runtime, span, error=error)
                raise
            if event is None:
                _end_event_span(self._runtime, span, result="empty")
                continue
            _end_event_span(self._runtime, span, result="delivered")
            self._runtime.increment_metric("events.deliver")
            yield event

    def listen(
        self, channel: str, handler: "SyncEventHandler", *, poll_interval: float | None = None, auto_ack: bool = True
    ) -> SyncEventListener:
        """Start a background thread that invokes handler for each event.

        Args:
            channel: Channel to listen on.
            handler: Callback function for each event.
            poll_interval: Override default poll interval.
            auto_ack: Automatically acknowledge events after handler completes.

        Returns:
            SyncEventListener that can be used to stop the listener.
        """
        channel = _normalize_channel_name(channel)
        if not getattr(self._backend, "supports_sync", False):
            msg = "Current events backend does not support sync listeners"
            raise ImproperConfigurationError(msg)
        interval = _resolve_poll_interval(poll_interval, self._poll_interval_default)
        listener_id = uuid4().hex
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._run_listener, args=(listener_id, channel, handler, stop_event, interval, auto_ack), daemon=True
        )
        listener = SyncEventListener(listener_id, channel, thread, stop_event, interval)
        self._listeners[listener_id] = listener
        self._runtime.increment_metric("events.listener.start")
        thread.start()
        return listener

    def stop_listener(self, listener_id: str) -> None:
        """Stop a running listener.

        Args:
            listener_id: ID of the listener to stop.
        """
        listener = self._listeners.pop(listener_id, None)
        if listener is None:
            return
        listener.stop()
        self._runtime.increment_metric("events.listener.stop")

    def ack(self, event_id: str) -> None:
        """Acknowledge an event.

        Args:
            event_id: ID of the event to acknowledge.
        """
        if not getattr(self._backend, "supports_sync", False):
            msg = "Current events backend does not support sync ack"
            raise ImproperConfigurationError(msg)
        span = _start_event_span(self._runtime, "ack", self._backend_name, self._adapter_name, mode="sync")
        try:
            self._backend.ack_sync(event_id)
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="acked")

    def nack(self, event_id: str) -> None:
        """Return an event to the queue for redelivery.

        Args:
            event_id: ID of the event to return.
        """
        nack_method = getattr(self._backend, "nack_sync", None)
        if nack_method is None:
            msg = "Current events backend does not support nack"
            raise ImproperConfigurationError(msg)
        span = _start_event_span(self._runtime, "nack", self._backend_name, self._adapter_name, mode="sync")
        try:
            nack_method(event_id)
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="nacked")

    def shutdown(self) -> None:
        """Shutdown the event channel and release backend resources."""
        span = _start_event_span(self._runtime, "shutdown", self._backend_name, self._adapter_name, mode="sync")
        try:
            for listener_id in list(self._listeners):
                self.stop_listener(listener_id)
            backend_shutdown = getattr(self._backend, "shutdown_sync", None)
            if backend_shutdown is not None and callable(backend_shutdown):
                backend_shutdown()
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="shutdown")
        self._runtime.increment_metric("events.shutdown")

    def _run_listener(
        self,
        listener_id: str,
        channel: str,
        handler: "SyncEventHandler",
        stop_event: threading.Event,
        poll_interval: float,
        auto_ack: bool,
    ) -> None:
        """Internal listener loop."""
        try:
            while not stop_event.is_set():
                span = _start_event_span(
                    self._runtime, "dequeue", self._backend_name, self._adapter_name, channel, mode="sync"
                )
                try:
                    event = self._backend.dequeue_sync(channel, poll_interval)
                except Exception as error:
                    _end_event_span(self._runtime, span, error=error)
                    raise
                if event is None:
                    _end_event_span(self._runtime, span, result="empty")
                    continue
                _end_event_span(self._runtime, span, result="delivered")
                try:
                    handler(event)
                    if auto_ack:
                        self._backend.ack_sync(event.event_id)
                except Exception as error:  # pragma: no cover - logging path
                    logger.warning("sync listener %s handler error: %s", listener_id, error)
        finally:
            self._listeners.pop(listener_id, None)


class AsyncEventChannel:
    """Event channel for asynchronous database configurations.

    Provides publish/subscribe functionality using async database drivers.
    Use with async configs like AsyncpgConfig, AiosqliteConfig, PsycopgAsyncConfig.
    """

    __slots__ = (
        "_adapter_name",
        "_backend",
        "_backend_name",
        "_config",
        "_listeners",
        "_poll_interval_default",
        "_runtime",
    )

    def __init__(self, config: "AsyncDatabaseConfig[Any, Any, Any]") -> None:
        if not config.is_async:
            msg = "AsyncEventChannel requires an async configuration"
            raise ImproperConfigurationError(msg)
        extension_settings: dict[str, Any] = dict(config.extension_config.get("events", {}))
        self._adapter_name = _resolve_adapter_name(config)
        hints = get_runtime_hints(self._adapter_name, config)
        self._poll_interval_default = float(extension_settings.get("poll_interval") or hints.poll_interval)
        queue_backend = build_queue_backend(config, extension_settings, adapter_name=self._adapter_name, hints=hints)
        backend_name = config.driver_features.get("events_backend") or "table_queue"
        native_backend = _load_native_backend(config, backend_name, extension_settings)
        if native_backend is None:
            if backend_name not in {None, "table_queue"}:
                logger.warning("Events backend %s unavailable; defaulting to table_queue", backend_name)
            self._backend = queue_backend
            backend_label = "table_queue"
        else:
            self._backend = native_backend
            backend_label = getattr(native_backend, "backend_name", backend_name or "table_queue")
        self._config = config
        self._backend_name = backend_label
        self._runtime = config.get_observability_runtime()
        self._listeners: dict[str, AsyncEventListener] = {}

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        """Publish an event to a channel.

        Args:
            channel: Target channel name.
            payload: Event payload (must be JSON-serializable).
            metadata: Optional metadata dict.

        Returns:
            The event ID.
        """
        channel = _normalize_channel_name(channel)
        if not getattr(self._backend, "supports_async", False):
            msg = "Current events backend does not support async publishing"
            raise ImproperConfigurationError(msg)
        span = _start_event_span(
            self._runtime, "publish", self._backend_name, self._adapter_name, channel, mode="async"
        )
        try:
            event_id = await self._backend.publish_async(channel, payload, metadata)
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="published")
        return event_id

    async def iter_events(self, channel: str, *, poll_interval: float | None = None) -> AsyncIterator[EventMessage]:
        """Yield events as they become available.

        Args:
            channel: Channel to consume from.
            poll_interval: Override default poll interval.

        Yields:
            EventMessage objects.
        """
        channel = _normalize_channel_name(channel)
        if not getattr(self._backend, "supports_async", False):
            msg = "Current events backend does not support async consumption"
            raise ImproperConfigurationError(msg)
        interval = _resolve_poll_interval(poll_interval, self._poll_interval_default)
        while True:
            span = _start_event_span(
                self._runtime, "dequeue", self._backend_name, self._adapter_name, channel, mode="async"
            )
            try:
                event = await self._backend.dequeue_async(channel, interval)
            except Exception as error:
                _end_event_span(self._runtime, span, error=error)
                raise
            if event is None:
                _end_event_span(self._runtime, span, result="empty")
                continue
            _end_event_span(self._runtime, span, result="delivered")
            self._runtime.increment_metric("events.deliver")
            yield event

    def listen(
        self,
        channel: str,
        handler: "AsyncEventHandler | SyncEventHandler",
        *,
        poll_interval: float | None = None,
        auto_ack: bool = True,
    ) -> AsyncEventListener:
        """Start an async task that delivers events to handler.

        Args:
            channel: Channel to listen on.
            handler: Callback function for each event (sync or async).
            poll_interval: Override default poll interval.
            auto_ack: Automatically acknowledge events after handler completes.

        Returns:
            AsyncEventListener that can be used to stop the listener.
        """
        channel = _normalize_channel_name(channel)
        if not getattr(self._backend, "supports_async", False):
            msg = "Current events backend does not support async listeners"
            raise ImproperConfigurationError(msg)
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        interval = _resolve_poll_interval(poll_interval, self._poll_interval_default)
        listener_id = uuid4().hex
        task = loop.create_task(self._run_listener(listener_id, channel, handler, stop_event, interval, auto_ack))
        listener = AsyncEventListener(listener_id, channel, task, stop_event, interval)
        self._listeners[listener_id] = listener
        self._runtime.increment_metric("events.listener.start")
        return listener

    async def stop_listener(self, listener_id: str) -> None:
        """Stop a running listener.

        Args:
            listener_id: ID of the listener to stop.
        """
        listener = self._listeners.pop(listener_id, None)
        if listener is None:
            return
        await listener.stop()
        self._runtime.increment_metric("events.listener.stop")

    async def ack(self, event_id: str) -> None:
        """Acknowledge an event.

        Args:
            event_id: ID of the event to acknowledge.
        """
        if not getattr(self._backend, "supports_async", False):
            msg = "Current events backend does not support async ack"
            raise ImproperConfigurationError(msg)
        span = _start_event_span(self._runtime, "ack", self._backend_name, self._adapter_name, mode="async")
        try:
            await self._backend.ack_async(event_id)
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="acked")

    async def nack(self, event_id: str) -> None:
        """Return an event to the queue for redelivery.

        Args:
            event_id: ID of the event to return.
        """
        nack_method = getattr(self._backend, "nack_async", None)
        if nack_method is None:
            msg = "Current events backend does not support nack"
            raise ImproperConfigurationError(msg)
        span = _start_event_span(self._runtime, "nack", self._backend_name, self._adapter_name, mode="async")
        try:
            await nack_method(event_id)
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="nacked")

    async def shutdown(self) -> None:
        """Shutdown the event channel and release backend resources."""
        span = _start_event_span(self._runtime, "shutdown", self._backend_name, self._adapter_name, mode="async")
        try:
            for listener_id in list(self._listeners):
                await self.stop_listener(listener_id)
            backend_shutdown = getattr(self._backend, "shutdown_async", None)
            if backend_shutdown is not None and callable(backend_shutdown):
                result = backend_shutdown()
                if result is not None:
                    await cast("Awaitable[None]", result)
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="shutdown")
        self._runtime.increment_metric("events.shutdown")

    async def _run_listener(
        self,
        listener_id: str,
        channel: str,
        handler: "AsyncEventHandler | SyncEventHandler",
        stop_event: "asyncio.Event",
        poll_interval: float,
        auto_ack: bool,
    ) -> None:
        """Internal listener loop."""
        try:
            while not stop_event.is_set():
                span = _start_event_span(
                    self._runtime, "dequeue", self._backend_name, self._adapter_name, channel, mode="async"
                )
                try:
                    event = await self._backend.dequeue_async(channel, poll_interval)
                except Exception as error:
                    _end_event_span(self._runtime, span, error=error)
                    raise
                if event is None:
                    _end_event_span(self._runtime, span, result="empty")
                    continue
                _end_event_span(self._runtime, span, result="delivered")
                try:
                    result = handler(event)
                    if inspect.isawaitable(result):
                        await result
                    if auto_ack:
                        await self._backend.ack_async(event.event_id)
                except Exception as error:  # pragma: no cover - logging path
                    logger.warning("async listener %s handler error: %s", listener_id, error)
        finally:
            self._listeners.pop(listener_id, None)
