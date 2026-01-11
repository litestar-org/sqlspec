"""Event channel API with separate sync and async implementations."""

import asyncio
import importlib
import inspect
import logging
import threading
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.extensions.events._hints import get_runtime_hints, resolve_adapter_name
from sqlspec.extensions.events._models import EventMessage
from sqlspec.extensions.events._protocols import AsyncEventBackendProtocol, SyncEventBackendProtocol
from sqlspec.extensions.events._queue import build_queue_backend
from sqlspec.extensions.events._store import normalize_event_channel_name
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.type_guards import has_span_attribute
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
    from sqlspec.extensions.events._protocols import AsyncEventHandler, SyncEventHandler
    from sqlspec.observability import ObservabilityRuntime

logger = get_logger("sqlspec.events.channel")

__all__ = (
    "AsyncEventChannel",
    "AsyncEventListener",
    "EventMessage",
    "SyncEventChannel",
    "SyncEventListener",
    "load_native_backend",
    "resolve_poll_interval",
)

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


def resolve_poll_interval(poll_interval: "float | None", default: float) -> float:
    """Resolve poll interval with validation."""
    if poll_interval is None:
        return default
    if poll_interval <= 0:
        msg = "poll_interval must be greater than zero"
        raise ImproperConfigurationError(msg)
    return poll_interval


def _resolve_event_type(payload: "dict[str, Any]", metadata: "dict[str, Any] | None") -> "str | None":
    """Resolve event type from payload or metadata."""
    if metadata and metadata.get("event_type"):
        return str(metadata["event_type"])
    if payload.get("event_type") is not None:
        return str(payload["event_type"])
    if payload.get("type") is not None:
        return str(payload["type"])
    return None


_POSTGRES_ADAPTERS = frozenset({"asyncpg", "psycopg", "psqlpy"})


def _get_default_backend(adapter_name: "str | None") -> str:
    """Return the default events backend for an adapter."""
    if adapter_name in _POSTGRES_ADAPTERS:
        return "listen_notify"
    return "table_queue"


def load_native_backend(config: Any, backend_name: str | None, extension_settings: "dict[str, Any]") -> Any | None:
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
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name=adapter_name,
            backend_module=backend_module_name,
            status="backend_missing",
        )
        return None
    except ImportError as error:
        log_with_context(
            logger,
            logging.WARNING,
            "event.listen",
            adapter_name=adapter_name,
            backend_module=backend_module_name,
            error_type=type(error).__name__,
            status="backend_import_failed",
        )
        return None

    try:
        factory = backend_module.create_event_backend
    except AttributeError:
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name=adapter_name,
            backend_module=backend_module_name,
            status="backend_factory_missing",
        )
        return None
    try:
        backend = factory(config, backend_name, extension_settings)
    except MissingDependencyError as error:
        log_with_context(
            logger,
            logging.WARNING,
            "event.listen",
            adapter_name=adapter_name,
            backend_name=backend_name,
            error_type=type(error).__name__,
            status="backend_dependency_missing",
        )
        return None
    except ImproperConfigurationError as error:
        log_with_context(
            logger,
            logging.WARNING,
            "event.listen",
            adapter_name=adapter_name,
            backend_name=backend_name,
            error_type=type(error).__name__,
            status="backend_config_rejected",
        )
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
    if not runtime.span_manager.is_enabled:
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
    if result is not None and has_span_attribute(span):
        span.set_attribute("sqlspec.events.result", result)
    runtime.end_span(span, error=error)


class SyncEventChannel:
    """Event channel for synchronous database configurations."""

    __slots__ = (
        "_adapter_name",
        "_backend",
        "_backend_name",
        "_config",
        "_listeners",
        "_poll_interval_default",
        "_runtime",
    )

    _backend: "SyncEventBackendProtocol"

    def __init__(self, config: "SyncDatabaseConfig[Any, Any, Any]") -> None:
        if config.is_async:
            msg = "SyncEventChannel requires a sync configuration"
            raise ImproperConfigurationError(msg)
        extension_settings: dict[str, Any] = dict(config.extension_config.get("events", {}))
        self._adapter_name = resolve_adapter_name(config)
        hints = get_runtime_hints(self._adapter_name, config)
        self._poll_interval_default = float(extension_settings.get("poll_interval") or hints.poll_interval)
        queue_backend = build_queue_backend(config, extension_settings, adapter_name=self._adapter_name, hints=hints)
        backend_name = extension_settings.get("backend") or _get_default_backend(self._adapter_name)
        native_backend = load_native_backend(config, backend_name, extension_settings)
        if native_backend is None:
            if backend_name not in {None, "table_queue"}:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "event.listen",
                    adapter_name=self._adapter_name,
                    backend_name=backend_name,
                    fallback_backend="table_queue",
                    status="backend_unavailable",
                )
            self._backend = cast("SyncEventBackendProtocol", queue_backend)
            backend_label = "table_queue"
        else:
            self._backend = cast("SyncEventBackendProtocol", native_backend)
            if isinstance(native_backend, SyncEventBackendProtocol):
                backend_label = native_backend.backend_name
            else:
                backend_label = backend_name or "table_queue"
        self._config = config
        self._backend_name = backend_label
        self._runtime = config.get_observability_runtime()
        self._listeners: dict[str, SyncEventListener] = {}

    def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        """Publish an event to a channel."""
        channel = normalize_event_channel_name(channel)
        if not self._backend.supports_sync:
            msg = "Current events backend does not support sync publishing"
            raise ImproperConfigurationError(msg)
        span = _start_event_span(self._runtime, "publish", self._backend_name, self._adapter_name, channel, mode="sync")
        try:
            event_id = self._backend.publish(channel, payload, metadata)
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="published")
        log_with_context(
            logger,
            logging.DEBUG,
            "event.publish",
            adapter_name=self._adapter_name,
            backend_name=self._backend_name,
            channel=channel,
            event_id=event_id,
            event_type=_resolve_event_type(payload, metadata),
            mode="sync",
        )
        return event_id

    def iter_events(self, channel: str, *, poll_interval: float | None = None) -> Iterator[EventMessage]:
        """Yield events as they become available."""
        channel = normalize_event_channel_name(channel)
        if not self._backend.supports_sync:
            msg = "Current events backend does not support sync consumption"
            raise ImproperConfigurationError(msg)
        interval = resolve_poll_interval(poll_interval, self._poll_interval_default)
        while True:
            span = _start_event_span(
                self._runtime, "dequeue", self._backend_name, self._adapter_name, channel, mode="sync"
            )
            try:
                event = self._backend.dequeue(channel, interval)
            except Exception as error:
                _end_event_span(self._runtime, span, error=error)
                raise
            if event is None:
                _end_event_span(self._runtime, span, result="empty")
                continue
            _end_event_span(self._runtime, span, result="delivered")
            self._runtime.increment_metric("events.deliver")
            log_with_context(
                logger,
                logging.DEBUG,
                "event.receive",
                adapter_name=self._adapter_name,
                backend_name=self._backend_name,
                channel=channel,
                event_id=event.event_id,
                event_type=_resolve_event_type(event.payload, event.metadata),
                mode="sync",
            )
            yield event

    def listen(
        self, channel: str, handler: "SyncEventHandler", *, poll_interval: float | None = None, auto_ack: bool = True
    ) -> SyncEventListener:
        """Start a background thread that invokes handler for each event."""
        channel = normalize_event_channel_name(channel)
        if not self._backend.supports_sync:
            msg = "Current events backend does not support sync listeners"
            raise ImproperConfigurationError(msg)
        interval = resolve_poll_interval(poll_interval, self._poll_interval_default)
        listener_id = uuid4().hex
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._run_listener, args=(listener_id, channel, handler, stop_event, interval, auto_ack), daemon=True
        )
        listener = SyncEventListener(listener_id, channel, thread, stop_event, interval)
        self._listeners[listener_id] = listener
        self._runtime.increment_metric("events.listener.start")
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name=self._adapter_name,
            backend_name=self._backend_name,
            channel=channel,
            listener_id=listener_id,
            mode="sync",
            status="start",
        )
        thread.start()
        return listener

    def stop_listener(self, listener_id: str) -> None:
        """Stop a running listener."""
        listener = self._listeners.pop(listener_id, None)
        if listener is None:
            return
        listener.stop()
        self._runtime.increment_metric("events.listener.stop")
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name=self._adapter_name,
            backend_name=self._backend_name,
            channel=listener.channel,
            listener_id=listener_id,
            mode="sync",
            status="stop",
        )

    def ack(self, event_id: str) -> None:
        """Acknowledge an event."""
        if not self._backend.supports_sync:
            msg = "Current events backend does not support sync ack"
            raise ImproperConfigurationError(msg)
        span = _start_event_span(self._runtime, "ack", self._backend_name, self._adapter_name, mode="sync")
        try:
            self._backend.ack(event_id)
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="acked")

    def nack(self, event_id: str) -> None:
        """Return an event to the queue for redelivery."""
        span = _start_event_span(self._runtime, "nack", self._backend_name, self._adapter_name, mode="sync")
        try:
            self._backend.nack(event_id)
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
            self._backend.shutdown()
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
                    event = self._backend.dequeue(channel, poll_interval)
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
                        self._backend.ack(event.event_id)
                except Exception as error:
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "event.listen",
                        adapter_name=self._adapter_name,
                        backend_name=self._backend_name,
                        channel=channel,
                        listener_id=listener_id,
                        mode="sync",
                        error_type=type(error).__name__,
                        status="handler_error",
                        event_id=event.event_id,
                        event_type=_resolve_event_type(event.payload, event.metadata),
                    )
        finally:
            self._listeners.pop(listener_id, None)


class AsyncEventChannel:
    """Event channel for asynchronous database configurations."""

    __slots__ = (
        "_adapter_name",
        "_backend",
        "_backend_name",
        "_config",
        "_listeners",
        "_poll_interval_default",
        "_runtime",
    )

    _backend: "AsyncEventBackendProtocol"

    def __init__(self, config: "AsyncDatabaseConfig[Any, Any, Any]") -> None:
        if not config.is_async:
            msg = "AsyncEventChannel requires an async configuration"
            raise ImproperConfigurationError(msg)
        extension_settings: dict[str, Any] = dict(config.extension_config.get("events", {}))
        self._adapter_name = resolve_adapter_name(config)
        hints = get_runtime_hints(self._adapter_name, config)
        self._poll_interval_default = float(extension_settings.get("poll_interval") or hints.poll_interval)
        queue_backend = build_queue_backend(config, extension_settings, adapter_name=self._adapter_name, hints=hints)
        backend_name = extension_settings.get("backend") or _get_default_backend(self._adapter_name)
        native_backend = load_native_backend(config, backend_name, extension_settings)
        if native_backend is None:
            if backend_name not in {None, "table_queue"}:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "event.listen",
                    adapter_name=self._adapter_name,
                    backend_name=backend_name,
                    fallback_backend="table_queue",
                    status="backend_unavailable",
                )
            self._backend = cast("AsyncEventBackendProtocol", queue_backend)
            backend_label = "table_queue"
        else:
            self._backend = cast("AsyncEventBackendProtocol", native_backend)
            if isinstance(native_backend, AsyncEventBackendProtocol):
                backend_label = native_backend.backend_name
            else:
                backend_label = backend_name or "table_queue"
        self._config = config
        self._backend_name = backend_label
        self._runtime = config.get_observability_runtime()
        self._listeners: dict[str, AsyncEventListener] = {}

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        """Publish an event to a channel."""
        channel = normalize_event_channel_name(channel)
        if not self._backend.supports_async:
            msg = "Current events backend does not support async publishing"
            raise ImproperConfigurationError(msg)
        span = _start_event_span(
            self._runtime, "publish", self._backend_name, self._adapter_name, channel, mode="async"
        )
        try:
            event_id = await self._backend.publish(channel, payload, metadata)
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="published")
        log_with_context(
            logger,
            logging.DEBUG,
            "event.publish",
            adapter_name=self._adapter_name,
            backend_name=self._backend_name,
            channel=channel,
            event_id=event_id,
            event_type=_resolve_event_type(payload, metadata),
            mode="async",
        )
        return event_id

    async def iter_events(self, channel: str, *, poll_interval: float | None = None) -> AsyncIterator[EventMessage]:
        """Yield events as they become available."""
        channel = normalize_event_channel_name(channel)
        if not self._backend.supports_async:
            msg = "Current events backend does not support async consumption"
            raise ImproperConfigurationError(msg)
        interval = resolve_poll_interval(poll_interval, self._poll_interval_default)
        while True:
            span = _start_event_span(
                self._runtime, "dequeue", self._backend_name, self._adapter_name, channel, mode="async"
            )
            try:
                event = await self._backend.dequeue(channel, interval)
            except Exception as error:
                _end_event_span(self._runtime, span, error=error)
                raise
            if event is None:
                _end_event_span(self._runtime, span, result="empty")
                continue
            _end_event_span(self._runtime, span, result="delivered")
            self._runtime.increment_metric("events.deliver")
            log_with_context(
                logger,
                logging.DEBUG,
                "event.receive",
                adapter_name=self._adapter_name,
                backend_name=self._backend_name,
                channel=channel,
                event_id=event.event_id,
                event_type=_resolve_event_type(event.payload, event.metadata),
                mode="async",
            )
            yield event

    def listen(
        self,
        channel: str,
        handler: "AsyncEventHandler | SyncEventHandler",
        *,
        poll_interval: float | None = None,
        auto_ack: bool = True,
    ) -> AsyncEventListener:
        """Start an async task that delivers events to handler."""
        channel = normalize_event_channel_name(channel)
        if not self._backend.supports_async:
            msg = "Current events backend does not support async listeners"
            raise ImproperConfigurationError(msg)
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        interval = resolve_poll_interval(poll_interval, self._poll_interval_default)
        listener_id = uuid4().hex
        task = loop.create_task(self._run_listener(listener_id, channel, handler, stop_event, interval, auto_ack))
        listener = AsyncEventListener(listener_id, channel, task, stop_event, interval)
        self._listeners[listener_id] = listener
        self._runtime.increment_metric("events.listener.start")
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name=self._adapter_name,
            backend_name=self._backend_name,
            channel=channel,
            listener_id=listener_id,
            mode="async",
            status="start",
        )
        return listener

    async def stop_listener(self, listener_id: str) -> None:
        """Stop a running listener."""
        listener = self._listeners.pop(listener_id, None)
        if listener is None:
            return
        await listener.stop()
        self._runtime.increment_metric("events.listener.stop")
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name=self._adapter_name,
            backend_name=self._backend_name,
            channel=listener.channel,
            listener_id=listener_id,
            mode="async",
            status="stop",
        )

    async def ack(self, event_id: str) -> None:
        """Acknowledge an event."""
        if not self._backend.supports_async:
            msg = "Current events backend does not support async ack"
            raise ImproperConfigurationError(msg)
        span = _start_event_span(self._runtime, "ack", self._backend_name, self._adapter_name, mode="async")
        try:
            await self._backend.ack(event_id)
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        _end_event_span(self._runtime, span, result="acked")

    async def nack(self, event_id: str) -> None:
        """Return an event to the queue for redelivery."""
        span = _start_event_span(self._runtime, "nack", self._backend_name, self._adapter_name, mode="async")
        try:
            await self._backend.nack(event_id)
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
            await self._backend.shutdown()
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
                    event = await self._backend.dequeue(channel, poll_interval)
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
                        await self._backend.ack(event.event_id)
                except Exception as error:
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "event.listen",
                        adapter_name=self._adapter_name,
                        backend_name=self._backend_name,
                        channel=channel,
                        listener_id=listener_id,
                        mode="async",
                        error_type=type(error).__name__,
                        status="handler_error",
                        event_id=event.event_id,
                        event_type=_resolve_event_type(event.payload, event.metadata),
                    )
        finally:
            self._listeners.pop(listener_id, None)
