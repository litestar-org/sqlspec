"""Event channel API with separate sync and async implementations."""

import asyncio
import importlib
import inspect
import logging
import threading
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.extensions.events._hints import get_runtime_hints, resolve_adapter_name
from sqlspec.extensions.events._models import EventMessage
from sqlspec.extensions.events._names import normalize_event_channel_name
from sqlspec.extensions.events._protocols import AsyncEventBackendProtocol, SyncEventBackendProtocol
from sqlspec.extensions.events._queue import build_queue_backend
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.type_guards import has_span_attribute
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
    from sqlspec.extensions.events._hints import EventRuntimeHints
    from sqlspec.extensions.events._protocols import AsyncEventHandler, SyncEventHandler
    from sqlspec.observability import ObservabilityRuntime

__all__ = (
    "AsyncEventChannel",
    "AsyncEventListener",
    "EventMessage",
    "SyncEventChannel",
    "SyncEventListener",
    "load_native_backend",
    "resolve_event_poll_interval",
    "resolve_poll_interval",
)

logger = get_logger("sqlspec.events.channel")
_LISTENER_SHUTDOWN_TIMEOUT = 0.5


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
            self.task.cancel()
        with suppress(asyncio.CancelledError):
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
        self.thread.join(timeout=_LISTENER_SHUTDOWN_TIMEOUT)


def resolve_poll_interval(poll_interval: "float | None", default: float) -> float:
    """Resolve poll interval with validation."""
    if poll_interval is None:
        return default
    if poll_interval <= 0:
        msg = "poll_interval must be greater than zero"
        raise ImproperConfigurationError(msg)
    return poll_interval


def resolve_event_poll_interval(
    event_poll_interval: "float | None", poll_interval: "float | None", default: float
) -> float:
    """Resolve the event reconciliation interval with compatibility precedence."""
    resolved = event_poll_interval if event_poll_interval is not None else poll_interval
    if resolved is None:
        resolved = default
    if resolved <= 0:
        msg = "event_poll_interval must be greater than zero"
        raise ImproperConfigurationError(msg)
    return resolved


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
_EVENT_BACKENDS = frozenset({"notify", "notify_queue", "poll_queue", "aq", "txeventq"})
_RETIRED_EVENT_BACKENDS = {
    "listen_notify": "notify",
    "listen_notify_durable": "notify_queue",
    "table_queue": "poll_queue",
}


def _get_default_backend(adapter_name: "str | None") -> str:
    """Return the default events backend for an adapter."""
    if adapter_name in _POSTGRES_ADAPTERS:
        return "notify"
    return "poll_queue"


def _resolve_backend_name(config: Any, extension_settings: "dict[str, Any]", adapter_name: "str | None") -> str:
    """Resolve and validate event backend configuration."""
    backend_name = extension_settings.get("backend")
    if backend_name is None:
        driver_features = getattr(config, "driver_features", {})
        if isinstance(driver_features, dict):
            backend_name = driver_features.get("events_backend")
    if backend_name is None:
        return _get_default_backend(adapter_name)
    if backend_name in _RETIRED_EVENT_BACKENDS:
        replacement = _RETIRED_EVENT_BACKENDS[backend_name]
        msg = f"Event backend {backend_name!r} was removed; use {replacement!r}"
        raise ImproperConfigurationError(msg)
    if backend_name not in _EVENT_BACKENDS:
        valid = ", ".join(sorted(_EVENT_BACKENDS))
        msg = f"Unknown event backend {backend_name!r}; expected one of: {valid}"
        raise ImproperConfigurationError(msg)
    return cast("str", backend_name)


def load_native_backend(
    config: Any, backend_name: str | None, extension_settings: "dict[str, Any]", adapter_name: "str | None" = None
) -> Any | None:
    """Load adapter-specific native backend if available."""
    if backend_name in {None, "poll_queue"}:
        return None
    adapter_name = adapter_name or resolve_adapter_name(config)
    if adapter_name is None:
        return None
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


def _resolve_event_backend(
    config: Any,
    extension_settings: "dict[str, Any]",
    adapter_name: "str | None",
    hints: "EventRuntimeHints",
    *,
    protocol_type: "type[Any]",
) -> "tuple[Any, str]":
    """Resolve the event backend and label for one configuration.

    Falls back to the table queue backend when no native backend is available,
    logging a warning when a non-default backend was requested.

    Args:
        config: Database configuration instance.
        extension_settings: Events extension settings for the configuration.
        adapter_name: Resolved adapter name, if any.
        hints: Adapter event runtime hints.
        protocol_type: Backend protocol the native backend must satisfy to
            report its own backend name.

    Returns:
        Tuple of resolved backend and backend label.
    """
    queue_backend = build_queue_backend(config, extension_settings, adapter_name=adapter_name, hints=hints)
    backend_name = _resolve_backend_name(config, extension_settings, adapter_name)
    native_backend = load_native_backend(config, backend_name, extension_settings, adapter_name=adapter_name)
    if native_backend is None:
        if backend_name not in {None, "poll_queue"}:
            log_with_context(
                logger,
                logging.WARNING,
                "event.listen",
                adapter_name=adapter_name,
                backend_name=backend_name,
                fallback_backend="poll_queue",
                status="backend_unavailable",
            )
        return queue_backend, "poll_queue"
    if isinstance(native_backend, protocol_type):
        return native_backend, cast("str", native_backend.backend_name)
    return native_backend, backend_name or "poll_queue"


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


@contextmanager
def _event_span(
    runtime: "ObservabilityRuntime",
    operation: str,
    backend_name: str,
    adapter_name: "str | None",
    channel: "str | None" = None,
    *,
    mode: str = "sync",
    result: str,
) -> "Iterator[Any]":
    """Manage an observability span around one event operation.

    Starts a span, ends it with ``error`` when the wrapped operation raises,
    and ends it with ``result`` when the operation completes.
    """
    span = _start_event_span(runtime, operation, backend_name, adapter_name, channel, mode=mode)
    try:
        yield span
    except Exception as error:
        _end_event_span(runtime, span, error=error)
        raise
    _end_event_span(runtime, span, result=result)


def _record_event_delivery(
    runtime: "ObservabilityRuntime",
    backend_name: str,
    adapter_name: "str | None",
    channel: str,
    event: EventMessage,
    mode: str,
) -> None:
    """Record delivery metrics and debug logging for iterated events."""
    runtime.increment_metric("events.deliver")
    log_with_context(
        logger,
        logging.DEBUG,
        "event.receive",
        adapter_name=adapter_name,
        backend_name=backend_name,
        channel=channel,
        event_id=event.event_id,
        event_type=_resolve_event_type(event.payload, event.metadata),
        mode=mode,
    )


class _SyncEventIterator:
    """Explicit sync iterator for event channel consumption."""

    __slots__ = ("_adapter_name", "_backend", "_backend_name", "_channel", "_closed", "_poll_interval", "_runtime")

    def __init__(
        self,
        *,
        backend: "SyncEventBackendProtocol",
        runtime: "ObservabilityRuntime",
        backend_name: str,
        adapter_name: "str | None",
        channel: str,
        poll_interval: float,
    ) -> None:
        self._backend = backend
        self._runtime = runtime
        self._backend_name = backend_name
        self._adapter_name = adapter_name
        self._channel = channel
        self._poll_interval = poll_interval
        self._closed = False

    def __iter__(self) -> Iterator[EventMessage]:
        """Return the iterator."""
        return self

    def __next__(self) -> EventMessage:
        """Return the next available event."""
        if self._closed:
            raise StopIteration
        while True:
            span = _start_event_span(
                self._runtime, "dequeue", self._backend_name, self._adapter_name, self._channel, mode="sync"
            )
            try:
                event = self._backend.dequeue(self._channel, self._poll_interval)
            except Exception as error:
                _end_event_span(self._runtime, span, error=error)
                raise
            if event is None:
                _end_event_span(self._runtime, span, result="empty")
                continue
            _end_event_span(self._runtime, span, result="delivered")
            _record_event_delivery(
                self._runtime, self._backend_name, self._adapter_name, self._channel, event, mode="sync"
            )
            return event

    def close(self) -> None:
        """Close the iterator."""
        self._closed = True


class _AsyncEventIterator:
    """Explicit async iterator for event channel consumption."""

    __slots__ = ("_adapter_name", "_backend", "_backend_name", "_channel", "_closed", "_poll_interval", "_runtime")

    def __init__(
        self,
        *,
        backend: "AsyncEventBackendProtocol",
        runtime: "ObservabilityRuntime",
        backend_name: str,
        adapter_name: "str | None",
        channel: str,
        poll_interval: float,
    ) -> None:
        self._backend = backend
        self._runtime = runtime
        self._backend_name = backend_name
        self._adapter_name = adapter_name
        self._channel = channel
        self._poll_interval = poll_interval
        self._closed = False

    def __aiter__(self) -> AsyncIterator[EventMessage]:
        """Return the async iterator."""
        return self

    async def __anext__(self) -> EventMessage:
        """Return the next available event."""
        if self._closed:
            raise StopAsyncIteration
        while True:
            span = _start_event_span(
                self._runtime, "dequeue", self._backend_name, self._adapter_name, self._channel, mode="async"
            )
            try:
                event = await self._backend.dequeue(self._channel, self._poll_interval)
            except Exception as error:
                _end_event_span(self._runtime, span, error=error)
                raise
            if event is None:
                _end_event_span(self._runtime, span, result="empty")
                continue
            _end_event_span(self._runtime, span, result="delivered")
            _record_event_delivery(
                self._runtime, self._backend_name, self._adapter_name, self._channel, event, mode="async"
            )
            return event

    async def aclose(self) -> None:
        """Close the async iterator."""
        self._closed = True


class SyncEventChannel:
    """Event channel for synchronous database configurations."""

    __slots__ = (
        "_adapter_name",
        "_backend",
        "_backend_name",
        "_config",
        "_event_poll_interval",
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
        self._event_poll_interval = resolve_event_poll_interval(
            extension_settings.get("event_poll_interval"), extension_settings.get("poll_interval"), hints.poll_interval
        )
        self._poll_interval_default = self._event_poll_interval
        backend, backend_label = _resolve_event_backend(
            config, extension_settings, self._adapter_name, hints, protocol_type=SyncEventBackendProtocol
        )
        self._backend = cast("SyncEventBackendProtocol", backend)
        self._config = config
        self._backend_name = backend_label
        self._runtime = config.get_observability_runtime()
        self._runtime.record_metric("events.poll.interval", self._event_poll_interval)
        self._runtime.increment_metric(f"events.backend.{self._backend_name}.resolved")
        log_with_context(
            logger,
            logging.DEBUG,
            "event.configure",
            adapter_name=self._adapter_name,
            backend_name=self._backend_name,
            event_poll_interval=self._event_poll_interval,
        )
        self._listeners: dict[str, SyncEventListener] = {}

    def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        """Publish an event to a channel."""
        channel = normalize_event_channel_name(channel)
        if not self._backend.supports_sync:
            msg = "Current events backend does not support sync publishing"
            raise ImproperConfigurationError(msg)
        with _event_span(
            self._runtime, "publish", self._backend_name, self._adapter_name, channel, mode="sync", result="published"
        ):
            event_id = self._backend.publish(channel, payload, metadata)
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

    def publish_many(self, events: "Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]") -> list[str]:
        """Publish independent events in one grouped operation when supported.

        Backend-native implementations are atomic per grouped call. A backend
        without ``publish_many`` uses an ordered single-event fallback, which is
        not atomic across the full batch.
        """
        normalized = [
            (normalize_event_channel_name(channel), payload, metadata) for channel, payload, metadata in events
        ]
        if not normalized:
            return []
        if not self._backend.supports_sync:
            msg = "Current events backend does not support sync publishing"
            raise ImproperConfigurationError(msg)
        started_at = perf_counter()
        with _event_span(
            self._runtime, "publish_many", self._backend_name, self._adapter_name, mode="sync", result="published"
        ):
            publish_many = getattr(cast("Any", self._backend), "publish_many", None)
            if publish_many is None:
                self._runtime.increment_metric("events.publish.batch_fallback")
                event_ids = [
                    self._backend.publish(channel, payload, metadata) for channel, payload, metadata in normalized
                ]
            else:
                event_ids = cast("list[str]", publish_many(normalized))
        self._runtime.increment_metric("events.publish.batch")
        self._runtime.increment_metric("events.publish.batch_size", len(normalized))
        self._runtime.record_metric("events.publish.batch_latency_ms", (perf_counter() - started_at) * 1000)
        log_with_context(
            logger,
            logging.DEBUG,
            "event.publish_batch",
            adapter_name=self._adapter_name,
            backend_name=self._backend_name,
            batch_size=len(normalized),
            mode="sync",
        )
        return event_ids

    def iter_events(
        self, channel: str, *, event_poll_interval: float | None = None, poll_interval: float | None = None
    ) -> Iterator[EventMessage]:
        """Yield events as they become available."""
        channel = normalize_event_channel_name(channel)
        if not self._backend.supports_sync:
            msg = "Current events backend does not support sync consumption"
            raise ImproperConfigurationError(msg)
        interval = resolve_event_poll_interval(event_poll_interval, poll_interval, self._event_poll_interval)
        return _SyncEventIterator(
            backend=self._backend,
            runtime=self._runtime,
            backend_name=self._backend_name,
            adapter_name=self._adapter_name,
            channel=channel,
            poll_interval=interval,
        )

    def listen(
        self,
        channel: str,
        handler: "SyncEventHandler",
        *,
        event_poll_interval: float | None = None,
        poll_interval: float | None = None,
        auto_ack: bool = True,
    ) -> SyncEventListener:
        """Start a background thread that invokes handler for each event."""
        channel = normalize_event_channel_name(channel)
        if not self._backend.supports_sync:
            msg = "Current events backend does not support sync listeners"
            raise ImproperConfigurationError(msg)
        interval = resolve_event_poll_interval(event_poll_interval, poll_interval, self._event_poll_interval)
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
        with _event_span(self._runtime, "ack", self._backend_name, self._adapter_name, mode="sync", result="acked"):
            self._backend.ack(event_id)

    def nack(self, event_id: str) -> None:
        """Return an event to the queue for redelivery."""
        if not self._backend.supports_sync:
            msg = "Current events backend does not support sync nack"
            raise ImproperConfigurationError(msg)
        with _event_span(self._runtime, "nack", self._backend_name, self._adapter_name, mode="sync", result="nacked"):
            self._backend.nack(event_id)

    def shutdown(self) -> None:
        """Shutdown the event channel and release backend resources."""
        started_at = perf_counter()
        span = _start_event_span(self._runtime, "shutdown", self._backend_name, self._adapter_name, mode="sync")
        listeners = list(self._listeners.values())
        self._listeners.clear()
        for listener in listeners:
            listener.stop_event.set()
        try:
            self._backend.shutdown()
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        finally:
            deadline = started_at + _LISTENER_SHUTDOWN_TIMEOUT
            for listener in listeners:
                listener.thread.join(timeout=max(deadline - perf_counter(), 0.0))
            self._runtime.record_metric("events.shutdown.duration_ms", (perf_counter() - started_at) * 1000)
        self._runtime.increment_metric("events.listener.stop", len(listeners))
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
        "_event_poll_interval",
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
        self._event_poll_interval = resolve_event_poll_interval(
            extension_settings.get("event_poll_interval"), extension_settings.get("poll_interval"), hints.poll_interval
        )
        self._poll_interval_default = self._event_poll_interval
        backend, backend_label = _resolve_event_backend(
            config, extension_settings, self._adapter_name, hints, protocol_type=AsyncEventBackendProtocol
        )
        self._backend = cast("AsyncEventBackendProtocol", backend)
        self._config = config
        self._backend_name = backend_label
        self._runtime = config.get_observability_runtime()
        self._runtime.record_metric("events.poll.interval", self._event_poll_interval)
        self._runtime.increment_metric(f"events.backend.{self._backend_name}.resolved")
        log_with_context(
            logger,
            logging.DEBUG,
            "event.configure",
            adapter_name=self._adapter_name,
            backend_name=self._backend_name,
            event_poll_interval=self._event_poll_interval,
        )
        self._listeners: dict[str, AsyncEventListener] = {}

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        """Publish an event to a channel."""
        channel = normalize_event_channel_name(channel)
        if not self._backend.supports_async:
            msg = "Current events backend does not support async publishing"
            raise ImproperConfigurationError(msg)
        with _event_span(
            self._runtime, "publish", self._backend_name, self._adapter_name, channel, mode="async", result="published"
        ):
            event_id = await self._backend.publish(channel, payload, metadata)
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

    async def publish_many(self, events: "Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]") -> list[str]:
        """Publish independent events in one grouped operation when supported.

        Backend-native implementations are atomic per grouped call. A backend
        without ``publish_many`` uses an ordered single-event fallback, which is
        not atomic across the full batch.
        """
        normalized = [
            (normalize_event_channel_name(channel), payload, metadata) for channel, payload, metadata in events
        ]
        if not normalized:
            return []
        if not self._backend.supports_async:
            msg = "Current events backend does not support async publishing"
            raise ImproperConfigurationError(msg)
        started_at = perf_counter()
        with _event_span(
            self._runtime, "publish_many", self._backend_name, self._adapter_name, mode="async", result="published"
        ):
            publish_many = getattr(cast("Any", self._backend), "publish_many", None)
            if publish_many is None:
                self._runtime.increment_metric("events.publish.batch_fallback")
                event_ids = [
                    await self._backend.publish(channel, payload, metadata) for channel, payload, metadata in normalized
                ]
            else:
                event_ids = cast("list[str]", await publish_many(normalized))
        self._runtime.increment_metric("events.publish.batch")
        self._runtime.increment_metric("events.publish.batch_size", len(normalized))
        self._runtime.record_metric("events.publish.batch_latency_ms", (perf_counter() - started_at) * 1000)
        log_with_context(
            logger,
            logging.DEBUG,
            "event.publish_batch",
            adapter_name=self._adapter_name,
            backend_name=self._backend_name,
            batch_size=len(normalized),
            mode="async",
        )
        return event_ids

    def iter_events(
        self, channel: str, *, event_poll_interval: float | None = None, poll_interval: float | None = None
    ) -> AsyncIterator[EventMessage]:
        """Yield events as they become available."""
        channel = normalize_event_channel_name(channel)
        if not self._backend.supports_async:
            msg = "Current events backend does not support async consumption"
            raise ImproperConfigurationError(msg)
        interval = resolve_event_poll_interval(event_poll_interval, poll_interval, self._event_poll_interval)
        return _AsyncEventIterator(
            backend=self._backend,
            runtime=self._runtime,
            backend_name=self._backend_name,
            adapter_name=self._adapter_name,
            channel=channel,
            poll_interval=interval,
        )

    def listen(
        self,
        channel: str,
        handler: "AsyncEventHandler | SyncEventHandler",
        *,
        event_poll_interval: float | None = None,
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
        interval = resolve_event_poll_interval(event_poll_interval, poll_interval, self._event_poll_interval)
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
        with _event_span(self._runtime, "ack", self._backend_name, self._adapter_name, mode="async", result="acked"):
            await self._backend.ack(event_id)

    async def nack(self, event_id: str) -> None:
        """Return an event to the queue for redelivery."""
        if not self._backend.supports_async:
            msg = "Current events backend does not support async nack"
            raise ImproperConfigurationError(msg)
        with _event_span(self._runtime, "nack", self._backend_name, self._adapter_name, mode="async", result="nacked"):
            await self._backend.nack(event_id)

    async def shutdown(self) -> None:
        """Shutdown the event channel and release backend resources."""
        started_at = perf_counter()
        span = _start_event_span(self._runtime, "shutdown", self._backend_name, self._adapter_name, mode="async")
        listeners = list(self._listeners.values())
        self._listeners.clear()
        try:
            await asyncio.gather(*(listener.stop() for listener in listeners))
            await self._backend.shutdown()
        except Exception as error:
            _end_event_span(self._runtime, span, error=error)
            raise
        finally:
            self._runtime.record_metric("events.shutdown.duration_ms", (perf_counter() - started_at) * 1000)
        self._runtime.increment_metric("events.listener.stop", len(listeners))
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
