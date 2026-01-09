"""Psqlpy LISTEN/NOTIFY and hybrid event backends."""

# pyright: ignore=reportPrivateUsage
import asyncio
import contextlib
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlspec.core import SQL
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import (
    AsyncTableEventQueue,
    EventMessage,
    build_queue_backend,
    decode_notify_payload,
    encode_notify_payload,
)
from sqlspec.extensions.events import normalize_event_channel_name as _normalize_channel
from sqlspec.protocols import NotificationProtocol
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.serializers import to_json
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from psqlpy import Listener

    from sqlspec.adapters.psqlpy.config import PsqlpyConfig


logger = get_logger("sqlspec.events.psqlpy")

__all__ = ("PsqlpyEventsBackend", "PsqlpyHybridEventsBackend", "create_event_backend")

_PSQLPY_CALLBACK_MIN_ARGS = 3


class _PsqlpyNotifyCapture:
    __slots__ = ("_channel", "_event", "payload")

    def __init__(self, channel: str, event: asyncio.Event) -> None:
        self._channel = channel
        self._event = event
        self.payload: str | None = None

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
            else:
                return

        if notified_channel == self._channel and payload is not None and self.payload is None:
            self.payload = payload
            self._event.set()


class _PsqlpyNotifySignal:
    __slots__ = ("_channel", "_event")

    def __init__(self, channel: str, event: asyncio.Event) -> None:
        self._channel = channel
        self._event = event

    async def __call__(self, *args: Any) -> None:
        if not args:
            return

        notified_channel: str | None = None
        if len(args) == 1:
            message = args[0]
            if isinstance(message, NotificationProtocol):
                notified_channel = message.channel
        elif len(args) >= _PSQLPY_CALLBACK_MIN_ARGS:
            value1 = cast("str", args[1])
            value2 = cast("str", args[2])
            if value1 == self._channel:
                notified_channel = value1
            elif value2 == self._channel:
                notified_channel = value2

        if notified_channel == self._channel:
            self._event.set()


class PsqlpyEventsBackend:
    """Native LISTEN/NOTIFY backend for psqlpy adapters.

    Uses psqlpy's Listener API which provides a dedicated connection for
    receiving PostgreSQL NOTIFY messages via callbacks or async iteration.
    """

    __slots__ = ("_config", "_listener", "_listener_started", "_runtime")

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify"

    def __init__(self, config: "PsqlpyConfig") -> None:
        if "psqlpy" not in type(config).__module__:
            msg = "Psqlpy events backend requires a Psqlpy adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()
        self._listener: Any | None = None
        self._listener_started: bool = False
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name="psqlpy",
            backend_name=self.backend_name,
            mode="async",
            status="backend_ready",
        )

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        async with self._config.provide_session() as driver:
            await driver.execute_script(
                SQL("SELECT pg_notify($1, $2)", channel, encode_notify_payload(event_id, payload, metadata))
            )
            await driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        listener = await self._ensure_listener(channel)
        event = asyncio.Event()
        callback = _PsqlpyNotifyCapture(channel, event)
        await listener.add_callback(channel=channel, callback=callback.__call__)

        if not self._listener_started:
            listener.listen()
            self._listener_started = True
            await asyncio.sleep(0.05)

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(event.wait(), timeout=poll_interval)

        await listener.clear_channel_callbacks(channel=channel)
        return decode_notify_payload(channel, callback.payload) if callback.payload is not None else None

    async def ack(self, _event_id: str) -> None:
        self._runtime.increment_metric("events.ack")

    async def nack(self, _event_id: str) -> None:
        """Return an event to the queue (no-op for native LISTEN/NOTIFY)."""

    async def shutdown(self) -> None:
        """Shutdown the listener and release resources."""
        if self._listener is not None:
            if self._listener_started:
                self._listener.abort_listen()
                self._listener_started = False
            await self._listener.shutdown()
            self._listener = None

    async def _ensure_listener(self, channel: str) -> "Listener":
        """Ensure a listener is created for receiving notifications."""
        _normalize_channel(channel)
        if self._listener is None:
            pool = await self._config.provide_pool()
            self._listener = pool.listener()
            await self._listener.startup()
        return self._listener


class PsqlpyHybridEventsBackend:
    """Durable hybrid backend combining queue storage with LISTEN/NOTIFY wakeups.

    Uses psqlpy's Listener API for real-time notifications while persisting
    events to a durable queue table.
    """

    __slots__ = ("_config", "_listener", "_listener_started", "_queue", "_runtime")

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify_durable"

    def __init__(self, config: "PsqlpyConfig", queue: "AsyncTableEventQueue") -> None:
        if "psqlpy" not in type(config).__module__:
            msg = "Psqlpy hybrid backend requires a Psqlpy adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._queue = queue
        self._runtime = config.get_observability_runtime()
        self._listener: Any | None = None
        self._listener_started: bool = False
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name="psqlpy",
            backend_name=self.backend_name,
            mode="async",
            status="backend_ready",
        )

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        await self._publish_durable(channel, event_id, payload, metadata)
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        listener = await self._ensure_listener(channel)
        event = asyncio.Event()
        callback = _PsqlpyNotifySignal(channel, event)
        await listener.add_callback(channel=channel, callback=callback.__call__)

        if not self._listener_started:
            listener.listen()
            self._listener_started = True
            await asyncio.sleep(0.05)

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(event.wait(), timeout=poll_interval)

        await listener.clear_channel_callbacks(channel=channel)
        return await self._queue.dequeue(channel, poll_interval)

    async def ack(self, event_id: str) -> None:
        await self._queue.ack(event_id)
        self._runtime.increment_metric("events.ack")

    async def nack(self, event_id: str) -> None:
        await self._queue.nack(event_id)
        self._runtime.increment_metric("events.nack")

    async def shutdown(self) -> None:
        """Shutdown the listener and release resources."""
        if self._listener is not None:
            if self._listener_started:
                self._listener.abort_listen()
                self._listener_started = False
            await self._listener.shutdown()
            self._listener = None

    async def _ensure_listener(self, channel: str) -> "Listener":
        """Ensure a listener is created for receiving notifications."""
        _normalize_channel(channel)
        if self._listener is None:
            pool = await self._config.provide_pool()
            self._listener = pool.listener()
            await self._listener.startup()
        return self._listener

    async def _publish_durable(
        self, channel: str, event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None"
    ) -> None:
        now = datetime.now(timezone.utc)
        async with self._config.provide_session() as driver:
            await driver.execute(
                SQL(
                    self._queue._upsert_sql,  # pyright: ignore[reportPrivateUsage]
                    {
                        "event_id": event_id,
                        "channel": channel,
                        "payload_json": payload,
                        "metadata_json": metadata,
                        "status": "pending",
                        "available_at": now,
                        "lease_expires_at": None,
                        "attempts": 0,
                        "created_at": now,
                    },
                    statement_config=self._queue._statement_config,  # pyright: ignore[reportPrivateUsage]
                )
            )
            await driver.execute_script(SQL("SELECT pg_notify($1, $2)", channel, to_json({"event_id": event_id})))
            await driver.commit()


def create_event_backend(
    config: "PsqlpyConfig", backend_name: str, extension_settings: "dict[str, Any]"
) -> PsqlpyEventsBackend | PsqlpyHybridEventsBackend | None:
    """Factory used by EventChannel to create the native psqlpy backend."""
    match backend_name:
        case "listen_notify":
            try:
                return PsqlpyEventsBackend(config)
            except ImproperConfigurationError:
                return None
        case "listen_notify_durable":
            queue = cast("AsyncTableEventQueue", build_queue_backend(config, extension_settings, adapter_name="psqlpy"))
            try:
                return PsqlpyHybridEventsBackend(config, queue)
            except ImproperConfigurationError:
                return None
        case _:
            return None
