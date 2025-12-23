"""Psqlpy LISTEN/NOTIFY and hybrid event backends."""

import asyncio
import contextlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.core import SQL
from sqlspec.exceptions import EventChannelError, ImproperConfigurationError
from sqlspec.extensions.events import EventMessage
from sqlspec.extensions.events._payload import decode_notify_payload, encode_notify_payload
from sqlspec.extensions.events._queue import AsyncQueueEventBackend, build_queue_backend
from sqlspec.extensions.events._store import normalize_event_channel_name as _normalize_channel
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from psqlpy import Listener

    from sqlspec.adapters.psqlpy.config import PsqlpyConfig


logger = get_logger("events.psqlpy")

__all__ = ("PsqlpyEventsBackend", "PsqlpyHybridEventsBackend", "create_event_backend")


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
        received_payload: str | None = None
        event = asyncio.Event()

        async def _callback(_connection: Any, payload: str, notified_channel: str, _process_id: int) -> None:
            nonlocal received_payload
            if notified_channel == channel and received_payload is None:
                received_payload = payload
                event.set()

        await listener.add_callback(channel=channel, callback=_callback)

        if not self._listener_started:
            listener.listen()
            self._listener_started = True
            await asyncio.sleep(0.05)

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(event.wait(), timeout=poll_interval)

        await listener.clear_channel_callbacks(channel=channel)
        return decode_notify_payload(channel, received_payload) if received_payload is not None else None

    async def ack(self, _event_id: str) -> None:
        self._runtime.increment_metric("events.ack")

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

    def __init__(self, config: "PsqlpyConfig", queue_backend: "AsyncQueueEventBackend") -> None:
        if "psqlpy" not in type(config).__module__:
            msg = "Psqlpy hybrid backend requires a Psqlpy adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._queue = queue_backend
        self._runtime = config.get_observability_runtime()
        self._listener: Any | None = None
        self._listener_started: bool = False

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        await self._publish_durable(channel, event_id, payload, metadata)
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        listener = await self._ensure_listener(channel)
        event = asyncio.Event()

        async def _callback(_connection: Any, _payload: str, notified_channel: str, _process_id: int) -> None:
            if notified_channel == channel:
                event.set()

        await listener.add_callback(channel=channel, callback=_callback)

        if not self._listener_started:
            listener.listen()
            self._listener_started = True
            await asyncio.sleep(0.05)

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(event.wait(), timeout=poll_interval)

        await listener.clear_channel_callbacks(channel=channel)
        return await self._queue.dequeue(channel, poll_interval=poll_interval)

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
        queue = getattr(self._queue, "_queue", None)
        if queue is None:
            msg = "Hybrid queue backend missing queue reference"
            raise EventChannelError(msg)
        async with self._config.provide_session() as driver:
            await driver.execute(
                SQL(
                    queue._upsert_sql,
                    {
                        "event_id": event_id,
                        "channel": channel,
                        "payload_json": queue._encode_json(payload),
                        "metadata_json": queue._encode_json(metadata),
                        "status": "pending",
                        "available_at": now,
                        "lease_expires_at": None,
                        "attempts": 0,
                        "created_at": now,
                    },
                    statement_config=queue.statement_config,
                )
            )
            await driver.execute_script(SQL("SELECT pg_notify($1, $2)", channel, to_json({"event_id": event_id})))
            await driver.commit()


def create_event_backend(
    config: "PsqlpyConfig", backend_name: str, extension_settings: "dict[str, Any]"
) -> PsqlpyEventsBackend | PsqlpyHybridEventsBackend | None:
    """Factory used by EventChannel to create the native psqlpy backend."""
    from typing import cast

    match backend_name:
        case "listen_notify":
            try:
                return PsqlpyEventsBackend(config)
            except ImproperConfigurationError:
                return None
        case "listen_notify_durable":
            queue_backend = cast("AsyncQueueEventBackend", build_queue_backend(config, extension_settings, adapter_name="psqlpy"))
            try:
                return PsqlpyHybridEventsBackend(config, queue_backend)
            except ImproperConfigurationError:
                return None
        case _:
            return None
