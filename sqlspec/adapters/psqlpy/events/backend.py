"""Psqlpy LISTEN/NOTIFY and hybrid event backends."""

import asyncio
import contextlib
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.core import SQL
from sqlspec.exceptions import EventChannelError, ImproperConfigurationError
from sqlspec.extensions.events import EventMessage
from sqlspec.extensions.events._queue import QueueEventBackend, TableEventQueue
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from psqlpy import Listener

    from sqlspec.adapters.psqlpy.config import PsqlpyConfig


logger = get_logger("events.psqlpy")

__all__ = ("PsqlpyEventsBackend", "PsqlpyHybridEventsBackend", "create_event_backend")


MAX_NOTIFY_BYTES = 8000


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

    async def publish_async(
        self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None
    ) -> str:
        event_id = uuid.uuid4().hex
        envelope = self._encode_payload(event_id, payload, metadata)
        if len(envelope.encode("utf-8")) > MAX_NOTIFY_BYTES:
            msg = "PostgreSQL NOTIFY payload exceeds 8 KB limit"
            raise EventChannelError(msg)
        async with self._config.provide_session() as driver:
            await driver.execute_script(SQL("SELECT pg_notify($1, $2)", channel, envelope))
            await driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def publish_sync(self, *_: Any, **__: Any) -> str:
        msg = "publish_sync is not supported for async-only Psqlpy backend"
        raise ImproperConfigurationError(msg)

    async def dequeue_async(self, channel: str, poll_interval: float) -> EventMessage | None:
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

        if received_payload is not None:
            return self._decode_payload(channel, received_payload)
        return None

    def dequeue_sync(self, *_: Any, **__: Any) -> EventMessage | None:
        msg = "dequeue_sync is not supported for async-only Psqlpy backend"
        raise ImproperConfigurationError(msg)

    async def ack_async(self, _event_id: str) -> None:
        self._runtime.increment_metric("events.ack")

    def ack_sync(self, _event_id: str) -> None:
        msg = "ack_sync is not supported for async-only Psqlpy backend"
        raise ImproperConfigurationError(msg)

    async def _ensure_listener(self, channel: str) -> "Listener":
        """Ensure a listener is created for receiving notifications.

        Args:
            channel: The channel name (used for context but subscription
                happens via add_callback in the calling method).

        Returns:
            The initialized psqlpy Listener instance.
        """
        if self._listener is None:
            pool = await self._config.provide_pool()
            self._listener = pool.listener()
            await self._listener.startup()
        return self._listener

    async def shutdown_async(self) -> None:
        """Shutdown the listener and release resources."""
        if self._listener is not None:
            if self._listener_started:
                self._listener.abort_listen()
                self._listener_started = False
            await self._listener.shutdown()
            self._listener = None

    @staticmethod
    def _encode_payload(event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None") -> str:
        return to_json({
            "event_id": event_id,
            "payload": payload,
            "metadata": metadata,
            "published_at": datetime.now(timezone.utc).isoformat(),
        })

    @staticmethod
    def _decode_payload(channel: str, payload: str) -> EventMessage:
        data = from_json(payload)
        if not isinstance(data, dict):
            data = {"payload": data}
        event_id = data.get("event_id", uuid.uuid4().hex)
        payload_obj = data.get("payload")
        if not isinstance(payload_obj, dict):
            payload_obj = {"value": payload_obj}
        metadata_obj = data.get("metadata")
        if not (metadata_obj is None or isinstance(metadata_obj, dict)):
            metadata_obj = {"value": metadata_obj}
        published_at = data.get("published_at")
        timestamp = PsqlpyEventsBackend._parse_timestamp(published_at)
        return EventMessage(
            event_id=event_id,
            channel=channel,
            payload=payload_obj,
            metadata=metadata_obj,
            attempts=0,
            available_at=timestamp,
            lease_expires_at=None,
            created_at=timestamp,
        )

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        """Parse a timestamp value into a timezone-aware datetime.

        Handles ISO format strings, datetime objects, and falls back to
        current UTC time for invalid or missing values.
        """
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            with contextlib.suppress(ValueError):
                parsed = datetime.fromisoformat(value)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed
        return datetime.now(timezone.utc)


class PsqlpyHybridEventsBackend:
    """Durable hybrid backend combining queue storage with LISTEN/NOTIFY wakeups.

    Uses psqlpy's Listener API for real-time notifications while persisting
    events to a durable queue table.
    """

    __slots__ = ("_config", "_listener", "_listener_started", "_queue", "_runtime")

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify_durable"

    def __init__(self, config: "PsqlpyConfig", queue_backend: "QueueEventBackend") -> None:
        if "psqlpy" not in type(config).__module__:
            msg = "Psqlpy hybrid backend requires a Psqlpy adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._queue = queue_backend
        self._runtime = config.get_observability_runtime()
        self._listener: Any | None = None
        self._listener_started: bool = False

    async def publish_async(
        self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None
    ) -> str:
        event_id = uuid.uuid4().hex
        await self._publish_durable_async(channel, event_id, payload, metadata)
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def publish_sync(self, *_: Any, **__: Any) -> str:
        msg = "publish_sync is not supported for async-only Psqlpy backend"
        raise ImproperConfigurationError(msg)

    async def dequeue_async(self, channel: str, poll_interval: float) -> EventMessage | None:
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
        return await self._queue.dequeue_async(channel, poll_interval=poll_interval)

    def dequeue_sync(self, *_: Any, **__: Any) -> EventMessage | None:
        msg = "dequeue_sync is not supported for async-only Psqlpy backend"
        raise ImproperConfigurationError(msg)

    async def ack_async(self, event_id: str) -> None:
        await self._queue.ack_async(event_id)
        self._runtime.increment_metric("events.ack")

    async def nack_async(self, event_id: str) -> None:
        await self._queue.nack_async(event_id)
        self._runtime.increment_metric("events.nack")

    def ack_sync(self, _event_id: str) -> None:
        msg = "ack_sync is not supported for async-only Psqlpy backend"
        raise ImproperConfigurationError(msg)

    async def _ensure_listener(self, channel: str) -> "Listener":
        """Ensure a listener is created for receiving notifications.

        Args:
            channel: The channel name (used for context but subscription
                happens via add_callback in the calling method).

        Returns:
            The initialized psqlpy Listener instance.
        """
        if self._listener is None:
            pool = await self._config.provide_pool()
            self._listener = pool.listener()
            await self._listener.startup()
        return self._listener

    async def shutdown_async(self) -> None:
        """Shutdown the listener and release resources."""
        if self._listener is not None:
            if self._listener_started:
                self._listener.abort_listen()
                self._listener_started = False
            await self._listener.shutdown()
            self._listener = None

    async def _publish_durable_async(
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
    config: "PsqlpyConfig", backend_name: str, extension_settings: dict[str, Any]
) -> PsqlpyEventsBackend | PsqlpyHybridEventsBackend | None:
    """Factory used by EventChannel to create the native psqlpy backend."""
    if backend_name == "listen_notify":
        try:
            return PsqlpyEventsBackend(config)
        except ImproperConfigurationError:
            return None
    if backend_name == "listen_notify_durable":
        queue = TableEventQueue(
            config,
            queue_table=extension_settings.get("queue_table"),
            lease_seconds=extension_settings.get("lease_seconds"),
            retention_seconds=extension_settings.get("retention_seconds"),
            select_for_update=extension_settings.get("select_for_update"),
            skip_locked=extension_settings.get("skip_locked"),
            json_passthrough=extension_settings.get("json_passthrough"),
        )
        queue_backend = QueueEventBackend(queue)
        try:
            return PsqlpyHybridEventsBackend(config, queue_backend)
        except ImproperConfigurationError:
            return None
    return None
