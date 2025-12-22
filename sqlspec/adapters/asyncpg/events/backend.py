# pyright: reportPrivateUsage=false
"""Native and hybrid PostgreSQL backends for EventChannel."""

import asyncio
import contextlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.core import SQL
from sqlspec.exceptions import EventChannelError, ImproperConfigurationError
from sqlspec.extensions.events import EventMessage
from sqlspec.extensions.events._queue import QueueEventBackend, TableEventQueue, build_queue_backend
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig

logger = get_logger("events.postgres")

__all__ = ("AsyncpgEventsBackend", "AsyncpgHybridEventsBackend", "create_event_backend")

MAX_NOTIFY_BYTES = 8000


class AsyncpgHybridEventsBackend:
    """Hybrid backend combining durable queue with LISTEN/NOTIFY wakeups."""

    __slots__ = ("_config", "_listen_connection", "_listen_connection_cm", "_queue", "_runtime")

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify_durable"

    def __init__(self, config: "AsyncpgConfig", queue: "QueueEventBackend") -> None:
        if not config.is_async:
            msg = "Asyncpg hybrid backend requires an async adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()
        self._queue = queue
        self._listen_connection: Any | None = None
        self._listen_connection_cm: Any | None = None

    async def publish_async(
        self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None
    ) -> str:
        event_id = uuid4().hex
        await self._publish_durable(channel, event_id, payload, metadata)
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def dequeue_async(self, channel: str, poll_interval: float) -> EventMessage | None:
        connection = await self._ensure_listener(channel)
        notifies_queue = getattr(connection, "notifies", None)
        if notifies_queue is not None:
            message = await self._dequeue_with_notifies(connection, channel, poll_interval)
        else:
            message = await self._queue.dequeue_async(channel, poll_interval=poll_interval)
        return message

    async def _ensure_listener(self, channel: str) -> Any:
        """Ensure a dedicated connection is listening on the given channel.

        Creates and caches a connection for LISTEN operations. The connection
        is reused across dequeue calls to maintain the subscription.
        """
        if self._listen_connection is None:
            self._listen_connection_cm = self._config.provide_connection()
            self._listen_connection = await self._listen_connection_cm.__aenter__()
            if self._listen_connection is not None:
                await self._listen_connection.execute(f"LISTEN {channel}")
        return self._listen_connection

    async def ack_async(self, event_id: str) -> None:
        await self._queue.ack_async(event_id)
        self._runtime.increment_metric("events.ack")

    async def nack_async(self, event_id: str) -> None:
        await self._queue.nack_async(event_id)
        self._runtime.increment_metric("events.nack")

    def publish_sync(self, *_: Any, **__: Any) -> str:
        msg = "publish_sync is not supported for async-only Postgres backend"
        raise ImproperConfigurationError(msg)

    def dequeue_sync(self, *_: Any, **__: Any) -> EventMessage | None:
        msg = "dequeue_sync is not supported for async-only Postgres backend"
        raise ImproperConfigurationError(msg)

    def ack_sync(self, _event_id: str) -> None:
        msg = "ack_sync is not supported for async-only backend"
        raise ImproperConfigurationError(msg)

    async def shutdown_async(self) -> None:
        """Shutdown the listener connection and release resources."""
        if self._listen_connection_cm is not None:
            with contextlib.suppress(Exception):
                await self._listen_connection_cm.__aexit__(None, None, None)
            self._listen_connection = None
            self._listen_connection_cm = None

    async def _publish_durable(
        self, channel: str, event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None"
    ) -> None:
        """Insert event into durable queue and send NOTIFY wakeup signal."""
        now = datetime.now(timezone.utc)
        queue_backend = self._get_queue_backend()
        statement_config = queue_backend.statement_config
        async with self._config.provide_session() as driver:
            await driver.execute(
                SQL(
                    queue_backend._upsert_sql,
                    {
                        "event_id": event_id,
                        "channel": channel,
                        "payload_json": queue_backend._encode_json(payload),
                        "metadata_json": queue_backend._encode_json(metadata),
                        "status": "pending",
                        "available_at": now,
                        "lease_expires_at": None,
                        "attempts": 0,
                        "created_at": now,
                    },
                    statement_config=statement_config,
                )
            )
            await driver.execute(SQL("SELECT pg_notify($1, $2)", channel, to_json({"event_id": event_id})))
            await driver.commit()

    def _get_queue_backend(self) -> "TableEventQueue":
        """Return the underlying TableEventQueue from the wrapper.

        Raises:
            EventChannelError: If the queue backend reference is missing.
        """
        queue_backend = self._queue._queue if hasattr(self._queue, "_queue") else None
        if queue_backend is None:
            msg = "Hybrid queue backend missing queue reference"
            raise EventChannelError(msg)
        return queue_backend

    async def _dequeue_with_notifies(self, connection: Any, channel: str, poll_interval: float) -> EventMessage | None:
        """Wait for a NOTIFY wakeup then dequeue from the durable table.

        The connection is already listening from _ensure_listener. This method
        waits for a notification signal, then fetches the event from storage.
        """
        try:
            notify = await asyncio.wait_for(connection.notifies.get(), timeout=poll_interval)
        except asyncio.TimeoutError:
            return None
        notify_payload = notify.payload if hasattr(notify, "payload") else None
        if notify_payload:
            return await self._queue.dequeue_async(channel)
        return None


class AsyncpgEventsBackend:
    """Async backend that relies on PostgreSQL LISTEN/NOTIFY primitives.

    This backend uses asyncpg's native LISTEN/NOTIFY support for real-time
    event delivery. Messages are ephemeral and not persisted.
    """

    __slots__ = ("_config", "_listen_connection", "_listen_connection_cm", "_notify_mode", "_runtime")

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify"

    def __init__(self, config: "AsyncpgConfig") -> None:
        if not config.is_async:
            msg = "AsyncpgEventsBackend requires an async adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()
        self._listen_connection: Any | None = None
        self._listen_connection_cm: Any | None = None
        self._notify_mode: str | None = None

    async def publish_async(
        self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None
    ) -> str:
        event_id = uuid4().hex
        envelope = self._encode_payload(event_id, payload, metadata)
        async with self._config.provide_session() as driver:
            await driver.execute(SQL("SELECT pg_notify($1, $2)", channel, envelope))
            await driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def publish_sync(self, *_: Any, **__: Any) -> str:
        msg = "publish_sync is not supported for async-only Postgres backend"
        raise ImproperConfigurationError(msg)

    async def dequeue_async(self, channel: str, poll_interval: float) -> EventMessage | None:
        connection = await self._ensure_listener(channel)
        if self._notify_mode == "add_listener":
            return await self._dequeue_with_listener(connection, channel, poll_interval)
        if self._notify_mode == "notifies":
            return await self._dequeue_with_notifies(connection, channel, poll_interval)
        msg = "PostgreSQL connection does not support LISTEN/NOTIFY callbacks"
        raise EventChannelError(msg)

    def dequeue_sync(self, *_: Any, **__: Any) -> EventMessage | None:
        msg = "dequeue_sync is not supported for async-only Postgres backend"
        raise ImproperConfigurationError(msg)

    async def ack_async(self, _event_id: str) -> None:
        """Acknowledge an event. Native notifications are fire-and-forget."""
        self._runtime.increment_metric("events.ack")

    def ack_sync(self, _event_id: str) -> None:
        msg = "ack_sync is not supported for async-only backend"
        raise ImproperConfigurationError(msg)

    async def shutdown_async(self) -> None:
        """Shutdown the listener connection and release resources."""
        if self._listen_connection_cm is not None:
            with contextlib.suppress(Exception):
                await self._listen_connection_cm.__aexit__(None, None, None)
            self._listen_connection = None
            self._listen_connection_cm = None
            self._notify_mode = None

    async def _ensure_listener(self, channel: str) -> Any:
        """Ensure a dedicated connection is listening and detect notify mode.

        Creates and caches a connection for LISTEN operations. Also detects
        the appropriate notification mode (add_listener or notifies queue)
        based on available asyncpg connection capabilities.

        Returns:
            The cached asyncpg connection.

        Raises:
            EventChannelError: If the connection lacks LISTEN/NOTIFY support.
        """
        if self._listen_connection is None:
            self._listen_connection_cm = self._config.provide_connection()
            self._listen_connection = await self._listen_connection_cm.__aenter__()
            add_listener = getattr(self._listen_connection, "add_listener", None)
            if add_listener is not None and callable(add_listener):
                self._notify_mode = "add_listener"
            elif getattr(self._listen_connection, "notifies", None) is not None:
                self._notify_mode = "notifies"
                if self._listen_connection is not None:
                    await self._listen_connection.execute(f"LISTEN {channel}")
            else:
                msg = "PostgreSQL connection does not support LISTEN/NOTIFY callbacks"
                raise EventChannelError(msg)
        return self._listen_connection

    async def _dequeue_with_listener(self, connection: Any, channel: str, poll_interval: float) -> EventMessage | None:
        """Wait for notification using add_listener callback API.

        This mode uses asyncpg's callback-based listener which automatically
        manages the LISTEN subscription.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()

        def _listener(_conn: Any, _pid: int, notified_channel: str, payload: str) -> None:
            if notified_channel != channel or future.done():
                return
            loop.call_soon_threadsafe(future.set_result, payload)

        await connection.add_listener(channel, _listener)
        try:
            payload = await asyncio.wait_for(future, timeout=poll_interval)
        except asyncio.TimeoutError:
            return None
        finally:
            with contextlib.suppress(Exception):
                await connection.remove_listener(channel, _listener)
        return self._decode_payload(channel, payload)

    async def _dequeue_with_notifies(self, connection: Any, channel: str, poll_interval: float) -> EventMessage | None:
        """Wait for notification using notifies queue API.

        The connection is already listening from _ensure_listener.
        """
        try:
            notify = await asyncio.wait_for(connection.notifies.get(), timeout=poll_interval)
        except asyncio.TimeoutError:
            return None
        notify_channel = notify.channel if hasattr(notify, "channel") else None
        if notify_channel != channel:
            return None
        return self._decode_payload(channel, notify.payload)

    def _encode_payload(self, event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None") -> str:
        """Encode event data as JSON for NOTIFY payload.

        Raises:
            EventChannelError: If encoded payload exceeds PostgreSQL's 8KB limit.
        """
        envelope = {
            "event_id": event_id,
            "payload": payload,
            "metadata": metadata,
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
        encoded = to_json(envelope)
        if len(encoded.encode("utf-8")) > MAX_NOTIFY_BYTES:
            msg = "PostgreSQL NOTIFY payload exceeds 8 KB limit"
            raise EventChannelError(msg)
        return encoded

    def _decode_payload(self, channel: str, payload: str) -> "EventMessage":
        """Decode JSON payload from NOTIFY into an EventMessage."""
        data = from_json(payload)
        if not isinstance(data, dict):
            data = {"payload": data}
        event_id = data.get("event_id", uuid4().hex)
        payload_obj = data.get("payload")
        if not isinstance(payload_obj, dict):
            payload_obj = {"value": payload_obj}
        metadata_obj = data.get("metadata")
        if not (metadata_obj is None or isinstance(metadata_obj, dict)):
            metadata_obj = {"value": metadata_obj}
        published = data.get("published_at")
        available_at = self._parse_timestamp(published)
        return EventMessage(
            event_id=event_id,
            channel=channel,
            payload=payload_obj,
            metadata=metadata_obj,
            attempts=0,
            available_at=available_at,
            lease_expires_at=None,
            created_at=available_at,
        )

    @staticmethod
    def _parse_timestamp(value: Any) -> "datetime":
        """Parse a timestamp value into a timezone-aware datetime.

        Handles ISO format strings, datetime objects, and falls back to
        current UTC time for invalid or missing values.
        """
        if isinstance(value, str):
            with contextlib.suppress(ValueError):
                parsed = datetime.fromisoformat(value)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc)


def create_event_backend(
    config: "AsyncpgConfig", backend_name: str, extension_settings: "dict[str, Any]"
) -> AsyncpgEventsBackend | AsyncpgHybridEventsBackend | None:
    """Factory used by EventChannel to create the native backend."""

    if backend_name == "listen_notify":
        try:
            return AsyncpgEventsBackend(config)
        except ImproperConfigurationError:
            return None
    if backend_name == "listen_notify_durable":
        queue_backend = build_queue_backend(config, extension_settings, adapter_name="asyncpg")
        try:
            return AsyncpgHybridEventsBackend(config, queue_backend)
        except ImproperConfigurationError:
            return None
    return None
