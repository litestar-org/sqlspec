# pyright: reportPrivateUsage=false
"""Native and hybrid PostgreSQL backends for EventChannel."""

import asyncio
import contextlib
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlspec.core import SQL
from sqlspec.exceptions import EventChannelError, ImproperConfigurationError
from sqlspec.extensions.events import (
    AsyncTableEventQueue,
    EventMessage,
    build_queue_backend,
    decode_notify_payload,
    encode_notify_payload,
    normalize_event_channel_name,
)
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_guards import has_add_listener, has_notifies, is_notification
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig

logger = get_logger("sqlspec.events.postgres")

__all__ = ("AsyncpgEventsBackend", "AsyncpgHybridEventsBackend", "create_event_backend")


class _AsyncpgNotificationListener:
    __slots__ = ("_channel", "_future", "_loop")

    def __init__(self, channel: str, future: "asyncio.Future[str]", loop: "asyncio.AbstractEventLoop") -> None:
        self._channel = channel
        self._future = future
        self._loop = loop

    def __call__(self, _conn: Any, _pid: int, notified_channel: str, payload: str) -> None:
        if notified_channel != self._channel or self._future.done():
            return
        self._loop.call_soon_threadsafe(self._future.set_result, payload)


class AsyncpgHybridEventsBackend:
    """Hybrid backend combining durable queue with LISTEN/NOTIFY wakeups."""

    __slots__ = ("_config", "_listen_connection", "_listen_connection_cm", "_queue", "_runtime")

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify_durable"

    def __init__(self, config: "AsyncpgConfig", queue: "AsyncTableEventQueue") -> None:
        if not config.is_async:
            msg = "Asyncpg hybrid backend requires an async adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()
        self._queue = queue
        self._listen_connection: Any | None = None
        self._listen_connection_cm: Any | None = None
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name="asyncpg",
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
        connection = await self._ensure_listener(channel)
        if has_notifies(connection):
            message = await self._dequeue_with_notifies(connection, channel, poll_interval)
        else:
            message = await self._queue.dequeue(channel, poll_interval)
        return message

    async def ack(self, event_id: str) -> None:
        await self._queue.ack(event_id)
        self._runtime.increment_metric("events.ack")

    async def nack(self, event_id: str) -> None:
        await self._queue.nack(event_id)
        self._runtime.increment_metric("events.nack")

    async def shutdown(self) -> None:
        """Shutdown the listener connection and release resources."""
        if self._listen_connection_cm is not None:
            with contextlib.suppress(Exception):
                await self._listen_connection_cm.__aexit__(None, None, None)
            self._listen_connection = None
            self._listen_connection_cm = None

    async def _ensure_listener(self, channel: str) -> Any:
        """Ensure a dedicated connection is listening on the given channel."""
        if self._listen_connection is None:
            validated_channel = normalize_event_channel_name(channel)
            self._listen_connection_cm = self._config.provide_connection()
            self._listen_connection = await self._listen_connection_cm.__aenter__()
            if self._listen_connection is not None:
                await self._listen_connection.execute(f"LISTEN {validated_channel}")
        return self._listen_connection

    async def _publish_durable(
        self, channel: str, event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None"
    ) -> None:
        """Insert event into durable queue and send NOTIFY wakeup signal."""
        now = datetime.now(timezone.utc)
        async with self._config.provide_session() as driver:
            await driver.execute(
                SQL(
                    self._queue._upsert_sql,
                    {
                        "event_id": event_id,
                        "channel": channel,
                        "payload_json": to_json(payload),
                        "metadata_json": to_json(metadata) if metadata else None,
                        "status": "pending",
                        "available_at": now,
                        "lease_expires_at": None,
                        "attempts": 0,
                        "created_at": now,
                    },
                    statement_config=self._queue._statement_config,
                )
            )
            await driver.execute(SQL("SELECT pg_notify($1, $2)", channel, to_json({"event_id": event_id})))
            await driver.commit()

    async def _dequeue_with_notifies(self, connection: Any, channel: str, poll_interval: float) -> EventMessage | None:
        """Wait for a NOTIFY wakeup then dequeue from the durable table."""
        try:
            notify = await asyncio.wait_for(connection.notifies.get(), timeout=poll_interval)
        except asyncio.TimeoutError:
            return None
        notify_payload = notify.payload if is_notification(notify) else None
        if notify_payload:
            return await self._queue.dequeue(channel)
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
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name="asyncpg",
            backend_name=self.backend_name,
            mode="async",
            status="backend_ready",
        )

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        async with self._config.provide_session() as driver:
            await driver.execute(
                SQL("SELECT pg_notify($1, $2)", channel, encode_notify_payload(event_id, payload, metadata))
            )
            await driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        connection = await self._ensure_listener(channel)
        match self._notify_mode:
            case "add_listener":
                return await self._dequeue_with_listener(connection, channel, poll_interval)
            case "notifies":
                return await self._dequeue_with_notifies(connection, channel, poll_interval)
            case _:
                msg = "PostgreSQL connection does not support LISTEN/NOTIFY callbacks"
                raise EventChannelError(msg)

    async def ack(self, _event_id: str) -> None:
        """Acknowledge an event. Native notifications are fire-and-forget."""
        self._runtime.increment_metric("events.ack")

    async def nack(self, _event_id: str) -> None:
        """Return an event to the queue (no-op for native LISTEN/NOTIFY)."""

    async def shutdown(self) -> None:
        """Shutdown the listener connection and release resources."""
        if self._listen_connection_cm is not None:
            with contextlib.suppress(Exception):
                await self._listen_connection_cm.__aexit__(None, None, None)
            self._listen_connection = None
            self._listen_connection_cm = None
            self._notify_mode = None

    async def _ensure_listener(self, channel: str) -> Any:
        """Ensure a dedicated connection is listening and detect notify mode."""
        if self._listen_connection is None:
            validated_channel = normalize_event_channel_name(channel)
            self._listen_connection_cm = self._config.provide_connection()
            self._listen_connection = await self._listen_connection_cm.__aenter__()
            if self._listen_connection is not None and has_add_listener(self._listen_connection):
                self._notify_mode = "add_listener"
            elif self._listen_connection is not None and has_notifies(self._listen_connection):
                self._notify_mode = "notifies"
                if self._listen_connection is not None:
                    await self._listen_connection.execute(f"LISTEN {validated_channel}")
            else:
                msg = "PostgreSQL connection does not support LISTEN/NOTIFY callbacks"
                raise EventChannelError(msg)
        return self._listen_connection

    async def _dequeue_with_listener(self, connection: Any, channel: str, poll_interval: float) -> EventMessage | None:
        """Wait for notification using add_listener callback API."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        listener = _AsyncpgNotificationListener(channel, future, loop)

        await connection.add_listener(channel, listener)
        try:
            payload_str = await asyncio.wait_for(future, timeout=poll_interval)
        except asyncio.TimeoutError:
            return None
        finally:
            with contextlib.suppress(Exception):
                await connection.remove_listener(channel, listener)
        return decode_notify_payload(channel, payload_str)

    async def _dequeue_with_notifies(self, connection: Any, channel: str, poll_interval: float) -> EventMessage | None:
        """Wait for notification using notifies queue API."""
        try:
            notify = await asyncio.wait_for(connection.notifies.get(), timeout=poll_interval)
        except asyncio.TimeoutError:
            return None
        notify_channel = notify.channel if is_notification(notify) else None
        if notify_channel != channel:
            return None
        return decode_notify_payload(channel, notify.payload)


def create_event_backend(
    config: "AsyncpgConfig", backend_name: str, extension_settings: "dict[str, Any]"
) -> AsyncpgEventsBackend | AsyncpgHybridEventsBackend | None:
    """Factory used by EventChannel to create the native backend."""
    match backend_name:
        case "listen_notify":
            try:
                return AsyncpgEventsBackend(config)
            except ImproperConfigurationError:
                return None
        case "listen_notify_durable":
            queue_backend = cast(
                "AsyncTableEventQueue", build_queue_backend(config, extension_settings, adapter_name="asyncpg")
            )
            try:
                return AsyncpgHybridEventsBackend(config, queue_backend)
            except ImproperConfigurationError:
                return None
        case _:
            return None
