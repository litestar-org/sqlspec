"""Native and hybrid PostgreSQL backends for EventChannel."""

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
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig
    from sqlspec.extensions.events._queue import QueueEventBackend

logger = get_logger("events.postgres")

__all__ = ("AsyncpgEventsBackend", "AsyncpgHybridEventsBackend", "create_event_backend")

MAX_NOTIFY_BYTES = 8000


class AsyncpgHybridEventsBackend:
    """Hybrid backend combining durable queue with LISTEN/NOTIFY wakeups."""

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

    async def publish_async(
        self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None
    ) -> str:
        event_id = uuid.uuid4().hex
        envelope = {
            "event_id": event_id,
            "payload": payload,
            "metadata": metadata,
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
        encoded_payload = to_json(envelope)
        await self._publish_durable(channel, event_id, payload, metadata)
        await self._notify(channel, encoded_payload)
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def dequeue_async(self, channel: str, poll_interval: float) -> EventMessage | None:
        connection_cm = self._config.provide_connection()
        connection = await connection_cm.__aenter__()
        try:
            if hasattr(connection, "notifies"):
                message = await self._dequeue_with_notifies(connection, channel, poll_interval)
            else:
                message = await self._queue.dequeue_async(channel)
        finally:
            with contextlib.suppress(Exception):
                await connection_cm.__aexit__(None, None, None)
        return message

    async def ack_async(self, event_id: str) -> None:
        await self._queue.ack_async(event_id)
        self._runtime.increment_metric("events.ack")

    def publish(self, *_: Any, **__: Any) -> str:
        msg = "publish is not supported for async-only Postgres backend"
        raise ImproperConfigurationError(msg)

    def dequeue(self, *_: Any, **__: Any) -> EventMessage | None:
        msg = "dequeue is not supported for async-only Postgres backend"
        raise ImproperConfigurationError(msg)

    def ack(self, _event_id: str) -> None:
        msg = "ack is not supported for async-only backend"
        raise ImproperConfigurationError(msg)

    async def _publish_durable(
        self, channel: str, event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None"
    ) -> None:
        now = datetime.now(timezone.utc)
        async with self._config.provide_session() as driver:
            queue_backend = getattr(self._queue, "_queue", None)
            if queue_backend is None:
                msg = "Hybrid queue backend missing queue reference"
                raise EventChannelError(msg)
            statement_config = queue_backend.statement_config
            await driver.execute(
                SQL(
                    queue_backend._upsert_sql,  # type: ignore[attr-defined]
                    {
                        "event_id": event_id,
                        "channel": channel,
                        "payload_json": queue_backend._encode_json(payload),  # type: ignore[attr-defined]
                        "metadata_json": queue_backend._encode_json(metadata),  # type: ignore[attr-defined]
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

    async def _dequeue_with_notifies(self, connection: Any, channel: str, poll_interval: float) -> EventMessage | None:
        listen_sql = f"LISTEN {channel}"
        unlisten_sql = f"UNLISTEN {channel}"
        await connection.execute(listen_sql)
        try:
            try:
                notify = await asyncio.wait_for(connection.notifies.get(), timeout=poll_interval)
            except asyncio.TimeoutError:
                return None
            payload = getattr(notify, "payload", None)
            if payload:
                return await self._queue.dequeue_async(channel)
            return None
        finally:
            with contextlib.suppress(Exception):
                await connection.execute(unlisten_sql)


class AsyncpgEventsBackend:
    """Async backend that relies on PostgreSQL LISTEN/NOTIFY primitives."""

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify"

    def __init__(self, config: "AsyncpgConfig") -> None:
        if not config.is_async:
            msg = "AsyncpgEventsBackend requires an async adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()

    async def publish_async(self, channel: str, payload: dict[str, Any], metadata: dict[str, Any] | None = None) -> str:
        event_id = uuid.uuid4().hex
        envelope = self._encode_payload(event_id, payload, metadata)
        async with self._config.provide_session() as driver:
            await driver.execute(SQL("SELECT pg_notify($1, $2)", channel, envelope))
            await driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def publish(self, *_: Any, **__: Any) -> str:
        msg = "publish is not supported for async-only Postgres backend"
        raise ImproperConfigurationError(msg)

    async def dequeue_async(self, channel: str, poll_interval: float) -> EventMessage | None:
        connection_cm = self._config.provide_connection()
        connection = await connection_cm.__aenter__()
        try:
            if hasattr(connection, "add_listener") and callable(connection.add_listener):
                return await self._dequeue_with_listener(connection, channel, poll_interval)
            if hasattr(connection, "notifies"):
                return await self._dequeue_with_notifies(connection, channel, poll_interval)
            msg = "PostgreSQL connection does not support LISTEN/NOTIFY callbacks"
            raise EventChannelError(msg)
        finally:
            with contextlib.suppress(Exception):
                await connection_cm.__aexit__(None, None, None)

    def dequeue(self, *_: Any, **__: Any) -> EventMessage | None:
        msg = "dequeue is not supported for async-only Postgres backend"
        raise ImproperConfigurationError(msg)

    async def ack_async(self, _event_id: str) -> None:
        # Native notifications are fire-and-forget; nothing to acknowledge.
        self._runtime.increment_metric("events.ack")

    def ack(self, _event_id: str) -> None:
        msg = "ack is not supported for async-only backend"
        raise ImproperConfigurationError(msg)

    async def _dequeue_with_listener(self, connection: Any, channel: str, poll_interval: float) -> EventMessage | None:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()

        def _listener(_conn: Any, _pid: int, notified_channel: str, payload: str) -> None:
            if notified_channel != channel or future.done():
                return
            loop.call_soon_threadsafe(future.set_result, payload)

        connection.add_listener(channel, _listener)
        try:
            try:
                payload = await asyncio.wait_for(future, timeout=poll_interval)
            except asyncio.TimeoutError:
                return None
            return self._decode_payload(channel, payload)
        finally:
            with contextlib.suppress(Exception):
                connection.remove_listener(channel, _listener)

    async def _dequeue_with_notifies(self, connection: Any, channel: str, poll_interval: float) -> EventMessage | None:
        listen_sql = f"LISTEN {channel}"
        unlisten_sql = f"UNLISTEN {channel}"
        await connection.execute(listen_sql)
        try:
            try:
                notify = await asyncio.wait_for(connection.notifies.get(), timeout=poll_interval)
            except asyncio.TimeoutError:
                return None
            if getattr(notify, "channel", None) != channel:
                return None
            return self._decode_payload(channel, notify.payload)
        finally:
            with contextlib.suppress(Exception):
                await connection.execute(unlisten_sql)

    def _encode_payload(self, event_id: str, payload: dict[str, Any], metadata: dict[str, Any] | None) -> str:
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

    def _decode_payload(self, channel: str, payload: str) -> EventMessage:
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
    def _parse_timestamp(value: Any) -> datetime:
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
            return AsyncpgHybridEventsBackend(config, queue_backend)
        except ImproperConfigurationError:
            return None
    return None
