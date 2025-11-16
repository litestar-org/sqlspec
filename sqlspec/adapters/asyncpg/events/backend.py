"""Native PostgreSQL LISTEN/NOTIFY backend for EventChannel."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.core import SQL
from sqlspec.exceptions import EventChannelError, ImproperConfigurationError
from sqlspec.extensions.events import EventMessage
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig

logger = get_logger("events.postgres")

__all__ = ("AsyncpgEventsBackend", "create_event_backend")

MAX_NOTIFY_BYTES = 8000


class AsyncpgEventsBackend:
    """Async backend that relies on PostgreSQL LISTEN/NOTIFY primitives."""

    supports_sync = False
    supports_async = True
    backend_name = "native_postgres"

    def __init__(self, config: AsyncpgConfig) -> None:
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
    config: AsyncpgConfig, backend_name: str, _extension_settings: dict[str, Any]
) -> AsyncpgEventsBackend | None:
    """Factory used by EventChannel to create the native backend."""

    if backend_name != "native_postgres":
        return None
    try:
        return AsyncpgEventsBackend(config)
    except ImproperConfigurationError:
        return None
