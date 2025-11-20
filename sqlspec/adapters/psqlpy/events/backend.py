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
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig


logger = get_logger("events.psqlpy")

__all__ = (
    "PsqlpyEventsBackend",
    "PsqlpyHybridEventsBackend",
    "create_event_backend",
)


class PsqlpyEventsBackend:
    """Native LISTEN/NOTIFY backend for psqlpy adapters."""

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify"

    def __init__(self, config: "PsqlpyConfig") -> None:
        if "psqlpy" not in type(config).__module__:
            msg = "Psqlpy events backend requires a Psqlpy adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()
        self._listen_connection_async: Any | None = None
        self._listen_connection_async_cm: Any | None = None

    async def publish_async(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid.uuid4().hex
        envelope = self._encode_payload(event_id, payload, metadata)
        async with self._config.provide_session() as driver:
            await driver.execute_script(SQL("SELECT pg_notify($1, $2)", channel, envelope))
            await driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def publish(self, *_: Any, **__: Any) -> str:
        msg = "publish is not supported for sync Psqlpy backends"
        raise ImproperConfigurationError(msg)

    async def dequeue_async(self, channel: str, poll_interval: float) -> EventMessage | None:
        connection = await self._ensure_async_listener(channel)
        await connection.execute(f"LISTEN {channel}")
        future: asyncio.Future[str] = asyncio.get_running_loop().create_future()

        def _callback(_conn: Any, _pid: int, notified_channel: str, payload: str) -> None:
            if notified_channel != channel or future.done():
                return
            future.set_result(payload)

        connection.add_listener(channel, _callback)
        try:
            try:
                payload = await asyncio.wait_for(future, timeout=poll_interval)
            except asyncio.TimeoutError:
                return None
            return self._decode_payload(channel, payload)
        finally:
            with contextlib.suppress(Exception):
                connection.remove_listener(channel, _callback)

    def dequeue(self, *_: Any, **__: Any) -> EventMessage | None:
        msg = "dequeue is not supported for sync Psqlpy backends"
        raise ImproperConfigurationError(msg)

    async def ack_async(self, _event_id: str) -> None:
        self._runtime.increment_metric("events.ack")

    def ack(self, _event_id: str) -> None:
        msg = "ack is not supported for sync Psqlpy backends"
        raise ImproperConfigurationError(msg)

    async def _ensure_async_listener(self, channel: str) -> Any:
        if self._listen_connection_async is None:
            self._listen_connection_async_cm = self._config.provide_connection()
            self._listen_connection_async = await self._listen_connection_async_cm.__aenter__()
            try:
                await self._listen_connection_async.set_autocommit(True)  # type: ignore[attr-defined]
            except Exception:
                pass
        return self._listen_connection_async

    @staticmethod
    def _encode_payload(event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None") -> str:
        return to_json(
            {
                "event_id": event_id,
                "payload": payload,
                "metadata": metadata,
                "published_at": datetime.now(timezone.utc).isoformat(),
            }
        )

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
    """Durable hybrid backend combining queue storage with LISTEN/NOTIFY wakeups."""

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify_durable"

    def __init__(self, config: "PsqlpyConfig", queue_backend: QueueEventBackend) -> None:
        if "psqlpy" not in type(config).__module__:
            msg = "Psqlpy hybrid backend requires a Psqlpy adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._queue = queue_backend
        self._runtime = config.get_observability_runtime()

    async def publish_async(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid.uuid4().hex
        await self._publish_durable_async(channel, event_id, payload, metadata)
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def publish(self, *_: Any, **__: Any) -> str:
        msg = "publish is not supported for sync Psqlpy backends"
        raise ImproperConfigurationError(msg)

    async def dequeue_async(self, channel: str, poll_interval: float) -> EventMessage | None:
        connection_cm = self._config.provide_connection()
        connection = await connection_cm.__aenter__()
        try:
            listener = getattr(connection, "add_listener", None)
            if listener is None:
                return await self._queue.dequeue_async(channel)
            future: asyncio.Future[str] = asyncio.get_running_loop().create_future()

            def _callback(_conn: Any, _pid: int, notified_channel: str, payload: str) -> None:
                if notified_channel != channel or future.done():
                    return
                future.set_result(payload)

            listener(channel, _callback)
            try:
                await asyncio.wait_for(future, timeout=poll_interval)
            except asyncio.TimeoutError:
                return await self._queue.dequeue_async(channel)
            return await self._queue.dequeue_async(channel)
        finally:
            with contextlib.suppress(Exception):
                remove = getattr(connection, "remove_listener", None)
                if remove:
                    remove(channel, _callback)
                await connection_cm.__aexit__(None, None, None)

    async def ack_async(self, event_id: str) -> None:
        await self._queue.ack_async(event_id)
        self._runtime.increment_metric("events.ack")

    def ack(self, _event_id: str) -> None:
        msg = "ack is not supported for sync Psqlpy backends"
        raise ImproperConfigurationError(msg)

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
                    queue._upsert_sql,  # type: ignore[attr-defined]
                    {
                        "event_id": event_id,
                        "channel": channel,
                        "payload_json": queue._encode_json(payload),  # type: ignore[attr-defined]
                        "metadata_json": queue._encode_json(metadata),  # type: ignore[attr-defined]
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
            json_passthrough=extension_settings.get("json_passthrough", True),
        )
        queue_backend = QueueEventBackend(queue)
        try:
            return PsqlpyHybridEventsBackend(config, queue_backend)
        except ImproperConfigurationError:
            return None
    return None
