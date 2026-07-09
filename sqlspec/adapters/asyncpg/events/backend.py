# pyright: reportPrivateUsage=false
"""Native and hybrid PostgreSQL backends for EventChannel."""

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.asyncpg.events._hub import AsyncpgListenerHub
from sqlspec.core import SQL
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import (
    AsyncTableEventQueue,
    EventMessage,
    build_queue_backend,
    decode_notify_payload,
    encode_notify_payload,
)
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig

__all__ = ("AsyncpgEventsBackend", "AsyncpgHybridEventsBackend", "create_event_backend")

logger = get_logger("sqlspec.events.postgres")


class AsyncpgHybridEventsBackend:
    """Hybrid backend combining durable queue with LISTEN/NOTIFY wakeups."""

    __slots__ = ("_config", "_hub", "_queue", "_runtime")

    supports_sync = False
    supports_async = True
    backend_name = "notify_queue"

    def __init__(self, config: "AsyncpgConfig", queue: "AsyncTableEventQueue") -> None:
        if not config.is_async:
            msg = "Asyncpg hybrid backend requires an async adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()
        self._queue = queue
        self._hub: AsyncpgListenerHub | None = None
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
        hub = self._ensure_hub()
        payload = await hub.dequeue(channel, poll_interval)
        if payload is None:
            return await self._queue.dequeue(channel)
        event_id = _extract_event_id(payload)
        if event_id is not None:
            event = await self._queue.dequeue_by_event_id(event_id)
            if event is not None:
                return event
        return await self._queue.dequeue(channel)

    async def ack(self, event_id: str) -> None:
        await self._queue.ack(event_id)
        self._runtime.increment_metric("events.ack")

    async def nack(self, event_id: str) -> None:
        await self._queue.nack(event_id)
        self._runtime.increment_metric("events.nack")

    async def shutdown(self) -> None:
        hub = self._hub
        if hub is not None:
            self._hub = None
            await hub.shutdown()

    def _ensure_hub(self) -> AsyncpgListenerHub:
        if self._hub is None:
            self._hub = AsyncpgListenerHub(self._config)
        return self._hub

    async def _publish_durable(
        self, channel: str, event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None"
    ) -> None:
        now = datetime.now(timezone.utc)
        async with self._config.provide_session() as driver:
            await driver.execute(
                SQL(
                    self._queue._insert_statement,
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


class AsyncpgEventsBackend:
    """Native LISTEN/NOTIFY backend backed by a persistent listener hub."""

    __slots__ = ("_config", "_hub", "_runtime")

    supports_sync = False
    supports_async = True
    backend_name = "notify"

    def __init__(self, config: "AsyncpgConfig") -> None:
        if not config.is_async:
            msg = "AsyncpgEventsBackend requires an async adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()
        self._hub: AsyncpgListenerHub | None = None
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
        hub = self._ensure_hub()
        payload = await hub.dequeue(channel, poll_interval)
        if payload is None:
            return None
        return decode_notify_payload(channel, payload)

    async def ack(self, _event_id: str) -> None:
        self._runtime.increment_metric("events.ack")

    async def nack(self, _event_id: str) -> None:
        """Return an event to the queue (no-op for native LISTEN/NOTIFY)."""

    async def shutdown(self) -> None:
        hub = self._hub
        if hub is not None:
            self._hub = None
            await hub.shutdown()

    def _ensure_hub(self) -> AsyncpgListenerHub:
        if self._hub is None:
            self._hub = AsyncpgListenerHub(self._config)
        return self._hub


def create_event_backend(
    config: "AsyncpgConfig", backend_name: str, extension_settings: "dict[str, Any]"
) -> AsyncpgEventsBackend | AsyncpgHybridEventsBackend | None:
    """EventChannel factory for the native backend."""
    match backend_name:
        case "notify":
            try:
                return AsyncpgEventsBackend(config)
            except ImproperConfigurationError:
                return None
        case "notify_queue":
            queue_backend = cast(
                "AsyncTableEventQueue", build_queue_backend(config, extension_settings, adapter_name="asyncpg")
            )
            try:
                return AsyncpgHybridEventsBackend(config, queue_backend)
            except ImproperConfigurationError:
                return None
        case _:
            return None


def _extract_event_id(payload: "str | None") -> "str | None":
    if not payload:
        return None
    raw = from_json(payload)
    if isinstance(raw, dict):
        event_id = raw.get("event_id")
        return event_id if isinstance(event_id, str) else None
    return None
