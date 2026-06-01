# pyright: reportPrivateUsage=false
"""Psycopg LISTEN/NOTIFY and hybrid event backends."""

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.psycopg.events._hub import PsycopgAsyncListenerHub, PsycopgSyncListenerHub
from sqlspec.core import SQL
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import (
    AsyncTableEventQueue,
    EventMessage,
    SyncTableEventQueue,
    build_queue_backend,
    decode_notify_payload,
    encode_notify_payload,
)
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig

__all__ = (
    "PsycopgAsyncEventsBackend",
    "PsycopgAsyncHybridEventsBackend",
    "PsycopgSyncEventsBackend",
    "PsycopgSyncHybridEventsBackend",
    "create_event_backend",
)

logger = get_logger("sqlspec.events.psycopg")


def _extract_event_id(payload: "str | None") -> "str | None":
    if not payload:
        return None
    raw = from_json(payload)
    if isinstance(raw, dict):
        event_id = raw.get("event_id")
        return event_id if isinstance(event_id, str) else None
    return None


class PsycopgSyncEventsBackend:
    """Native LISTEN/NOTIFY backend for sync psycopg adapters."""

    __slots__ = ("_config", "_hub", "_runtime")

    supports_sync = True
    supports_async = False
    backend_name = "listen_notify"

    def __init__(self, config: "PsycopgSyncConfig") -> None:
        if "psycopg" not in type(config).__module__:
            msg = "Psycopg events backend requires a Psycopg adapter"
            raise ImproperConfigurationError(msg)
        if config.is_async:
            msg = "PsycopgSyncEventsBackend requires a sync adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()
        self._hub: PsycopgSyncListenerHub | None = None
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name="psycopg",
            backend_name=self.backend_name,
            mode="sync",
            status="backend_ready",
        )

    def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        with self._config.provide_session() as driver:
            driver.execute(
                SQL(
                    "SELECT pg_notify(:channel, :payload)",
                    {"channel": channel, "payload": encode_notify_payload(event_id, payload, metadata)},
                )
            )
            driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        hub = self._ensure_hub()
        payload = hub.dequeue(channel, poll_interval)
        if payload is None:
            return None
        return decode_notify_payload(channel, payload)

    def ack(self, _event_id: str) -> None:
        self._runtime.increment_metric("events.ack")

    def nack(self, _event_id: str) -> None:
        """Return an event to the queue (no-op for native LISTEN/NOTIFY)."""

    def shutdown(self) -> None:
        hub = self._hub
        if hub is not None:
            self._hub = None
            hub.shutdown()

    def _ensure_hub(self) -> PsycopgSyncListenerHub:
        if self._hub is None:
            self._hub = PsycopgSyncListenerHub(self._config)
        return self._hub


class PsycopgAsyncEventsBackend:
    """Native LISTEN/NOTIFY backend for async psycopg adapters."""

    __slots__ = ("_config", "_hub", "_runtime")

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify"

    def __init__(self, config: "PsycopgAsyncConfig") -> None:
        if "psycopg" not in type(config).__module__:
            msg = "Psycopg events backend requires a Psycopg adapter"
            raise ImproperConfigurationError(msg)
        if not config.is_async:
            msg = "PsycopgAsyncEventsBackend requires an async adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()
        self._hub: PsycopgAsyncListenerHub | None = None

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        async with self._config.provide_session() as driver:
            await driver.execute(
                SQL(
                    "SELECT pg_notify(:channel, :payload)",
                    {"channel": channel, "payload": encode_notify_payload(event_id, payload, metadata)},
                )
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

    def _ensure_hub(self) -> PsycopgAsyncListenerHub:
        if self._hub is None:
            self._hub = PsycopgAsyncListenerHub(self._config)
        return self._hub


class PsycopgSyncHybridEventsBackend:
    """Durable hybrid backend for sync psycopg adapters."""

    __slots__ = ("_config", "_hub", "_queue", "_runtime")

    supports_sync = True
    supports_async = False
    backend_name = "listen_notify_durable"

    def __init__(self, config: "PsycopgSyncConfig", queue: "SyncTableEventQueue") -> None:
        if "psycopg" not in type(config).__module__:
            msg = "Psycopg hybrid backend requires a Psycopg adapter"
            raise ImproperConfigurationError(msg)
        if config.is_async:
            msg = "PsycopgSyncHybridEventsBackend requires a sync adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._queue = queue
        self._runtime = config.get_observability_runtime()
        self._hub: PsycopgSyncListenerHub | None = None
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name="psycopg",
            backend_name=self.backend_name,
            mode="sync",
            status="backend_ready",
        )

    def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        self._publish_durable(channel, event_id, payload, metadata)
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        hub = self._ensure_hub()
        payload = hub.dequeue(channel, poll_interval)
        if payload is None:
            return self._queue.dequeue(channel)
        event_id = _extract_event_id(payload)
        if event_id is not None:
            event = self._queue.dequeue_by_event_id(event_id)
            if event is not None:
                return event
        return self._queue.dequeue(channel)

    def ack(self, event_id: str) -> None:
        self._queue.ack(event_id)
        self._runtime.increment_metric("events.ack")

    def nack(self, event_id: str) -> None:
        self._queue.nack(event_id)
        self._runtime.increment_metric("events.nack")

    def shutdown(self) -> None:
        hub = self._hub
        if hub is not None:
            self._hub = None
            hub.shutdown()

    def _ensure_hub(self) -> PsycopgSyncListenerHub:
        if self._hub is None:
            self._hub = PsycopgSyncListenerHub(self._config)
        return self._hub

    def _publish_durable(
        self, channel: str, event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None"
    ) -> None:
        now = datetime.now(timezone.utc)
        with self._config.provide_session() as driver:
            driver.execute(
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
            driver.execute(
                SQL(
                    "SELECT pg_notify(:channel, :payload)",
                    {"channel": channel, "payload": to_json({"event_id": event_id})},
                )
            )
            driver.commit()


class PsycopgAsyncHybridEventsBackend:
    """Durable hybrid backend for async psycopg adapters."""

    __slots__ = ("_config", "_hub", "_queue", "_runtime")

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify_durable"

    def __init__(self, config: "PsycopgAsyncConfig", queue: "AsyncTableEventQueue") -> None:
        if "psycopg" not in type(config).__module__:
            msg = "Psycopg hybrid backend requires a Psycopg adapter"
            raise ImproperConfigurationError(msg)
        if not config.is_async:
            msg = "PsycopgAsyncHybridEventsBackend requires an async adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._queue = queue
        self._runtime = config.get_observability_runtime()
        self._hub: PsycopgAsyncListenerHub | None = None

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

    def _ensure_hub(self) -> PsycopgAsyncListenerHub:
        if self._hub is None:
            self._hub = PsycopgAsyncListenerHub(self._config)
        return self._hub

    async def _publish_durable(
        self, channel: str, event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None"
    ) -> None:
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
            await driver.execute(
                SQL(
                    "SELECT pg_notify(:channel, :payload)",
                    {"channel": channel, "payload": to_json({"event_id": event_id})},
                )
            )
            await driver.commit()


def create_event_backend(
    config: "PsycopgAsyncConfig | PsycopgSyncConfig", backend_name: str, extension_settings: "dict[str, Any]"
) -> (
    PsycopgSyncEventsBackend
    | PsycopgAsyncEventsBackend
    | PsycopgSyncHybridEventsBackend
    | PsycopgAsyncHybridEventsBackend
    | None
):
    """Factory used by EventChannel to create the native psycopg backend."""
    is_async = config.is_async
    match (backend_name, is_async):
        case ("listen_notify", False):
            try:
                return PsycopgSyncEventsBackend(config)  # type: ignore[arg-type]
            except ImproperConfigurationError:
                return None
        case ("listen_notify", True):
            try:
                return PsycopgAsyncEventsBackend(config)  # type: ignore[arg-type]
            except ImproperConfigurationError:
                return None
        case ("listen_notify_durable", False):
            sync_queue = cast(
                "SyncTableEventQueue", build_queue_backend(config, extension_settings, adapter_name="psycopg")
            )
            try:
                return PsycopgSyncHybridEventsBackend(config, sync_queue)  # type: ignore[arg-type]
            except ImproperConfigurationError:
                return None
        case ("listen_notify_durable", True):
            async_queue = cast(
                "AsyncTableEventQueue", build_queue_backend(config, extension_settings, adapter_name="psycopg")
            )
            try:
                return PsycopgAsyncHybridEventsBackend(config, async_queue)  # type: ignore[arg-type]
            except ImproperConfigurationError:
                return None
        case _:
            return None
