"""Psqlpy LISTEN/NOTIFY and hybrid event backends."""

# pyright: reportPrivateUsage=false

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.psqlpy.events._hub import PsqlpyListenerHub
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
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig

__all__ = ("PsqlpyEventsBackend", "PsqlpyHybridEventsBackend", "create_event_backend")


logger = get_logger("sqlspec.events.psqlpy")


class PsqlpyEventsBackend:
    """Native LISTEN/NOTIFY backend for psqlpy adapters."""

    __slots__ = ("_config", "_hub", "_runtime")

    supports_sync = False
    supports_async = True
    backend_name = "listen_notify"

    def __init__(self, config: "PsqlpyConfig") -> None:
        if "psqlpy" not in type(config).__module__:
            msg = "Psqlpy events backend requires a Psqlpy adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()
        self._hub: PsqlpyListenerHub | None = None
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

    def _ensure_hub(self) -> PsqlpyListenerHub:
        if self._hub is None:
            self._hub = PsqlpyListenerHub(self._config)
        return self._hub


class PsqlpyHybridEventsBackend:
    """Durable hybrid backend combining queue storage with LISTEN/NOTIFY wakeups."""

    __slots__ = ("_config", "_hub", "_queue", "_runtime")

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
        self._hub: PsqlpyListenerHub | None = None
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

    def _ensure_hub(self) -> PsqlpyListenerHub:
        if self._hub is None:
            self._hub = PsqlpyListenerHub(self._config)
        return self._hub

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


def _extract_event_id(payload: "str | None") -> "str | None":
    if not payload:
        return None
    raw = from_json(payload)
    if isinstance(raw, dict):
        event_id = raw.get("event_id")
        return event_id if isinstance(event_id, str) else None
    return None
