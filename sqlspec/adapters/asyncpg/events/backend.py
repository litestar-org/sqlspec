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
    from collections.abc import Sequence

    from sqlspec.adapters.asyncpg.config import AsyncpgConfig

__all__ = ("AsyncpgEventsBackend", "AsyncpgHybridEventsBackend", "create_event_backend")

logger = get_logger("sqlspec.events.postgres")
_MIN_LISTENER_POOL_SIZE = 2
_MAX_SEEN_MARKERS = 1_024


class _MarkerDrainState:
    __slots__ = ("_pending", "_seen")

    def __init__(self) -> None:
        self._pending: dict[str, int] = {}
        self._seen: dict[tuple[str, str], None] = {}

    def take(self, channel: str) -> bool:
        pending = self._pending.get(channel, 0)
        if pending <= 0:
            return False
        if pending == 1:
            self._pending.pop(channel, None)
        else:
            self._pending[channel] = pending - 1
        return True

    def register(self, channel: str, marker_id: "str | None", batch_size: int) -> "tuple[bool, bool]":
        is_new = marker_id is None or (channel, marker_id) not in self._seen
        if is_new:
            if marker_id is not None:
                if len(self._seen) >= _MAX_SEEN_MARKERS:
                    self._seen.pop(next(iter(self._seen)))
                self._seen[(channel, marker_id)] = None
            self._pending[channel] = self._pending.get(channel, 0) + max(batch_size, 1)
        return is_new, self.take(channel)

    def clear(self, channel: str) -> None:
        self._pending.pop(channel, None)

    def reset(self) -> None:
        self._pending.clear()
        self._seen.clear()


class AsyncpgHybridEventsBackend:
    """Hybrid backend combining durable queue with LISTEN/NOTIFY wakeups."""

    __slots__ = ("_config", "_hub", "_marker_state", "_queue", "_runtime")

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
        self._marker_state = _MarkerDrainState()
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
        self._runtime.increment_metric("events.publisher.session")
        event_id = uuid4().hex
        await self._publish_durable(channel, event_id, payload, metadata)
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def publish_many(self, events: "Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]") -> list[str]:
        """Persist a batch and publish one compact wakeup marker per channel."""
        if not events:
            return []
        now = datetime.now(timezone.utc)
        event_ids: list[str] = []
        records: list[dict[str, Any]] = []
        marker_counts: dict[str, int] = {}
        for channel, payload, metadata in events:
            event_id = uuid4().hex
            event_ids.append(event_id)
            records.append({
                "event_id": event_id,
                "channel": channel,
                "payload_json": to_json(payload),
                "metadata_json": to_json(metadata) if metadata is not None else None,
                "status": "pending",
                "available_at": now,
                "lease_expires_at": None,
                "attempts": 0,
                "created_at": now,
            })
            marker_counts[channel] = marker_counts.get(channel, 0) + 1
        marker_id = uuid4().hex
        markers = [
            (channel, to_json({"batch_size": count, "marker_id": marker_id}))
            for channel, count in marker_counts.items()
        ]
        self._runtime.increment_metric("events.publisher.session")
        async with self._config.provide_session() as driver:
            await driver.execute_many(
                self._queue._insert_statement, records, statement_config=self._queue._statement_config
            )
            await driver.execute_many("SELECT pg_notify($1, $2)", markers)
            await driver.commit()
        self._runtime.increment_metric("events.publisher.statement", 2)
        self._runtime.increment_metric("events.publish.native", len(records))
        return event_ids

    async def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        if self._marker_state.take(channel):
            return await self._dequeue_hinted(channel)
        hub = self._ensure_hub()
        while True:
            payload = await hub.dequeue(channel, poll_interval)
            if payload is None:
                return await self._dequeue_reconciled(channel)
            self._queue._reset_empty_poll_delay(channel)
            self._runtime.increment_metric("events.listener.wakeup")
            batch_size = _extract_batch_size(payload)
            is_new, should_drain = self._marker_state.register(channel, _extract_marker_id(payload), batch_size)
            if is_new and batch_size > 1:
                self._runtime.increment_metric("events.marker.coalesced", batch_size - 1)
            if not should_drain:
                self._runtime.increment_metric("events.marker.duplicate")
                continue
            event_id = _extract_event_id(payload)
            if event_id is not None:
                event = await self._queue.dequeue_by_event_id(event_id)
                if event is not None:
                    self._runtime.increment_metric("events.marker.drain")
                    _record_dequeue_result(self._runtime, event)
                    return event
            return await self._dequeue_hinted(channel)

    async def ack(self, event_id: str) -> None:
        await self._queue.ack(event_id)
        self._runtime.increment_metric("events.ack")

    async def nack(self, event_id: str) -> None:
        await self._queue.nack(event_id)
        self._runtime.increment_metric("events.nack")

    async def shutdown(self) -> None:
        hub = self._hub
        try:
            if hub is not None:
                self._hub = None
                await hub.shutdown()
        finally:
            self._marker_state.reset()

    def _ensure_hub(self) -> AsyncpgListenerHub:
        if self._hub is None:
            self._hub = AsyncpgListenerHub(self._config, self.backend_name)
        return self._hub

    async def _dequeue_hinted(self, channel: str) -> EventMessage | None:
        event = await self._queue.dequeue(channel)
        if event is None:
            self._marker_state.clear(channel)
            self._runtime.increment_metric("events.marker.shortfall")
            return None
        self._runtime.increment_metric("events.marker.drain")
        _record_dequeue_result(self._runtime, event)
        return event

    async def _dequeue_reconciled(self, channel: str) -> EventMessage | None:
        self._runtime.increment_metric("events.poll.fallback")
        event = await self._queue.dequeue(channel)
        if event is None:
            self._runtime.increment_metric("events.listener.timeout")
            return None
        self._runtime.increment_metric("events.marker.miss")
        _record_dequeue_result(self._runtime, event)
        return event

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
                        "metadata_json": to_json(metadata) if metadata is not None else None,
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
        self._runtime.increment_metric("events.publisher.session")
        event_id = uuid4().hex
        async with self._config.provide_session() as driver:
            await driver.execute(
                SQL("SELECT pg_notify($1, $2)", channel, encode_notify_payload(event_id, payload, metadata))
            )
            await driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def publish_many(self, events: "Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]") -> list[str]:
        """Publish unchanged per-event envelopes through one producer session."""
        if not events:
            return []
        event_ids: list[str] = []
        parameters: list[tuple[str, str]] = []
        for channel, payload, metadata in events:
            event_id = uuid4().hex
            event_ids.append(event_id)
            parameters.append((channel, encode_notify_payload(event_id, payload, metadata)))
        self._runtime.increment_metric("events.publisher.session")
        async with self._config.provide_session() as driver:
            await driver.execute_many("SELECT pg_notify($1, $2)", parameters)
            await driver.commit()
        self._runtime.increment_metric("events.publisher.statement")
        self._runtime.increment_metric("events.publish.native", len(parameters))
        return event_ids

    async def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        hub = self._ensure_hub()
        payload = await hub.dequeue(channel, poll_interval)
        if payload is None:
            self._runtime.increment_metric("events.listener.timeout")
            return None
        self._runtime.increment_metric("events.listener.wakeup")
        event = decode_notify_payload(channel, payload)
        _record_dequeue_result(self._runtime, event)
        return event

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
            self._hub = AsyncpgListenerHub(self._config, self.backend_name)
        return self._hub


def create_event_backend(
    config: "AsyncpgConfig", backend_name: str, extension_settings: "dict[str, Any]"
) -> AsyncpgEventsBackend | AsyncpgHybridEventsBackend | None:
    """EventChannel factory for the native backend."""
    if backend_name in {"notify", "notify_queue"}:
        _validate_listener_pool_capacity(config)
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


def _validate_listener_pool_capacity(config: "AsyncpgConfig") -> None:
    max_size = config.connection_config.get("max_size")
    if max_size is not None and max_size < _MIN_LISTENER_POOL_SIZE:
        msg = f"{type(config).__name__} native event listeners require pool max_size >= {_MIN_LISTENER_POOL_SIZE}"
        raise ImproperConfigurationError(msg)


def _extract_event_id(payload: "str | None") -> "str | None":
    if not payload:
        return None
    raw = from_json(payload)
    if isinstance(raw, dict):
        event_id = raw.get("event_id")
        return event_id if isinstance(event_id, str) else None
    return None


def _extract_batch_size(payload: "str | None") -> int:
    if not payload:
        return 0
    raw = from_json(payload)
    if isinstance(raw, dict):
        batch_size = raw.get("batch_size")
        return batch_size if isinstance(batch_size, int) else 0
    return 0


def _extract_marker_id(payload: "str | None") -> "str | None":
    if not payload:
        return None
    raw = from_json(payload)
    if isinstance(raw, dict):
        marker_id = raw.get("marker_id", raw.get("event_id"))
        return marker_id if isinstance(marker_id, str) else None
    return None


def _record_dequeue_result(runtime: Any, event: "EventMessage | None") -> None:
    if event is None:
        return
    latency_ms = max(0.0, (datetime.now(timezone.utc) - event.created_at).total_seconds() * 1000)
    runtime.record_metric("events.dequeue.latency_ms", latency_ms)
