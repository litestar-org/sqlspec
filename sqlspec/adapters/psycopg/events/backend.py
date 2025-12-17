# pyright: reportPrivateUsage=false
"""Psycopg LISTEN/NOTIFY and hybrid event backends."""

import contextlib
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.core import SQL
from sqlspec.exceptions import EventChannelError, ImproperConfigurationError
from sqlspec.extensions.events import EventMessage
from sqlspec.extensions.events._queue import QueueEventBackend, TableEventQueue
from sqlspec.extensions.events._store import normalize_event_channel_name
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
    from sqlspec.config import DatabaseConfigProtocol


logger = get_logger("events.psycopg")

__all__ = ("PsycopgEventsBackend", "PsycopgHybridEventsBackend", "create_event_backend")


class PsycopgEventsBackend:
    """Native LISTEN/NOTIFY backend for psycopg adapters."""

    __slots__ = (
        "_config",
        "_listen_connection_async",
        "_listen_connection_async_cm",
        "_listen_connection_sync",
        "_listen_connection_sync_cm",
        "_runtime",
    )

    supports_sync = True
    supports_async = True
    backend_name = "listen_notify"

    def __init__(self, config: "PsycopgAsyncConfig | PsycopgSyncConfig") -> None:
        if "psycopg" not in type(config).__module__:
            msg = "Psycopg events backend requires a Psycopg adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._runtime = config.get_observability_runtime()
        self._listen_connection_async: Any | None = None
        self._listen_connection_async_cm: Any | None = None
        self._listen_connection_sync: Any | None = None
        self._listen_connection_sync_cm: Any | None = None

    async def publish_async(
        self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None
    ) -> str:
        event_id = uuid.uuid4().hex
        envelope = self._encode_payload(event_id, payload, metadata)
        session_cm = self._config.provide_session()
        async with session_cm as driver:  # type: ignore[union-attr]
            await driver.execute(SQL("SELECT pg_notify(:channel, :payload)", {"channel": channel, "payload": envelope}))
            await driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def publish_sync(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid.uuid4().hex
        envelope = self._encode_payload(event_id, payload, metadata)
        session_cm = self._config.provide_session()
        with session_cm as driver:  # type: ignore[union-attr]
            driver.execute(SQL("SELECT pg_notify(:channel, :payload)", {"channel": channel, "payload": envelope}))
            driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def dequeue_async(self, channel: str, poll_interval: float) -> EventMessage | None:
        """Dequeue an event from the channel asynchronously.

        Args:
            channel: Channel name to listen on.
            poll_interval: Timeout in seconds to wait for a notification.

        Returns:
            EventMessage if a notification was received, None otherwise.
        """
        connection = await self._ensure_async_listener(channel)
        async for notify in connection.notifies(timeout=poll_interval, stop_after=1):
            if notify.channel == channel:
                return self._decode_payload(channel, notify.payload)
        return None

    def dequeue_sync(self, channel: str, poll_interval: float) -> EventMessage | None:
        """Dequeue an event from the channel synchronously.

        Args:
            channel: Channel name to listen on.
            poll_interval: Timeout in seconds to wait for a notification.

        Returns:
            EventMessage if a notification was received, None otherwise.
        """
        connection = self._ensure_sync_listener(channel)
        notify_iter = connection.notifies(timeout=poll_interval, stop_after=1)
        with contextlib.suppress(StopIteration):
            notify = next(notify_iter)
            if notify.channel == channel:
                return self._decode_payload(channel, notify.payload)
        return None

    async def ack_async(self, _event_id: str) -> None:
        self._runtime.increment_metric("events.ack")

    def ack_sync(self, _event_id: str) -> None:
        self._runtime.increment_metric("events.ack")

    async def _ensure_async_listener(self, channel: str) -> Any:
        """Ensure async listener connection is established for the channel.

        Args:
            channel: Channel name to listen on.

        Returns:
            The async connection configured for LISTEN/NOTIFY.
        """
        if self._listen_connection_async is None:
            validated_channel = normalize_event_channel_name(channel)
            self._listen_connection_async_cm = self._config.provide_connection()
            self._listen_connection_async = await self._listen_connection_async_cm.__aenter__()  # type: ignore[union-attr]
            if self._listen_connection_async is not None:
                await self._listen_connection_async.set_autocommit(True)
                await self._listen_connection_async.execute(f"LISTEN {validated_channel}")
        return self._listen_connection_async

    def _ensure_sync_listener(self, channel: str) -> Any:
        """Ensure sync listener connection is established for the channel.

        Args:
            channel: Channel name to listen on.

        Returns:
            The sync connection configured for LISTEN/NOTIFY.
        """
        if self._listen_connection_sync is None:
            validated_channel = normalize_event_channel_name(channel)
            self._listen_connection_sync_cm = self._config.provide_connection()
            self._listen_connection_sync = self._listen_connection_sync_cm.__enter__()  # type: ignore[union-attr]
            if self._listen_connection_sync is not None:
                self._listen_connection_sync.autocommit = True
                self._listen_connection_sync.execute(f"LISTEN {validated_channel}")
        return self._listen_connection_sync

    async def shutdown_async(self) -> None:
        """Shutdown the async listener and release resources."""
        if self._listen_connection_async_cm is not None:
            with contextlib.suppress(Exception):
                await self._listen_connection_async_cm.__aexit__(None, None, None)
            self._listen_connection_async = None
            self._listen_connection_async_cm = None

    def shutdown_sync(self) -> None:
        """Shutdown the sync listener and release resources."""
        if self._listen_connection_sync_cm is not None:
            with contextlib.suppress(Exception):
                self._listen_connection_sync_cm.__exit__(None, None, None)
            self._listen_connection_sync = None
            self._listen_connection_sync_cm = None

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
        timestamp = PsycopgEventsBackend._parse_timestamp(published_at)
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


class PsycopgHybridEventsBackend:
    """Durable hybrid backend combining queue storage with LISTEN/NOTIFY wakeups."""

    __slots__ = (
        "_config",
        "_listen_connection_async",
        "_listen_connection_async_cm",
        "_listen_connection_sync",
        "_listen_connection_sync_cm",
        "_queue",
        "_runtime",
    )

    supports_sync = True
    supports_async = True
    backend_name = "listen_notify_durable"

    def __init__(self, config: "DatabaseConfigProtocol[Any, Any, Any]", queue_backend: "QueueEventBackend") -> None:
        if "psycopg" not in type(config).__module__:
            msg = "Psycopg hybrid backend requires a Psycopg adapter"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._queue = queue_backend
        self._runtime = config.get_observability_runtime()
        self._listen_connection_async: Any | None = None
        self._listen_connection_async_cm: Any | None = None
        self._listen_connection_sync: Any | None = None
        self._listen_connection_sync_cm: Any | None = None

    async def publish_async(
        self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None
    ) -> str:
        event_id = uuid.uuid4().hex
        await self._publish_durable_async(channel, event_id, payload, metadata)
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def publish_sync(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid.uuid4().hex
        self._publish_durable_sync(channel, event_id, payload, metadata)
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def dequeue_async(self, channel: str, poll_interval: float) -> EventMessage | None:
        connection = await self._ensure_async_listener(channel)
        async for _notify in connection.notifies(timeout=poll_interval, stop_after=1):
            break
        return await self._queue.dequeue_async(channel, poll_interval=poll_interval)

    def dequeue_sync(self, channel: str, poll_interval: float) -> EventMessage | None:
        connection = self._ensure_sync_listener(channel)
        notify_iter = connection.notifies(timeout=poll_interval, stop_after=1)
        with contextlib.suppress(StopIteration):
            next(notify_iter)
        return self._queue.dequeue_sync(channel, poll_interval=poll_interval)

    async def _ensure_async_listener(self, channel: str) -> Any:
        """Ensure async listener connection is established for the channel.

        Args:
            channel: Channel name to listen on.

        Returns:
            The async connection configured for LISTEN/NOTIFY.
        """
        if self._listen_connection_async is None:
            validated_channel = normalize_event_channel_name(channel)
            self._listen_connection_async_cm = self._config.provide_connection()
            self._listen_connection_async = await self._listen_connection_async_cm.__aenter__()  # type: ignore[union-attr]
            if self._listen_connection_async is not None:
                await self._listen_connection_async.set_autocommit(True)
                await self._listen_connection_async.execute(f"LISTEN {validated_channel}")
        return self._listen_connection_async

    def _ensure_sync_listener(self, channel: str) -> Any:
        """Ensure sync listener connection is established for the channel.

        Args:
            channel: Channel name to listen on.

        Returns:
            The sync connection configured for LISTEN/NOTIFY.
        """
        if self._listen_connection_sync is None:
            validated_channel = normalize_event_channel_name(channel)
            self._listen_connection_sync_cm = self._config.provide_connection()
            self._listen_connection_sync = self._listen_connection_sync_cm.__enter__()  # type: ignore[union-attr]
            if self._listen_connection_sync is not None:
                self._listen_connection_sync.autocommit = True
                self._listen_connection_sync.execute(f"LISTEN {validated_channel}")
        return self._listen_connection_sync

    async def ack_async(self, event_id: str) -> None:
        """Acknowledge an event asynchronously.

        Args:
            event_id: The event identifier to acknowledge.
        """
        await self._queue.ack_async(event_id)
        self._runtime.increment_metric("events.ack")

    def ack_sync(self, event_id: str) -> None:
        """Acknowledge an event synchronously.

        Args:
            event_id: The event identifier to acknowledge.
        """
        self._queue.ack_sync(event_id)
        self._runtime.increment_metric("events.ack")

    async def shutdown_async(self) -> None:
        """Shutdown the async listener and release resources."""
        if self._listen_connection_async_cm is not None:
            with contextlib.suppress(Exception):
                await self._listen_connection_async_cm.__aexit__(None, None, None)
            self._listen_connection_async = None
            self._listen_connection_async_cm = None

    def shutdown_sync(self) -> None:
        """Shutdown the sync listener and release resources."""
        if self._listen_connection_sync_cm is not None:
            with contextlib.suppress(Exception):
                self._listen_connection_sync_cm.__exit__(None, None, None)
            self._listen_connection_sync = None
            self._listen_connection_sync_cm = None

    def _get_table_queue(self) -> "TableEventQueue":
        """Get the underlying TableEventQueue from the queue backend.

        Returns:
            The TableEventQueue instance.

        Raises:
            EventChannelError: If the queue reference is missing.
        """
        queue = self._queue._queue
        if queue is None:
            msg = "Hybrid queue backend missing queue reference"
            raise EventChannelError(msg)
        return queue

    async def _publish_durable_async(
        self, channel: str, event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None"
    ) -> None:
        """Publish event to durable queue and send NOTIFY asynchronously.

        Args:
            channel: Channel name to publish to.
            event_id: Unique event identifier.
            payload: Event payload data.
            metadata: Optional event metadata.
        """
        now = datetime.now(timezone.utc)
        queue = self._get_table_queue()
        session_cm = self._config.provide_session()
        async with session_cm as driver:  # type: ignore[union-attr]
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
            await driver.execute(
                SQL(
                    "SELECT pg_notify(:channel, :payload)",
                    {"channel": channel, "payload": to_json({"event_id": event_id})},
                )
            )
            await driver.commit()

    def _publish_durable_sync(
        self, channel: str, event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None"
    ) -> None:
        """Publish event to durable queue and send NOTIFY synchronously.

        Args:
            channel: Channel name to publish to.
            event_id: Unique event identifier.
            payload: Event payload data.
            metadata: Optional event metadata.
        """
        now = datetime.now(timezone.utc)
        queue = self._get_table_queue()
        session_cm = self._config.provide_session()
        with session_cm as driver:  # type: ignore[union-attr]
            driver.execute(
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
            driver.execute(
                SQL(
                    "SELECT pg_notify(:channel, :payload)",
                    {"channel": channel, "payload": to_json({"event_id": event_id})},
                )
            )
            driver.commit()


def create_event_backend(
    config: "PsycopgAsyncConfig | PsycopgSyncConfig", backend_name: str, extension_settings: dict[str, Any]
) -> PsycopgEventsBackend | PsycopgHybridEventsBackend | None:
    """Factory used by EventChannel to create the native psycopg backend."""
    if backend_name == "listen_notify":
        try:
            return PsycopgEventsBackend(config)
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
            return PsycopgHybridEventsBackend(config, queue_backend)
        except ImproperConfigurationError:
            return None
    return None
