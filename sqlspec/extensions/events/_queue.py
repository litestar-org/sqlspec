"""Fallback table-backed queue implementation for EventChannel."""

import asyncio
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlspec.core import SQL, StatementConfig
from sqlspec.exceptions import EventChannelError
from sqlspec.extensions.events._hints import EventRuntimeHints, get_runtime_hints
from sqlspec.extensions.events._models import EventMessage
from sqlspec.extensions.events._store import normalize_queue_table_name
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol

logger = get_logger("events.queue")

__all__ = ("QueueEvent", "QueueEventBackend", "TableEventQueue", "build_queue_backend")

_ADAPTER_MODULE_PARTS = 3

_PENDING_STATUS = "pending"
_LEASED_STATUS = "leased"
_ACKED_STATUS = "acked"
_DEFAULT_TABLE = "sqlspec_event_queue"


@dataclass(slots=True)
class QueueEvent:
    """Represents an event fetched from the fallback queue."""

    event_id: str
    channel: str
    payload: "dict[str, Any]"
    metadata: "dict[str, Any] | None"
    attempts: int
    available_at: "datetime"
    lease_expires_at: "datetime | None"
    created_at: "datetime"


class TableEventQueue:
    """Queue backend that stores events inside a managed table."""

    __slots__ = (
        "_ack_sql",
        "_acked_cleanup_sql",
        "_claim_sql",
        "_config",
        "_dialect",
        "_json_passthrough",
        "_lease_seconds",
        "_max_claim_attempts",
        "_nack_sql",
        "_retention_seconds",
        "_runtime",
        "_select_for_update",
        "_select_sql",
        "_skip_locked",
        "_statement_config",
        "_table_name",
        "_upsert_sql",
    )

    def __init__(
        self,
        config: "DatabaseConfigProtocol[Any, Any, Any]",
        *,
        queue_table: str | None = None,
        lease_seconds: int | None = None,
        retention_seconds: int | None = None,
        select_for_update: bool | None = None,
        skip_locked: bool | None = None,
        json_passthrough: bool | None = None,
    ) -> None:
        """Initialize the queue backend with configuration defaults."""

        self._config = config
        self._statement_config = config.statement_config
        self._runtime = config.get_observability_runtime()
        self._dialect = str(self._statement_config.dialect or "").lower() if self._statement_config else ""
        try:
            self._table_name = normalize_queue_table_name(queue_table or _DEFAULT_TABLE)
        except ValueError as error:  # pragma: no cover - invalid identifier path
            raise EventChannelError(str(error)) from error
        self._lease_seconds = lease_seconds or 30
        self._retention_seconds = retention_seconds or 86_400
        self._max_claim_attempts = 5
        self._select_for_update = bool(select_for_update)
        self._skip_locked = bool(skip_locked)
        self._json_passthrough = bool(json_passthrough)
        self._upsert_sql = self._build_insert_sql()
        self._select_sql = self._build_select_sql()
        self._claim_sql = self._build_claim_sql()
        self._ack_sql = self._build_ack_sql()
        self._nack_sql = self._build_nack_sql()
        self._acked_cleanup_sql = self._build_cleanup_sql()

    @property
    def statement_config(self) -> "StatementConfig":
        """Return the statement configuration associated with the adapter."""

        return self._statement_config

    def _build_insert_sql(self) -> str:
        columns = "event_id, channel, payload_json, metadata_json, status, available_at, lease_expires_at, attempts, created_at"
        values = ":event_id, :channel, :payload_json, :metadata_json, :status, :available_at, :lease_expires_at, :attempts, :created_at"
        return f"INSERT INTO {self._table_name} ({columns}) VALUES ({values})"

    def _build_select_sql(self) -> str:
        limit_clause = " FETCH FIRST 1 ROWS ONLY" if "oracle" in self._dialect else " LIMIT 1"
        base = (
            f"SELECT event_id, channel, payload_json, metadata_json, attempts, available_at, lease_expires_at, created_at "
            f"FROM {self._table_name} "
            "WHERE channel = :channel AND available_at <= :available_cutoff AND ("
            "status = :pending_status OR (status = :leased_status AND (lease_expires_at IS NULL OR lease_expires_at <= :lease_cutoff))"
            ") ORDER BY created_at ASC"
        )
        locking_clause = ""
        if self._select_for_update:
            locking_clause = " FOR UPDATE"
            if self._skip_locked:
                locking_clause += " SKIP LOCKED"
        return base + limit_clause + locking_clause

    def _build_claim_sql(self) -> str:
        return (
            f"UPDATE {self._table_name} SET status = :claimed_status, lease_expires_at = :lease_expires_at, attempts = attempts + 1 "
            "WHERE event_id = :event_id AND ("
            "status = :pending_status OR (status = :leased_status AND (lease_expires_at IS NULL OR lease_expires_at <= :lease_reentry_cutoff))"
            ")"
        )

    def _build_ack_sql(self) -> str:
        return f"UPDATE {self._table_name} SET status = :acked, acknowledged_at = :acked_at WHERE event_id = :event_id"

    def _build_nack_sql(self) -> str:
        return f"UPDATE {self._table_name} SET status = :pending, lease_expires_at = NULL WHERE event_id = :event_id"

    def _build_cleanup_sql(self) -> str:
        return f"DELETE FROM {self._table_name} WHERE status = :acked AND acknowledged_at IS NOT NULL AND acknowledged_at <= :cutoff"

    @staticmethod
    def _utcnow() -> "datetime":
        return datetime.now(timezone.utc)

    async def publish_async(
        self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None
    ) -> str:
        """Insert a new event row asynchronously and return its identifier."""

        event_id = uuid4().hex
        now = self._utcnow()
        await self._execute_async(
            self._upsert_sql,
            {
                "event_id": event_id,
                "channel": channel,
                "payload_json": self._encode_json(payload),
                "metadata_json": self._encode_json(metadata),
                "status": _PENDING_STATUS,
                "available_at": now,
                "lease_expires_at": None,
                "attempts": 0,
                "created_at": now,
            },
        )
        self._runtime.increment_metric("events.publish")
        return event_id

    def publish_sync(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        """Insert a new event row using synchronous drivers."""

        event_id = uuid4().hex
        now = self._utcnow()
        self._execute_sync(
            self._upsert_sql,
            {
                "event_id": event_id,
                "channel": channel,
                "payload_json": self._encode_json(payload),
                "metadata_json": self._encode_json(metadata),
                "status": _PENDING_STATUS,
                "available_at": now,
                "lease_expires_at": None,
                "attempts": 0,
                "created_at": now,
            },
        )
        self._runtime.increment_metric("events.publish")
        return event_id

    async def dequeue_async(self, channel: str, poll_interval: float | None = None) -> "QueueEvent | None":
        """Fetch the next available event for the given channel asynchronously."""

        attempt = 0
        while attempt < self._max_claim_attempts:
            attempt += 1
            row = await self._fetch_candidate_async(channel)
            if row is None:
                if poll_interval is not None and poll_interval > 0:
                    await asyncio.sleep(poll_interval)
                return None
            now = self._utcnow()
            leased_until = now + timedelta(seconds=self._lease_seconds)
            claimed = await self._execute_async(
                self._claim_sql,
                {
                    "claimed_status": _LEASED_STATUS,
                    "lease_expires_at": leased_until,
                    "event_id": row["event_id"],
                    "pending_status": _PENDING_STATUS,
                    "leased_status": _LEASED_STATUS,
                    "lease_reentry_cutoff": now,
                },
            )
            if claimed:
                return self._hydrate_event(row, leased_until)
        return None

    def dequeue_sync(self, channel: str, poll_interval: float | None = None) -> "QueueEvent | None":
        """Fetch the next available event synchronously."""

        attempt = 0
        while attempt < self._max_claim_attempts:
            attempt += 1
            row = self._fetch_candidate_sync(channel)
            if row is None:
                if poll_interval is not None and poll_interval > 0:
                    time.sleep(poll_interval)
                return None
            now = self._utcnow()
            leased_until = now + timedelta(seconds=self._lease_seconds)
            claimed = self._execute_sync(
                self._claim_sql,
                {
                    "claimed_status": _LEASED_STATUS,
                    "lease_expires_at": leased_until,
                    "event_id": row["event_id"],
                    "pending_status": _PENDING_STATUS,
                    "leased_status": _LEASED_STATUS,
                    "lease_reentry_cutoff": now,
                },
            )
            if claimed:
                return self._hydrate_event(row, leased_until)
        return None

    async def ack_async(self, event_id: str) -> None:
        """Mark an event as acknowledged and schedule cleanup asynchronously."""

        now = self._utcnow()
        await self._execute_async(self._ack_sql, {"acked": _ACKED_STATUS, "acked_at": now, "event_id": event_id})
        await self._cleanup_async(now)
        self._runtime.increment_metric("events.ack")

    def ack_sync(self, event_id: str) -> None:
        """Mark an event as acknowledged using synchronous drivers."""

        now = self._utcnow()
        self._execute_sync(self._ack_sql, {"acked": _ACKED_STATUS, "acked_at": now, "event_id": event_id})
        self._cleanup_sync(now)
        self._runtime.increment_metric("events.ack")

    async def nack_async(self, event_id: str) -> None:
        """Return an event to the queue for redelivery asynchronously.

        Resets the event status to pending and clears the lease, allowing it
        to be picked up by another consumer. The attempts counter remains
        unchanged (was already incremented during claim).
        """
        await self._execute_async(self._nack_sql, {"pending": _PENDING_STATUS, "event_id": event_id})
        self._runtime.increment_metric("events.nack")

    def nack_sync(self, event_id: str) -> None:
        """Return an event to the queue for redelivery synchronously.

        Resets the event status to pending and clears the lease, allowing it
        to be picked up by another consumer. The attempts counter remains
        unchanged (was already incremented during claim).
        """
        self._execute_sync(self._nack_sql, {"pending": _PENDING_STATUS, "event_id": event_id})
        self._runtime.increment_metric("events.nack")

    async def _cleanup_async(self, reference: "datetime") -> None:
        cutoff = reference - timedelta(seconds=self._retention_seconds)
        await self._execute_async(self._acked_cleanup_sql, {"acked": _ACKED_STATUS, "cutoff": cutoff})

    def _cleanup_sync(self, reference: "datetime") -> None:
        cutoff = reference - timedelta(seconds=self._retention_seconds)
        self._execute_sync(self._acked_cleanup_sql, {"acked": _ACKED_STATUS, "cutoff": cutoff})

    async def _fetch_candidate_async(self, channel: str) -> "dict[str, Any] | None":
        current_time = self._utcnow()
        session_cm = self._config.provide_session()
        async with session_cm as driver:  # type: ignore[union-attr]
            result: dict[str, Any] | None = await driver.select_one_or_none(
                SQL(
                    self._select_sql,
                    {
                        "channel": channel,
                        "available_cutoff": current_time,
                        "pending_status": _PENDING_STATUS,
                        "leased_status": _LEASED_STATUS,
                        "lease_cutoff": current_time,
                    },
                    statement_config=self._statement_config,
                )
            )
            return result

    def _fetch_candidate_sync(self, channel: str) -> "dict[str, Any] | None":
        current_time = self._utcnow()
        session_cm = self._config.provide_session()
        with session_cm as driver:  # type: ignore[union-attr]
            result: dict[str, Any] | None = driver.select_one_or_none(
                SQL(
                    self._select_sql,
                    {
                        "channel": channel,
                        "available_cutoff": current_time,
                        "pending_status": _PENDING_STATUS,
                        "leased_status": _LEASED_STATUS,
                        "lease_cutoff": current_time,
                    },
                    statement_config=self._statement_config,
                )
            )
            return result

    async def _execute_async(self, sql: str, parameters: "dict[str, Any]") -> int:
        session_cm = self._config.provide_session()
        async with session_cm as driver:  # type: ignore[union-attr]
            result = await driver.execute(SQL(sql, parameters, statement_config=self._statement_config))
            await driver.commit()
            rows_affected = result.rows_affected
            return int(rows_affected) if rows_affected is not None else 0

    def _execute_sync(self, sql: str, parameters: "dict[str, Any]") -> int:
        session_factory = getattr(self._config, "provide_write_session", None)
        use_write_session = session_factory is not None
        session_cm = self._config.provide_session() if session_factory is None else cast("Any", session_factory)()
        with session_cm as driver:  # type: ignore[union-attr]
            result = driver.execute(SQL(sql, parameters, statement_config=self._statement_config))
            if not use_write_session:
                driver.commit()
            rows_affected = result.rows_affected
            return int(rows_affected) if rows_affected is not None else 0

    @staticmethod
    def _coerce_datetime(value: Any) -> "datetime":
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        if isinstance(value, str):
            with suppress(ValueError):
                parsed = datetime.fromisoformat(value)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed
        return TableEventQueue._utcnow()

    @classmethod
    def _hydrate_event(cls, row: "dict[str, Any]", lease_expires_at: "datetime | None") -> QueueEvent:
        payload_raw = row.get("payload_json")
        metadata_raw = row.get("metadata_json")
        if isinstance(payload_raw, dict):
            payload_obj = payload_raw
        elif payload_raw is not None:
            payload_obj = from_json(payload_raw)
        else:
            payload_obj = {}
        if isinstance(metadata_raw, dict):
            metadata_obj = metadata_raw
        elif metadata_raw is not None:
            metadata_obj = from_json(metadata_raw)
        else:
            metadata_obj = None
        payload_value = payload_obj if isinstance(payload_obj, dict) else {"value": payload_obj}
        metadata_value = (
            metadata_obj if isinstance(metadata_obj, dict) or metadata_obj is None else {"value": metadata_obj}
        )
        available_at = cls._coerce_datetime(row.get("available_at"))
        created_at = cls._coerce_datetime(row.get("created_at"))
        lease_value = lease_expires_at or row.get("lease_expires_at")
        lease_at = cls._coerce_datetime(lease_value) if lease_value is not None else None
        return QueueEvent(
            event_id=row["event_id"],
            channel=row["channel"],
            payload=payload_value,
            metadata=metadata_value,
            attempts=int(row.get("attempts", 0)),
            available_at=available_at,
            lease_expires_at=lease_at,
            created_at=created_at,
        )

    def _encode_json(self, value: "dict[str, Any] | None") -> "dict[str, Any] | str | None":
        if value is None:
            return None
        if self._json_passthrough:
            return value
        return to_json(value)


def build_queue_backend(
    config: "DatabaseConfigProtocol[Any, Any, Any]",
    extension_settings: "dict[str, Any] | None" = None,
    *,
    adapter_name: "str | None" = None,
    hints: "EventRuntimeHints | None" = None,
) -> "QueueEventBackend":
    """Build a QueueEventBackend using adapter hints and extension overrides."""

    settings = dict(extension_settings or {})
    resolved_adapter = adapter_name or _resolve_adapter_name(config)
    runtime_hints = hints or get_runtime_hints(resolved_adapter, config)
    queue = TableEventQueue(
        config,
        queue_table=settings.get("queue_table"),
        lease_seconds=_resolve_int_setting(settings, "lease_seconds", runtime_hints.lease_seconds),
        retention_seconds=_resolve_int_setting(settings, "retention_seconds", runtime_hints.retention_seconds),
        select_for_update=_resolve_bool_setting(settings, "select_for_update", runtime_hints.select_for_update),
        skip_locked=_resolve_bool_setting(settings, "skip_locked", runtime_hints.skip_locked),
        json_passthrough=_resolve_bool_setting(settings, "json_passthrough", runtime_hints.json_passthrough),
    )
    return QueueEventBackend(queue)


def _resolve_adapter_name(config: "DatabaseConfigProtocol[Any, Any, Any]") -> "str | None":
    module_name = type(config).__module__
    parts = module_name.split(".")
    if len(parts) >= _ADAPTER_MODULE_PARTS and parts[0] == "sqlspec" and parts[1] == "adapters":
        return parts[2]
    return None


def _resolve_bool_setting(settings: "dict[str, Any]", key: str, default: bool) -> bool:
    if key not in settings:
        return bool(default)
    value = settings.get(key)
    if value is None:
        return bool(default)
    return bool(value)


def _resolve_int_setting(settings: "dict[str, Any]", key: str, default: int) -> int:
    if key not in settings:
        return int(default)
    value = settings.get(key)
    if value is None:
        return int(default)
    return int(value)


class QueueEventBackend:
    """Adapter-facing wrapper that exposes TableEventQueue via EventMessage objects."""

    supports_sync = True
    supports_async = True
    backend_name = "table_queue"

    def __init__(self, table_queue: TableEventQueue) -> None:
        self._queue = table_queue

    async def publish_async(
        self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None
    ) -> str:
        return await self._queue.publish_async(channel, payload, metadata)

    def publish_sync(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        return self._queue.publish_sync(channel, payload, metadata)

    async def dequeue_async(self, channel: str, poll_interval: float | None = None) -> "EventMessage | None":
        event = await self._queue.dequeue_async(channel, poll_interval=poll_interval)
        if event is None:
            return None
        return self._to_message(event)

    def dequeue_sync(self, channel: str, poll_interval: float | None = None) -> "EventMessage | None":
        event = self._queue.dequeue_sync(channel, poll_interval=poll_interval)
        if event is None:
            return None
        return self._to_message(event)

    async def ack_async(self, event_id: str) -> None:
        await self._queue.ack_async(event_id)

    def ack_sync(self, event_id: str) -> None:
        self._queue.ack_sync(event_id)

    async def nack_async(self, event_id: str) -> None:
        await self._queue.nack_async(event_id)

    def nack_sync(self, event_id: str) -> None:
        self._queue.nack_sync(event_id)

    @staticmethod
    def _to_message(event: QueueEvent) -> EventMessage:
        return EventMessage(
            event_id=event.event_id,
            channel=event.channel,
            payload=event.payload,
            metadata=event.metadata,
            attempts=event.attempts,
            available_at=event.available_at,
            lease_expires_at=event.lease_expires_at,
            created_at=event.created_at,
        )
