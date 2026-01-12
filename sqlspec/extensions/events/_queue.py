"""Table-backed queue implementation for EventChannel."""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlspec.core import SQL, StatementConfig
from sqlspec.extensions.events._hints import EventRuntimeHints, get_runtime_hints, resolve_adapter_name
from sqlspec.extensions.events._models import EventMessage
from sqlspec.extensions.events._payload import parse_event_timestamp
from sqlspec.extensions.events._store import normalize_queue_table_name
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

logger = get_logger("sqlspec.events.queue")

__all__ = ("AsyncTableEventQueue", "SyncTableEventQueue", "build_queue_backend")

_PENDING_STATUS = "pending"
_LEASED_STATUS = "leased"
_ACKED_STATUS = "acked"
_DEFAULT_TABLE = "sqlspec_event_queue"


class _BaseTableEventQueue:
    """Base class with shared SQL generation and hydration logic."""

    __slots__ = (
        "_ack_sql",
        "_acked_cleanup_sql",
        "_claim_sql",
        "_config",
        "_dialect",
        "_lease_seconds",
        "_max_claim_attempts",
        "_nack_sql",
        "_retention_seconds",
        "_runtime",
        "_select_by_id_sql",
        "_select_sql",
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
    ) -> None:
        self._config = config
        self._statement_config = config.statement_config
        self._runtime = config.get_observability_runtime()
        self._dialect = str(self._statement_config.dialect or "").lower() if self._statement_config else ""
        self._table_name = normalize_queue_table_name(queue_table or _DEFAULT_TABLE)
        self._lease_seconds = lease_seconds or 30
        self._retention_seconds = retention_seconds or 86_400
        self._max_claim_attempts = 5
        self._upsert_sql = self._build_insert_sql()
        self._select_sql = self._build_select_sql(bool(select_for_update), bool(skip_locked))
        self._select_by_id_sql = self._build_select_by_id_sql()
        self._claim_sql = self._build_claim_sql()
        self._ack_sql = self._build_ack_sql()
        self._nack_sql = self._build_nack_sql()
        self._acked_cleanup_sql = self._build_cleanup_sql()

    @property
    def statement_config(self) -> "StatementConfig":
        return self._statement_config

    def _build_insert_sql(self) -> str:
        columns = "event_id, channel, payload_json, metadata_json, status, available_at, lease_expires_at, attempts, created_at"
        values = ":event_id, :channel, :payload_json, :metadata_json, :status, :available_at, :lease_expires_at, :attempts, :created_at"
        return f"INSERT INTO {self._table_name} ({columns}) VALUES ({values})"

    def _build_select_sql(self, select_for_update: bool, skip_locked: bool) -> str:
        limit_clause = " FETCH FIRST 1 ROWS ONLY" if "oracle" in self._dialect else " LIMIT 1"
        base = (
            f"SELECT event_id, channel, payload_json, metadata_json, attempts, available_at, lease_expires_at, created_at "
            f"FROM {self._table_name} "
            "WHERE channel = :channel AND available_at <= :available_cutoff AND ("
            "status = :pending_status OR (status = :leased_status AND (lease_expires_at IS NULL OR lease_expires_at <= :lease_cutoff))"
            ") ORDER BY created_at ASC"
        )
        locking_clause = ""
        if select_for_update:
            locking_clause = " FOR UPDATE"
            if skip_locked:
                locking_clause += " SKIP LOCKED"
        return base + limit_clause + locking_clause

    def _build_select_by_id_sql(self) -> str:
        limit_clause = " FETCH FIRST 1 ROWS ONLY" if "oracle" in self._dialect else " LIMIT 1"
        return (
            f"SELECT event_id, channel, payload_json, metadata_json, attempts, available_at, lease_expires_at, created_at "
            f"FROM {self._table_name} WHERE event_id = :event_id" + limit_clause
        )

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
        return f"UPDATE {self._table_name} SET status = :pending, lease_expires_at = NULL, attempts = attempts + 1 WHERE event_id = :event_id"

    def _build_cleanup_sql(self) -> str:
        return f"DELETE FROM {self._table_name} WHERE status = :acked AND acknowledged_at IS NOT NULL AND acknowledged_at <= :cutoff"

    @staticmethod
    def _utcnow() -> "datetime":
        return datetime.now(timezone.utc)

    @staticmethod
    def _hydrate_event(row: "dict[str, Any]", lease_expires_at: "datetime | None") -> EventMessage:
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
        available_at = parse_event_timestamp(row.get("available_at"))
        created_at = parse_event_timestamp(row.get("created_at"))
        lease_value = lease_expires_at or row.get("lease_expires_at")
        lease_at = parse_event_timestamp(lease_value) if lease_value is not None else None
        return EventMessage(
            event_id=row["event_id"],
            channel=row["channel"],
            payload=payload_value,
            metadata=metadata_value,
            attempts=int(row.get("attempts", 0)),
            available_at=available_at,
            lease_expires_at=lease_at,
            created_at=created_at,
        )


class SyncTableEventQueue(_BaseTableEventQueue):
    """Sync table queue implementation."""

    __slots__ = ()

    supports_sync = True
    supports_async = False
    backend_name = "table_queue"

    def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        now = self._utcnow()
        self._execute(
            self._upsert_sql,
            {
                "event_id": event_id,
                "channel": channel,
                "payload_json": payload,
                "metadata_json": metadata,
                "status": _PENDING_STATUS,
                "available_at": now,
                "lease_expires_at": None,
                "attempts": 0,
                "created_at": now,
            },
        )
        self._runtime.increment_metric("events.publish")
        return event_id

    def dequeue(self, channel: str, poll_interval: float | None = None) -> "EventMessage | None":
        attempt = 0
        while attempt < self._max_claim_attempts:
            attempt += 1
            row = self._fetch_candidate(channel)
            if row is None:
                if poll_interval is not None and poll_interval > 0:
                    time.sleep(poll_interval)
                return None
            now = self._utcnow()
            leased_until = now + timedelta(seconds=self._lease_seconds)
            claimed = self._execute(
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

    def dequeue_by_event_id(self, event_id: str) -> "EventMessage | None":
        row = self._fetch_by_event_id(event_id)
        if row is None:
            return None
        now = self._utcnow()
        leased_until = now + timedelta(seconds=self._lease_seconds)
        claimed = self._execute(
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

    def ack(self, event_id: str) -> None:
        now = self._utcnow()
        self._execute(self._ack_sql, {"acked": _ACKED_STATUS, "acked_at": now, "event_id": event_id})
        self._cleanup(now)
        self._runtime.increment_metric("events.ack")

    def nack(self, event_id: str) -> None:
        self._execute(self._nack_sql, {"pending": _PENDING_STATUS, "event_id": event_id})
        self._runtime.increment_metric("events.nack")

    def shutdown(self) -> None:
        """Shutdown the backend (no-op for table queue)."""

    def _cleanup(self, reference: "datetime") -> None:
        cutoff = reference - timedelta(seconds=self._retention_seconds)
        self._execute(self._acked_cleanup_sql, {"acked": _ACKED_STATUS, "cutoff": cutoff})

    def _fetch_candidate(self, channel: str) -> "dict[str, Any] | None":
        current_time = self._utcnow()
        with cast("AbstractContextManager[SyncDriverAdapterBase]", self._config.provide_session()) as driver:
            return driver.select_one_or_none(
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

    def _fetch_by_event_id(self, event_id: str) -> "dict[str, Any] | None":
        with cast("AbstractContextManager[SyncDriverAdapterBase]", self._config.provide_session()) as driver:
            return driver.select_one_or_none(
                SQL(self._select_by_id_sql, {"event_id": event_id}, statement_config=self._statement_config)
            )

    def _execute(self, sql: str, parameters: "dict[str, Any]") -> int:
        with cast(
            "AbstractContextManager[SyncDriverAdapterBase]", self._config.provide_session(transaction=True)
        ) as driver:
            result = driver.execute(SQL(sql, parameters, statement_config=self._statement_config))
            driver.commit()
            return result.rows_affected


class AsyncTableEventQueue(_BaseTableEventQueue):
    """Async table queue implementation."""

    __slots__ = ()

    supports_sync = False
    supports_async = True
    backend_name = "table_queue"

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        now = self._utcnow()
        await self._execute(
            self._upsert_sql,
            {
                "event_id": event_id,
                "channel": channel,
                "payload_json": payload,
                "metadata_json": metadata,
                "status": _PENDING_STATUS,
                "available_at": now,
                "lease_expires_at": None,
                "attempts": 0,
                "created_at": now,
            },
        )
        self._runtime.increment_metric("events.publish")
        return event_id

    async def dequeue(self, channel: str, poll_interval: float | None = None) -> "EventMessage | None":
        attempt = 0
        while attempt < self._max_claim_attempts:
            attempt += 1
            row = await self._fetch_candidate(channel)
            if row is None:
                if poll_interval is not None and poll_interval > 0:
                    await asyncio.sleep(poll_interval)
                return None
            now = self._utcnow()
            leased_until = now + timedelta(seconds=self._lease_seconds)
            claimed = await self._execute(
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

    async def dequeue_by_event_id(self, event_id: str) -> "EventMessage | None":
        row = await self._fetch_by_event_id(event_id)
        if row is None:
            return None
        now = self._utcnow()
        leased_until = now + timedelta(seconds=self._lease_seconds)
        claimed = await self._execute(
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

    async def ack(self, event_id: str) -> None:
        now = self._utcnow()
        await self._execute(self._ack_sql, {"acked": _ACKED_STATUS, "acked_at": now, "event_id": event_id})
        await self._cleanup(now)
        self._runtime.increment_metric("events.ack")

    async def nack(self, event_id: str) -> None:
        await self._execute(self._nack_sql, {"pending": _PENDING_STATUS, "event_id": event_id})
        self._runtime.increment_metric("events.nack")

    async def shutdown(self) -> None:
        """Shutdown the backend (no-op for table queue)."""

    async def _cleanup(self, reference: "datetime") -> None:
        cutoff = reference - timedelta(seconds=self._retention_seconds)
        await self._execute(self._acked_cleanup_sql, {"acked": _ACKED_STATUS, "cutoff": cutoff})

    async def _fetch_candidate(self, channel: str) -> "dict[str, Any] | None":
        current_time = self._utcnow()
        async with cast(
            "AbstractAsyncContextManager[AsyncDriverAdapterBase]", self._config.provide_session()
        ) as driver:
            return await driver.select_one_or_none(
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

    async def _fetch_by_event_id(self, event_id: str) -> "dict[str, Any] | None":
        async with cast(
            "AbstractAsyncContextManager[AsyncDriverAdapterBase]", self._config.provide_session()
        ) as driver:
            return await driver.select_one_or_none(
                SQL(self._select_by_id_sql, {"event_id": event_id}, statement_config=self._statement_config)
            )

    async def _execute(self, sql: str, parameters: "dict[str, Any]") -> int:
        async with cast(
            "AbstractAsyncContextManager[AsyncDriverAdapterBase]", self._config.provide_session(transaction=True)
        ) as driver:
            result = await driver.execute(SQL(sql, parameters, statement_config=self._statement_config))
            await driver.commit()
            return result.rows_affected


def build_queue_backend(
    config: "DatabaseConfigProtocol[Any, Any, Any]",
    extension_settings: "dict[str, Any] | None" = None,
    *,
    adapter_name: "str | None" = None,
    hints: "EventRuntimeHints | None" = None,
) -> "SyncTableEventQueue | AsyncTableEventQueue":
    """Build a table queue backend using adapter hints and extension overrides."""
    settings = dict(extension_settings or {})
    resolved_adapter = adapter_name or resolve_adapter_name(config)
    runtime_hints = hints or get_runtime_hints(resolved_adapter, config)
    kwargs: dict[str, Any] = {
        "queue_table": settings.get("queue_table"),
        "lease_seconds": _resolve_int_setting(settings, "lease_seconds", runtime_hints.lease_seconds),
        "retention_seconds": _resolve_int_setting(settings, "retention_seconds", runtime_hints.retention_seconds),
        "select_for_update": _resolve_bool_setting(settings, "select_for_update", runtime_hints.select_for_update),
        "skip_locked": _resolve_bool_setting(settings, "skip_locked", runtime_hints.skip_locked),
    }
    if config.is_async:
        return AsyncTableEventQueue(config, **kwargs)
    return SyncTableEventQueue(config, **kwargs)


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
