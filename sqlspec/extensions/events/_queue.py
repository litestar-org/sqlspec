"""Table-backed queue implementation for EventChannel."""

import asyncio
import time
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, cast

from typing_extensions import final

from sqlspec.core import SQL, StatementConfig
from sqlspec.extensions.events._hints import EventRuntimeHints, get_runtime_hints, resolve_adapter_name
from sqlspec.extensions.events._models import EventMessage
from sqlspec.extensions.events._names import normalize_queue_table_name
from sqlspec.extensions.events._payload import coerce_dict, coerce_optional_dict, parse_event_timestamp
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Sequence
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("AsyncTableEventQueue", "SyncTableEventQueue", "build_queue_backend")

logger = get_logger("sqlspec.events.queue")


_PENDING_STATUS = "pending"
_LEASED_STATUS = "leased"
_ACKED_STATUS = "acked"
_DEFAULT_TABLE = "sqlspec_event_queue"
_MAX_EMPTY_POLL_CHANNELS = 1_024


class _BaseTableEventQueue:
    """Base class with shared SQL generation and hydration logic."""

    __slots__ = (
        "_ack_statement",
        "_acked_cleanup_statement",
        "_claim_statement",
        "_config",
        "_dialect",
        "_empty_poll_delays",
        "_insert_statement",
        "_lease_seconds",
        "_max_claim_attempts",
        "_nack_statement",
        "_retention_seconds",
        "_runtime",
        "_select_by_id_statement",
        "_select_for_update",
        "_select_statement",
        "_statement_config",
        "_table_name",
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
        self._empty_poll_delays: dict[str, float] = {}
        self._table_name = normalize_queue_table_name(queue_table or _DEFAULT_TABLE)
        self._lease_seconds = lease_seconds or 30
        self._retention_seconds = retention_seconds or 86_400
        self._max_claim_attempts = 5
        self._select_for_update = bool(select_for_update)
        self._insert_statement = self._insert_sql()
        self._select_statement = self._select_sql(self._select_for_update, bool(skip_locked))
        self._select_by_id_statement = self._select_by_id_sql()
        self._claim_statement = self._claim_sql()
        self._ack_statement = self._ack_sql()
        self._nack_statement = self._nack_sql()
        self._acked_cleanup_statement = self._cleanup_sql()

    @property
    def statement_config(self) -> "StatementConfig":
        return self._statement_config

    def _insert_sql(self) -> str:
        columns = "event_id, channel, payload_json, metadata_json, status, available_at, lease_expires_at, attempts, created_at"
        values = ":event_id, :channel, :payload_json, :metadata_json, :status, :available_at, :lease_expires_at, :attempts, :created_at"
        return f"INSERT INTO {self._table_name} ({columns}) VALUES ({values})"

    def _select_sql(self, select_for_update: bool, skip_locked: bool) -> str:
        top_clause = "TOP 1 " if self._uses_tsql_limit() else ""
        limit_clause = "" if self._uses_oracle_locking_select(select_for_update) else self._row_limit_clause()
        base = f"SELECT {top_clause}event_id, channel, payload_json, metadata_json, attempts, available_at, lease_expires_at, created_at FROM {self._table_name} WHERE channel = :channel AND available_at <= :available_cutoff AND (status = :pending_status OR (status = :leased_status AND (lease_expires_at IS NULL OR lease_expires_at <= :lease_cutoff))) ORDER BY created_at ASC, event_id ASC"
        locking_clause = ""
        if select_for_update:
            locking_clause = " FOR UPDATE"
            if skip_locked:
                locking_clause += " SKIP LOCKED"
        return base + limit_clause + locking_clause

    def _select_by_id_sql(self) -> str:
        top_clause = "TOP 1 " if self._uses_tsql_limit() else ""
        limit_clause = self._row_limit_clause()
        base = f"SELECT {top_clause}event_id, channel, payload_json, metadata_json, attempts, available_at, lease_expires_at, created_at FROM {self._table_name} WHERE event_id = :event_id"
        return base + limit_clause

    def _uses_tsql_limit(self) -> bool:
        return self._dialect in {"mssql", "tsql"} or "sql server" in self._dialect

    def _row_limit_clause(self) -> str:
        if self._uses_tsql_limit():
            return ""
        if "oracle" in self._dialect:
            return " FETCH FIRST 1 ROWS ONLY"
        return " LIMIT 1"

    def _uses_oracle_locking_select(self, select_for_update: bool | None = None) -> bool:
        locking_enabled = self._select_for_update if select_for_update is None else select_for_update
        return bool(locking_enabled) and "oracle" in self._dialect

    def _next_empty_poll_delay(self, channel: str, poll_interval: "float | None") -> float:
        if poll_interval is None or poll_interval <= 0:
            return 0.0
        if channel not in self._empty_poll_delays and len(self._empty_poll_delays) >= _MAX_EMPTY_POLL_CHANNELS:
            self._empty_poll_delays.pop(next(iter(self._empty_poll_delays)))
        delay = poll_interval
        self._empty_poll_delays[channel] = delay
        self._runtime.record_metric("events.poll.backoff", delay)
        return delay

    def _reset_empty_poll_delay(self, channel: str) -> None:
        self._empty_poll_delays.pop(channel, None)

    def _claim_sql(self) -> str:
        return f"UPDATE {self._table_name} SET status = :claimed_status, lease_expires_at = :lease_expires_at, attempts = attempts + 1 WHERE event_id = :event_id AND (status = :pending_status OR (status = :leased_status AND (lease_expires_at IS NULL OR lease_expires_at <= :lease_reentry_cutoff)))"

    def _ack_sql(self) -> str:
        return f"UPDATE {self._table_name} SET status = :acked, acknowledged_at = :acked_at WHERE event_id = :event_id"

    def _nack_sql(self) -> str:
        return f"UPDATE {self._table_name} SET status = :pending, lease_expires_at = NULL, attempts = attempts + 1 WHERE event_id = :event_id"

    def _cleanup_sql(self) -> str:
        return f"DELETE FROM {self._table_name} WHERE status = :acked AND acknowledged_at IS NOT NULL AND acknowledged_at <= :cutoff"

    @staticmethod
    def _candidate_parameters(channel: str, current_time: "datetime") -> "dict[str, Any]":
        return {
            "channel": channel,
            "available_cutoff": current_time,
            "pending_status": _PENDING_STATUS,
            "leased_status": _LEASED_STATUS,
            "lease_cutoff": current_time,
        }

    @staticmethod
    def _claim_parameters(row: "dict[str, Any]", now: "datetime", leased_until: "datetime") -> "dict[str, Any]":
        return {
            "claimed_status": _LEASED_STATUS,
            "lease_expires_at": leased_until,
            "event_id": row["event_id"],
            "pending_status": _PENDING_STATUS,
            "leased_status": _LEASED_STATUS,
            "lease_reentry_cutoff": now,
        }

    @staticmethod
    def _utcnow() -> "datetime":
        return datetime.now(timezone.utc)

    @classmethod
    def _batch_insert_parameters(
        cls, events: "Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]"
    ) -> "tuple[list[str], list[dict[str, Any]]]":
        now = cls._utcnow()
        event_ids: list[str] = []
        records: list[dict[str, Any]] = []
        for index, (channel, payload, metadata) in enumerate(events):
            event_id = uuid4().hex
            event_ids.append(event_id)
            records.append({
                "event_id": event_id,
                "channel": channel,
                "payload_json": payload,
                "metadata_json": metadata,
                "status": _PENDING_STATUS,
                "available_at": now,
                "lease_expires_at": None,
                "attempts": 0,
                "created_at": now + timedelta(microseconds=index),
            })
        return event_ids, records

    @staticmethod
    def _claim_verified(row: "dict[str, Any] | None", leased_until: "datetime") -> bool:
        """Confirm claim ownership by matching the stored lease against the claimer's token.

        Drivers that cannot report rows affected return zero for a successful
        claim UPDATE, so a zero rowcount alone cannot distinguish a won claim
        from a lost race. The persisted ``lease_expires_at`` value identifies
        the winning claimer.
        """
        if row is None:
            return False
        lease_value = row.get("lease_expires_at")
        if lease_value is None:
            return False
        return parse_event_timestamp(lease_value) == leased_until

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
        metadata_obj: Any | None
        if isinstance(metadata_raw, dict):
            metadata_obj = metadata_raw
        elif metadata_raw is not None:
            metadata_obj = from_json(metadata_raw)
        else:
            metadata_obj = None
        payload_value = coerce_dict(payload_obj)
        metadata_value = coerce_optional_dict(metadata_obj)
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


@final
class SyncTableEventQueue(_BaseTableEventQueue):
    """Sync table queue implementation."""

    __slots__ = ()

    supports_sync: ClassVar[bool] = True
    supports_async: ClassVar[bool] = False
    backend_name: ClassVar[str] = "poll_queue"

    def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        now = self._utcnow()
        self._execute(
            self._insert_statement,
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

    def publish_many(self, events: "Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]") -> list[str]:
        """Bulk-insert independent events in one transaction."""
        if not events:
            return []
        event_ids, records = self._batch_insert_parameters(events)
        with cast(
            "AbstractContextManager[SyncDriverAdapterBase]", self._config.provide_session(transaction=True)
        ) as driver:
            driver.execute_many(self._insert_statement, records, statement_config=self._statement_config)
            driver.commit()
        self._runtime.increment_metric("events.publisher.session")
        self._runtime.increment_metric("events.publisher.statement")
        self._runtime.increment_metric("events.publish", len(records))
        return event_ids

    def dequeue(self, channel: str, poll_interval: float | None = None) -> "EventMessage | None":
        attempt = 0
        while attempt < self._max_claim_attempts:
            attempt += 1
            if self._select_for_update:
                event = self._claim_locked_candidate(channel)
                if event is not None:
                    self._reset_empty_poll_delay(channel)
                    return event
                self._runtime.increment_metric("events.poll.empty")
                delay = self._next_empty_poll_delay(channel, poll_interval)
                if delay > 0:
                    time.sleep(delay)
                return None
            row = self._fetch_candidate(channel)
            if row is None:
                self._runtime.increment_metric("events.poll.empty")
                delay = self._next_empty_poll_delay(channel, poll_interval)
                if delay > 0:
                    time.sleep(delay)
                return None
            now = self._utcnow()
            leased_until = now + timedelta(seconds=self._lease_seconds)
            claimed = self._execute(self._claim_statement, self._claim_parameters(row, now, leased_until))
            if not claimed:
                claimed = self._claim_verified(self._fetch_by_event_id(row["event_id"]), leased_until)
            if claimed:
                self._reset_empty_poll_delay(channel)
                return self._hydrate_event(row, leased_until)
        return None

    def dequeue_by_event_id(self, event_id: str) -> "EventMessage | None":
        row = self._fetch_by_event_id(event_id)
        if row is None:
            return None
        now = self._utcnow()
        leased_until = now + timedelta(seconds=self._lease_seconds)
        claimed = self._execute(self._claim_statement, self._claim_parameters(row, now, leased_until))
        if not claimed:
            claimed = self._claim_verified(self._fetch_by_event_id(row["event_id"]), leased_until)
        if claimed:
            event = self._hydrate_event(row, leased_until)
            self._reset_empty_poll_delay(event.channel)
            return event
        return None

    def ack(self, event_id: str) -> None:
        now = self._utcnow()
        self._execute(self._ack_statement, {"acked": _ACKED_STATUS, "acked_at": now, "event_id": event_id})
        self._cleanup(now)
        self._runtime.increment_metric("events.ack")

    def nack(self, event_id: str) -> None:
        self._execute(self._nack_statement, {"pending": _PENDING_STATUS, "event_id": event_id})
        self._runtime.increment_metric("events.nack")

    def shutdown(self) -> None:
        """Shutdown the backend (no-op for table queue)."""

    def _cleanup(self, reference: "datetime") -> None:
        cutoff = reference - timedelta(seconds=self._retention_seconds)
        self._execute(self._acked_cleanup_statement, {"acked": _ACKED_STATUS, "cutoff": cutoff})

    def _fetch_candidate(self, channel: str) -> "dict[str, Any] | None":
        current_time = self._utcnow()
        self._runtime.increment_metric("events.poll.query")
        with cast("AbstractContextManager[SyncDriverAdapterBase]", self._config.provide_session()) as driver:
            return driver.select_one_or_none(
                # SQL allocation here is intentional: DB round-trip dominates by >=100x,
                # and the pipeline LRU avoids re-parsing after first use via structural_fingerprint.
                SQL(
                    self._select_statement,
                    self._candidate_parameters(channel, current_time),
                    statement_config=self._statement_config,
                )
            )

    def _fetch_by_event_id(self, event_id: str) -> "dict[str, Any] | None":
        with cast("AbstractContextManager[SyncDriverAdapterBase]", self._config.provide_session()) as driver:
            return driver.select_one_or_none(
                SQL(self._select_by_id_statement, {"event_id": event_id}, statement_config=self._statement_config)
            )

    def _execute(self, sql: str, parameters: "dict[str, Any]") -> int:
        with cast(
            "AbstractContextManager[SyncDriverAdapterBase]", self._config.provide_session(transaction=True)
        ) as driver:
            rows_affected = self._execute_with_driver(driver, sql, parameters)
            driver.commit()
            return rows_affected

    def _claim_locked_candidate(self, channel: str) -> "EventMessage | None":
        current_time = self._utcnow()
        with cast(
            "AbstractContextManager[SyncDriverAdapterBase]", self._config.provide_session(transaction=True)
        ) as driver:
            try:
                row = self._fetch_candidate_with_driver(driver, channel, current_time)
                if row is None:
                    driver.rollback()
                    return None
                now = self._utcnow()
                leased_until = now + timedelta(seconds=self._lease_seconds)
                claimed = self._execute_with_driver(
                    driver, self._claim_statement, self._claim_parameters(row, now, leased_until)
                )
                if not claimed:
                    verify_row = driver.select_one_or_none(
                        SQL(
                            self._select_by_id_statement,
                            {"event_id": row["event_id"]},
                            statement_config=self._statement_config,
                        )
                    )
                    claimed = self._claim_verified(verify_row, leased_until)
                if not claimed:
                    driver.rollback()
                    return None
                driver.commit()
                return self._hydrate_event(row, leased_until)
            except Exception:
                with suppress(Exception):
                    driver.rollback()
                raise

    def _fetch_candidate_with_driver(
        self, driver: "SyncDriverAdapterBase", channel: str, current_time: "datetime"
    ) -> "dict[str, Any] | None":
        statement = SQL(
            self._select_statement,
            self._candidate_parameters(channel, current_time),
            statement_config=self._statement_config,
        )
        if self._uses_oracle_locking_select():
            return self._fetch_oracle_candidate_with_driver(driver, statement)
        return driver.select_one_or_none(statement)

    def _fetch_oracle_candidate_with_driver(
        self, driver: "SyncDriverAdapterBase", statement: "SQL"
    ) -> "dict[str, Any] | None":
        from sqlspec.adapters.oracledb.core import collect_sync_rows

        oracle_driver = cast("Any", driver)
        sql, prepared_parameters = oracle_driver._compiled_sql(statement, self._statement_config)
        with oracle_driver.with_cursor(oracle_driver.connection) as cursor:
            cursor.execute(sql, prepared_parameters or {})
            row = cursor.fetchone()
            if row is None:
                return None
            column_names, requires_lob_coercion = oracle_driver._resolve_row_metadata(cursor.description)
            rows, column_names = collect_sync_rows(
                [row],
                cursor.description,
                oracle_driver.driver_features,
                column_names=column_names,
                requires_lob_coercion=requires_lob_coercion,
            )
        if not rows:
            return None
        return dict(zip(column_names, rows[0], strict=False))

    def _execute_with_driver(self, driver: "SyncDriverAdapterBase", sql: str, parameters: "dict[str, Any]") -> int:
        result = driver.execute(SQL(sql, parameters, statement_config=self._statement_config))
        return result.rows_affected


@final
class AsyncTableEventQueue(_BaseTableEventQueue):
    """Async table queue implementation."""

    __slots__ = ()

    supports_sync: ClassVar[bool] = False
    supports_async: ClassVar[bool] = True
    backend_name: ClassVar[str] = "poll_queue"

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        now = self._utcnow()
        await self._execute(
            self._insert_statement,
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

    async def publish_many(self, events: "Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]") -> list[str]:
        """Bulk-insert independent events in one transaction."""
        if not events:
            return []
        event_ids, records = self._batch_insert_parameters(events)
        async with cast(
            "AbstractAsyncContextManager[AsyncDriverAdapterBase]", self._config.provide_session(transaction=True)
        ) as driver:
            await driver.execute_many(self._insert_statement, records, statement_config=self._statement_config)
            await driver.commit()
        self._runtime.increment_metric("events.publisher.session")
        self._runtime.increment_metric("events.publisher.statement")
        self._runtime.increment_metric("events.publish", len(records))
        return event_ids

    async def dequeue(self, channel: str, poll_interval: float | None = None) -> "EventMessage | None":
        attempt = 0
        while attempt < self._max_claim_attempts:
            attempt += 1
            if self._select_for_update:
                event = await self._claim_locked_candidate(channel)
                if event is not None:
                    self._reset_empty_poll_delay(channel)
                    return event
                self._runtime.increment_metric("events.poll.empty")
                delay = self._next_empty_poll_delay(channel, poll_interval)
                if delay > 0:
                    await asyncio.sleep(delay)
                return None
            row = await self._fetch_candidate(channel)
            if row is None:
                self._runtime.increment_metric("events.poll.empty")
                delay = self._next_empty_poll_delay(channel, poll_interval)
                if delay > 0:
                    await asyncio.sleep(delay)
                return None
            now = self._utcnow()
            leased_until = now + timedelta(seconds=self._lease_seconds)
            claimed = await self._execute(self._claim_statement, self._claim_parameters(row, now, leased_until))
            if not claimed:
                claimed = self._claim_verified(await self._fetch_by_event_id(row["event_id"]), leased_until)
            if claimed:
                self._reset_empty_poll_delay(channel)
                return self._hydrate_event(row, leased_until)
        return None

    async def dequeue_by_event_id(self, event_id: str) -> "EventMessage | None":
        row = await self._fetch_by_event_id(event_id)
        if row is None:
            return None
        now = self._utcnow()
        leased_until = now + timedelta(seconds=self._lease_seconds)
        claimed = await self._execute(self._claim_statement, self._claim_parameters(row, now, leased_until))
        if not claimed:
            claimed = self._claim_verified(await self._fetch_by_event_id(row["event_id"]), leased_until)
        if claimed:
            event = self._hydrate_event(row, leased_until)
            self._reset_empty_poll_delay(event.channel)
            return event
        return None

    async def ack(self, event_id: str) -> None:
        now = self._utcnow()
        await self._execute(self._ack_statement, {"acked": _ACKED_STATUS, "acked_at": now, "event_id": event_id})
        await self._cleanup(now)
        self._runtime.increment_metric("events.ack")

    async def nack(self, event_id: str) -> None:
        await self._execute(self._nack_statement, {"pending": _PENDING_STATUS, "event_id": event_id})
        self._runtime.increment_metric("events.nack")

    async def shutdown(self) -> None:
        """Shutdown the backend (no-op for table queue)."""

    async def _cleanup(self, reference: "datetime") -> None:
        cutoff = reference - timedelta(seconds=self._retention_seconds)
        await self._execute(self._acked_cleanup_statement, {"acked": _ACKED_STATUS, "cutoff": cutoff})

    async def _fetch_candidate(self, channel: str) -> "dict[str, Any] | None":
        current_time = self._utcnow()
        self._runtime.increment_metric("events.poll.query")
        async with cast(
            "AbstractAsyncContextManager[AsyncDriverAdapterBase]", self._config.provide_session()
        ) as driver:
            return await driver.select_one_or_none(
                # SQL allocation here is intentional: DB round-trip dominates by >=100x,
                # and the pipeline LRU avoids re-parsing after first use via structural_fingerprint.
                SQL(
                    self._select_statement,
                    self._candidate_parameters(channel, current_time),
                    statement_config=self._statement_config,
                )
            )

    async def _fetch_by_event_id(self, event_id: str) -> "dict[str, Any] | None":
        async with cast(
            "AbstractAsyncContextManager[AsyncDriverAdapterBase]", self._config.provide_session()
        ) as driver:
            return await driver.select_one_or_none(
                SQL(self._select_by_id_statement, {"event_id": event_id}, statement_config=self._statement_config)
            )

    async def _execute(self, sql: str, parameters: "dict[str, Any]") -> int:
        async with cast(
            "AbstractAsyncContextManager[AsyncDriverAdapterBase]", self._config.provide_session(transaction=True)
        ) as driver:
            rows_affected = await self._execute_with_driver(driver, sql, parameters)
            await driver.commit()
            return rows_affected

    async def _claim_locked_candidate(self, channel: str) -> "EventMessage | None":
        current_time = self._utcnow()
        async with cast(
            "AbstractAsyncContextManager[AsyncDriverAdapterBase]", self._config.provide_session(transaction=True)
        ) as driver:
            try:
                row = await self._fetch_candidate_with_driver(driver, channel, current_time)
                if row is None:
                    await driver.rollback()
                    return None
                now = self._utcnow()
                leased_until = now + timedelta(seconds=self._lease_seconds)
                claimed = await self._execute_with_driver(
                    driver, self._claim_statement, self._claim_parameters(row, now, leased_until)
                )
                if not claimed:
                    verify_row = await driver.select_one_or_none(
                        SQL(
                            self._select_by_id_statement,
                            {"event_id": row["event_id"]},
                            statement_config=self._statement_config,
                        )
                    )
                    claimed = self._claim_verified(verify_row, leased_until)
                if not claimed:
                    await driver.rollback()
                    return None
                await driver.commit()
                return self._hydrate_event(row, leased_until)
            except Exception:
                with suppress(Exception):
                    await driver.rollback()
                raise

    async def _fetch_candidate_with_driver(
        self, driver: "AsyncDriverAdapterBase", channel: str, current_time: "datetime"
    ) -> "dict[str, Any] | None":
        statement = SQL(
            self._select_statement,
            self._candidate_parameters(channel, current_time),
            statement_config=self._statement_config,
        )
        if self._uses_oracle_locking_select():
            return await self._fetch_oracle_candidate_with_driver(driver, statement)
        return await driver.select_one_or_none(statement)

    async def _fetch_oracle_candidate_with_driver(
        self, driver: "AsyncDriverAdapterBase", statement: "SQL"
    ) -> "dict[str, Any] | None":
        from sqlspec.adapters.oracledb.core import collect_async_rows

        oracle_driver = cast("Any", driver)
        sql, prepared_parameters = oracle_driver._compiled_sql(statement, self._statement_config)
        async with oracle_driver.with_cursor(oracle_driver.connection) as cursor:
            await cursor.execute(sql, prepared_parameters or {})
            row = await cursor.fetchone()
            if row is None:
                return None
            column_names, requires_lob_coercion = oracle_driver._resolve_row_metadata(cursor.description)
            rows, column_names = await collect_async_rows(
                [row],
                cursor.description,
                oracle_driver.driver_features,
                column_names=column_names,
                requires_lob_coercion=requires_lob_coercion,
            )
        if not rows:
            return None
        return dict(zip(column_names, rows[0], strict=False))

    async def _execute_with_driver(
        self, driver: "AsyncDriverAdapterBase", sql: str, parameters: "dict[str, Any]"
    ) -> int:
        result = await driver.execute(SQL(sql, parameters, statement_config=self._statement_config))
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
