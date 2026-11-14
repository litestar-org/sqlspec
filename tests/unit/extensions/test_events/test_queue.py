# pyright: reportPrivateUsage=false
"""Unit tests for SyncTableEventQueue and AsyncTableEventQueue."""

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.events.store import AiosqliteEventQueueStore
from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.core import StatementConfig
from sqlspec.core.parameters import structural_fingerprint
from sqlspec.exceptions import EventChannelError, ImproperConfigurationError
from sqlspec.extensions.events import (
    AsyncTableEventQueue,
    EventMessage,
    SyncTableEventQueue,
    build_queue_backend,
    parse_event_timestamp,
)
from tests.conftest import is_compiled


def _event_row(event_id: str = "event-1") -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "event_id": event_id,
        "channel": "alerts",
        "payload_json": {"ok": True},
        "metadata_json": None,
        "attempts": 0,
        "available_at": now,
        "lease_expires_at": None,
        "created_at": now,
    }


def test_table_event_queue_backend_capabilities() -> None:
    assert SyncTableEventQueue.supports_sync is True
    assert SyncTableEventQueue.supports_async is False
    assert AsyncTableEventQueue.supports_sync is False
    assert AsyncTableEventQueue.supports_async is True
    assert SyncTableEventQueue.backend_name == "poll_queue"
    assert AsyncTableEventQueue.backend_name == "poll_queue"


@pytest.mark.skipif(is_compiled(), reason="mypyc direct method calls bypass queue method monkeypatches")
def test_sync_table_queue_empty_poll_backoff_is_bounded_and_resets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    config = SqliteConfig(connection_config={"database": str(tmp_path / "sync-backoff.db")})
    queue = SyncTableEventQueue(config)
    rows = iter([None, None, None, _event_row(), None])
    sleeps: list[float] = []

    monkeypatch.setattr(SyncTableEventQueue, "_fetch_candidate", lambda *_args: next(rows))
    monkeypatch.setattr(SyncTableEventQueue, "_execute", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr("sqlspec.extensions.events._queue.time.sleep", sleeps.append)

    assert queue.dequeue("alerts", 0.08) is None
    assert queue.dequeue("alerts", 0.08) is None
    assert queue.dequeue("alerts", 0.01) is None
    assert queue.dequeue("alerts", 0.08) is not None
    assert queue.dequeue("alerts", 0.08) is None

    assert sleeps == [0.08, 0.08, 0.01, 0.08]


@pytest.mark.skipif(is_compiled(), reason="mypyc direct method calls bypass queue method monkeypatches")
@pytest.mark.anyio
async def test_async_table_queue_empty_poll_backoff_is_bounded_and_resets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "async-backoff.db")})
    queue = AsyncTableEventQueue(config)
    rows = iter([None, None, None, _event_row(), None])
    sleeps: list[float] = []

    async def _fetch_candidate(*_args: Any) -> dict[str, Any] | None:
        return next(rows)

    async def _execute(*_args: Any, **_kwargs: Any) -> int:
        return 1

    async def _sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(AsyncTableEventQueue, "_fetch_candidate", _fetch_candidate)
    monkeypatch.setattr(AsyncTableEventQueue, "_execute", _execute)
    monkeypatch.setattr("sqlspec.extensions.events._queue.asyncio.sleep", _sleep)

    assert await queue.dequeue("alerts", 0.08) is None
    assert await queue.dequeue("alerts", 0.08) is None
    assert await queue.dequeue("alerts", 0.01) is None
    assert await queue.dequeue("alerts", 0.08) is not None
    assert await queue.dequeue("alerts", 0.08) is None

    assert sleeps == [0.08, 0.08, 0.01, 0.08]


def test_table_queue_empty_poll_backoff_state_is_bounded(tmp_path: Any) -> None:
    config = SqliteConfig(connection_config={"database": str(tmp_path / "bounded-backoff.db")})
    queue = SyncTableEventQueue(config)

    for index in range(1_025):
        queue._next_empty_poll_delay(f"channel-{index}", 0.1)

    assert len(queue._empty_poll_delays) == 1_024
    assert "channel-0" not in queue._empty_poll_delays


def test_table_queue_batch_records_have_deterministic_created_order() -> None:
    payloads = [{"index": index} for index in range(3)]
    metadata = {"source": "test"}
    _, records = SyncTableEventQueue._batch_insert_parameters([("events", payload, metadata) for payload in payloads])

    created_at = [record["created_at"] for record in records]
    assert created_at == sorted(created_at)
    assert len(set(created_at)) == 3
    assert [record["payload_json"] for record in records] == payloads
    assert all(record["metadata_json"] is metadata for record in records)


def test_table_event_queue_default_table_name(tmp_path) -> None:
    """Default table name is sqlspec_event_queue."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    assert queue._table_name == "sqlspec_event_queue"


def test_table_event_queue_custom_table_name(tmp_path) -> None:
    """Custom table names are accepted and normalized."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, queue_table="custom_events")
    assert queue._table_name == "custom_events"


def test_table_event_queue_schema_qualified_table(tmp_path) -> None:
    """Schema-qualified table names are supported."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, queue_table="app_schema.events")
    assert queue._table_name == "app_schema.events"


def test_table_event_queue_invalid_table_name_raises(tmp_path) -> None:
    """Invalid table names raise EventChannelError."""

    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        SyncTableEventQueue(config, queue_table="invalid-name")


def test_table_event_queue_lease_seconds_default(tmp_path) -> None:
    """Default lease duration is 30 seconds."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    assert queue._lease_seconds == 30


def test_table_event_queue_custom_lease_seconds(tmp_path) -> None:
    """Custom lease duration is respected."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, lease_seconds=60)
    assert queue._lease_seconds == 60


def test_table_event_queue_retention_seconds_default(tmp_path) -> None:
    """Default retention is 86400 seconds (1 day)."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    assert queue._retention_seconds == 86_400


def test_table_event_queue_custom_retention_seconds(tmp_path) -> None:
    """Custom retention duration is respected."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, retention_seconds=3600)
    assert queue._retention_seconds == 3600


def test_table_event_queue_select_for_update_disabled(tmp_path) -> None:
    """SELECT FOR UPDATE is disabled by default."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    assert "FOR UPDATE" not in queue._select_statement.upper()


def test_table_event_queue_select_for_update_enabled(tmp_path) -> None:
    """SELECT FOR UPDATE clause is added when enabled."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, select_for_update=True)
    assert "FOR UPDATE" in queue._select_statement.upper()


def test_table_event_queue_skip_locked_requires_for_update(tmp_path) -> None:
    """SKIP LOCKED is only added when FOR UPDATE is enabled."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, skip_locked=True)
    assert "SKIP LOCKED" not in queue._select_statement.upper()

    queue_with_both = SyncTableEventQueue(config, select_for_update=True, skip_locked=True)
    assert "FOR UPDATE SKIP LOCKED" in queue_with_both._select_statement.upper()


def test_table_event_queue_insert_sql_contains_table(tmp_path) -> None:
    """Insert SQL references the configured table name."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, queue_table="my_events")
    assert "my_events" in queue._insert_statement


def test_table_event_queue_select_sql_contains_table(tmp_path) -> None:
    """Select SQL references the configured table name."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, queue_table="my_events")
    assert "my_events" in queue._select_statement


def test_table_event_queue_oracle_dialect_uses_fetch_first(tmp_path) -> None:
    """Oracle dialect uses FETCH FIRST instead of LIMIT."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, statement_config=StatementConfig(dialect="oracle")
    )
    queue = SyncTableEventQueue(config)
    assert "FETCH FIRST 1 ROWS ONLY" in queue._select_statement.upper()
    assert "LIMIT" not in queue._select_statement.upper()


def test_table_event_queue_oracle_dialect_locks_without_fetch_first(tmp_path) -> None:
    """Oracle locked candidate SQL avoids the invalid FETCH FIRST + FOR UPDATE shape."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, statement_config=StatementConfig(dialect="oracle")
    )
    queue = SyncTableEventQueue(config, select_for_update=True, skip_locked=True)
    select_sql = queue._select_statement.upper()
    assert "FOR UPDATE SKIP LOCKED" in select_sql
    assert "FETCH FIRST" not in select_sql


def test_table_event_queue_statement_config_property(tmp_path) -> None:
    """_statement_config property returns config's statement_config."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    assert queue._statement_config is config.statement_config


def test_fetch_candidate_parameter_fingerprint_stable_across_datetime_values() -> None:
    first = datetime(2026, 1, 1, tzinfo=UTC)
    second = datetime(2026, 1, 2, tzinfo=UTC)

    first_fingerprint = structural_fingerprint({
        "channel": "events",
        "available_cutoff": first,
        "pending_status": "pending",
        "leased_status": "leased",
        "lease_cutoff": first,
    })
    second_fingerprint = structural_fingerprint({
        "channel": "events",
        "available_cutoff": second,
        "pending_status": "pending",
        "leased_status": "leased",
        "lease_cutoff": second,
    })

    assert first_fingerprint == second_fingerprint


def test_ack_parameter_fingerprint_stable_across_event_ids() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)

    first = structural_fingerprint({"acked": "acked", "acked_at": now, "event_id": "event-1"})
    second = structural_fingerprint({"acked": "acked", "acked_at": now, "event_id": "event-2"})

    assert first == second


def test_statement_config_enables_caching_by_default() -> None:
    assert StatementConfig().enable_caching is True


def test_event_message_dataclass_fields() -> None:
    """EventMessage dataclass has expected fields."""
    now = datetime.now(UTC)
    event = EventMessage(
        event_id="abc123",
        channel="notifications",
        payload={"action": "test"},
        metadata={"source": "unit_test"},
        attempts=1,
        available_at=now,
        lease_expires_at=now,
        created_at=now,
    )
    assert event.event_id == "abc123"
    assert event.channel == "notifications"
    assert event.payload == {"action": "test"}
    assert event.metadata == {"source": "unit_test"}
    assert event.attempts == 1


def test_event_message_metadata_none() -> None:
    """EventMessage allows None metadata."""
    now = datetime.now(UTC)
    event = EventMessage(
        event_id="abc123",
        channel="notifications",
        payload={"action": "test"},
        metadata=None,
        attempts=0,
        available_at=now,
        lease_expires_at=None,
        created_at=now,
    )
    assert event.metadata is None
    assert event.lease_expires_at is None


def test_table_event_queue_hydrate_event_dict_payload(tmp_path) -> None:
    """_hydrate_event handles dict payloads directly."""
    now = datetime.now(UTC)
    row = {
        "event_id": "test123",
        "channel": "notifications",
        "payload_json": {"action": "refresh"},
        "metadata_json": None,
        "attempts": 0,
        "available_at": now,
        "lease_expires_at": None,
        "created_at": now,
    }
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    event = queue._hydrate_event(row, None)
    assert event.payload == {"action": "refresh"}
    assert event.metadata is None


def test_table_event_queue_hydrate_event_string_payload(tmp_path) -> None:
    """_hydrate_event deserializes JSON string payloads."""
    import json

    now = datetime.now(UTC)
    row = {
        "event_id": "test456",
        "channel": "events",
        "payload_json": json.dumps({"action": "update"}),
        "metadata_json": json.dumps({"user": "admin"}),
        "attempts": 2,
        "available_at": now,
        "lease_expires_at": now,
        "created_at": now,
    }
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    event = queue._hydrate_event(row, now)
    assert event.payload == {"action": "update"}
    assert event.metadata == {"user": "admin"}
    assert event.lease_expires_at == now


def test_table_event_queue_hydrate_event_non_dict_payload(tmp_path) -> None:
    """Non-dict payloads are wrapped in a value key."""
    import json

    now = datetime.now(UTC)
    row = {
        "event_id": "test789",
        "channel": "events",
        "payload_json": json.dumps("simple_string"),
        "metadata_json": json.dumps(42),
        "attempts": 0,
        "available_at": now,
        "lease_expires_at": None,
        "created_at": now,
    }
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    event = queue._hydrate_event(row, None)
    assert event.payload == {"value": "simple_string"}
    assert event.metadata == {"value": 42}


def test_parse_event_timestamp_from_string() -> None:
    """ISO format strings are parsed to datetime."""
    result = parse_event_timestamp("2024-01-15T10:30:00Z")
    assert isinstance(result, datetime)
    assert result.tzinfo is not None


def test_parse_event_timestamp_naive_string() -> None:
    """Naive datetime strings get UTC timezone added."""
    result = parse_event_timestamp("2024-01-15T10:30:00")
    assert result.tzinfo == UTC


def test_parse_event_timestamp_from_datetime() -> None:
    """Datetime objects are passed through."""
    now = datetime.now(UTC)
    result = parse_event_timestamp(now)
    assert result is now


def test_parse_event_timestamp_naive_datetime() -> None:
    """Naive datetime objects get UTC timezone added."""
    naive = datetime(2024, 1, 15, 10, 30, 0)
    result = parse_event_timestamp(naive)
    assert result.tzinfo == UTC


def test_parse_event_timestamp_invalid() -> None:
    """Invalid values return current UTC time."""
    result = parse_event_timestamp("not a date")
    assert isinstance(result, datetime)
    assert result.tzinfo is not None


def test_parse_event_timestamp_none() -> None:
    """None values return current UTC time."""
    result = parse_event_timestamp(None)
    assert isinstance(result, datetime)


def test_sync_table_event_queue_backend_name() -> None:
    """SyncTableEventQueue has correct backend_name."""
    assert SyncTableEventQueue.backend_name == "poll_queue"


def test_sync_table_event_queue_supports_sync() -> None:
    """SyncTableEventQueue supports sync operations only."""
    assert SyncTableEventQueue.supports_sync is True
    assert SyncTableEventQueue.supports_async is False


@pytest.mark.anyio
async def test_sync_and_async_queue_ack_skips_cleanup_when_cleanup_on_ack_false() -> None:
    """ack() skips synchronous _cleanup on both sync and async queues when cleanup_on_ack is False."""
    conn_cfg = {"project": "test-proj", "instance_id": "test-inst", "database_id": "test-db"}
    sync_config = SpannerSyncConfig(connection_config=conn_cfg)
    async_config = SpannerAsyncConfig(connection_config=conn_cfg)

    built_sync = build_queue_backend(sync_config, {}, adapter_name="spanner")
    built_async = build_queue_backend(async_config, {}, adapter_name="spanner")
    assert built_sync._cleanup_on_ack is False
    assert built_sync._use_run_in_transaction is True
    assert built_async._cleanup_on_ack is False
    assert built_async._use_run_in_transaction is True

    sync_statements: list[str] = []

    class FakeSyncSessionDriver:
        def execute(self, statement: Any, *_args: Any, **_kwargs: Any) -> Any:
            sync_statements.append(str(statement))
            return SimpleNamespace(rows_affected=1)

        def commit(self) -> None:
            pass

    class SyncConfigStub:
        _DEFAULT_SESSION_TRANSACTION = False
        statement_config = StatementConfig(dialect="spanner")
        supports_async = False
        extension_config: dict[str, Any] = {}

        def get_observability_runtime(self) -> Any:
            return SimpleNamespace(increment_metric=lambda *_args, **_kwargs: None)

        @contextmanager
        def provide_session(self, **_kwargs: Any) -> Iterator[Any]:
            yield FakeSyncSessionDriver()

    sync_queue = SyncTableEventQueue(cast("Any", SyncConfigStub()), cleanup_on_ack=False)
    sync_queue.ack("event-1")
    assert len(sync_statements) == 1
    assert "DELETE FROM" not in sync_statements[0]

    async_statements: list[str] = []

    class FakeAsyncSessionDriver:
        async def execute(self, statement: Any, *_args: Any, **_kwargs: Any) -> Any:
            async_statements.append(str(statement))
            return SimpleNamespace(rows_affected=1)

        async def commit(self) -> None:
            pass

    class AsyncConfigStub:
        _DEFAULT_SESSION_TRANSACTION = False
        statement_config = StatementConfig(dialect="spanner")
        supports_async = True
        extension_config: dict[str, Any] = {}

        def get_observability_runtime(self) -> Any:
            return SimpleNamespace(increment_metric=lambda *_args, **_kwargs: None)

        @asynccontextmanager
        async def provide_session(self, **_kwargs: Any) -> AsyncIterator[Any]:
            yield FakeAsyncSessionDriver()

    async_queue = AsyncTableEventQueue(cast("Any", AsyncConfigStub()), cleanup_on_ack=False)
    await async_queue.ack("event-1")
    assert len(async_statements) == 1
    assert "DELETE FROM" not in async_statements[0]


@pytest.mark.anyio
async def test_fetch_candidate_and_fetch_by_event_id_use_read_only_session_when_unlocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read-only candidate fetches pass transaction=False on SpannerSyncConfig and SpannerAsyncConfig."""
    sync_session_calls: list[dict[str, Any]] = []

    class FakeSyncDriver:
        def select_one_or_none(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    @contextmanager
    def _provide_sync_session(*_args: Any, **kwargs: Any) -> Iterator[Any]:
        sync_session_calls.append(kwargs)
        yield FakeSyncDriver()

    conn_cfg = {"project": "test-proj", "instance_id": "test-inst", "database_id": "test-db"}
    sync_config = SpannerSyncConfig(connection_config=conn_cfg)
    monkeypatch.setattr(sync_config, "provide_session", _provide_sync_session)
    unlocked_sync_queue = SyncTableEventQueue(sync_config, select_for_update=False)
    assert unlocked_sync_queue._fetch_candidate("alerts") is None
    assert unlocked_sync_queue._fetch_by_event_id("event-1") is None
    assert sync_session_calls == [{"transaction": False}, {"transaction": False}]

    sync_session_calls.clear()
    locked_sync_queue = SyncTableEventQueue(sync_config, select_for_update=True)
    assert locked_sync_queue._fetch_candidate("alerts") is None
    assert sync_session_calls == [{}]

    async_session_calls: list[dict[str, Any]] = []

    class FakeAsyncDriver:
        async def select_one_or_none(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    @asynccontextmanager
    async def _provide_async_session(*_args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        async_session_calls.append(kwargs)
        yield FakeAsyncDriver()

    async_config = SpannerAsyncConfig(connection_config=conn_cfg)
    monkeypatch.setattr(async_config, "provide_session", _provide_async_session)
    unlocked_async_queue = AsyncTableEventQueue(async_config, select_for_update=False)
    assert await unlocked_async_queue._fetch_candidate("alerts") is None
    assert await unlocked_async_queue._fetch_by_event_id("event-1") is None
    assert async_session_calls == [{"transaction": False}, {"transaction": False}]

    async_session_calls.clear()
    locked_async_queue = AsyncTableEventQueue(async_config, select_for_update=True)
    assert await locked_async_queue._fetch_candidate("alerts") is None
    assert async_session_calls == [{}]


@pytest.mark.anyio
async def test_execute_and_publish_many_route_through_driver_run_in_transaction_with_tx_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_execute and publish_many route DML through driver.run_in_transaction(fn) using tx_driver."""
    conn_cfg = {"project": "test-proj", "instance_id": "test-inst", "database_id": "test-db"}
    sync_session_kwargs: list[dict[str, Any]] = []
    sync_tx_calls: list[tuple[str, Any]] = []

    class FakeSyncTxDriver:
        def execute(self, statement: Any, *_args: Any, **_kwargs: Any) -> Any:
            sync_tx_calls.append(("execute", statement))
            return SimpleNamespace(rows_affected=1)

        def execute_many(self, sql: str, params: Any, **_kwargs: Any) -> Any:
            sync_tx_calls.append(("execute_many", (sql, len(params))))
            return SimpleNamespace(rows_affected=len(params))

    class FakeSyncOuterDriver:
        def run_in_transaction(self, fn: Any) -> Any:
            return fn(FakeSyncTxDriver())

    @contextmanager
    def _provide_sync_tx_session(*_args: Any, **kwargs: Any) -> Iterator[Any]:
        sync_session_kwargs.append(kwargs)
        yield FakeSyncOuterDriver()

    sync_config = SpannerSyncConfig(connection_config=conn_cfg)
    monkeypatch.setattr(sync_config, "provide_session", _provide_sync_tx_session)
    sync_queue = SyncTableEventQueue(sync_config, use_run_in_transaction=True)
    assert sync_queue._execute("UPDATE t SET x = 1", {"id": "1"}) == 1
    assert len(sync_queue.publish_many([("alerts", {"a": 1}, None), ("alerts", {"a": 2}, None)])) == 2
    assert sync_session_kwargs == [{"transaction": False}, {"transaction": False}]
    assert [kind for kind, _ in sync_tx_calls] == ["execute", "execute_many"]

    async_session_kwargs: list[dict[str, Any]] = []
    async_tx_calls: list[tuple[str, Any]] = []

    class FakeAsyncTxDriver:
        async def execute(self, statement: Any, *_args: Any, **_kwargs: Any) -> Any:
            async_tx_calls.append(("execute", statement))
            return SimpleNamespace(rows_affected=1)

        async def execute_many(self, sql: str, params: Any, **_kwargs: Any) -> Any:
            async_tx_calls.append(("execute_many", (sql, len(params))))
            return SimpleNamespace(rows_affected=len(params))

    class FakeAsyncOuterDriver:
        async def run_in_transaction(self, fn: Any) -> Any:
            return await fn(FakeAsyncTxDriver())

    @asynccontextmanager
    async def _provide_async_tx_session(*_args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        async_session_kwargs.append(kwargs)
        yield FakeAsyncOuterDriver()

    async_config = SpannerAsyncConfig(connection_config=conn_cfg)
    monkeypatch.setattr(async_config, "provide_session", _provide_async_tx_session)
    async_queue = AsyncTableEventQueue(async_config, use_run_in_transaction=True)
    assert await async_queue._execute("UPDATE t SET x = 1", {"id": "1"}) == 1
    assert len(await async_queue.publish_many([("alerts", {"a": 1}, None), ("alerts", {"a": 2}, None)])) == 2
    assert async_session_kwargs == [{"transaction": False}, {"transaction": False}]
    assert [kind for kind, _ in async_tx_calls] == ["execute", "execute_many"]


@pytest.mark.parametrize(
    ("max_attempts", "retry_backoff_seconds"),
    [(0, 1.0), (-2, 1.0), (False, 1.0), (5, -1.0), (5, float("inf")), (5, float("-inf")), (5, float("nan")), (5, True)],
    ids=[
        "zero_attempts",
        "negative_attempts",
        "bool_attempts",
        "negative_backoff",
        "inf_backoff",
        "neg_inf_backoff",
        "nan_backoff",
        "bool_backoff",
    ],
)
def test_table_event_queue_rejects_invalid_retry_settings(
    tmp_path: Any, max_attempts: Any, retry_backoff_seconds: Any
) -> None:
    """Invalid max_attempts or retry_backoff_seconds raise ImproperConfigurationError."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "invalid.db")})
    with pytest.raises(ImproperConfigurationError):
        SyncTableEventQueue(config, max_attempts=max_attempts, retry_backoff_seconds=retry_backoff_seconds)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("claim_channel", "future_offset", "initial_attempts", "expect_claimed", "expected_status", "expected_attempts"),
    [
        ("billing", 0, 0, True, "leased", 1),
        ("public_feed", 0, 0, False, "pending", 0),
        ("billing", 120, 0, False, "pending", 0),
        ("billing", 0, 5, False, "pending", 5),
    ],
    ids=["matching_channel", "mismatched_channel", "future_available_at", "exhausted_attempts"],
)
async def test_dequeue_by_event_id_enforces_channel_availability_and_attempt_bound(
    tmp_path: Any,
    is_async: bool,
    claim_channel: str,
    future_offset: int,
    initial_attempts: int,
    expect_claimed: bool,
    expected_status: str,
    expected_attempts: int,
) -> None:
    """dequeue_by_event_id claims only due rows on the requested channel below max_attempts."""
    db_file = str(tmp_path / f"claim_by_id_{is_async}_{claim_channel}_{future_offset}_{initial_attempts}.db")
    if is_async:
        async_cfg = AiosqliteConfig(connection_config={"database": db_file})
        async with async_cfg.provide_session() as driver:
            for ddl in AiosqliteEventQueueStore(async_cfg).create_statements():
                await driver.execute_script(ddl)
        async_queue = AsyncTableEventQueue(async_cfg, max_attempts=5)
        try:
            event_id = await async_queue.publish("billing", {"amount": 42})
            if future_offset or initial_attempts:
                target_time = async_queue._utcnow() + timedelta(seconds=future_offset)
                async with async_cfg.provide_session(transaction=True) as driver:
                    await driver.execute(
                        "UPDATE sqlspec_event_queue SET available_at = :available_at, attempts = :attempts WHERE event_id = :event_id",
                        {"available_at": target_time, "attempts": initial_attempts, "event_id": event_id},
                    )
                    await driver.commit()
            claimed = await async_queue.dequeue_by_event_id(event_id, claim_channel)
            async with async_cfg.provide_session() as driver:
                stored = await driver.select_one(
                    "SELECT status, attempts FROM sqlspec_event_queue WHERE event_id = :event_id",
                    {"event_id": event_id},
                )
        finally:
            await async_cfg.close_pool()
    else:
        sync_cfg = SqliteConfig(connection_config={"database": db_file})
        with sync_cfg.provide_session() as driver:
            for ddl in SqliteEventQueueStore(sync_cfg).create_statements():
                driver.execute_script(ddl)
        sync_queue = SyncTableEventQueue(sync_cfg, max_attempts=5)
        try:
            event_id = sync_queue.publish("billing", {"amount": 42})
            if future_offset or initial_attempts:
                target_time = sync_queue._utcnow() + timedelta(seconds=future_offset)
                with sync_cfg.provide_session(transaction=True) as driver:
                    driver.execute(
                        "UPDATE sqlspec_event_queue SET available_at = :available_at, attempts = :attempts WHERE event_id = :event_id",
                        {"available_at": target_time, "attempts": initial_attempts, "event_id": event_id},
                    )
                    driver.commit()
            claimed = sync_queue.dequeue_by_event_id(event_id, claim_channel)
            with sync_cfg.provide_session() as driver:
                stored = driver.select_one(
                    "SELECT status, attempts FROM sqlspec_event_queue WHERE event_id = :event_id",
                    {"event_id": event_id},
                )
        finally:
            sync_cfg.close_pool()

    assert (claimed is not None) is expect_claimed
    if claimed is not None:
        assert claimed.channel == "billing"
        assert claimed.attempts == expected_attempts
    assert stored["status"] == expected_status
    assert int(stored["attempts"]) == expected_attempts


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("attempts", "expected_status", "expected_backoff"),
    [(1, "pending", 1.0), (2, "pending", 2.0), (3, "pending", 4.0), (4, "pending", 8.0), (5, "failed", 0.0)],
)
async def test_nack_applies_exponential_backoff_and_fails_at_max_attempts(
    tmp_path: Any, is_async: bool, attempts: int, expected_status: str, expected_backoff: float
) -> None:
    """nack delays leased events exponentially below max_attempts and marks exhausted rows failed."""
    db_file = str(tmp_path / f"nack_{is_async}_{attempts}.db")
    fixed_now = datetime(2026, 11, 13, 12, 0, 0, tzinfo=UTC)

    if is_async:
        async_cfg = AiosqliteConfig(connection_config={"database": db_file})
        async with async_cfg.provide_session() as driver:
            for ddl in AiosqliteEventQueueStore(async_cfg).create_statements():
                await driver.execute_script(ddl)
        async_queue = AsyncTableEventQueue(async_cfg, max_attempts=5, retry_backoff_seconds=1.0)
        try:
            event_id = await async_queue.publish("orders", {"order": 1})
            async with async_cfg.provide_session(transaction=True) as driver:
                await driver.execute(
                    "UPDATE sqlspec_event_queue SET status = 'leased', attempts = :attempts, available_at = :now WHERE event_id = :event_id",
                    {"attempts": attempts, "now": fixed_now, "event_id": event_id},
                )
                await driver.commit()
            before = datetime.now(UTC)
            await async_queue.nack(event_id)
            after = datetime.now(UTC)
            async with async_cfg.provide_session() as driver:
                stored = await driver.select_one(
                    "SELECT status, available_at, lease_expires_at FROM sqlspec_event_queue WHERE event_id = :event_id",
                    {"event_id": event_id},
                )
            if expected_status == "failed":
                assert await async_queue.dequeue("orders") is None
                assert await async_queue.dequeue_by_event_id(event_id, "orders") is None
        finally:
            await async_cfg.close_pool()
    else:
        sync_cfg = SqliteConfig(connection_config={"database": db_file})
        with sync_cfg.provide_session() as driver:
            for ddl in SqliteEventQueueStore(sync_cfg).create_statements():
                driver.execute_script(ddl)
        sync_queue = SyncTableEventQueue(sync_cfg, max_attempts=5, retry_backoff_seconds=1.0)
        try:
            event_id = sync_queue.publish("orders", {"order": 1})
            with sync_cfg.provide_session(transaction=True) as driver:
                driver.execute(
                    "UPDATE sqlspec_event_queue SET status = 'leased', attempts = :attempts, available_at = :now WHERE event_id = :event_id",
                    {"attempts": attempts, "now": fixed_now, "event_id": event_id},
                )
                driver.commit()
            before = datetime.now(UTC)
            sync_queue.nack(event_id)
            after = datetime.now(UTC)
            with sync_cfg.provide_session() as driver:
                stored = driver.select_one(
                    "SELECT status, available_at, lease_expires_at FROM sqlspec_event_queue WHERE event_id = :event_id",
                    {"event_id": event_id},
                )
            if expected_status == "failed":
                assert sync_queue.dequeue("orders") is None
                assert sync_queue.dequeue_by_event_id(event_id, "orders") is None
        finally:
            sync_cfg.close_pool()

    assert stored["status"] == expected_status
    assert stored["lease_expires_at"] is None
    if expected_status == "pending":
        available_at = parse_event_timestamp(stored["available_at"])
        assert (
            (before.timestamp() + expected_backoff - 0.5)
            <= available_at.timestamp()
            <= (after.timestamp() + expected_backoff + 0.5)
        )


@pytest.mark.parametrize("initial_status", ["pending", "acked", "failed"])
def test_nack_ignores_non_leased_rows_and_dequeue_skips_attempt_bound(tmp_path: Any, initial_status: str) -> None:
    """nack leaves non-leased rows untouched and dequeue skips rows that reached max_attempts."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / f"non_leased_{initial_status}.db")})
    with config.provide_session() as driver:
        for ddl in SqliteEventQueueStore(config).create_statements():
            driver.execute_script(ddl)
    queue = SyncTableEventQueue(config, max_attempts=3, retry_backoff_seconds=5.0)
    baseline_time = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    try:
        event_id = queue.publish("alerts", {"n": 1})
        with config.provide_session(transaction=True) as driver:
            driver.execute(
                "UPDATE sqlspec_event_queue SET status = :status, available_at = :available_at, attempts = 3 WHERE event_id = :event_id",
                {"status": initial_status, "available_at": baseline_time, "event_id": event_id},
            )
            driver.commit()
        queue.nack(event_id)
        with config.provide_session() as driver:
            row = driver.select_one(
                "SELECT status, available_at, attempts FROM sqlspec_event_queue WHERE event_id = :event_id",
                {"event_id": event_id},
            )
        assert row["status"] == initial_status
        assert parse_event_timestamp(row["available_at"]) == baseline_time
        assert int(row["attempts"]) == 3
        assert queue.dequeue("alerts") is None
    finally:
        config.close_pool()
