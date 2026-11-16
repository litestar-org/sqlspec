"""Lease ownership for table event queue ack and nack."""

import inspect
import sqlite3
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.events.store import AiosqliteEventQueueStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.extensions.events import AsyncTableEventQueue, SyncTableEventQueue
from sqlspec.extensions.events._queue import _BaseTableEventQueue

if TYPE_CHECKING:
    from sqlspec.config import ExtensionConfigs

_QUEUE_TABLE = "evq_lease_ownership"


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


class _Harness:
    def __init__(
        self, config: Any, queue_class: Any, database: str, lease_seconds: int = 30, cleanup_on_ack: bool = False
    ) -> None:
        self.config = config
        self.database = database
        self.queue_class = queue_class
        self.lease_seconds = lease_seconds
        self.cleanup_on_ack = cleanup_on_ack

    def new_queue(self) -> Any:
        return self.queue_class(
            self.config,
            queue_table=_QUEUE_TABLE,
            lease_seconds=self.lease_seconds,
            cleanup_on_ack=self.cleanup_on_ack,
            retry_backoff_seconds=0.0,
        )

    async def status(self, event_id: str) -> "tuple[str, int] | None":
        sql = f"SELECT status, attempts FROM {_QUEUE_TABLE} WHERE event_id = ?"
        async with _session(self.config) as driver:
            row = await _maybe_await(driver.select_one_or_none(sql, event_id))
        return None if row is None else (row["status"], row["attempts"])


class _session:
    def __init__(self, config: Any) -> None:
        self._ctx = config.provide_session()

    async def __aenter__(self) -> Any:
        if hasattr(self._ctx, "__aenter__"):
            return await self._ctx.__aenter__()
        return self._ctx.__enter__()

    async def __aexit__(self, *exc: Any) -> None:
        if hasattr(self._ctx, "__aexit__"):
            await self._ctx.__aexit__(*exc)
        else:
            self._ctx.__exit__(*exc)


@pytest.fixture(params=["sqlite", "aiosqlite"])
async def harness(request: pytest.FixtureRequest, tmp_path: Path) -> "AsyncIterator[_Harness]":
    database = str(tmp_path / "evq.db")
    extension_config: ExtensionConfigs = {"events": {"queue_table": _QUEUE_TABLE}}
    if request.param == "sqlite":
        config: Any = SqliteConfig(connection_config={"database": database}, extension_config=extension_config)
        store: Any = SqliteEventQueueStore(config)
        with config.provide_session() as driver:
            for statement in store.create_statements():
                driver.execute_script(statement)
        queue_class: Any = SyncTableEventQueue
    else:
        config = AiosqliteConfig(connection_config={"database": database}, extension_config=extension_config)
        store = AiosqliteEventQueueStore(config)
        async with config.provide_session() as driver:
            for statement in store.create_statements():
                await driver.execute_script(statement)
        queue_class = AsyncTableEventQueue
    try:
        yield _Harness(config, queue_class, database)
    finally:
        await _maybe_await(config.close_pool())


async def test_foreign_ack_does_not_settle_unleased_event(harness: _Harness) -> None:
    owner = harness.new_queue()
    foreign = harness.new_queue()
    event_id = await _maybe_await(owner.publish("tenant_a", {"n": 1}))

    acked = await _maybe_await(foreign.ack(event_id))

    assert acked is False
    assert await harness.status(event_id) == ("pending", 0)


async def test_foreign_nack_does_not_requeue_leased_event(harness: _Harness) -> None:
    owner = harness.new_queue()
    foreign = harness.new_queue()
    event_id = await _maybe_await(owner.publish("tenant_b", {"n": 2}))
    leased = await _maybe_await(owner.dequeue("tenant_b"))
    assert leased is not None

    nacked = await _maybe_await(foreign.nack(event_id))

    assert nacked is False
    assert await harness.status(event_id) == ("leased", 1)


@pytest.mark.parametrize("cleanup_on_ack", [False, True], ids=["keep-acked", "cleanup-on-ack"])
async def test_owner_ack_settles_event(harness: _Harness, cleanup_on_ack: bool) -> None:
    harness.cleanup_on_ack = cleanup_on_ack
    owner = harness.new_queue()
    event_id = await _maybe_await(owner.publish("tenant_b", {"n": 2}))
    leased = await _maybe_await(owner.dequeue("tenant_b"))
    assert leased is not None

    acked = await _maybe_await(owner.ack(event_id))

    assert acked is True
    status = await harness.status(event_id)
    assert status is None or status[0] == "acked"


async def test_owner_nack_requeues_event(harness: _Harness) -> None:
    owner = harness.new_queue()
    event_id = await _maybe_await(owner.publish("tenant_b", {"n": 2}))
    assert await _maybe_await(owner.dequeue("tenant_b")) is not None

    nacked = await _maybe_await(owner.nack(event_id))

    assert nacked is True
    assert await harness.status(event_id) == ("pending", 1)


async def test_ack_twice_is_noop_second_time(harness: _Harness) -> None:
    owner = harness.new_queue()
    event_id = await _maybe_await(owner.publish("tenant_b", {"n": 2}))
    assert await _maybe_await(owner.dequeue("tenant_b")) is not None

    assert await _maybe_await(owner.ack(event_id)) is True
    assert await _maybe_await(owner.ack(event_id)) is False


async def test_ack_after_lease_reclaimed_is_noop(harness: _Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    harness.lease_seconds = 1
    first = harness.new_queue()
    second = harness.new_queue()
    event_id = await _maybe_await(first.publish("c", {"n": 3}))
    assert await _maybe_await(first.dequeue("c")) is not None
    after_expiry = datetime.now(UTC) + timedelta(seconds=harness.lease_seconds + 2)
    with monkeypatch.context() as clock:
        clock.setattr(_BaseTableEventQueue, "_utcnow", staticmethod(lambda: after_expiry))
        assert await _maybe_await(second.dequeue("c")) is not None

    acked = await _maybe_await(first.ack(event_id))

    assert acked is False
    assert await harness.status(event_id) == ("leased", 2)
    assert await _maybe_await(second.ack(event_id)) is True


@pytest.mark.parametrize("operation", ["ack", "nack"])
async def test_settle_after_lease_expired_is_refused(
    harness: _Harness, operation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = harness.new_queue()
    event_id = await _maybe_await(owner.publish("expired", {"n": 4}))
    assert await _maybe_await(owner.dequeue("expired")) is not None
    later = datetime.now(UTC) + timedelta(seconds=harness.lease_seconds + 1)
    monkeypatch.setattr(_BaseTableEventQueue, "_utcnow", staticmethod(lambda: later))

    settled = await _maybe_await(getattr(owner, operation)(event_id))

    assert settled is False
    assert await harness.status(event_id) == ("leased", 1)


@pytest.mark.parametrize("operation", ["ack", "nack"])
@pytest.mark.parametrize("cleanup_on_ack", [False, True], ids=["keep-acked", "cleanup-on-ack"])
async def test_settle_losing_race_after_ownership_check_is_refused(
    harness: _Harness, operation: str, cleanup_on_ack: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness.cleanup_on_ack = cleanup_on_ack
    owner = harness.new_queue()
    event_id = await _maybe_await(owner.publish("race", {"n": 5}))
    assert await _maybe_await(owner.dequeue("race")) is not None
    original_owned_row = _BaseTableEventQueue._owned_row  # pyright: ignore[reportPrivateUsage]

    def reclaim_after_check(self: Any, row: Any, lease: Any) -> Any:
        owned = original_owned_row(self, row, lease)
        connection = sqlite3.connect(harness.database)
        try:
            connection.execute(
                f"UPDATE {_QUEUE_TABLE} SET lease_expires_at = ? WHERE event_id = ?",
                ((datetime.now(UTC) + timedelta(hours=1)).isoformat(), event_id),
            )
            connection.commit()
        finally:
            connection.close()
        return owned

    monkeypatch.setattr(_BaseTableEventQueue, "_owned_row", reclaim_after_check)

    settled = await _maybe_await(getattr(owner, operation)(event_id))

    assert settled is False
    assert await harness.status(event_id) == ("leased", 1)
