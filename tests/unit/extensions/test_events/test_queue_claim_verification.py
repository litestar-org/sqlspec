# pyright: reportPrivateUsage=false
"""Unit tests for table-queue claim verification on rowcount-blind drivers.

Some drivers (arrow-odbc) cannot report rows affected and return zero for
every DML statement. The table queue must confirm claim ownership through the
persisted lease token instead of trusting the zero rowcount.
"""

from datetime import timedelta
from typing import Any

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.events.store import AiosqliteEventQueueStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.extensions.events import AsyncTableEventQueue, SyncTableEventQueue
from tests.conftest import requires_interpreted

_QUEUE_TABLE = "evq_claims"


def _create_sync_claim_queue(tmp_path: Any) -> "tuple[SqliteConfig, SyncTableEventQueue]":
    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "claims.db")},
        extension_config={"events": {"queue_table": _QUEUE_TABLE}},
    )
    store = SqliteEventQueueStore(config)
    with config.provide_session() as driver:
        for statement in store.create_statements():
            driver.execute_script(statement)
    return config, SyncTableEventQueue(config, queue_table=_QUEUE_TABLE)


async def _create_async_claim_queue(tmp_path: Any) -> "tuple[AiosqliteConfig, AsyncTableEventQueue]":
    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "claims_async.db")},
        extension_config={"events": {"queue_table": _QUEUE_TABLE}},
    )
    store = AiosqliteEventQueueStore(config)
    async with config.provide_session() as driver:
        for statement in store.create_statements():
            await driver.execute_script(statement)
    return config, AsyncTableEventQueue(config, queue_table=_QUEUE_TABLE)


@requires_interpreted
def test_sync_dequeue_delivers_when_rows_affected_unreported(tmp_path: Any, monkeypatch: Any) -> None:
    """A successful claim UPDATE reported as zero rows still delivers the event."""
    config, queue = _create_sync_claim_queue(tmp_path)
    try:
        event_id = queue.publish("alerts", {"type": "alert"})
        original = SyncTableEventQueue._execute_with_driver

        def _zero_reporting(self: Any, driver: Any, sql: str, parameters: "dict[str, Any]") -> int:
            original(self, driver, sql, parameters)
            return 0

        monkeypatch.setattr(SyncTableEventQueue, "_execute_with_driver", _zero_reporting)
        event = queue.dequeue("alerts")
        assert event is not None
        assert event.event_id == event_id
        assert queue.dequeue("alerts") is None
    finally:
        config.close_pool()


@requires_interpreted
def test_sync_dequeue_by_event_id_delivers_when_rows_affected_unreported(tmp_path: Any, monkeypatch: Any) -> None:
    """dequeue_by_event_id verifies claim ownership when the rowcount is zero."""
    config, queue = _create_sync_claim_queue(tmp_path)
    try:
        event_id = queue.publish("alerts", {"type": "alert"})
        original = SyncTableEventQueue._execute_with_driver

        def _zero_reporting(self: Any, driver: Any, sql: str, parameters: "dict[str, Any]") -> int:
            original(self, driver, sql, parameters)
            return 0

        monkeypatch.setattr(SyncTableEventQueue, "_execute_with_driver", _zero_reporting)
        event = queue.dequeue_by_event_id(event_id)
        assert event is not None
        assert event.event_id == event_id
    finally:
        config.close_pool()


@requires_interpreted
def test_sync_dequeue_ignores_claim_owned_by_other_consumer(tmp_path: Any, monkeypatch: Any) -> None:
    """A zero rowcount with a foreign lease token is a genuinely lost claim."""
    config, queue = _create_sync_claim_queue(tmp_path)
    try:
        queue.publish("alerts", {"type": "alert"})
        original = SyncTableEventQueue._execute_with_driver
        rival_lease = queue._utcnow() + timedelta(seconds=300)

        def _rival_claim(self: Any, driver: Any, sql: str, parameters: "dict[str, Any]") -> int:
            if sql == self._claim_statement:
                original(self, driver, sql, {**parameters, "lease_expires_at": rival_lease})
                return 0
            return original(self, driver, sql, parameters)

        monkeypatch.setattr(SyncTableEventQueue, "_execute_with_driver", _rival_claim)
        assert queue.dequeue("alerts") is None
    finally:
        config.close_pool()


@requires_interpreted
async def test_async_dequeue_delivers_when_rows_affected_unreported(tmp_path: Any, monkeypatch: Any) -> None:
    """The async queue verifies claims through the lease token as well."""
    config, queue = await _create_async_claim_queue(tmp_path)
    try:
        event_id = await queue.publish("alerts", {"type": "alert"})
        original = AsyncTableEventQueue._execute_with_driver

        async def _zero_reporting(self: Any, driver: Any, sql: str, parameters: "dict[str, Any]") -> int:
            await original(self, driver, sql, parameters)
            return 0

        monkeypatch.setattr(AsyncTableEventQueue, "_execute_with_driver", _zero_reporting)
        event = await queue.dequeue("alerts")
        assert event is not None
        assert event.event_id == event_id
        assert await queue.dequeue("alerts") is None
    finally:
        await config.close_pool()
