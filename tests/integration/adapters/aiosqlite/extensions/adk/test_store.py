"""Integration tests for AioSQLite ADK session/event store."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
from sqlspec.extensions.adk import EventRecord

pytestmark = pytest.mark.xdist_group("sqlite")


async def _build_store(tmp_path: Path) -> tuple[AiosqliteConfig, AiosqliteADKStore]:
    db_path = tmp_path / "test_adk_store.db"
    config = AiosqliteConfig(connection_config={"database": str(db_path)})
    store = AiosqliteADKStore(config)
    await store.create_tables()
    return config, store


async def test_aiosqlite_session_owner_column_is_created_when_configured(tmp_path: Path) -> None:
    """Owner-column DDL matches create_session's optional owner_id insert path."""
    db_path = tmp_path / "test_adk_owner.db"
    config = AiosqliteConfig(
        connection_config={"database": str(db_path)}, extension_config={"adk": {"owner_id_column": "owner_id TEXT"}}
    )
    store = AiosqliteADKStore(config)
    try:
        await store.create_tables()
        await store.create_session("session-owner", "app", "user", {}, owner_id="tenant-1")

        async with config.provide_connection() as conn:
            cursor = await conn.execute("SELECT owner_id FROM adk_sessions WHERE id = ?", ("session-owner",))
            row = await cursor.fetchone()

        assert row == ("tenant-1",)
    finally:
        await config.close_pool()


async def test_aiosqlite_session_empty_state_round_trip(tmp_path: Path) -> None:
    """Empty session state is persisted as JSON, not NULL."""
    config, store = await _build_store(tmp_path)
    try:
        created = await store.create_session("session-empty", "app", "user", {})
        fetched = await store.get_session("session-empty")

        assert created["state"] == {}
        assert fetched is not None
        assert fetched["state"] == {}
    finally:
        await config.close_pool()


async def test_aiosqlite_append_event_and_update_state_is_atomic_contract(tmp_path: Path) -> None:
    """Event append and durable state update happen through the clean-break method."""
    config, store = await _build_store(tmp_path)
    try:
        session_id = "session-append-state"
        await store.create_session(session_id, "app", "user", {})

        event: EventRecord = {
            "session_id": session_id,
            "invocation_id": "inv-1",
            "author": "user",
            "timestamp": datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc),
            "event_json": {"id": "event-1", "content": {"parts": [{"text": "hello"}]}},
        }
        await store.append_event_and_update_state(event, session_id, {"turn": 1})

        session = await store.get_session(session_id)
        events = await store.get_events(session_id)

        assert session is not None
        assert session["state"] == {"turn": 1}
        assert len(events) == 1
        assert events[0]["invocation_id"] == "inv-1"
        assert events[0]["event_json"] == {"id": "event-1", "content": {"parts": [{"text": "hello"}]}}
    finally:
        await config.close_pool()


async def test_aiosqlite_get_events_filters_by_timestamp_and_limit(tmp_path: Path) -> None:
    """Event reads honor the clean-break after_timestamp and limit contract."""
    config, store = await _build_store(tmp_path)
    try:
        session_id = "session-filter"
        await store.create_session(session_id, "app", "user", {})
        base = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)

        for index in range(3):
            event: EventRecord = {
                "session_id": session_id,
                "invocation_id": f"inv-{index}",
                "author": "user",
                "timestamp": base + timedelta(seconds=index),
                "event_json": {"id": f"event-{index}"},
            }
            await store.append_event(event)

        events = await store.get_events(session_id, after_timestamp=base + timedelta(milliseconds=500), limit=1)

        assert len(events) == 1
        assert events[0]["invocation_id"] == "inv-1"
        assert events[0]["event_json"] == {"id": "event-1"}
    finally:
        await config.close_pool()
