"""Integration tests for SQLite ADK session/event store."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk import SqliteADKStore
from sqlspec.extensions.adk import EventRecord
from tests.integration.adapters._adk_contract_helpers import (
    assert_session_event_cleanup_contract,
    assert_session_event_store_contract,
    assert_session_get_session_renewal_contract,
    assert_session_table_lifecycle_contract,
)

pytestmark = pytest.mark.xdist_group("sqlite")


async def _build_store(tmp_path: Path) -> tuple[SqliteConfig, SqliteADKStore]:
    db_path = tmp_path / "test_adk_store.db"
    config = SqliteConfig(connection_config={"database": str(db_path)})
    store = SqliteADKStore(config)
    await store.create_tables()
    return config, store


async def test_sqlite_session_empty_state_round_trip(tmp_path: Path) -> None:
    """Empty session state is persisted as JSON, not NULL."""
    config, store = await _build_store(tmp_path)
    try:
        created = await store.create_session("session-empty", "app", "user", {})
        fetched = await store.get_session("session-empty")

        assert created["state"] == {}
        assert fetched is not None
        assert fetched["state"] == {}
    finally:
        config.close_pool()


async def test_sqlite_session_event_store_shared_contract(tmp_path: Path) -> None:
    """SQLite satisfies the shared ADK session/event store acceptance contract."""
    config, store = await _build_store(tmp_path)
    try:
        await assert_session_event_store_contract(store, marker="sqlite")
    finally:
        config.close_pool()


async def test_sqlite_session_event_cleanup_contract(tmp_path: Path) -> None:
    """SQLite satisfies the shared ADK cleanup hook contract."""
    config, store = await _build_store(tmp_path)
    try:
        await assert_session_event_cleanup_contract(store, marker="sqlite")
    finally:
        config.close_pool()


async def test_sqlite_session_get_session_renewal_contract(tmp_path: Path) -> None:
    """SQLite can touch session update_time while reading a session."""
    config, store = await _build_store(tmp_path)
    try:
        await assert_session_get_session_renewal_contract(store, marker="sqlite")
    finally:
        config.close_pool()


async def test_sqlite_session_table_lifecycle_contract(tmp_path: Path) -> None:
    """SQLite can drop and recreate its ADK session tables programmatically."""
    config, store = await _build_store(tmp_path)
    try:
        await assert_session_table_lifecycle_contract(store, marker="sqlite")
    finally:
        config.close_pool()


async def test_sqlite_append_event_and_update_state_is_atomic_contract(tmp_path: Path) -> None:
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
            "event_data": {"id": "event-1", "content": {"parts": [{"text": "hello"}]}},
        }
        await store.append_event_and_update_state(event, session_id, {"turn": 1})

        session = await store.get_session(session_id)
        events = await store.get_events(session_id)

        assert session is not None
        assert session["state"] == {"turn": 1}
        assert len(events) == 1
        assert events[0]["invocation_id"] == "inv-1"
        assert events[0]["event_data"] == {"id": "event-1", "content": {"parts": [{"text": "hello"}]}}
    finally:
        config.close_pool()


async def test_sqlite_reads_return_empty_when_tables_missing(tmp_path: Path) -> None:
    """Read paths must match the asyncpg reference: None/[] when tables don't exist."""
    db_path = tmp_path / "test_adk_no_tables.db"
    config = SqliteConfig(connection_config={"database": str(db_path)})
    store = SqliteADKStore(config)
    try:
        assert await store.get_session("missing") is None
        assert await store.list_sessions("app") == []
        assert await store.list_sessions("app", "user") == []
        assert await store.get_events("session-x") == []
    finally:
        config.close_pool()


async def test_sqlite_get_events_filters_by_timestamp_and_limit(tmp_path: Path) -> None:
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
                "event_data": {"id": f"event-{index}"},
            }
            await store.append_event(event)

        events = await store.get_events(session_id, after_timestamp=base + timedelta(milliseconds=500), limit=1)

        assert len(events) == 1
        assert events[0]["invocation_id"] == "inv-1"
        assert events[0]["event_data"] == {"id": "event-1"}
    finally:
        config.close_pool()
