"""Behavior helpers for shared ADK session/event store contract tests."""

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlspec.extensions.adk import EventRecord


async def _aclose(config: Any) -> None:
    result = config.close_pool()
    if result is not None and hasattr(result, "__await__"):
        await result


def _event(session_id: str, index: int, when: datetime) -> EventRecord:
    return {
        "session_id": session_id,
        "invocation_id": f"inv-{index}",
        "author": "user",
        "timestamp": when,
        "event_json": {"id": f"event-{index}", "content": {"parts": [{"text": f"hello-{index}"}]}},
    }


async def assert_adk_create_tables_idempotent_contract(make_store: Any) -> None:
    """Creating ADK tables twice is a safe no-op."""
    config, store = make_store()
    try:
        await store.create_tables()
        await store.create_tables()
        assert await store.get_session("missing") is None
    finally:
        await _aclose(config)


async def assert_adk_session_round_trip_contract(make_store: Any) -> None:
    """Sessions persist empty and populated state as JSON through a round-trip."""
    config, store = make_store()
    try:
        await store.create_tables()
        empty = await store.create_session("session-empty", "app", "user", {})
        assert empty["state"] == {}

        created = await store.create_session("session-state", "app", "user", {"turn": 1, "nested": {"a": [1, 2]}})
        fetched = await store.get_session("session-state")
        assert created["state"] == {"turn": 1, "nested": {"a": [1, 2]}}
        assert fetched is not None
        assert fetched["state"] == {"turn": 1, "nested": {"a": [1, 2]}}
    finally:
        await _aclose(config)


async def assert_adk_get_nonexistent_session_contract(make_store: Any) -> None:
    """Reading a missing session returns None."""
    config, store = make_store()
    try:
        await store.create_tables()
        assert await store.get_session("nope") is None
    finally:
        await _aclose(config)


async def assert_adk_update_session_state_contract(make_store: Any) -> None:
    """update_session_state replaces the durable session state."""
    config, store = make_store()
    try:
        await store.create_tables()
        await store.create_session("session-update", "app", "user", {"count": 0})
        await store.update_session_state("session-update", {"count": 5})
        fetched = await store.get_session("session-update")
        assert fetched is not None
        assert fetched["state"] == {"count": 5}
    finally:
        await _aclose(config)


async def assert_adk_list_sessions_contract(make_store: Any) -> None:
    """list_sessions filters by app and optional user for tenant isolation."""
    config, store = make_store()
    try:
        await store.create_tables()
        await store.create_session("s1", "app", "user-a", {})
        await store.create_session("s2", "app", "user-a", {})
        await store.create_session("s3", "app", "user-b", {})
        await store.create_session("s4", "other", "user-a", {})

        app_sessions = await store.list_sessions("app")
        assert {row["id"] for row in app_sessions} == {"s1", "s2", "s3"}

        user_sessions = await store.list_sessions("app", "user-a")
        assert {row["id"] for row in user_sessions} == {"s1", "s2"}

        assert await store.list_sessions("empty-app") == []
    finally:
        await _aclose(config)


async def assert_adk_delete_session_cascade_contract(make_store: Any) -> None:
    """Deleting a session removes the session and its events."""
    config, store = make_store()
    try:
        await store.create_tables()
        session_id = "session-delete"
        await store.create_session(session_id, "app", "user", {})
        await store.append_event(_event(session_id, 0, datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)))

        await store.delete_session(session_id)
        assert await store.get_session(session_id) is None
        assert await store.get_events(session_id) == []
    finally:
        await _aclose(config)


async def assert_adk_append_and_get_events_contract(make_store: Any) -> None:
    """Appended events round-trip through get_events."""
    config, store = make_store()
    try:
        await store.create_tables()
        session_id = "session-events"
        await store.create_session(session_id, "app", "user", {})
        await store.append_event(_event(session_id, 1, datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)))

        events = await store.get_events(session_id)
        assert len(events) == 1
        assert events[0]["invocation_id"] == "inv-1"
        assert events[0]["event_json"] == {"id": "event-1", "content": {"parts": [{"text": "hello-1"}]}}
    finally:
        await _aclose(config)


async def assert_adk_append_event_and_update_state_contract(make_store: Any) -> None:
    """Atomic append updates state and stores the event in one round-trip."""
    config, store = make_store()
    try:
        await store.create_tables()
        session_id = "session-atomic"
        await store.create_session(session_id, "app", "user", {})
        updated = await store.append_event_and_update_state(
            _event(session_id, 1, datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)), session_id, {"turn": 1}
        )
        assert updated["state"] == {"turn": 1}

        session = await store.get_session(session_id)
        events = await store.get_events(session_id)
        assert session is not None
        assert session["state"] == {"turn": 1}
        assert len(events) == 1
        assert events[0]["invocation_id"] == "inv-1"
    finally:
        await _aclose(config)


async def assert_adk_get_events_filtering_contract(make_store: Any) -> None:
    """get_events honors after_timestamp and limit."""
    config, store = make_store()
    try:
        await store.create_tables()
        session_id = "session-filter"
        await store.create_session(session_id, "app", "user", {})
        base = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
        for index in range(3):
            await store.append_event(_event(session_id, index, base + timedelta(seconds=index)))

        events = await store.get_events(session_id, after_timestamp=base + timedelta(milliseconds=500), limit=1)
        assert len(events) == 1
        assert events[0]["invocation_id"] == "inv-1"
    finally:
        await _aclose(config)


async def assert_adk_reads_empty_when_tables_missing_contract(make_store: Any) -> None:
    """Read paths return None/empty when the ADK tables do not exist."""
    config, store = make_store()
    try:
        assert await store.get_session("missing") is None
        assert await store.list_sessions("app") == []
        assert await store.list_sessions("app", "user") == []
        assert await store.get_events("session-x") == []
    finally:
        await _aclose(config)
