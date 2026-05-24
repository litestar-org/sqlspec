"""Integration tests for Spanner ADK store."""

import json
from datetime import datetime, timezone
from typing import Any

import pytest

from sqlspec.extensions.adk import EventRecord
from tests.integration.adapters._adk_contract_helpers import (
    assert_session_atomic_scoped_write_contract,
    assert_session_empty_state_roundtrip,
    assert_session_scoped_state_contract,
    assert_session_sibling_app_isolation,
    assert_session_sibling_user_isolation,
    assert_session_temp_state_not_persisted,
)

pytestmark = [pytest.mark.spanner, pytest.mark.integration]


async def test_create_and_get_session(spanner_adk_store: Any) -> None:
    session_id = "session-create"
    await spanner_adk_store.delete_session(session_id)
    created = await spanner_adk_store.create_session(session_id, "app", "user", {"a": 1})
    assert created["id"] == session_id

    fetched = await spanner_adk_store.get_session(session_id)
    assert fetched is not None
    assert fetched["state"] == {"a": 1}


async def test_spanner_session_scoped_state_contract(spanner_adk_store: Any) -> None:
    """Spanner service reads merge app/user state from dedicated scoped tables."""
    await assert_session_scoped_state_contract(spanner_adk_store, marker="spanner")


async def test_spanner_session_atomic_scoped_write_contract(spanner_adk_store: Any) -> None:
    """Spanner routes scoped-state upserts inside the append/update transaction."""
    await assert_session_atomic_scoped_write_contract(spanner_adk_store, marker="spanner")


async def test_spanner_session_temp_state_not_persisted(spanner_adk_store: Any) -> None:
    """Spanner never persists temp:* through the service-level append_event path."""
    await assert_session_temp_state_not_persisted(spanner_adk_store, marker="spanner")


async def test_spanner_session_empty_state_roundtrip(spanner_adk_store: Any) -> None:
    """Spanner preserves empty session/app/user state through append_event_and_update_state."""
    await assert_session_empty_state_roundtrip(spanner_adk_store, marker="spanner")


async def test_spanner_session_sibling_app_isolation(spanner_adk_store: Any) -> None:
    """Spanner isolates app:* writes per app_name across sibling sessions."""
    await assert_session_sibling_app_isolation(spanner_adk_store, marker="spanner")


async def test_spanner_session_sibling_user_isolation(spanner_adk_store: Any) -> None:
    """Spanner isolates user:* writes per (app_name, user_id) across sibling sessions."""
    await assert_session_sibling_user_isolation(spanner_adk_store, marker="spanner")


async def test_update_session_state(spanner_adk_store: Any) -> None:
    session_id = "session-update"
    await spanner_adk_store.delete_session(session_id)
    await spanner_adk_store.create_session(session_id, "app", "user", {"a": 1})

    await spanner_adk_store.update_session_state(session_id, {"a": 2, "b": True})

    fetched = await spanner_adk_store.get_session(session_id)
    assert fetched is not None
    assert fetched["state"] == {"a": 2, "b": True}


async def test_list_sessions(spanner_adk_store: Any) -> None:
    await spanner_adk_store.delete_session("session-list-1")
    await spanner_adk_store.delete_session("session-list-2")
    await spanner_adk_store.delete_session("session-list-3")
    await spanner_adk_store.create_session("session-list-1", "app-list", "user1", {"v": 1})
    await spanner_adk_store.create_session("session-list-2", "app-list", "user1", {"v": 2})
    await spanner_adk_store.create_session("session-list-3", "app-list", "user2", {"v": 3})

    sessions = await spanner_adk_store.list_sessions("app-list", "user1")
    session_ids = {s["id"] for s in sessions}
    assert session_ids == {"session-list-1", "session-list-2"}


async def test_delete_session(spanner_adk_store: Any) -> None:
    session_id = "session-delete"
    await spanner_adk_store.delete_session(session_id)
    await spanner_adk_store.create_session(session_id, "app", "user", {"k": "v"})
    await spanner_adk_store.delete_session(session_id)

    assert await spanner_adk_store.get_session(session_id) is None


async def test_create_and_list_events(spanner_adk_store: Any) -> None:
    session_id = "session-events"
    await spanner_adk_store.delete_session(session_id)
    await spanner_adk_store.create_session(session_id, "app", "user", {"x": 1})

    event_one: EventRecord = {
        "session_id": session_id,
        "invocation_id": "event-1",
        "author": "user",
        "timestamp": datetime.now(timezone.utc),
        "event_data": {"id": "event-1", "content": {"msg": "hi"}, "app_name": "app", "user_id": "user"},
    }
    event_two: EventRecord = {
        "session_id": session_id,
        "invocation_id": "event-2",
        "author": "assistant",
        "timestamp": datetime.now(timezone.utc),
        "event_data": {"id": "event-2", "content": {"msg": "ok"}, "app_name": "app", "user_id": "user"},
    }

    await spanner_adk_store.append_event(event_one)
    await spanner_adk_store.append_event(event_two)

    events = await spanner_adk_store.get_events(session_id)
    assert len(events) == 2
    assert events[0]["author"] == "user"
    assert events[1]["author"] == "assistant"

    # Content is inside event_data in the new 5-column schema
    event0_data = (
        json.loads(events[0]["event_data"]) if isinstance(events[0]["event_data"], str) else events[0]["event_data"]
    )
    event1_data = (
        json.loads(events[1]["event_data"]) if isinstance(events[1]["event_data"], str) else events[1]["event_data"]
    )
    assert event0_data["content"] == {"msg": "hi"}
    assert event1_data["content"] == {"msg": "ok"}
