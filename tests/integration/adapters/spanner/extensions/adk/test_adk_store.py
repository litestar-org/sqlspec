"""Integration tests for Spanner ADK store (sync)."""

import json
from typing import Any

import pytest

pytestmark = [pytest.mark.spanner, pytest.mark.integration]


def test_create_and_get_session(spanner_adk_store: Any) -> None:
    session_id = "session-create"
    spanner_adk_store.delete_session(session_id)
    created = spanner_adk_store.create_session(session_id, "app", "user", {"a": 1})
    assert created["id"] == session_id

    fetched = spanner_adk_store.get_session(session_id)
    assert fetched is not None
    assert fetched["state"] == {"a": 1}


def test_update_session_state(spanner_adk_store: Any) -> None:
    session_id = "session-update"
    spanner_adk_store.delete_session(session_id)
    spanner_adk_store.create_session(session_id, "app", "user", {"a": 1})

    spanner_adk_store.update_session_state(session_id, {"a": 2, "b": True})

    fetched = spanner_adk_store.get_session(session_id)
    assert fetched is not None
    assert fetched["state"] == {"a": 2, "b": True}


def test_list_sessions(spanner_adk_store: Any) -> None:
    spanner_adk_store.delete_session("session-list-1")
    spanner_adk_store.delete_session("session-list-2")
    spanner_adk_store.delete_session("session-list-3")
    spanner_adk_store.create_session("session-list-1", "app-list", "user1", {"v": 1})
    spanner_adk_store.create_session("session-list-2", "app-list", "user1", {"v": 2})
    spanner_adk_store.create_session("session-list-3", "app-list", "user2", {"v": 3})

    sessions = spanner_adk_store.list_sessions("app-list", "user1")
    session_ids = {s["id"] for s in sessions}
    assert session_ids == {"session-list-1", "session-list-2"}


def test_delete_session(spanner_adk_store: Any) -> None:
    session_id = "session-delete"
    spanner_adk_store.delete_session(session_id)
    spanner_adk_store.create_session(session_id, "app", "user", {"k": "v"})
    spanner_adk_store.delete_session(session_id)

    assert spanner_adk_store.get_session(session_id) is None


def test_create_and_list_events(spanner_adk_store: Any) -> None:
    session_id = "session-events"
    spanner_adk_store.delete_session(session_id)
    spanner_adk_store.create_session(session_id, "app", "user", {"x": 1})

    spanner_adk_store.create_event("event-1", session_id, "app", "user", author="user", content={"msg": "hi"})
    spanner_adk_store.create_event(
        "event-2",
        session_id,
        "app",
        "user",
        author="assistant",
        content={"msg": "ok"},
    )

    events = spanner_adk_store.list_events(session_id)
    assert len(events) == 2
    assert events[0]["author"] == "user"
    assert events[1]["author"] == "assistant"

    # Content is inside event_json in the new 5-column schema
    event0_data = json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    event1_data = json.loads(events[1]["event_json"]) if isinstance(events[1]["event_json"], str) else events[1]["event_json"]
    assert event0_data["content"] == {"msg": "hi"}
    assert event1_data["content"] == {"msg": "ok"}
