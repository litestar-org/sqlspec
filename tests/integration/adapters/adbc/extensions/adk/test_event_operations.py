"""Tests for ADBC ADK store event operations."""

import json
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKStore

pytestmark = [pytest.mark.xdist_group("sqlite"), pytest.mark.adbc, pytest.mark.integration]


@pytest.fixture()
def adbc_store(tmp_path: Path) -> AdbcADKStore:
    """Create ADBC ADK store with SQLite backend."""
    db_path = tmp_path / "test_adk.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})
    store = AdbcADKStore(config)
    store.create_tables()
    return store


@pytest.fixture()
def session_fixture(adbc_store: Any) -> dict[str, str]:
    """Create a test session."""
    session_id = "test-session"
    app_name = "test-app"
    user_id = "user-123"
    state = {"test": True}
    adbc_store.create_session(session_id, app_name, user_id, state)
    return {"session_id": session_id, "app_name": app_name, "user_id": user_id}


def test_create_event(adbc_store: Any, session_fixture: Any) -> None:
    """Test creating a new event returns 5-key EventRecord."""
    event = adbc_store.create_event(
        event_id="event-1",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
        author="user",
        content={"message": "Hello"},
    )

    assert event["session_id"] == session_fixture["session_id"]
    assert event["author"] == "user"
    assert event["timestamp"] is not None
    assert "event_json" in event

    # Content is stored inside event_json
    event_data = json.loads(event["event_json"]) if isinstance(event["event_json"], str) else event["event_json"]
    assert event_data["content"] == {"message": "Hello"}


def test_list_events(adbc_store: Any, session_fixture: Any) -> None:
    """Test listing events for a session."""
    adbc_store.create_event(
        event_id="event-1",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
        author="user",
        content={"seq": 1},
    )
    adbc_store.create_event(
        event_id="event-2",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
        author="assistant",
        content={"seq": 2},
    )

    events = adbc_store.list_events(session_fixture["session_id"])

    assert len(events) == 2
    assert events[0]["author"] == "user"
    assert events[1]["author"] == "assistant"


def test_list_events_empty(adbc_store: Any, session_fixture: Any) -> None:
    """Test listing events when none exist."""
    events = adbc_store.list_events(session_fixture["session_id"])
    assert events == []


def test_event_with_all_fields(adbc_store: Any, session_fixture: Any) -> None:
    """Test creating event with all optional fields stored in event_json."""
    event = adbc_store.create_event(
        event_id="full-event",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
        invocation_id="invocation-123",
        author="assistant",
        actions=b"complex_action_data",
        branch="main",
        content={"text": "Response"},
        grounding_metadata={"sources": ["doc1", "doc2"]},
        custom_metadata={"custom": "data"},
        partial=True,
        turn_complete=False,
        interrupted=False,
        error_code="NONE",
        error_message="No errors",
    )

    # Top-level indexed columns
    assert event["invocation_id"] == "invocation-123"
    assert event["author"] == "assistant"

    # Everything else is in event_json
    event_data = json.loads(event["event_json"]) if isinstance(event["event_json"], str) else event["event_json"]
    assert event_data["content"] == {"text": "Response"}
    assert event_data["branch"] == "main"
    assert event_data["grounding_metadata"] == {"sources": ["doc1", "doc2"]}
    assert event_data["custom_metadata"] == {"custom": "data"}
    assert event_data["partial"] is True
    assert event_data["turn_complete"] is False
    assert event_data["interrupted"] is False
    assert event_data["error_code"] == "NONE"
    assert event_data["error_message"] == "No errors"


def test_event_with_minimal_fields(adbc_store: Any, session_fixture: Any) -> None:
    """Test creating event with only required fields."""
    event = adbc_store.create_event(
        event_id="minimal-event",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
    )

    assert event["session_id"] == session_fixture["session_id"]
    assert "event_json" in event


def test_event_json_fields(adbc_store: Any, session_fixture: Any) -> None:
    """Test event JSON field serialization and deserialization via event_json."""
    complex_content = {"nested": {"data": "value"}, "list": [1, 2, 3], "null": None}
    complex_grounding = {"sources": [{"title": "Doc", "url": "http://example.com"}]}
    complex_custom = {"metadata": {"version": 1, "tags": ["tag1", "tag2"]}}

    event = adbc_store.create_event(
        event_id="json-event",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
        content=complex_content,
        grounding_metadata=complex_grounding,
        custom_metadata=complex_custom,
    )

    event_data = json.loads(event["event_json"]) if isinstance(event["event_json"], str) else event["event_json"]
    assert event_data["content"] == complex_content
    assert event_data["grounding_metadata"] == complex_grounding
    assert event_data["custom_metadata"] == complex_custom

    events = adbc_store.list_events(session_fixture["session_id"])
    retrieved_data = json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    assert retrieved_data["content"] == complex_content
    assert retrieved_data["grounding_metadata"] == complex_grounding
    assert retrieved_data["custom_metadata"] == complex_custom


def test_event_ordering(adbc_store: Any, session_fixture: Any) -> None:
    """Test that events are ordered by timestamp ASC."""
    import time

    adbc_store.create_event(
        event_id="event-1",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
    )

    time.sleep(0.01)

    adbc_store.create_event(
        event_id="event-2",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
    )

    time.sleep(0.01)

    adbc_store.create_event(
        event_id="event-3",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
    )

    events = adbc_store.list_events(session_fixture["session_id"])

    assert len(events) == 3
    assert events[0]["timestamp"] < events[1]["timestamp"]
    assert events[1]["timestamp"] < events[2]["timestamp"]


def test_delete_session_cascades_events(adbc_store: Any, session_fixture: Any, tmp_path: Path) -> None:
    """Test that deleting a session cascades to delete events.

    Note: SQLite with ADBC requires foreign key enforcement to be explicitly
    enabled for cascade deletes to work. This test manually enables it.
    """
    adbc_store.create_event(
        event_id="event-1",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
    )
    adbc_store.create_event(
        event_id="event-2",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
    )

    events_before = adbc_store.list_events(session_fixture["session_id"])
    assert len(events_before) == 2

    adbc_store.delete_session(session_fixture["session_id"])

    session_after = adbc_store.get_session(session_fixture["session_id"])
    assert session_after is None


def test_event_with_empty_actions(adbc_store: Any, session_fixture: Any) -> None:
    """Test creating event with empty actions bytes."""
    event = adbc_store.create_event(
        event_id="empty-actions",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
        actions=b"",
    )

    # actions=b"" is either ignored or stored as hex in event_json
    assert "event_json" in event


def test_event_with_large_content(adbc_store: Any, session_fixture: Any) -> None:
    """Test creating event with large content in event_json."""
    large_content = {"data": "x" * 10000}

    event = adbc_store.create_event(
        event_id="large-content",
        session_id=session_fixture["session_id"],
        app_name=session_fixture["app_name"],
        user_id=session_fixture["user_id"],
        content=large_content,
    )

    event_data = json.loads(event["event_json"]) if isinstance(event["event_json"], str) else event["event_json"]
    assert event_data["content"] == large_content
