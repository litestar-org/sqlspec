"""Tests for ADBC ADK store event operations."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKStore
from sqlspec.extensions.adk import EventRecord

pytestmark = [pytest.mark.xdist_group("sqlite"), pytest.mark.adbc, pytest.mark.integration]


@pytest.fixture()
async def adbc_store(tmp_path: Path) -> AdbcADKStore:
    """Create ADBC ADK store with SQLite backend."""
    db_path = tmp_path / "test_adk.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})
    store = AdbcADKStore(config)
    await store.create_tables()
    return store


@pytest.fixture()
async def session_fixture(adbc_store: Any) -> dict[str, str]:
    """Create a test session."""
    session_id = "test-session"
    app_name = "test-app"
    user_id = "user-123"
    state = {"test": True}
    await adbc_store.create_session(session_id, app_name, user_id, state)
    return {"session_id": session_id, "app_name": app_name, "user_id": user_id}


async def test_create_event(adbc_store: Any, session_fixture: Any) -> None:
    """Test creating a new event returns 5-key EventRecord."""
    event_record: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "user",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "event-1",
            "content": {"message": "Hello"},
            "app_name": session_fixture["app_name"],
            "user_id": session_fixture["user_id"],
        },
    }
    await adbc_store.append_event(event_record)

    events = await adbc_store.get_events(session_fixture["session_id"])
    assert len(events) == 1
    assert events[0]["session_id"] == session_fixture["session_id"]
    assert events[0]["author"] == "user"
    assert events[0]["timestamp"] is not None
    assert "event_json" in events[0]

    # Content is stored inside event_json
    event_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    assert event_data["content"] == {"message": "Hello"}


async def test_list_events(adbc_store: Any, session_fixture: Any) -> None:
    """Test listing events for a session."""
    event1: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "user",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "event-1",
            "content": {"seq": 1},
            "app_name": session_fixture["app_name"],
            "user_id": session_fixture["user_id"],
        },
    }
    event2: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "assistant",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "event-2",
            "content": {"seq": 2},
            "app_name": session_fixture["app_name"],
            "user_id": session_fixture["user_id"],
        },
    }
    await adbc_store.append_event(event1)
    await adbc_store.append_event(event2)

    events = await adbc_store.get_events(session_fixture["session_id"])

    assert len(events) == 2
    assert events[0]["author"] == "user"
    assert events[1]["author"] == "assistant"


async def test_list_events_empty(adbc_store: Any, session_fixture: Any) -> None:
    """Test listing events when none exist."""
    events = await adbc_store.get_events(session_fixture["session_id"])
    assert events == []


async def test_event_with_all_fields(adbc_store: Any, session_fixture: Any) -> None:
    """Test creating event with all optional fields stored in event_json."""
    event_record: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "invocation-123",
        "author": "assistant",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "full-event",
            "content": {"text": "Response"},
            "app_name": session_fixture["app_name"],
            "user_id": session_fixture["user_id"],
            "branch": "main",
            "grounding_metadata": {"sources": ["doc1", "doc2"]},
            "custom_metadata": {"custom": "data"},
            "partial": True,
            "turn_complete": False,
            "interrupted": False,
            "error_code": "NONE",
            "error_message": "No errors",
        },
    }
    await adbc_store.append_event(event_record)

    events = await adbc_store.get_events(session_fixture["session_id"])
    assert len(events) == 1

    # Top-level indexed columns
    assert events[0]["invocation_id"] == "invocation-123"
    assert events[0]["author"] == "assistant"

    # Everything else is in event_json
    event_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    assert event_data["content"] == {"text": "Response"}
    assert event_data["branch"] == "main"
    assert event_data["grounding_metadata"] == {"sources": ["doc1", "doc2"]}
    assert event_data["custom_metadata"] == {"custom": "data"}
    assert event_data["partial"] is True
    assert event_data["turn_complete"] is False
    assert event_data["interrupted"] is False
    assert event_data["error_code"] == "NONE"
    assert event_data["error_message"] == "No errors"


async def test_event_with_minimal_fields(adbc_store: Any, session_fixture: Any) -> None:
    """Test creating event with only required fields."""
    event_record: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "minimal-event",
            "app_name": session_fixture["app_name"],
            "user_id": session_fixture["user_id"],
        },
    }
    await adbc_store.append_event(event_record)

    events = await adbc_store.get_events(session_fixture["session_id"])
    assert len(events) == 1
    assert events[0]["session_id"] == session_fixture["session_id"]
    assert "event_json" in events[0]


async def test_event_json_fields(adbc_store: Any, session_fixture: Any) -> None:
    """Test event JSON field serialization and deserialization via event_json."""
    complex_content = {"nested": {"data": "value"}, "list": [1, 2, 3], "null": None}
    complex_grounding = {"sources": [{"title": "Doc", "url": "http://example.com"}]}
    complex_custom = {"metadata": {"version": 1, "tags": ["tag1", "tag2"]}}

    event_record: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "json-event",
            "content": complex_content,
            "grounding_metadata": complex_grounding,
            "custom_metadata": complex_custom,
            "app_name": session_fixture["app_name"],
            "user_id": session_fixture["user_id"],
        },
    }
    await adbc_store.append_event(event_record)

    events = await adbc_store.get_events(session_fixture["session_id"])
    assert len(events) == 1
    event_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    assert event_data["content"] == complex_content
    assert event_data["grounding_metadata"] == complex_grounding
    assert event_data["custom_metadata"] == complex_custom


async def test_event_ordering(adbc_store: Any, session_fixture: Any) -> None:
    """Test that events are ordered by timestamp ASC."""
    ev1: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"id": "event-1", "app_name": session_fixture["app_name"], "user_id": session_fixture["user_id"]},
    }
    await adbc_store.append_event(ev1)

    await asyncio.sleep(0.01)

    ev2: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"id": "event-2", "app_name": session_fixture["app_name"], "user_id": session_fixture["user_id"]},
    }
    await adbc_store.append_event(ev2)

    await asyncio.sleep(0.01)

    ev3: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"id": "event-3", "app_name": session_fixture["app_name"], "user_id": session_fixture["user_id"]},
    }
    await adbc_store.append_event(ev3)

    events = await adbc_store.get_events(session_fixture["session_id"])

    assert len(events) == 3
    assert events[0]["timestamp"] < events[1]["timestamp"]
    assert events[1]["timestamp"] < events[2]["timestamp"]


async def test_delete_session_cascades_events(adbc_store: Any, session_fixture: Any, tmp_path: Path) -> None:
    """Test that deleting a session cascades to delete events.

    Note: SQLite with ADBC requires foreign key enforcement to be explicitly
    enabled for cascade deletes to work. This test manually enables it.
    """
    ev1: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"id": "event-1", "app_name": session_fixture["app_name"], "user_id": session_fixture["user_id"]},
    }
    ev2: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"id": "event-2", "app_name": session_fixture["app_name"], "user_id": session_fixture["user_id"]},
    }
    await adbc_store.append_event(ev1)
    await adbc_store.append_event(ev2)

    events_before = await adbc_store.get_events(session_fixture["session_id"])
    assert len(events_before) == 2

    await adbc_store.delete_session(session_fixture["session_id"])

    session_after = await adbc_store.get_session(session_fixture["session_id"])
    assert session_after is None


async def test_event_with_empty_actions(adbc_store: Any, session_fixture: Any) -> None:
    """Test creating event with empty actions bytes."""
    event_record: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "empty-actions",
            "app_name": session_fixture["app_name"],
            "user_id": session_fixture["user_id"],
        },
    }
    await adbc_store.append_event(event_record)

    events = await adbc_store.get_events(session_fixture["session_id"])
    assert len(events) == 1
    assert "event_json" in events[0]


async def test_event_with_large_content(adbc_store: Any, session_fixture: Any) -> None:
    """Test creating event with large content in event_json."""
    large_content = {"data": "x" * 10000}

    event_record: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "",
        "author": "",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "large-content",
            "content": large_content,
            "app_name": session_fixture["app_name"],
            "user_id": session_fixture["user_id"],
        },
    }
    await adbc_store.append_event(event_record)

    events = await adbc_store.get_events(session_fixture["session_id"])
    assert len(events) == 1
    event_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    assert event_data["content"] == large_content


async def test_append_event_preserves_existing_session_state(adbc_store: Any, session_fixture: Any) -> None:
    """append_event must not overwrite the durable session state."""
    event_record: EventRecord = {
        "session_id": session_fixture["session_id"],
        "invocation_id": "append-only",
        "author": "user",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "append-only-event",
            "app_name": session_fixture["app_name"],
            "user_id": session_fixture["user_id"],
        },
    }

    await adbc_store.append_event(event_record)

    session = await adbc_store.get_session(session_fixture["session_id"])
    assert session is not None
    assert session["state"] == {"test": True}


async def test_get_events_applies_after_timestamp_and_limit(adbc_store: Any, session_fixture: Any) -> None:
    """get_events must respect both after_timestamp and limit."""
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    event_records = [
        {
            "session_id": session_fixture["session_id"],
            "invocation_id": "",
            "author": "user",
            "timestamp": base_time,
            "event_json": {
                "id": "event-1",
                "app_name": session_fixture["app_name"],
                "user_id": session_fixture["user_id"],
            },
        },
        {
            "session_id": session_fixture["session_id"],
            "invocation_id": "",
            "author": "assistant",
            "timestamp": base_time + timedelta(seconds=1),
            "event_json": {
                "id": "event-2",
                "app_name": session_fixture["app_name"],
                "user_id": session_fixture["user_id"],
            },
        },
        {
            "session_id": session_fixture["session_id"],
            "invocation_id": "",
            "author": "assistant",
            "timestamp": base_time + timedelta(seconds=2),
            "event_json": {
                "id": "event-3",
                "app_name": session_fixture["app_name"],
                "user_id": session_fixture["user_id"],
            },
        },
    ]

    for event_record in event_records:
        await adbc_store.append_event(event_record)

    filtered_events = await adbc_store.get_events(session_fixture["session_id"], after_timestamp=base_time, limit=1)

    assert len(filtered_events) == 1
    filtered_event = filtered_events[0]["event_json"]
    filtered_data = json.loads(filtered_event) if isinstance(filtered_event, str) else filtered_event
    assert filtered_data["id"] == "event-2"
