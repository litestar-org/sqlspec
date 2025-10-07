"""Tests for BigQuery ADK store event operations."""

from datetime import datetime, timezone
from typing import Any

import pytest

pytestmark = [pytest.mark.xdist_group("bigquery"), pytest.mark.bigquery, pytest.mark.integration]


@pytest.mark.asyncio
async def test_append_event(bigquery_adk_store: Any, session_fixture: Any) -> None:
    """Test appending an event to a session."""
    from sqlspec.extensions.adk._types import EventRecord

    event_record: EventRecord = {
        "id": "event-1",
        "session_id": session_fixture["session_id"],
        "app_name": session_fixture["app_name"],
        "user_id": session_fixture["user_id"],
        "invocation_id": "inv-1",
        "author": "user",
        "actions": b"serialized_actions",
        "long_running_tool_ids_json": None,
        "branch": None,
        "timestamp": datetime.now(timezone.utc),
        "content": {"message": "Hello"},
        "grounding_metadata": None,
        "custom_metadata": None,
        "partial": None,
        "turn_complete": None,
        "interrupted": None,
        "error_code": None,
        "error_message": None,
    }

    await bigquery_adk_store.append_event(event_record)

    events = await bigquery_adk_store.get_events(session_fixture["session_id"])
    assert len(events) == 1
    assert events[0]["id"] == "event-1"
    assert events[0]["content"] == {"message": "Hello"}


@pytest.mark.asyncio
async def test_get_events(bigquery_adk_store: Any, session_fixture: Any) -> None:
    """Test retrieving events for a session."""
    from sqlspec.extensions.adk._types import EventRecord

    event1: EventRecord = {
        "id": "event-1",
        "session_id": session_fixture["session_id"],
        "app_name": session_fixture["app_name"],
        "user_id": session_fixture["user_id"],
        "invocation_id": "inv-1",
        "author": "user",
        "actions": b"",
        "long_running_tool_ids_json": None,
        "branch": None,
        "timestamp": datetime.now(timezone.utc),
        "content": {"seq": 1},
        "grounding_metadata": None,
        "custom_metadata": None,
        "partial": None,
        "turn_complete": None,
        "interrupted": None,
        "error_code": None,
        "error_message": None,
    }

    event2: EventRecord = {
        "id": "event-2",
        "session_id": session_fixture["session_id"],
        "app_name": session_fixture["app_name"],
        "user_id": session_fixture["user_id"],
        "invocation_id": "inv-2",
        "author": "assistant",
        "actions": b"",
        "long_running_tool_ids_json": None,
        "branch": None,
        "timestamp": datetime.now(timezone.utc),
        "content": {"seq": 2},
        "grounding_metadata": None,
        "custom_metadata": None,
        "partial": None,
        "turn_complete": None,
        "interrupted": None,
        "error_code": None,
        "error_message": None,
    }

    await bigquery_adk_store.append_event(event1)
    await bigquery_adk_store.append_event(event2)

    events = await bigquery_adk_store.get_events(session_fixture["session_id"])

    assert len(events) == 2
    assert events[0]["id"] == "event-1"
    assert events[1]["id"] == "event-2"


@pytest.mark.asyncio
async def test_get_events_empty(bigquery_adk_store: Any, session_fixture: Any) -> None:
    """Test retrieving events when none exist."""
    events = await bigquery_adk_store.get_events(session_fixture["session_id"])
    assert events == []


@pytest.mark.asyncio
async def test_get_events_with_after_timestamp(bigquery_adk_store: Any, session_fixture: Any) -> None:
    """Test retrieving events after a specific timestamp."""
    import asyncio

    from sqlspec.extensions.adk._types import EventRecord

    timestamp1 = datetime.now(timezone.utc)
    await asyncio.sleep(0.1)
    timestamp_cutoff = datetime.now(timezone.utc)
    await asyncio.sleep(0.1)

    event1: EventRecord = {
        "id": "event-1",
        "session_id": session_fixture["session_id"],
        "app_name": session_fixture["app_name"],
        "user_id": session_fixture["user_id"],
        "invocation_id": "inv-1",
        "author": "user",
        "actions": b"",
        "long_running_tool_ids_json": None,
        "branch": None,
        "timestamp": timestamp1,
        "content": None,
        "grounding_metadata": None,
        "custom_metadata": None,
        "partial": None,
        "turn_complete": None,
        "interrupted": None,
        "error_code": None,
        "error_message": None,
    }

    event2: EventRecord = {
        "id": "event-2",
        "session_id": session_fixture["session_id"],
        "app_name": session_fixture["app_name"],
        "user_id": session_fixture["user_id"],
        "invocation_id": "inv-2",
        "author": "assistant",
        "actions": b"",
        "long_running_tool_ids_json": None,
        "branch": None,
        "timestamp": datetime.now(timezone.utc),
        "content": None,
        "grounding_metadata": None,
        "custom_metadata": None,
        "partial": None,
        "turn_complete": None,
        "interrupted": None,
        "error_code": None,
        "error_message": None,
    }

    await bigquery_adk_store.append_event(event1)
    await bigquery_adk_store.append_event(event2)

    events = await bigquery_adk_store.get_events(session_fixture["session_id"], after_timestamp=timestamp_cutoff)

    assert len(events) == 1
    assert events[0]["id"] == "event-2"


@pytest.mark.asyncio
async def test_get_events_with_limit(bigquery_adk_store: Any, session_fixture: Any) -> None:
    """Test retrieving limited number of events."""
    from sqlspec.extensions.adk._types import EventRecord

    for i in range(5):
        event: EventRecord = {
            "id": f"event-{i}",
            "session_id": session_fixture["session_id"],
            "app_name": session_fixture["app_name"],
            "user_id": session_fixture["user_id"],
            "invocation_id": f"inv-{i}",
            "author": "user",
            "actions": b"",
            "long_running_tool_ids_json": None,
            "branch": None,
            "timestamp": datetime.now(timezone.utc),
            "content": None,
            "grounding_metadata": None,
            "custom_metadata": None,
            "partial": None,
            "turn_complete": None,
            "interrupted": None,
            "error_code": None,
            "error_message": None,
        }
        await bigquery_adk_store.append_event(event)

    events = await bigquery_adk_store.get_events(session_fixture["session_id"], limit=3)

    assert len(events) == 3


@pytest.mark.asyncio
async def test_event_with_all_fields(bigquery_adk_store: Any, session_fixture: Any) -> None:
    """Test event with all optional fields populated."""
    from sqlspec.extensions.adk._types import EventRecord

    timestamp = datetime.now(timezone.utc)
    event: EventRecord = {
        "id": "full-event",
        "session_id": session_fixture["session_id"],
        "app_name": session_fixture["app_name"],
        "user_id": session_fixture["user_id"],
        "invocation_id": "invocation-123",
        "author": "assistant",
        "actions": b"complex_action_data",
        "long_running_tool_ids_json": '["tool1", "tool2"]',
        "branch": "main",
        "timestamp": timestamp,
        "content": {"text": "Response"},
        "grounding_metadata": {"sources": ["doc1", "doc2"]},
        "custom_metadata": {"custom": "data"},
        "partial": True,
        "turn_complete": False,
        "interrupted": False,
        "error_code": "NONE",
        "error_message": "No errors",
    }

    await bigquery_adk_store.append_event(event)

    events = await bigquery_adk_store.get_events(session_fixture["session_id"])
    retrieved = events[0]

    assert retrieved["invocation_id"] == "invocation-123"
    assert retrieved["author"] == "assistant"
    assert retrieved["actions"] == b"complex_action_data"
    assert retrieved["long_running_tool_ids_json"] == '["tool1", "tool2"]'
    assert retrieved["branch"] == "main"
    assert retrieved["content"] == {"text": "Response"}
    assert retrieved["grounding_metadata"] == {"sources": ["doc1", "doc2"]}
    assert retrieved["custom_metadata"] == {"custom": "data"}
    assert retrieved["partial"] is True
    assert retrieved["turn_complete"] is False
    assert retrieved["interrupted"] is False
    assert retrieved["error_code"] == "NONE"
    assert retrieved["error_message"] == "No errors"


@pytest.mark.asyncio
async def test_delete_session_cascades_events(bigquery_adk_store: Any, session_fixture: Any) -> None:
    """Test that deleting a session deletes associated events."""
    from sqlspec.extensions.adk._types import EventRecord

    event: EventRecord = {
        "id": "event-1",
        "session_id": session_fixture["session_id"],
        "app_name": session_fixture["app_name"],
        "user_id": session_fixture["user_id"],
        "invocation_id": "inv-1",
        "author": "user",
        "actions": b"",
        "long_running_tool_ids_json": None,
        "branch": None,
        "timestamp": datetime.now(timezone.utc),
        "content": None,
        "grounding_metadata": None,
        "custom_metadata": None,
        "partial": None,
        "turn_complete": None,
        "interrupted": None,
        "error_code": None,
        "error_message": None,
    }

    await bigquery_adk_store.append_event(event)

    events_before = await bigquery_adk_store.get_events(session_fixture["session_id"])
    assert len(events_before) == 1

    await bigquery_adk_store.delete_session(session_fixture["session_id"])

    events_after = await bigquery_adk_store.get_events(session_fixture["session_id"])
    assert len(events_after) == 0


@pytest.mark.asyncio
async def test_event_json_fields(bigquery_adk_store: Any, session_fixture: Any) -> None:
    """Test event JSON field serialization and deserialization."""
    from sqlspec.extensions.adk._types import EventRecord

    complex_content = {"nested": {"data": "value"}, "list": [1, 2, 3], "null": None}
    complex_grounding = {"sources": [{"title": "Doc", "url": "http://example.com"}]}
    complex_custom = {"metadata": {"version": 1, "tags": ["tag1", "tag2"]}}

    event: EventRecord = {
        "id": "json-event",
        "session_id": session_fixture["session_id"],
        "app_name": session_fixture["app_name"],
        "user_id": session_fixture["user_id"],
        "invocation_id": "inv-1",
        "author": "user",
        "actions": b"",
        "long_running_tool_ids_json": None,
        "branch": None,
        "timestamp": datetime.now(timezone.utc),
        "content": complex_content,
        "grounding_metadata": complex_grounding,
        "custom_metadata": complex_custom,
        "partial": None,
        "turn_complete": None,
        "interrupted": None,
        "error_code": None,
        "error_message": None,
    }

    await bigquery_adk_store.append_event(event)

    events = await bigquery_adk_store.get_events(session_fixture["session_id"])
    retrieved = events[0]

    assert retrieved["content"] == complex_content
    assert retrieved["grounding_metadata"] == complex_grounding
    assert retrieved["custom_metadata"] == complex_custom
