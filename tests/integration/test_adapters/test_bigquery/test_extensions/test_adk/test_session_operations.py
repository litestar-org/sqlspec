"""Tests for BigQuery ADK store session operations."""

from typing import Any

import pytest

pytestmark = [pytest.mark.xdist_group("bigquery"), pytest.mark.bigquery, pytest.mark.integration]


async def test_create_session(bigquery_adk_store: Any) -> None:
    """Test creating a new session."""
    session_id = "session-123"
    app_name = "test-app"
    user_id = "user-456"
    state = {"key": "value"}

    session = await bigquery_adk_store.create_session(session_id, app_name, user_id, state)

    assert session["id"] == session_id
    assert session["app_name"] == app_name
    assert session["user_id"] == user_id
    assert session["state"] == state


async def test_get_session(bigquery_adk_store: Any) -> None:
    """Test retrieving a session by ID."""
    session_id = "session-get"
    app_name = "test-app"
    user_id = "user-123"
    state = {"test": True}

    await bigquery_adk_store.create_session(session_id, app_name, user_id, state)

    retrieved = await bigquery_adk_store.get_session(session_id)

    assert retrieved is not None
    assert retrieved["id"] == session_id
    assert retrieved["app_name"] == app_name
    assert retrieved["user_id"] == user_id
    assert retrieved["state"] == state


async def test_get_nonexistent_session(bigquery_adk_store: Any) -> None:
    """Test retrieving a session that doesn't exist."""
    result = await bigquery_adk_store.get_session("nonexistent")
    assert result is None


async def test_update_session_state(bigquery_adk_store: Any) -> None:
    """Test updating session state."""
    session_id = "session-update"
    app_name = "test-app"
    user_id = "user-123"
    initial_state = {"count": 0}
    updated_state = {"count": 5, "updated": True}

    await bigquery_adk_store.create_session(session_id, app_name, user_id, initial_state)

    await bigquery_adk_store.update_session_state(session_id, updated_state)

    retrieved = await bigquery_adk_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == updated_state


async def test_list_sessions(bigquery_adk_store: Any) -> None:
    """Test listing sessions for an app and user."""
    app_name = "list-test-app"
    user_id = "user-list"

    await bigquery_adk_store.create_session("session-1", app_name, user_id, {"num": 1})
    await bigquery_adk_store.create_session("session-2", app_name, user_id, {"num": 2})
    await bigquery_adk_store.create_session("session-3", "other-app", user_id, {"num": 3})

    sessions = await bigquery_adk_store.list_sessions(app_name, user_id)

    assert len(sessions) == 2
    session_ids = {s["id"] for s in sessions}
    assert session_ids == {"session-1", "session-2"}


async def test_list_sessions_empty(bigquery_adk_store: Any) -> None:
    """Test listing sessions when none exist."""
    sessions = await bigquery_adk_store.list_sessions("nonexistent-app", "nonexistent-user")
    assert sessions == []


async def test_delete_session(bigquery_adk_store: Any) -> None:
    """Test deleting a session."""
    session_id = "session-delete"
    app_name = "test-app"
    user_id = "user-123"

    await bigquery_adk_store.create_session(session_id, app_name, user_id, {"test": True})

    await bigquery_adk_store.delete_session(session_id)

    retrieved = await bigquery_adk_store.get_session(session_id)
    assert retrieved is None


async def test_session_with_complex_state(bigquery_adk_store: Any) -> None:
    """Test session with complex nested state."""
    session_id = "complex-session"
    complex_state = {"nested": {"data": "value", "list": [1, 2, 3]}, "boolean": True, "number": 42, "null": None}

    await bigquery_adk_store.create_session(session_id, "test-app", "user-123", complex_state)

    retrieved = await bigquery_adk_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == complex_state


async def test_session_with_empty_state(bigquery_adk_store: Any) -> None:
    """Test session with empty state."""
    session_id = "empty-state"

    await bigquery_adk_store.create_session(session_id, "test-app", "user-123", {})

    retrieved = await bigquery_adk_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == {}


async def test_session_timestamps(bigquery_adk_store: Any) -> None:
    """Test that session timestamps are set correctly."""
    import asyncio
    from datetime import datetime

    session_id = "timestamp-session"

    session = await bigquery_adk_store.create_session(session_id, "test-app", "user-123", {"test": True})

    assert isinstance(session["create_time"], datetime)
    assert isinstance(session["update_time"], datetime)
    assert session["create_time"] == session["update_time"]

    await asyncio.sleep(0.1)

    await bigquery_adk_store.update_session_state(session_id, {"updated": True})

    retrieved = await bigquery_adk_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["update_time"] > retrieved["create_time"]
