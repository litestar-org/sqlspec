"""Tests for AsyncPG ADK store session operations."""

from typing import Any

import pytest

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.asyncpg, pytest.mark.integration]


async def test_create_session(asyncpg_adk_store: Any) -> None:
    """Test creating a new session."""
    session_id = "session-123"
    app_name = "test-app"
    user_id = "user-456"
    state = {"key": "value"}

    session = await asyncpg_adk_store.create_session(session_id, app_name, user_id, state)

    assert session["id"] == session_id
    assert session["app_name"] == app_name
    assert session["user_id"] == user_id
    assert session["state"] == state


async def test_get_session(asyncpg_adk_store: Any) -> None:
    """Test retrieving a session by ID."""
    session_id = "session-get"
    app_name = "test-app"
    user_id = "user-123"
    state = {"test": True}

    await asyncpg_adk_store.create_session(session_id, app_name, user_id, state)

    retrieved = await asyncpg_adk_store.get_session(session_id)

    assert retrieved is not None
    assert retrieved["id"] == session_id
    assert retrieved["app_name"] == app_name
    assert retrieved["user_id"] == user_id
    assert retrieved["state"] == state


async def test_get_nonexistent_session(asyncpg_adk_store: Any) -> None:
    """Test retrieving a session that doesn't exist."""
    result = await asyncpg_adk_store.get_session("nonexistent")
    assert result is None


async def test_update_session_state(asyncpg_adk_store: Any) -> None:
    """Test updating session state."""
    session_id = "session-update"
    app_name = "test-app"
    user_id = "user-123"
    initial_state = {"count": 0}
    updated_state = {"count": 5, "updated": True}

    await asyncpg_adk_store.create_session(session_id, app_name, user_id, initial_state)

    await asyncpg_adk_store.update_session_state(session_id, updated_state)

    retrieved = await asyncpg_adk_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == updated_state


async def test_list_sessions(asyncpg_adk_store: Any) -> None:
    """Test listing sessions for an app and user."""
    app_name = "list-test-app"
    user_id = "user-list"

    await asyncpg_adk_store.create_session("session-1", app_name, user_id, {"num": 1})
    await asyncpg_adk_store.create_session("session-2", app_name, user_id, {"num": 2})
    await asyncpg_adk_store.create_session("session-3", "other-app", user_id, {"num": 3})

    sessions = await asyncpg_adk_store.list_sessions(app_name, user_id)

    assert len(sessions) == 2
    session_ids = {s["id"] for s in sessions}
    assert session_ids == {"session-1", "session-2"}


async def test_list_sessions_empty(asyncpg_adk_store: Any) -> None:
    """Test listing sessions when none exist."""
    sessions = await asyncpg_adk_store.list_sessions("nonexistent-app", "nonexistent-user")
    assert sessions == []


async def test_delete_session(asyncpg_adk_store: Any) -> None:
    """Test deleting a session."""
    session_id = "session-delete"
    app_name = "test-app"
    user_id = "user-123"

    await asyncpg_adk_store.create_session(session_id, app_name, user_id, {"test": True})

    await asyncpg_adk_store.delete_session(session_id)

    retrieved = await asyncpg_adk_store.get_session(session_id)
    assert retrieved is None


async def test_delete_nonexistent_session(asyncpg_adk_store: Any) -> None:
    """Test deleting a session that doesn't exist doesn't raise error."""
    await asyncpg_adk_store.delete_session("nonexistent")


async def test_session_timestamps(asyncpg_adk_store: Any) -> None:
    """Test that create_time and update_time are set correctly."""
    session_id = "session-timestamps"
    session = await asyncpg_adk_store.create_session(session_id, "app", "user", {"test": True})

    assert session["create_time"] is not None
    assert session["update_time"] is not None
    assert session["create_time"] == session["update_time"]


async def test_complex_jsonb_state(asyncpg_adk_store: Any) -> None:
    """Test storing complex nested JSONB state."""
    session_id = "session-complex"
    complex_state = {
        "nested": {"level1": {"level2": {"data": [1, 2, 3], "flags": {"active": True, "verified": False}}}},
        "arrays": ["a", "b", "c"],
        "numbers": [1, 2.5, -3],
        "nulls": None,
        "booleans": [True, False],
    }

    session = await asyncpg_adk_store.create_session(session_id, "app", "user", complex_state)

    assert session["state"] == complex_state

    retrieved = await asyncpg_adk_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == complex_state
