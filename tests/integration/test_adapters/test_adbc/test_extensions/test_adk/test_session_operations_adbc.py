"""Tests for ADBC ADK store session operations."""

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


def test_create_session(adbc_store: Any) -> None:
    """Test creating a new session."""
    session_id = "test-session-1"
    app_name = "test-app"
    user_id = "user-123"
    state = {"key": "value", "count": 42}

    session = adbc_store.create_session(session_id, app_name, user_id, state)

    assert session["id"] == session_id
    assert session["app_name"] == app_name
    assert session["user_id"] == user_id
    assert session["state"] == state
    assert session["create_time"] is not None
    assert session["update_time"] is not None


def test_get_session(adbc_store: Any) -> None:
    """Test retrieving a session by ID."""
    session_id = "test-session-2"
    app_name = "test-app"
    user_id = "user-123"
    state = {"data": "test"}

    adbc_store.create_session(session_id, app_name, user_id, state)
    retrieved = adbc_store.get_session(session_id)

    assert retrieved is not None
    assert retrieved["id"] == session_id
    assert retrieved["state"] == state


def test_get_nonexistent_session(adbc_store: Any) -> None:
    """Test retrieving a session that doesn't exist."""
    result = adbc_store.get_session("nonexistent-id")
    assert result is None


def test_update_session_state(adbc_store: Any) -> None:
    """Test updating session state."""
    session_id = "test-session-3"
    app_name = "test-app"
    user_id = "user-123"
    initial_state = {"version": 1}

    adbc_store.create_session(session_id, app_name, user_id, initial_state)

    new_state = {"version": 2, "updated": True}
    adbc_store.update_session_state(session_id, new_state)

    updated = adbc_store.get_session(session_id)
    assert updated is not None
    assert updated["state"] == new_state
    assert updated["state"] != initial_state


def test_delete_session(adbc_store: Any) -> None:
    """Test deleting a session."""
    session_id = "test-session-4"
    app_name = "test-app"
    user_id = "user-123"
    state = {"data": "test"}

    adbc_store.create_session(session_id, app_name, user_id, state)
    assert adbc_store.get_session(session_id) is not None

    adbc_store.delete_session(session_id)
    assert adbc_store.get_session(session_id) is None


def test_list_sessions(adbc_store: Any) -> None:
    """Test listing sessions for an app and user."""
    app_name = "test-app"
    user_id = "user-123"

    adbc_store.create_session("session-1", app_name, user_id, {"num": 1})
    adbc_store.create_session("session-2", app_name, user_id, {"num": 2})
    adbc_store.create_session("session-3", "other-app", user_id, {"num": 3})

    sessions = adbc_store.list_sessions(app_name, user_id)

    assert len(sessions) == 2
    session_ids = {s["id"] for s in sessions}
    assert session_ids == {"session-1", "session-2"}


def test_list_sessions_empty(adbc_store: Any) -> None:
    """Test listing sessions when none exist."""
    sessions = adbc_store.list_sessions("nonexistent-app", "nonexistent-user")
    assert sessions == []


def test_session_state_with_complex_data(adbc_store: Any) -> None:
    """Test session state with nested complex data structures."""
    session_id = "complex-session"
    app_name = "test-app"
    user_id = "user-123"
    complex_state = {
        "nested": {"key": "value", "number": 42},
        "list": [1, 2, 3],
        "mixed": ["string", 123, {"nested": True}],
        "null_value": None,
    }

    session = adbc_store.create_session(session_id, app_name, user_id, complex_state)
    assert session["state"] == complex_state

    retrieved = adbc_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == complex_state


def test_session_state_empty_dict(adbc_store: Any) -> None:
    """Test creating session with empty state dictionary."""
    session_id = "empty-state-session"
    app_name = "test-app"
    user_id = "user-123"
    empty_state: dict[str, Any] = {}

    session = adbc_store.create_session(session_id, app_name, user_id, empty_state)
    assert session["state"] == empty_state

    retrieved = adbc_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == empty_state


def test_multiple_users_same_app(adbc_store: Any) -> None:
    """Test sessions for multiple users in the same app."""
    app_name = "test-app"
    user1 = "user-1"
    user2 = "user-2"

    adbc_store.create_session("session-user1-1", app_name, user1, {"user": 1})
    adbc_store.create_session("session-user1-2", app_name, user1, {"user": 1})
    adbc_store.create_session("session-user2-1", app_name, user2, {"user": 2})

    user1_sessions = adbc_store.list_sessions(app_name, user1)
    user2_sessions = adbc_store.list_sessions(app_name, user2)

    assert len(user1_sessions) == 2
    assert len(user2_sessions) == 1
    assert all(s["user_id"] == user1 for s in user1_sessions)
    assert all(s["user_id"] == user2 for s in user2_sessions)


def test_session_ordering(adbc_store: Any) -> None:
    """Test that sessions are ordered by update_time DESC."""
    app_name = "test-app"
    user_id = "user-123"

    adbc_store.create_session("session-1", app_name, user_id, {"order": 1})
    adbc_store.create_session("session-2", app_name, user_id, {"order": 2})
    adbc_store.create_session("session-3", app_name, user_id, {"order": 3})

    adbc_store.update_session_state("session-1", {"order": 1, "updated": True})

    sessions = adbc_store.list_sessions(app_name, user_id)

    assert len(sessions) == 3
    assert sessions[0]["id"] == "session-1"
