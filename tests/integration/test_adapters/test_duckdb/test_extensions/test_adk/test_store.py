"""Integration tests for DuckDB ADK session store."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from sqlspec.adapters.duckdb.adk.store import DuckdbADKStore
from sqlspec.adapters.duckdb.config import DuckDBConfig

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


@pytest.fixture
def duckdb_adk_store(tmp_path: Path, worker_id: str) -> DuckdbADKStore:
    """Create DuckDB ADK store with temporary file-based database.

    Args:
        tmp_path: Pytest fixture providing unique temporary directory per test.
        worker_id: Pytest-xdist fixture providing unique worker identifier.

    Yields:
        Configured DuckDB ADK store instance.

    Notes:
        Uses file-based database for thread-safe testing.
        Worker ID ensures parallel pytest-xdist workers use separate database files.
    """
    db_path = tmp_path / f"test_adk_{worker_id}.duckdb"
    try:
        config = DuckDBConfig(pool_config={"database": str(db_path)})
        store = DuckdbADKStore(config, session_table="test_sessions", events_table="test_events")
        store.create_tables()
        yield store
    finally:
        if db_path.exists():
            db_path.unlink()


def test_create_tables(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test table creation succeeds without errors."""
    assert duckdb_adk_store.session_table == "test_sessions"
    assert duckdb_adk_store.events_table == "test_events"


def test_create_and_get_session(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test creating and retrieving a session."""
    session_id = "session-001"
    app_name = "test-app"
    user_id = "user-001"
    state = {"key": "value", "count": 42}

    created_session = duckdb_adk_store.create_session(
        session_id=session_id, app_name=app_name, user_id=user_id, state=state
    )

    assert created_session["id"] == session_id
    assert created_session["app_name"] == app_name
    assert created_session["user_id"] == user_id
    assert created_session["state"] == state
    assert isinstance(created_session["create_time"], datetime)
    assert isinstance(created_session["update_time"], datetime)

    retrieved_session = duckdb_adk_store.get_session(session_id)
    assert retrieved_session is not None
    assert retrieved_session["id"] == session_id
    assert retrieved_session["state"] == state


def test_get_nonexistent_session(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test getting a non-existent session returns None."""
    result = duckdb_adk_store.get_session("nonexistent-session")
    assert result is None


def test_update_session_state(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test updating session state."""
    session_id = "session-002"
    initial_state = {"status": "active"}
    updated_state = {"status": "completed", "result": "success"}

    duckdb_adk_store.create_session(session_id=session_id, app_name="test-app", user_id="user-002", state=initial_state)

    session_before = duckdb_adk_store.get_session(session_id)
    assert session_before is not None
    assert session_before["state"] == initial_state

    duckdb_adk_store.update_session_state(session_id, updated_state)

    session_after = duckdb_adk_store.get_session(session_id)
    assert session_after is not None
    assert session_after["state"] == updated_state
    assert session_after["update_time"] >= session_before["update_time"]


def test_list_sessions(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test listing sessions for an app and user."""
    app_name = "test-app"
    user_id = "user-003"

    duckdb_adk_store.create_session("session-1", app_name, user_id, {"num": 1})
    duckdb_adk_store.create_session("session-2", app_name, user_id, {"num": 2})
    duckdb_adk_store.create_session("session-3", app_name, user_id, {"num": 3})
    duckdb_adk_store.create_session("session-other", "other-app", user_id, {"num": 999})

    sessions = duckdb_adk_store.list_sessions(app_name, user_id)

    assert len(sessions) == 3
    session_ids = {s["id"] for s in sessions}
    assert session_ids == {"session-1", "session-2", "session-3"}
    assert all(s["app_name"] == app_name for s in sessions)
    assert all(s["user_id"] == user_id for s in sessions)


def test_list_sessions_empty(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test listing sessions when none exist."""
    sessions = duckdb_adk_store.list_sessions("nonexistent-app", "nonexistent-user")
    assert sessions == []


def test_delete_session(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test deleting a session."""
    session_id = "session-to-delete"
    duckdb_adk_store.create_session(session_id, "test-app", "user-004", {"data": "test"})

    assert duckdb_adk_store.get_session(session_id) is not None

    duckdb_adk_store.delete_session(session_id)

    assert duckdb_adk_store.get_session(session_id) is None


def test_delete_session_cascade_events(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test deleting a session also deletes associated events."""
    session_id = "session-with-events"
    duckdb_adk_store.create_session(session_id, "test-app", "user-005", {"data": "test"})

    event = duckdb_adk_store.create_event(
        event_id="event-001",
        session_id=session_id,
        app_name="test-app",
        user_id="user-005",
        author="user",
        actions=b"test-actions",
        content={"message": "Hello"},
    )

    assert event["id"] == "event-001"
    events = duckdb_adk_store.list_events(session_id)
    assert len(events) == 1

    duckdb_adk_store.delete_session(session_id)

    assert duckdb_adk_store.get_session(session_id) is None
    events_after = duckdb_adk_store.list_events(session_id)
    assert len(events_after) == 0


def test_create_and_get_event(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test creating and retrieving an event."""
    session_id = "session-006"
    duckdb_adk_store.create_session(session_id, "test-app", "user-006", {})

    event_id = "event-002"
    timestamp = datetime.now(timezone.utc)
    content = {"text": "Test message", "role": "user"}
    custom_metadata = {"source": "test"}

    created_event = duckdb_adk_store.create_event(
        event_id=event_id,
        session_id=session_id,
        app_name="test-app",
        user_id="user-006",
        author="user",
        actions=b"pickled-actions",
        content=content,
        timestamp=timestamp,
        custom_metadata=custom_metadata,
    )

    assert created_event["id"] == event_id
    assert created_event["session_id"] == session_id
    assert created_event["author"] == "user"
    assert created_event["content"] == content
    assert created_event["custom_metadata"] == custom_metadata

    retrieved_event = duckdb_adk_store.get_event(event_id)
    assert retrieved_event is not None
    assert retrieved_event["id"] == event_id
    assert retrieved_event["content"] == content


def test_get_nonexistent_event(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test getting a non-existent event returns None."""
    result = duckdb_adk_store.get_event("nonexistent-event")
    assert result is None


def test_list_events(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test listing events for a session."""
    session_id = "session-007"
    duckdb_adk_store.create_session(session_id, "test-app", "user-007", {})

    duckdb_adk_store.create_event(
        event_id="event-1",
        session_id=session_id,
        app_name="test-app",
        user_id="user-007",
        author="user",
        content={"message": "First"},
    )
    duckdb_adk_store.create_event(
        event_id="event-2",
        session_id=session_id,
        app_name="test-app",
        user_id="user-007",
        author="assistant",
        content={"message": "Second"},
    )

    events = duckdb_adk_store.list_events(session_id)

    assert len(events) == 2
    assert events[0]["id"] == "event-1"
    assert events[1]["id"] == "event-2"
    assert events[0]["timestamp"] <= events[1]["timestamp"]


def test_list_events_empty(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test listing events when none exist."""
    session_id = "session-no-events"
    duckdb_adk_store.create_session(session_id, "test-app", "user-008", {})

    events = duckdb_adk_store.list_events(session_id)
    assert events == []


def test_event_with_optional_fields(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test creating events with all optional fields."""
    session_id = "session-008"
    duckdb_adk_store.create_session(session_id, "test-app", "user-008", {})

    event = duckdb_adk_store.create_event(
        event_id="event-full",
        session_id=session_id,
        app_name="test-app",
        user_id="user-008",
        author="assistant",
        actions=b"actions-data",
        content={"text": "Response"},
        invocation_id="inv-123",
        branch="main",
        grounding_metadata={"sources": ["doc1", "doc2"]},
        custom_metadata={"priority": "high"},
        partial=True,
        turn_complete=False,
        interrupted=False,
        error_code=None,
        error_message=None,
    )

    assert event["invocation_id"] == "inv-123"
    assert event["branch"] == "main"
    assert event["grounding_metadata"] == {"sources": ["doc1", "doc2"]}
    assert event["partial"] is True
    assert event["turn_complete"] is False

    retrieved = duckdb_adk_store.get_event("event-full")
    assert retrieved is not None
    assert retrieved["grounding_metadata"] == {"sources": ["doc1", "doc2"]}


def test_event_ordering_by_timestamp(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test events are ordered by timestamp ascending."""
    session_id = "session-009"
    duckdb_adk_store.create_session(session_id, "test-app", "user-009", {})

    t1 = datetime.now(timezone.utc)
    t2 = datetime.now(timezone.utc)
    t3 = datetime.now(timezone.utc)

    duckdb_adk_store.create_event(
        event_id="event-middle", session_id=session_id, app_name="test-app", user_id="user-009", timestamp=t2
    )
    duckdb_adk_store.create_event(
        event_id="event-last", session_id=session_id, app_name="test-app", user_id="user-009", timestamp=t3
    )
    duckdb_adk_store.create_event(
        event_id="event-first", session_id=session_id, app_name="test-app", user_id="user-009", timestamp=t1
    )

    events = duckdb_adk_store.list_events(session_id)

    assert len(events) == 3
    assert events[0]["id"] == "event-first"
    assert events[1]["id"] == "event-middle"
    assert events[2]["id"] == "event-last"


def test_session_state_with_complex_data(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test session state with nested JSON structures."""
    session_id = "session-complex"
    complex_state = {
        "user": {"name": "Alice", "preferences": {"theme": "dark", "language": "en"}},
        "conversation": {
            "topics": ["weather", "news", "sports"],
            "turn_count": 5,
            "metadata": {"started_at": "2025-10-06T12:00:00Z"},
        },
        "flags": [True, False, True],
    }

    duckdb_adk_store.create_session(session_id, "test-app", "user-010", complex_state)

    session = duckdb_adk_store.get_session(session_id)
    assert session is not None
    assert session["state"] == complex_state
    assert session["state"]["user"]["preferences"]["theme"] == "dark"
    assert session["state"]["conversation"]["turn_count"] == 5


def test_empty_state(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test creating session with empty state."""
    session_id = "session-empty-state"
    duckdb_adk_store.create_session(session_id, "test-app", "user-011", {})

    session = duckdb_adk_store.get_session(session_id)
    assert session is not None
    assert session["state"] == {}


def test_table_not_found_handling(tmp_path: Path, worker_id: str) -> None:
    """Test graceful handling when tables don't exist."""
    db_path = tmp_path / f"test_no_tables_{worker_id}.duckdb"
    try:
        config = DuckDBConfig(pool_config={"database": str(db_path)})
        store = DuckdbADKStore(config)

        result = store.get_session("nonexistent")
        assert result is None

        sessions = store.list_sessions("app", "user")
        assert sessions == []

        events = store.list_events("session")
        assert events == []
    finally:
        if db_path.exists():
            db_path.unlink()


def test_binary_actions_data(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test storing and retrieving binary actions data."""
    session_id = "session-binary"
    duckdb_adk_store.create_session(session_id, "test-app", "user-012", {})

    binary_data = bytes(range(256))

    event = duckdb_adk_store.create_event(
        event_id="event-binary",
        session_id=session_id,
        app_name="test-app",
        user_id="user-012",
        author="system",
        actions=binary_data,
    )

    assert event["actions"] == binary_data

    retrieved = duckdb_adk_store.get_event("event-binary")
    assert retrieved is not None
    assert retrieved["actions"] == binary_data
    assert len(retrieved["actions"]) == 256


def test_concurrent_session_updates(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test multiple updates to same session."""
    session_id = "session-concurrent"
    duckdb_adk_store.create_session(session_id, "test-app", "user-013", {"counter": 0})

    for i in range(10):
        session = duckdb_adk_store.get_session(session_id)
        assert session is not None
        current_counter = session["state"]["counter"]
        duckdb_adk_store.update_session_state(session_id, {"counter": current_counter + 1})

    final_session = duckdb_adk_store.get_session(session_id)
    assert final_session is not None
    assert final_session["state"]["counter"] == 10
