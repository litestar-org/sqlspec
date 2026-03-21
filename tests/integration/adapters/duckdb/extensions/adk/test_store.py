"""Integration tests for DuckDB ADK session store."""

import json
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from pathlib import Path

import pytest

from sqlspec.adapters.duckdb.adk.store import DuckdbADKStore
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.adk import EventRecord

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


@pytest.fixture
async def duckdb_adk_store(tmp_path: Path, worker_id: str) -> "AsyncGenerator[DuckdbADKStore, None]":
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
        config = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={"adk": {"session_table": "test_sessions", "events_table": "test_events"}},
        )
        store = DuckdbADKStore(config)
        await store.create_tables()
        yield store
    finally:
        if db_path.exists():
            db_path.unlink()


async def test_create_tables(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test table creation succeeds without errors."""
    assert duckdb_adk_store.session_table == "test_sessions"
    assert duckdb_adk_store.events_table == "test_events"


async def test_create_and_get_session(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test creating and retrieving a session."""
    session_id = "session-001"
    app_name = "test-app"
    user_id = "user-001"
    state = {"key": "value", "count": 42}

    created_session = await duckdb_adk_store.create_session(
        session_id=session_id, app_name=app_name, user_id=user_id, state=state
    )

    assert created_session["id"] == session_id
    assert created_session["app_name"] == app_name
    assert created_session["user_id"] == user_id
    assert created_session["state"] == state
    assert isinstance(created_session["create_time"], datetime)
    assert isinstance(created_session["update_time"], datetime)

    retrieved_session = await duckdb_adk_store.get_session(session_id)
    assert retrieved_session is not None
    assert retrieved_session["id"] == session_id
    assert retrieved_session["state"] == state


async def test_get_nonexistent_session(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await duckdb_adk_store.get_session("nonexistent-session")
    assert result is None


async def test_update_session_state(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test updating session state."""
    session_id = "session-002"
    initial_state = {"status": "active"}
    updated_state = {"status": "completed", "result": "success"}

    await duckdb_adk_store.create_session(
        session_id=session_id, app_name="test-app", user_id="user-002", state=initial_state
    )

    session_before = await duckdb_adk_store.get_session(session_id)
    assert session_before is not None
    assert session_before["state"] == initial_state

    await duckdb_adk_store.update_session_state(session_id, updated_state)

    session_after = await duckdb_adk_store.get_session(session_id)
    assert session_after is not None
    assert session_after["state"] == updated_state
    assert session_after["update_time"] >= session_before["update_time"]


async def test_list_sessions(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test listing sessions for an app and user."""
    app_name = "test-app"
    user_id = "user-003"

    await duckdb_adk_store.create_session("session-1", app_name, user_id, {"num": 1})
    await duckdb_adk_store.create_session("session-2", app_name, user_id, {"num": 2})
    await duckdb_adk_store.create_session("session-3", app_name, user_id, {"num": 3})
    await duckdb_adk_store.create_session("session-other", "other-app", user_id, {"num": 999})

    sessions = await duckdb_adk_store.list_sessions(app_name, user_id)

    assert len(sessions) == 3
    session_ids = {s["id"] for s in sessions}
    assert session_ids == {"session-1", "session-2", "session-3"}
    assert all(s["app_name"] == app_name for s in sessions)
    assert all(s["user_id"] == user_id for s in sessions)


async def test_list_sessions_empty(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test listing sessions when none exist."""
    sessions = await duckdb_adk_store.list_sessions("nonexistent-app", "nonexistent-user")
    assert sessions == []


async def test_delete_session(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test deleting a session."""
    session_id = "session-to-delete"
    await duckdb_adk_store.create_session(session_id, "test-app", "user-004", {"data": "test"})

    assert await duckdb_adk_store.get_session(session_id) is not None

    await duckdb_adk_store.delete_session(session_id)

    assert await duckdb_adk_store.get_session(session_id) is None


async def test_delete_session_cascade_events(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test deleting a session also deletes associated events."""
    session_id = "session-with-events"
    await duckdb_adk_store.create_session(session_id, "test-app", "user-005", {"data": "test"})

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "user",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "event-001",
            "content": {"message": "Hello"},
            "app_name": "test-app",
            "user_id": "user-005",
        },
    }
    await duckdb_adk_store.append_event(event_record)

    events = await duckdb_adk_store.get_events(session_id)
    assert len(events) == 1

    await duckdb_adk_store.delete_session(session_id)

    assert await duckdb_adk_store.get_session(session_id) is None
    events_after = await duckdb_adk_store.get_events(session_id)
    assert len(events_after) == 0


async def test_create_event(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test creating an event and verifying the returned 5-key EventRecord."""
    session_id = "session-006"
    await duckdb_adk_store.create_session(session_id, "test-app", "user-006", {})

    timestamp = datetime.now(timezone.utc)
    content = {"text": "Test message", "role": "user"}

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "user",
        "timestamp": timestamp,
        "event_json": {"id": "event-002", "content": content, "app_name": "test-app", "user_id": "user-006"},
    }
    await duckdb_adk_store.append_event(event_record)

    events = await duckdb_adk_store.get_events(session_id)
    assert len(events) == 1
    assert events[0]["session_id"] == session_id
    assert events[0]["author"] == "user"

    # Content is stored inside event_json
    event_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    assert event_data["content"] == content


async def test_list_events(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test listing events for a session."""
    session_id = "session-007"
    await duckdb_adk_store.create_session(session_id, "test-app", "user-007", {})

    event1: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "user",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"id": "event-1", "content": {"message": "First"}, "app_name": "test-app", "user_id": "user-007"},
    }
    event2: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "assistant",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "event-2",
            "content": {"message": "Second"},
            "app_name": "test-app",
            "user_id": "user-007",
        },
    }
    await duckdb_adk_store.append_event(event1)
    await duckdb_adk_store.append_event(event2)

    events = await duckdb_adk_store.get_events(session_id)

    assert len(events) == 2
    assert events[0]["author"] == "user"
    assert events[1]["author"] == "assistant"
    assert events[0]["timestamp"] <= events[1]["timestamp"]


async def test_list_events_empty(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test listing events when none exist."""
    session_id = "session-no-events"
    await duckdb_adk_store.create_session(session_id, "test-app", "user-008", {})

    events = await duckdb_adk_store.get_events(session_id)
    assert events == []


async def test_event_with_optional_fields(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test creating events with optional fields stored in event_json."""
    session_id = "session-008"
    await duckdb_adk_store.create_session(session_id, "test-app", "user-008", {})

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "inv-123",
        "author": "assistant",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "event-full",
            "content": {"text": "Response"},
            "app_name": "test-app",
            "user_id": "user-008",
            "branch": "main",
            "grounding_metadata": {"sources": ["doc1", "doc2"]},
            "custom_metadata": {"priority": "high"},
            "partial": True,
            "turn_complete": False,
            "interrupted": False,
        },
    }
    await duckdb_adk_store.append_event(event_record)

    events = await duckdb_adk_store.get_events(session_id)
    assert len(events) == 1

    # The 5-key record has invocation_id as a top-level indexed column
    assert events[0]["invocation_id"] == "inv-123"

    # Other fields are inside event_json
    event_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    assert event_data["branch"] == "main"
    assert event_data["grounding_metadata"] == {"sources": ["doc1", "doc2"]}
    assert event_data["partial"] is True
    assert event_data["turn_complete"] is False


async def test_event_ordering_by_timestamp(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test events are ordered by timestamp ascending."""
    session_id = "session-009"
    await duckdb_adk_store.create_session(session_id, "test-app", "user-009", {})

    t1 = datetime.now(timezone.utc)
    t2 = datetime.now(timezone.utc)
    t3 = datetime.now(timezone.utc)

    ev_middle: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "",
        "timestamp": t2,
        "event_json": {"id": "event-middle", "app_name": "test-app", "user_id": "user-009"},
    }
    ev_last: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "",
        "timestamp": t3,
        "event_json": {"id": "event-last", "app_name": "test-app", "user_id": "user-009"},
    }
    ev_first: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "",
        "timestamp": t1,
        "event_json": {"id": "event-first", "app_name": "test-app", "user_id": "user-009"},
    }

    await duckdb_adk_store.append_event(ev_middle)
    await duckdb_adk_store.append_event(ev_last)
    await duckdb_adk_store.append_event(ev_first)

    events = await duckdb_adk_store.get_events(session_id)

    assert len(events) == 3
    # Events should be ordered by timestamp ASC
    event_ids = []
    for e in events:
        data = json.loads(e["event_json"]) if isinstance(e["event_json"], str) else e["event_json"]
        event_ids.append(data["id"])
    assert event_ids == ["event-first", "event-middle", "event-last"]


async def test_session_state_with_complex_data(duckdb_adk_store: DuckdbADKStore) -> None:
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

    await duckdb_adk_store.create_session(session_id, "test-app", "user-010", complex_state)

    session = await duckdb_adk_store.get_session(session_id)
    assert session is not None
    assert session["state"] == complex_state
    assert session["state"]["user"]["preferences"]["theme"] == "dark"
    assert session["state"]["conversation"]["turn_count"] == 5


async def test_empty_state(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test creating session with empty state."""
    session_id = "session-empty-state"
    await duckdb_adk_store.create_session(session_id, "test-app", "user-011", {})

    session = await duckdb_adk_store.get_session(session_id)
    assert session is not None
    assert session["state"] == {}


async def test_table_not_found_handling(tmp_path: Path, worker_id: str) -> None:
    """Test graceful handling when tables don't exist."""
    db_path = tmp_path / f"test_no_tables_{worker_id}.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})
        store = DuckdbADKStore(config)

        result = await store.get_session("nonexistent")
        assert result is None

        sessions = await store.list_sessions("app", "user")
        assert sessions == []

        events = await store.get_events("session")
        assert events == []
    finally:
        if db_path.exists():
            db_path.unlink()


async def test_event_json_round_trip(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test storing and retrieving event data via event_json."""
    session_id = "session-json-rt"
    await duckdb_adk_store.create_session(session_id, "test-app", "user-012", {})

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "system",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"id": "event-json", "content": {"data": "value"}, "app_name": "test-app", "user_id": "user-012"},
    }
    await duckdb_adk_store.append_event(event_record)

    events = await duckdb_adk_store.get_events(session_id)
    assert len(events) == 1
    event_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    assert event_data["content"] == {"data": "value"}


async def test_concurrent_session_updates(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test multiple updates to same session."""
    session_id = "session-concurrent"
    await duckdb_adk_store.create_session(session_id, "test-app", "user-013", {"counter": 0})

    for i in range(10):
        session = await duckdb_adk_store.get_session(session_id)
        assert session is not None
        current_counter = session["state"]["counter"]
        await duckdb_adk_store.update_session_state(session_id, {"counter": current_counter + 1})

    final_session = await duckdb_adk_store.get_session(session_id)
    assert final_session is not None
    assert final_session["state"]["counter"] == 10


async def test_owner_id_column_with_integer(tmp_path: Path, worker_id: str) -> None:
    """Test owner ID column with INTEGER type."""
    db_path = tmp_path / f"test_owner_id_int_{worker_id}.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE tenants (id INTEGER PRIMARY KEY, name VARCHAR)")
            conn.execute("INSERT INTO tenants (id, name) VALUES (1, 'Tenant A'), (2, 'Tenant B')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_tenant",
                    "events_table": "events_with_tenant",
                    "owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        await store.create_tables()

        assert store.owner_id_column_name == "tenant_id"
        assert store.owner_id_column_ddl == "tenant_id INTEGER NOT NULL REFERENCES tenants(id)"

        session = await store.create_session(
            session_id="session-tenant-1", app_name="test-app", user_id="user-001", state={"data": "test"}, owner_id=1
        )

        assert session["id"] == "session-tenant-1"

        with config.provide_connection() as conn:
            cursor = conn.execute("SELECT tenant_id FROM sessions_with_tenant WHERE id = ?", ("session-tenant-1",))
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 1
    finally:
        if db_path.exists():
            db_path.unlink()


async def test_owner_id_column_with_ubigint(tmp_path: Path, worker_id: str) -> None:
    """Test owner ID column with DuckDB UBIGINT type."""
    db_path = tmp_path / f"test_owner_id_ubigint_{worker_id}.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE users (id UBIGINT PRIMARY KEY, email VARCHAR)")
            conn.execute("INSERT INTO users (id, email) VALUES (18446744073709551615, 'user@example.com')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_user",
                    "events_table": "events_with_user",
                    "owner_id_column": "owner_id UBIGINT REFERENCES users(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        await store.create_tables()

        assert store.owner_id_column_name == "owner_id"

        session = await store.create_session(
            session_id="session-user-1",
            app_name="test-app",
            user_id="user-001",
            state={"data": "test"},
            owner_id=18446744073709551615,
        )

        assert session["id"] == "session-user-1"

        with config.provide_connection() as conn:
            cursor = conn.execute("SELECT owner_id FROM sessions_with_user WHERE id = ?", ("session-user-1",))
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 18446744073709551615
    finally:
        if db_path.exists():
            db_path.unlink()


async def test_owner_id_column_foreign_key_constraint(tmp_path: Path, worker_id: str) -> None:
    """Test that FK constraint is enforced."""
    db_path = tmp_path / f"test_owner_id_constraint_{worker_id}.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE organizations (id INTEGER PRIMARY KEY, name VARCHAR)")
            conn.execute("INSERT INTO organizations (id, name) VALUES (100, 'Org A')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_org",
                    "events_table": "events_with_org",
                    "owner_id_column": "org_id INTEGER NOT NULL REFERENCES organizations(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        await store.create_tables()

        await store.create_session(
            session_id="session-org-1", app_name="test-app", user_id="user-001", state={"data": "test"}, owner_id=100
        )

        with pytest.raises(Exception) as exc_info:
            await store.create_session(
                session_id="session-org-invalid",
                app_name="test-app",
                user_id="user-002",
                state={"data": "test"},
                owner_id=999,
            )

        assert "FOREIGN KEY constraint" in str(exc_info.value) or "Constraint Error" in str(exc_info.value)
    finally:
        if db_path.exists():
            db_path.unlink()


async def test_owner_id_column_without_value(tmp_path: Path, worker_id: str) -> None:
    """Test creating session without owner_id when column is configured but nullable."""
    db_path = tmp_path / f"test_owner_id_nullable_{worker_id}.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, name VARCHAR)")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_nullable_fk",
                    "events_table": "events_nullable_fk",
                    "owner_id_column": "account_id INTEGER REFERENCES accounts(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        await store.create_tables()

        session = await store.create_session(
            session_id="session-no-fk", app_name="test-app", user_id="user-001", state={"data": "test"}, owner_id=None
        )

        assert session["id"] == "session-no-fk"

        retrieved = await store.get_session("session-no-fk")
        assert retrieved is not None
    finally:
        if db_path.exists():
            db_path.unlink()


async def test_owner_id_column_with_varchar(tmp_path: Path, worker_id: str) -> None:
    """Test owner ID column with VARCHAR type."""
    db_path = tmp_path / f"test_owner_id_varchar_{worker_id}.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE companies (code VARCHAR PRIMARY KEY, name VARCHAR)")
            conn.execute("INSERT INTO companies (code, name) VALUES ('ACME', 'Acme Corp'), ('INIT', 'Initech')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_company",
                    "events_table": "events_with_company",
                    "owner_id_column": "company_code VARCHAR NOT NULL REFERENCES companies(code)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        await store.create_tables()

        session = await store.create_session(
            session_id="session-company-1",
            app_name="test-app",
            user_id="user-001",
            state={"data": "test"},
            owner_id="ACME",
        )

        assert session["id"] == "session-company-1"

        with config.provide_connection() as conn:
            cursor = conn.execute("SELECT company_code FROM sessions_with_company WHERE id = ?", ("session-company-1",))
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "ACME"
    finally:
        if db_path.exists():
            db_path.unlink()


async def test_owner_id_column_multiple_sessions(tmp_path: Path, worker_id: str) -> None:
    """Test multiple sessions with same FK value."""
    db_path = tmp_path / f"test_owner_id_multiple_{worker_id}.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name VARCHAR)")
            conn.execute("INSERT INTO departments (id, name) VALUES (10, 'Engineering'), (20, 'Sales')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_dept",
                    "events_table": "events_with_dept",
                    "owner_id_column": "dept_id INTEGER NOT NULL REFERENCES departments(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        await store.create_tables()

        for i in range(5):
            await store.create_session(
                session_id=f"session-dept-{i}",
                app_name="test-app",
                user_id=f"user-{i}",
                state={"index": i},
                owner_id=10,
            )

        with config.provide_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM sessions_with_dept WHERE dept_id = ?", (10,))
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 5
    finally:
        if db_path.exists():
            db_path.unlink()


async def test_owner_id_column_query_by_fk(tmp_path: Path, worker_id: str) -> None:
    """Test querying sessions by FK column value."""
    db_path = tmp_path / f"test_owner_id_query_{worker_id}.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE projects (id INTEGER PRIMARY KEY, name VARCHAR)")
            conn.execute("INSERT INTO projects (id, name) VALUES (1, 'Project Alpha'), (2, 'Project Beta')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_project",
                    "events_table": "events_with_project",
                    "owner_id_column": "project_id INTEGER NOT NULL REFERENCES projects(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        await store.create_tables()

        await store.create_session("s1", "app", "u1", {"val": 1}, owner_id=1)
        await store.create_session("s2", "app", "u2", {"val": 2}, owner_id=1)
        await store.create_session("s3", "app", "u3", {"val": 3}, owner_id=2)

        with config.provide_connection() as conn:
            cursor = conn.execute("SELECT id FROM sessions_with_project WHERE project_id = ? ORDER BY id", (1,))
            rows = cursor.fetchall()
            assert len(rows) == 2
            assert rows[0][0] == "s1"
            assert rows[1][0] == "s2"

            cursor = conn.execute("SELECT id FROM sessions_with_project WHERE project_id = ?", (2,))
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "s3"
    finally:
        if db_path.exists():
            db_path.unlink()
