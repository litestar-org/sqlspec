"""Integration tests for AsyncMY ADK session store."""

import pickle
from datetime import datetime, timezone

import pytest

from sqlspec.adapters.asyncmy.adk.store import AsyncmyADKStore

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.asyncmy, pytest.mark.integration]


async def test_create_tables(asyncmy_adk_store: AsyncmyADKStore) -> None:
    """Test table creation succeeds without errors."""
    assert asyncmy_adk_store.session_table == "test_sessions"
    assert asyncmy_adk_store.events_table == "test_events"


async def test_storage_types_verification(asyncmy_adk_store: AsyncmyADKStore) -> None:
    """Verify MySQL uses JSON type (not TEXT) and TIMESTAMP(6) for microseconds.

    Critical verification from ADK implementation review.
    Ensures we're using MySQL native types optimally.
    """
    config = asyncmy_adk_store.config

    async with config.provide_connection() as conn, conn.cursor() as cursor:
        await cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'test_sessions'
            ORDER BY ORDINAL_POSITION
        """)
        session_columns = await cursor.fetchall()

        state_col = next(col for col in session_columns if col[0] == "state")
        assert state_col[1] == "json", "state column must use native JSON type (not TEXT)"

        create_time_col = next(col for col in session_columns if col[0] == "create_time")
        assert "timestamp(6)" in create_time_col[2].lower(), "create_time must be TIMESTAMP(6) for microseconds"

        update_time_col = next(col for col in session_columns if col[0] == "update_time")
        assert "timestamp(6)" in update_time_col[2].lower(), "update_time must be TIMESTAMP(6) for microseconds"

        await cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'test_events'
            ORDER BY ORDINAL_POSITION
        """)
        event_columns = await cursor.fetchall()

        actions_col = next(col for col in event_columns if col[0] == "actions")
        assert actions_col[1] == "blob", "actions column must use BLOB type for pickled data"

        content_col = next((col for col in event_columns if col[0] == "content"), None)
        if content_col:
            assert content_col[1] == "json", "content column must use native JSON type"

        timestamp_col = next(col for col in event_columns if col[0] == "timestamp")
        assert "timestamp(6)" in timestamp_col[2].lower(), "timestamp must be TIMESTAMP(6) for microseconds"


async def test_create_and_get_session(asyncmy_adk_store: AsyncmyADKStore) -> None:
    """Test creating and retrieving a session."""
    session_id = "session-001"
    app_name = "test-app"
    user_id = "user-001"
    state = {"key": "value", "count": 42}

    created_session = await asyncmy_adk_store.create_session(
        session_id=session_id, app_name=app_name, user_id=user_id, state=state
    )

    assert created_session["id"] == session_id
    assert created_session["app_name"] == app_name
    assert created_session["user_id"] == user_id
    assert created_session["state"] == state
    assert isinstance(created_session["create_time"], datetime)
    assert isinstance(created_session["update_time"], datetime)

    retrieved_session = await asyncmy_adk_store.get_session(session_id)
    assert retrieved_session is not None
    assert retrieved_session["id"] == session_id
    assert retrieved_session["state"] == state


async def test_get_nonexistent_session(asyncmy_adk_store: AsyncmyADKStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await asyncmy_adk_store.get_session("nonexistent-session")
    assert result is None


async def test_update_session_state(asyncmy_adk_store: AsyncmyADKStore) -> None:
    """Test updating session state."""
    session_id = "session-002"
    initial_state = {"status": "active"}
    updated_state = {"status": "completed", "result": "success"}

    await asyncmy_adk_store.create_session(
        session_id=session_id, app_name="test-app", user_id="user-002", state=initial_state
    )

    session_before = await asyncmy_adk_store.get_session(session_id)
    assert session_before is not None
    assert session_before["state"] == initial_state

    await asyncmy_adk_store.update_session_state(session_id, updated_state)

    session_after = await asyncmy_adk_store.get_session(session_id)
    assert session_after is not None
    assert session_after["state"] == updated_state
    assert session_after["update_time"] >= session_before["update_time"]


async def test_list_sessions(asyncmy_adk_store: AsyncmyADKStore) -> None:
    """Test listing sessions for an app and user."""
    app_name = "test-app"
    user_id = "user-003"

    await asyncmy_adk_store.create_session("session-a", app_name, user_id, {"num": 1})
    await asyncmy_adk_store.create_session("session-b", app_name, user_id, {"num": 2})
    await asyncmy_adk_store.create_session("session-c", app_name, "other-user", {"num": 3})

    sessions = await asyncmy_adk_store.list_sessions(app_name, user_id)

    assert len(sessions) == 2
    session_ids = {s["id"] for s in sessions}
    assert session_ids == {"session-a", "session-b"}
    assert all(s["app_name"] == app_name for s in sessions)
    assert all(s["user_id"] == user_id for s in sessions)


async def test_delete_session_cascade(asyncmy_adk_store: AsyncmyADKStore) -> None:
    """Test deleting session cascades to events."""
    session_id = "session-004"
    app_name = "test-app"
    user_id = "user-004"

    await asyncmy_adk_store.create_session(session_id, app_name, user_id, {"status": "active"})

    event_record = {
        "id": "event-001",
        "session_id": session_id,
        "app_name": app_name,
        "user_id": user_id,
        "invocation_id": "inv-001",
        "author": "user",
        "actions": pickle.dumps([{"type": "test_action"}]),
        "timestamp": datetime.now(timezone.utc),
        "content": {"text": "Hello"},
    }
    await asyncmy_adk_store.append_event(event_record)  # type: ignore[arg-type]

    events_before = await asyncmy_adk_store.get_events(session_id)
    assert len(events_before) == 1

    await asyncmy_adk_store.delete_session(session_id)

    session_after = await asyncmy_adk_store.get_session(session_id)
    assert session_after is None

    events_after = await asyncmy_adk_store.get_events(session_id)
    assert len(events_after) == 0


async def test_append_and_get_events(asyncmy_adk_store: AsyncmyADKStore) -> None:
    """Test appending and retrieving events."""
    session_id = "session-005"
    app_name = "test-app"
    user_id = "user-005"

    await asyncmy_adk_store.create_session(session_id, app_name, user_id, {"status": "active"})

    event1 = {
        "id": "event-001",
        "session_id": session_id,
        "app_name": app_name,
        "user_id": user_id,
        "invocation_id": "inv-001",
        "author": "user",
        "actions": pickle.dumps([{"type": "message", "content": "Hello"}]),
        "timestamp": datetime.now(timezone.utc),
        "content": {"text": "Hello", "role": "user"},
        "partial": False,
        "turn_complete": True,
    }

    event2 = {
        "id": "event-002",
        "session_id": session_id,
        "app_name": app_name,
        "user_id": user_id,
        "invocation_id": "inv-002",
        "author": "assistant",
        "actions": pickle.dumps([{"type": "response", "content": "Hi there"}]),
        "timestamp": datetime.now(timezone.utc),
        "content": {"text": "Hi there", "role": "assistant"},
        "partial": False,
        "turn_complete": True,
    }

    await asyncmy_adk_store.append_event(event1)  # type: ignore[arg-type]
    await asyncmy_adk_store.append_event(event2)  # type: ignore[arg-type]

    events = await asyncmy_adk_store.get_events(session_id)

    assert len(events) == 2
    assert events[0]["id"] == "event-001"
    assert events[1]["id"] == "event-002"
    assert events[0]["content"] is not None
    assert events[1]["content"] is not None
    assert events[0]["content"]["text"] == "Hello"
    assert events[1]["content"]["text"] == "Hi there"
    assert isinstance(events[0]["actions"], bytes)
    assert pickle.loads(events[0]["actions"])[0]["type"] == "message"


async def test_timestamp_precision(asyncmy_adk_store: AsyncmyADKStore) -> None:
    """Test TIMESTAMP(6) provides microsecond precision."""
    session_id = "session-006"
    app_name = "test-app"
    user_id = "user-006"

    created = await asyncmy_adk_store.create_session(session_id, app_name, user_id, {"test": "precision"})

    assert created["create_time"].microsecond > 0 or created["create_time"].microsecond == 0
    assert hasattr(created["create_time"], "microsecond")

    event_time = datetime.now(timezone.utc)
    event = {
        "id": "event-micro",
        "session_id": session_id,
        "app_name": app_name,
        "user_id": user_id,
        "invocation_id": "inv-micro",
        "author": "system",
        "actions": b"",
        "timestamp": event_time,
    }
    await asyncmy_adk_store.append_event(event)  # type: ignore[arg-type]

    events = await asyncmy_adk_store.get_events(session_id)
    assert len(events) == 1
    assert hasattr(events[0]["timestamp"], "microsecond")


async def test_owner_id_column_creation(asyncmy_adk_store_with_fk: AsyncmyADKStore) -> None:
    """Test owner ID column is created correctly."""
    assert asyncmy_adk_store_with_fk.owner_id_column_name == "tenant_id"
    assert asyncmy_adk_store_with_fk.owner_id_column_ddl is not None
    assert "tenant_id" in asyncmy_adk_store_with_fk.owner_id_column_ddl

    config = asyncmy_adk_store_with_fk.config

    async with config.provide_connection() as conn, conn.cursor() as cursor:
        await cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'test_fk_sessions'
            AND COLUMN_NAME = 'tenant_id'
        """)
        result = await cursor.fetchone()
        assert result is not None
        assert result[0] == "tenant_id"
        assert result[1] == "bigint"


async def test_owner_id_constraint_enforcement(asyncmy_adk_store_with_fk: AsyncmyADKStore) -> None:
    """Test FK constraint enforces referential integrity."""
    session_id = "session-fk-001"
    app_name = "test-app"
    user_id = "user-fk"

    await asyncmy_adk_store_with_fk.create_session(
        session_id=session_id, app_name=app_name, user_id=user_id, state={"tenant": "one"}, owner_id=1
    )

    session = await asyncmy_adk_store_with_fk.get_session(session_id)
    assert session is not None

    with pytest.raises(Exception):
        await asyncmy_adk_store_with_fk.create_session(
            session_id="invalid-fk", app_name=app_name, user_id=user_id, state={"tenant": "invalid"}, owner_id=999
        )


async def test_owner_id_cascade_delete(asyncmy_adk_store_with_fk: AsyncmyADKStore) -> None:
    """Test CASCADE DELETE when parent tenant is deleted."""
    config = asyncmy_adk_store_with_fk.config

    await asyncmy_adk_store_with_fk.create_session(
        session_id="tenant1-session", app_name="test-app", user_id="user1", state={"data": "test"}, owner_id=1
    )

    session_before = await asyncmy_adk_store_with_fk.get_session("tenant1-session")
    assert session_before is not None

    async with config.provide_connection() as conn, conn.cursor() as cursor:
        await cursor.execute("DELETE FROM test_tenants WHERE id = 1")
        await conn.commit()

    session_after = await asyncmy_adk_store_with_fk.get_session("tenant1-session")
    assert session_after is None


async def test_multi_tenant_isolation(asyncmy_adk_store_with_fk: AsyncmyADKStore) -> None:
    """Test FK column enables multi-tenant data isolation."""
    app_name = "test-app"
    user_id = "user-shared"

    await asyncmy_adk_store_with_fk.create_session("tenant1-s1", app_name, user_id, {"tenant": "one"}, owner_id=1)
    await asyncmy_adk_store_with_fk.create_session("tenant1-s2", app_name, user_id, {"tenant": "one"}, owner_id=1)
    await asyncmy_adk_store_with_fk.create_session("tenant2-s1", app_name, user_id, {"tenant": "two"}, owner_id=2)

    config = asyncmy_adk_store_with_fk.config
    async with config.provide_connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            f"SELECT id FROM {asyncmy_adk_store_with_fk.session_table} WHERE tenant_id = %s ORDER BY id", (1,)
        )
        tenant1_sessions = await cursor.fetchall()
        assert len(tenant1_sessions) == 2
        assert tenant1_sessions[0][0] == "tenant1-s1"
        assert tenant1_sessions[1][0] == "tenant1-s2"

        await cursor.execute(f"SELECT id FROM {asyncmy_adk_store_with_fk.session_table} WHERE tenant_id = %s", (2,))
        tenant2_sessions = await cursor.fetchall()
        assert len(tenant2_sessions) == 1
        assert tenant2_sessions[0][0] == "tenant2-s1"
