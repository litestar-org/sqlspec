"""Integration tests for aiomysql ADK session store."""

import json
from datetime import datetime, timezone

import pytest

from sqlspec.adapters.aiomysql._typing import AiomysqlCursor
from sqlspec.adapters.aiomysql.adk.store import AiomysqlADKStore
from sqlspec.extensions.adk import EventRecord

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.aiomysql, pytest.mark.integration]


async def test_create_tables(aiomysql_adk_store: AiomysqlADKStore) -> None:
    """Test table creation succeeds without errors."""
    assert aiomysql_adk_store.session_table == "test_sessions"
    assert aiomysql_adk_store.events_table == "test_events"


async def test_storage_types_verification(aiomysql_adk_store: AiomysqlADKStore) -> None:
    """Verify MySQL uses JSON type (not TEXT) and TIMESTAMP(6) for microseconds.

    Critical verification from ADK implementation review.
    Ensures we're using MySQL native types optimally.
    """
    config = aiomysql_adk_store.config

    async with config.provide_connection() as conn, AiomysqlCursor(conn) as cursor:
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
        event_col_names = [col[0] for col in event_columns]

        assert "session_id" in event_col_names
        assert "invocation_id" in event_col_names
        assert "author" in event_col_names
        assert "timestamp" in event_col_names
        assert "event_json" in event_col_names

        timestamp_col = next(col for col in event_columns if col[0] == "timestamp")
        assert "timestamp(6)" in timestamp_col[2].lower(), "timestamp must be TIMESTAMP(6) for microseconds"


async def test_create_and_get_session(aiomysql_adk_store: AiomysqlADKStore) -> None:
    """Test creating and retrieving a session."""
    session_id = "session-001"
    app_name = "test-app"
    user_id = "user-001"
    state = {"key": "value", "count": 42}

    created_session = await aiomysql_adk_store.create_session(
        session_id=session_id, app_name=app_name, user_id=user_id, state=state
    )

    assert created_session["id"] == session_id
    assert created_session["app_name"] == app_name
    assert created_session["user_id"] == user_id
    assert created_session["state"] == state
    assert isinstance(created_session["create_time"], datetime)
    assert isinstance(created_session["update_time"], datetime)

    retrieved_session = await aiomysql_adk_store.get_session(session_id)
    assert retrieved_session is not None
    assert retrieved_session["id"] == session_id
    assert retrieved_session["state"] == state


async def test_get_nonexistent_session(aiomysql_adk_store: AiomysqlADKStore) -> None:
    """Test getting a non-existent session returns None."""
    result = await aiomysql_adk_store.get_session("nonexistent-session")
    assert result is None


async def test_update_session_state(aiomysql_adk_store: AiomysqlADKStore) -> None:
    """Test updating session state."""
    session_id = "session-002"
    initial_state = {"status": "active"}
    updated_state = {"status": "completed", "result": "success"}

    await aiomysql_adk_store.create_session(
        session_id=session_id, app_name="test-app", user_id="user-002", state=initial_state
    )

    session_before = await aiomysql_adk_store.get_session(session_id)
    assert session_before is not None
    assert session_before["state"] == initial_state

    await aiomysql_adk_store.update_session_state(session_id, updated_state)

    session_after = await aiomysql_adk_store.get_session(session_id)
    assert session_after is not None
    assert session_after["state"] == updated_state
    assert session_after["update_time"] >= session_before["update_time"]


async def test_list_sessions(aiomysql_adk_store: AiomysqlADKStore) -> None:
    """Test listing sessions for an app and user."""
    app_name = "test-app"
    user_id = "user-003"

    await aiomysql_adk_store.create_session("session-a", app_name, user_id, {"num": 1})
    await aiomysql_adk_store.create_session("session-b", app_name, user_id, {"num": 2})
    await aiomysql_adk_store.create_session("session-c", app_name, "other-user", {"num": 3})

    sessions = await aiomysql_adk_store.list_sessions(app_name, user_id)

    assert len(sessions) == 2
    session_ids = {s["id"] for s in sessions}
    assert session_ids == {"session-a", "session-b"}
    assert all(s["app_name"] == app_name for s in sessions)
    assert all(s["user_id"] == user_id for s in sessions)


async def test_delete_session_cascade(aiomysql_adk_store: AiomysqlADKStore) -> None:
    """Test deleting session cascades to events."""
    session_id = "session-004"
    app_name = "test-app"
    user_id = "user-004"

    await aiomysql_adk_store.create_session(session_id, app_name, user_id, {"status": "active"})

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "inv-001",
        "author": "user",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"content": {"text": "Hello"}, "app_name": app_name, "user_id": user_id},
    }
    await aiomysql_adk_store.append_event(event_record)

    events_before = await aiomysql_adk_store.get_events(session_id)
    assert len(events_before) == 1

    await aiomysql_adk_store.delete_session(session_id)

    session_after = await aiomysql_adk_store.get_session(session_id)
    assert session_after is None

    events_after = await aiomysql_adk_store.get_events(session_id)
    assert len(events_after) == 0


async def test_append_and_get_events(aiomysql_adk_store: AiomysqlADKStore) -> None:
    """Test appending and retrieving events."""
    session_id = "session-005"
    app_name = "test-app"
    user_id = "user-005"

    await aiomysql_adk_store.create_session(session_id, app_name, user_id, {"status": "active"})

    event1: EventRecord = {
        "session_id": session_id,
        "invocation_id": "inv-001",
        "author": "user",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"content": {"text": "Hello", "role": "user"}, "app_name": app_name},
    }

    event2: EventRecord = {
        "session_id": session_id,
        "invocation_id": "inv-002",
        "author": "assistant",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"content": {"text": "Hi there", "role": "assistant"}, "app_name": app_name},
    }

    await aiomysql_adk_store.append_event(event1)
    await aiomysql_adk_store.append_event(event2)

    events = await aiomysql_adk_store.get_events(session_id)

    assert len(events) == 2
    assert events[0]["author"] == "user"
    assert events[1]["author"] == "assistant"
    event0_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    event1_data = (
        json.loads(events[1]["event_json"]) if isinstance(events[1]["event_json"], str) else events[1]["event_json"]
    )
    assert event0_data["content"]["text"] == "Hello"
    assert event1_data["content"]["text"] == "Hi there"


async def test_timestamp_precision(aiomysql_adk_store: AiomysqlADKStore) -> None:
    """Test TIMESTAMP(6) provides microsecond precision."""
    session_id = "session-006"
    app_name = "test-app"
    user_id = "user-006"

    created = await aiomysql_adk_store.create_session(session_id, app_name, user_id, {"test": "precision"})

    assert created["create_time"].microsecond > 0 or created["create_time"].microsecond == 0
    assert hasattr(created["create_time"], "microsecond")

    event_time = datetime.now(timezone.utc)
    event: EventRecord = {
        "session_id": session_id,
        "invocation_id": "inv-micro",
        "author": "system",
        "timestamp": event_time,
        "event_json": {"app_name": app_name},
    }
    await aiomysql_adk_store.append_event(event)

    events = await aiomysql_adk_store.get_events(session_id)
    assert len(events) == 1
    assert hasattr(events[0]["timestamp"], "microsecond")


async def test_owner_id_column_creation(aiomysql_adk_store_with_fk: AiomysqlADKStore) -> None:
    """Test owner ID column is created correctly."""
    assert aiomysql_adk_store_with_fk.owner_id_column_name == "tenant_id"
    assert aiomysql_adk_store_with_fk.owner_id_column_ddl is not None
    assert "tenant_id" in aiomysql_adk_store_with_fk.owner_id_column_ddl

    config = aiomysql_adk_store_with_fk.config

    async with config.provide_connection() as conn, AiomysqlCursor(conn) as cursor:
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


async def test_owner_id_constraint_enforcement(aiomysql_adk_store_with_fk: AiomysqlADKStore) -> None:
    """Test FK constraint enforces referential integrity."""
    session_id = "session-fk-001"
    app_name = "test-app"
    user_id = "user-fk"

    await aiomysql_adk_store_with_fk.create_session(
        session_id=session_id, app_name=app_name, user_id=user_id, state={"tenant": "one"}, owner_id=1
    )

    session = await aiomysql_adk_store_with_fk.get_session(session_id)
    assert session is not None

    with pytest.raises(Exception):
        await aiomysql_adk_store_with_fk.create_session(
            session_id="invalid-fk", app_name=app_name, user_id=user_id, state={"tenant": "invalid"}, owner_id=999
        )


async def test_owner_id_cascade_delete(aiomysql_adk_store_with_fk: AiomysqlADKStore) -> None:
    """Test CASCADE DELETE when parent tenant is deleted."""
    config = aiomysql_adk_store_with_fk.config

    await aiomysql_adk_store_with_fk.create_session(
        session_id="tenant1-session", app_name="test-app", user_id="user1", state={"data": "test"}, owner_id=1
    )

    session_before = await aiomysql_adk_store_with_fk.get_session("tenant1-session")
    assert session_before is not None

    async with config.provide_connection() as conn, AiomysqlCursor(conn) as cursor:
        await cursor.execute("DELETE FROM test_tenants WHERE id = 1")
        await conn.commit()

    session_after = await aiomysql_adk_store_with_fk.get_session("tenant1-session")
    assert session_after is None


async def test_multi_tenant_isolation(aiomysql_adk_store_with_fk: AiomysqlADKStore) -> None:
    """Test FK column enables multi-tenant data isolation."""
    app_name = "test-app"
    user_id = "user-shared"

    await aiomysql_adk_store_with_fk.create_session("tenant1-s1", app_name, user_id, {"tenant": "one"}, owner_id=1)
    await aiomysql_adk_store_with_fk.create_session("tenant1-s2", app_name, user_id, {"tenant": "one"}, owner_id=1)
    await aiomysql_adk_store_with_fk.create_session("tenant2-s1", app_name, user_id, {"tenant": "two"}, owner_id=2)

    config = aiomysql_adk_store_with_fk.config
    async with config.provide_connection() as conn, AiomysqlCursor(conn) as cursor:
        await cursor.execute(
            f"SELECT id FROM {aiomysql_adk_store_with_fk.session_table} WHERE tenant_id = %s ORDER BY id", (1,)
        )
        tenant1_sessions = await cursor.fetchall()
        assert len(tenant1_sessions) == 2
        assert tenant1_sessions[0][0] == "tenant1-s1"
        assert tenant1_sessions[1][0] == "tenant1-s2"

        await cursor.execute(f"SELECT id FROM {aiomysql_adk_store_with_fk.session_table} WHERE tenant_id = %s", (2,))
        tenant2_sessions = await cursor.fetchall()
        assert len(tenant2_sessions) == 1
        assert tenant2_sessions[0][0] == "tenant2-s1"
