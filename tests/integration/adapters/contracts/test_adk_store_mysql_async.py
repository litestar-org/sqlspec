"""MySQL async-family ADK store contract tests."""

import json
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql.adk.store import AiomysqlADKStore
from sqlspec.adapters.asyncmy.adk.store import AsyncmyADKStore
from sqlspec.extensions.adk import EventRecord
from tests.integration.adapters.contracts._mysql_async import (
    MYSQL_ASYNC_ADAPTERS,
    close_mysql_async_config,
    mysql_async_config,
)

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.integration]


def _adk_store_type(adapter: str) -> type[Any]:
    if adapter == "aiomysql":
        return AiomysqlADKStore
    return AsyncmyADKStore


async def _drop_adk_tables(config: Any, *, with_owner_id: bool = False) -> None:
    async with config.provide_session() as driver:
        await driver.execute("SET FOREIGN_KEY_CHECKS = 0")
        if with_owner_id:
            await driver.execute_script("""
                DROP TABLE IF EXISTS test_fk_events;
                DROP TABLE IF EXISTS test_fk_sessions;
                DROP TABLE IF EXISTS test_tenants;
            """)
        else:
            await driver.execute_script("""
                DROP TABLE IF EXISTS test_events;
                DROP TABLE IF EXISTS test_sessions;
            """)
        await driver.execute("SET FOREIGN_KEY_CHECKS = 1")
        await driver.commit()


@pytest.fixture(params=MYSQL_ASYNC_ADAPTERS)
async def mysql_async_adk_store(
    request: pytest.FixtureRequest, mysql_service: MySQLService
) -> AsyncGenerator[Any, None]:
    """Create a MySQL async-family ADK store."""
    config = mysql_async_config(
        str(request.param),
        mysql_service,
        autocommit=False,
        minsize=1,
        maxsize=5,
        extension_config={"adk": {"session_table": "test_sessions", "events_table": "test_events"}},
    )

    try:
        await _drop_adk_tables(config)
        store = _adk_store_type(str(request.param))(config)
        await store.create_tables()

        yield store

        await _drop_adk_tables(config)
    finally:
        await close_mysql_async_config(config)


@pytest.fixture(params=MYSQL_ASYNC_ADAPTERS)
async def mysql_async_adk_store_with_fk(
    request: pytest.FixtureRequest, mysql_service: MySQLService
) -> AsyncGenerator[Any, None]:
    """Create a MySQL async-family ADK store with owner ID foreign-key column."""
    config = mysql_async_config(
        str(request.param),
        mysql_service,
        autocommit=False,
        minsize=1,
        maxsize=5,
        extension_config={
            "adk": {
                "session_table": "test_fk_sessions",
                "events_table": "test_fk_events",
                "owner_id_column": "tenant_id BIGINT NOT NULL REFERENCES test_tenants(id) ON DELETE CASCADE",
            }
        },
    )

    try:
        await _drop_adk_tables(config, with_owner_id=True)
        async with config.provide_session() as driver:
            await driver.execute_script("""
                CREATE TABLE IF NOT EXISTS test_tenants (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(128) NOT NULL UNIQUE
                ) ENGINE=InnoDB
            """)
            await driver.execute("INSERT INTO test_tenants (name) VALUES (?), (?)", ("tenant1", "tenant2"))
            await driver.commit()

        store = _adk_store_type(str(request.param))(config)
        await store.create_tables()

        yield store

        await _drop_adk_tables(config, with_owner_id=True)
    finally:
        await close_mysql_async_config(config)


async def test_create_tables(mysql_async_adk_store: Any) -> None:
    """Test table creation succeeds without errors."""
    assert mysql_async_adk_store.session_table == "test_sessions"
    assert mysql_async_adk_store.events_table == "test_events"


async def test_storage_types_verification(mysql_async_adk_store: Any) -> None:
    """Verify MySQL uses native JSON and microsecond timestamp columns."""
    config = mysql_async_adk_store.config

    async with config.provide_session() as driver:
        session_result = await driver.execute("""
            SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, COLUMN_TYPE AS column_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'test_sessions'
            ORDER BY ORDINAL_POSITION
        """)
        session_columns = session_result.get_data()

        state_col = next(col for col in session_columns if col["column_name"] == "state")
        assert state_col["data_type"] == "json", "state column must use native JSON type (not TEXT)"

        create_time_col = next(col for col in session_columns if col["column_name"] == "create_time")
        assert "timestamp(6)" in create_time_col["column_type"].lower()

        update_time_col = next(col for col in session_columns if col["column_name"] == "update_time")
        assert "timestamp(6)" in update_time_col["column_type"].lower()

        event_result = await driver.execute("""
            SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, COLUMN_TYPE AS column_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'test_events'
            ORDER BY ORDINAL_POSITION
        """)
        event_columns = event_result.get_data()
        event_col_names = [col["column_name"] for col in event_columns]

        assert "session_id" in event_col_names
        assert "invocation_id" in event_col_names
        assert "author" in event_col_names
        assert "timestamp" in event_col_names
        assert "event_json" in event_col_names

        timestamp_col = next(col for col in event_columns if col["column_name"] == "timestamp")
        assert "timestamp(6)" in timestamp_col["column_type"].lower()


async def test_create_and_get_session(mysql_async_adk_store: Any) -> None:
    """Test creating and retrieving a session."""
    session_id = "session-001"
    app_name = "test-app"
    user_id = "user-001"
    state = {"key": "value", "count": 42}

    created_session = await mysql_async_adk_store.create_session(
        session_id=session_id, app_name=app_name, user_id=user_id, state=state
    )

    assert created_session["id"] == session_id
    assert created_session["app_name"] == app_name
    assert created_session["user_id"] == user_id
    assert created_session["state"] == state
    assert isinstance(created_session["create_time"], datetime)
    assert isinstance(created_session["update_time"], datetime)

    retrieved_session = await mysql_async_adk_store.get_session(session_id)
    assert retrieved_session is not None
    assert retrieved_session["id"] == session_id
    assert retrieved_session["state"] == state


async def test_get_nonexistent_session(mysql_async_adk_store: Any) -> None:
    """Test getting a non-existent session returns None."""
    result = await mysql_async_adk_store.get_session("nonexistent-session")
    assert result is None


async def test_update_session_state(mysql_async_adk_store: Any) -> None:
    """Test updating session state."""
    session_id = "session-002"
    initial_state = {"status": "active"}
    updated_state = {"status": "completed", "result": "success"}

    await mysql_async_adk_store.create_session(
        session_id=session_id, app_name="test-app", user_id="user-002", state=initial_state
    )

    session_before = await mysql_async_adk_store.get_session(session_id)
    assert session_before is not None
    assert session_before["state"] == initial_state

    await mysql_async_adk_store.update_session_state(session_id, updated_state)

    session_after = await mysql_async_adk_store.get_session(session_id)
    assert session_after is not None
    assert session_after["state"] == updated_state
    assert session_after["update_time"] >= session_before["update_time"]


async def test_list_sessions(mysql_async_adk_store: Any) -> None:
    """Test listing sessions for an app and user."""
    app_name = "test-app"
    user_id = "user-003"

    await mysql_async_adk_store.create_session("session-a", app_name, user_id, {"num": 1})
    await mysql_async_adk_store.create_session("session-b", app_name, user_id, {"num": 2})
    await mysql_async_adk_store.create_session("session-c", app_name, "other-user", {"num": 3})

    sessions = await mysql_async_adk_store.list_sessions(app_name, user_id)

    assert len(sessions) == 2
    session_ids = {s["id"] for s in sessions}
    assert session_ids == {"session-a", "session-b"}
    assert all(s["app_name"] == app_name for s in sessions)
    assert all(s["user_id"] == user_id for s in sessions)


async def test_delete_session_cascade(mysql_async_adk_store: Any) -> None:
    """Test deleting session cascades to events."""
    session_id = "session-004"
    app_name = "test-app"
    user_id = "user-004"

    await mysql_async_adk_store.create_session(session_id, app_name, user_id, {"status": "active"})

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "inv-001",
        "author": "user",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"content": {"text": "Hello"}, "app_name": app_name, "user_id": user_id},
    }
    await mysql_async_adk_store.append_event(event_record)

    events_before = await mysql_async_adk_store.get_events(session_id)
    assert len(events_before) == 1

    await mysql_async_adk_store.delete_session(session_id)

    session_after = await mysql_async_adk_store.get_session(session_id)
    assert session_after is None

    events_after = await mysql_async_adk_store.get_events(session_id)
    assert len(events_after) == 0


async def test_append_and_get_events(mysql_async_adk_store: Any) -> None:
    """Test appending and retrieving events."""
    session_id = "session-005"
    app_name = "test-app"
    user_id = "user-005"

    await mysql_async_adk_store.create_session(session_id, app_name, user_id, {"status": "active"})

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

    await mysql_async_adk_store.append_event(event1)
    await mysql_async_adk_store.append_event(event2)

    events = await mysql_async_adk_store.get_events(session_id)

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


async def test_timestamp_precision(mysql_async_adk_store: Any) -> None:
    """Test TIMESTAMP(6) provides microsecond precision."""
    session_id = "session-006"
    app_name = "test-app"
    user_id = "user-006"

    created = await mysql_async_adk_store.create_session(session_id, app_name, user_id, {"test": "precision"})

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
    await mysql_async_adk_store.append_event(event)

    events = await mysql_async_adk_store.get_events(session_id)
    assert len(events) == 1
    assert hasattr(events[0]["timestamp"], "microsecond")


async def test_owner_id_column_creation(mysql_async_adk_store_with_fk: Any) -> None:
    """Test owner ID column is created correctly."""
    assert mysql_async_adk_store_with_fk.owner_id_column_name == "tenant_id"
    assert mysql_async_adk_store_with_fk.owner_id_column_ddl is not None
    assert "tenant_id" in mysql_async_adk_store_with_fk.owner_id_column_ddl

    config = mysql_async_adk_store_with_fk.config
    async with config.provide_session() as driver:
        result = await driver.execute("""
            SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'test_fk_sessions'
            AND COLUMN_NAME = 'tenant_id'
        """)
        data = result.get_data()
        assert data
        assert data[0]["column_name"] == "tenant_id"
        assert data[0]["data_type"] == "bigint"


async def test_owner_id_constraint_enforcement(mysql_async_adk_store_with_fk: Any) -> None:
    """Test FK constraint enforces referential integrity."""
    session_id = "session-fk-001"
    app_name = "test-app"
    user_id = "user-fk"

    await mysql_async_adk_store_with_fk.create_session(
        session_id=session_id, app_name=app_name, user_id=user_id, state={"tenant": "one"}, owner_id=1
    )

    session = await mysql_async_adk_store_with_fk.get_session(session_id)
    assert session is not None

    with pytest.raises(Exception):
        await mysql_async_adk_store_with_fk.create_session(
            session_id="invalid-fk", app_name=app_name, user_id=user_id, state={"tenant": "invalid"}, owner_id=999
        )


async def test_owner_id_cascade_delete(mysql_async_adk_store_with_fk: Any) -> None:
    """Test CASCADE DELETE when parent tenant is deleted."""
    config = mysql_async_adk_store_with_fk.config

    await mysql_async_adk_store_with_fk.create_session(
        session_id="tenant1-session", app_name="test-app", user_id="user1", state={"data": "test"}, owner_id=1
    )

    session_before = await mysql_async_adk_store_with_fk.get_session("tenant1-session")
    assert session_before is not None

    async with config.provide_session() as driver:
        await driver.execute("DELETE FROM test_tenants WHERE id = ?", (1,))
        await driver.commit()

    session_after = await mysql_async_adk_store_with_fk.get_session("tenant1-session")
    assert session_after is None


async def test_multi_tenant_isolation(mysql_async_adk_store_with_fk: Any) -> None:
    """Test FK column enables multi-tenant data isolation."""
    app_name = "test-app"
    user_id = "user-shared"

    await mysql_async_adk_store_with_fk.create_session("tenant1-s1", app_name, user_id, {"tenant": "one"}, owner_id=1)
    await mysql_async_adk_store_with_fk.create_session("tenant1-s2", app_name, user_id, {"tenant": "one"}, owner_id=1)
    await mysql_async_adk_store_with_fk.create_session("tenant2-s1", app_name, user_id, {"tenant": "two"}, owner_id=2)

    config = mysql_async_adk_store_with_fk.config
    async with config.provide_session() as driver:
        tenant1_result = await driver.execute(
            f"SELECT id FROM {mysql_async_adk_store_with_fk.session_table} WHERE tenant_id = ? ORDER BY id", (1,)
        )
        tenant1_sessions = tenant1_result.get_data()
        assert len(tenant1_sessions) == 2
        assert tenant1_sessions[0]["id"] == "tenant1-s1"
        assert tenant1_sessions[1]["id"] == "tenant1-s2"

        tenant2_result = await driver.execute(
            f"SELECT id FROM {mysql_async_adk_store_with_fk.session_table} WHERE tenant_id = ?", (2,)
        )
        tenant2_sessions = tenant2_result.get_data()
        assert len(tenant2_sessions) == 1
        assert tenant2_sessions[0]["id"] == "tenant2-s1"
