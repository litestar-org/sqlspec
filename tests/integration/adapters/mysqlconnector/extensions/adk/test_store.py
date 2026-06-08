"""Integration tests for MysqlConnector ADK session store.

The shared session/event CRUD lifecycle (create_tables, session round-trip, list/delete,
append/get events, get_events filtering) is covered by
tests/integration/adapters/contracts/test_adk_store_contract.py. This module keeps the
adapter-specific coverage (owner_id_column, storage-type fidelity, timestamp precision,
concurrency, event ordering/JSON details) that is not portable across the contract matrix.
"""

from datetime import datetime, timezone
from typing import cast

import pytest

from sqlspec.adapters.mysqlconnector.adk import MysqlConnectorAsyncADKStore
from sqlspec.extensions.adk import EventRecord

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql_connector, pytest.mark.integration]


async def test_storage_types_verification(mysqlconnector_adk_store: MysqlConnectorAsyncADKStore) -> None:
    """Verify MySQL uses JSON type (not TEXT) and TIMESTAMP(6) for microseconds."""
    config = mysqlconnector_adk_store.config

    async with config.provide_connection() as conn:
        cursor = await conn.cursor()
        try:
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
            assert "timestamp(6)" in cast("str", create_time_col[2]).lower()

            update_time_col = next(col for col in session_columns if col[0] == "update_time")
            assert "timestamp(6)" in cast("str", update_time_col[2]).lower()

            await cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'test_events'
                ORDER BY ORDINAL_POSITION
            """)
            event_columns = await cursor.fetchall()
            event_col_names = [col[0] for col in event_columns]

            # New 5-column schema: session_id, invocation_id, author, timestamp, event_json
            assert "session_id" in event_col_names
            assert "invocation_id" in event_col_names
            assert "author" in event_col_names
            assert "timestamp" in event_col_names
            assert "event_json" in event_col_names

            timestamp_col = next(col for col in event_columns if col[0] == "timestamp")
            assert "timestamp(6)" in cast("str", timestamp_col[2]).lower()
        finally:
            await cursor.close()


async def test_timestamp_precision(mysqlconnector_adk_store: MysqlConnectorAsyncADKStore) -> None:
    """Test TIMESTAMP(6) provides microsecond precision."""
    session_id = "session-006"
    app_name = "test-app"
    user_id = "user-006"

    created = await mysqlconnector_adk_store.create_session(session_id, app_name, user_id, {"test": "precision"})

    assert hasattr(created["create_time"], "microsecond")

    event_time = datetime.now(timezone.utc)
    event: EventRecord = {
        "session_id": session_id,
        "invocation_id": "inv-micro",
        "author": "system",
        "timestamp": event_time,
        "event_json": {"app_name": app_name},
    }
    await mysqlconnector_adk_store.append_event(event)

    events = await mysqlconnector_adk_store.get_events(session_id)
    assert len(events) == 1
    assert hasattr(events[0]["timestamp"], "microsecond")


async def test_owner_id_column_creation(mysqlconnector_adk_store_with_fk: MysqlConnectorAsyncADKStore) -> None:
    """Test owner ID column is created correctly."""
    assert mysqlconnector_adk_store_with_fk.owner_id_column_name == "tenant_id"
    assert mysqlconnector_adk_store_with_fk.owner_id_column_ddl is not None
    assert "tenant_id" in mysqlconnector_adk_store_with_fk.owner_id_column_ddl

    config = mysqlconnector_adk_store_with_fk.config

    async with config.provide_connection() as conn:
        cursor = await conn.cursor()
        try:
            await cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'test_fk_sessions'
                AND COLUMN_NAME = 'tenant_id'
            """)
            result = await cursor.fetchone()
        finally:
            await cursor.close()
        assert result is not None
        assert result[0] == "tenant_id"
        assert result[1] == "bigint"


async def test_owner_id_constraint_enforcement(mysqlconnector_adk_store_with_fk: MysqlConnectorAsyncADKStore) -> None:
    """Test FK constraint enforces referential integrity."""
    session_id = "session-fk-001"
    app_name = "test-app"
    user_id = "user-fk"

    await mysqlconnector_adk_store_with_fk.create_session(
        session_id=session_id, app_name=app_name, user_id=user_id, state={"tenant": "one"}, owner_id=1
    )

    session = await mysqlconnector_adk_store_with_fk.get_session(session_id)
    assert session is not None

    with pytest.raises(Exception):
        await mysqlconnector_adk_store_with_fk.create_session(
            session_id="invalid-fk", app_name=app_name, user_id=user_id, state={"tenant": "invalid"}, owner_id=999
        )


async def test_owner_id_cascade_delete(mysqlconnector_adk_store_with_fk: MysqlConnectorAsyncADKStore) -> None:
    """Test CASCADE DELETE when parent tenant is deleted."""
    config = mysqlconnector_adk_store_with_fk.config

    await mysqlconnector_adk_store_with_fk.create_session(
        session_id="tenant1-session", app_name="test-app", user_id="user1", state={"data": "test"}, owner_id=1
    )

    session_before = await mysqlconnector_adk_store_with_fk.get_session("tenant1-session")
    assert session_before is not None

    async with config.provide_connection() as conn:
        cursor = await conn.cursor()
        try:
            await cursor.execute("DELETE FROM test_tenants WHERE id = 1")
            await conn.commit()
        finally:
            await cursor.close()

    session_after = await mysqlconnector_adk_store_with_fk.get_session("tenant1-session")
    assert session_after is None


async def test_multi_tenant_isolation(mysqlconnector_adk_store_with_fk: MysqlConnectorAsyncADKStore) -> None:
    """Test FK column enables multi-tenant data isolation."""
    app_name = "test-app"
    user_id = "user-shared"

    await mysqlconnector_adk_store_with_fk.create_session(
        "tenant1-s1", app_name, user_id, {"tenant": "one"}, owner_id=1
    )
    await mysqlconnector_adk_store_with_fk.create_session(
        "tenant1-s2", app_name, user_id, {"tenant": "one"}, owner_id=1
    )
    await mysqlconnector_adk_store_with_fk.create_session(
        "tenant2-s1", app_name, user_id, {"tenant": "two"}, owner_id=2
    )

    config = mysqlconnector_adk_store_with_fk.config
    async with config.provide_connection() as conn:
        cursor = await conn.cursor()
        try:
            await cursor.execute(
                f"SELECT id FROM {mysqlconnector_adk_store_with_fk.session_table} WHERE tenant_id = %s ORDER BY id",
                (1,),
            )
            tenant1_sessions = await cursor.fetchall()
            assert len(tenant1_sessions) == 2
            assert tenant1_sessions[0][0] == "tenant1-s1"
            assert tenant1_sessions[1][0] == "tenant1-s2"

            await cursor.execute(
                f"SELECT id FROM {mysqlconnector_adk_store_with_fk.session_table} WHERE tenant_id = %s", (2,)
            )
            tenant2_sessions = await cursor.fetchall()
            assert len(tenant2_sessions) == 1
            assert tenant2_sessions[0][0] == "tenant2-s1"
        finally:
            await cursor.close()
