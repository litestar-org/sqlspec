"""Oracle-specific ADK store tests for LOB handling, JSON types, and FK columns."""

import json
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any, cast
from uuid import uuid4

import oracledb
import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.adk import OracleAsyncADKStore, OracleSyncADKStore
from sqlspec.extensions.adk import EventRecord

pytestmark = [pytest.mark.xdist_group("oracle"), pytest.mark.oracledb, pytest.mark.integration]


def _unique_session_id(prefix: str) -> str:
    """Return a unique session id for test isolation."""
    return f"{prefix}-{uuid4().hex}"


def _drop_table_statements(store: object) -> "list[str]":
    """Return drop table statements for ADK stores."""
    dropper = cast("Any", getattr(store, "_get_drop_tables_sql"))
    return cast("list[str]", dropper())


async def _cleanup_async_store(store: "OracleAsyncADKStore", config: "OracleAsyncConfig") -> None:
    """Drop ADK tables for async stores."""
    async with config.provide_connection() as conn:
        cursor = conn.cursor()
        for stmt in _drop_table_statements(store):
            try:
                await cursor.execute(stmt)
            except Exception:
                pass
        await conn.commit()


def _cleanup_sync_store(store: "OracleSyncADKStore", config: "OracleSyncConfig") -> None:
    """Drop ADK tables for sync stores."""
    with config.provide_connection() as conn:
        cursor = conn.cursor()
        for stmt in _drop_table_statements(store):
            try:
                cursor.execute(stmt)
            except Exception:
                pass
        conn.commit()


@pytest.fixture
async def oracle_async_store(oracle_async_config: "OracleAsyncConfig") -> "AsyncGenerator[OracleAsyncADKStore, None]":
    """Create an async Oracle ADK store with tables created per test."""
    store = OracleAsyncADKStore(oracle_async_config)
    await store.create_tables()
    try:
        yield store
    finally:
        await _cleanup_async_store(store, oracle_async_config)


@pytest.fixture(scope="module")
async def oracle_sync_store(oracle_sync_config: "OracleSyncConfig") -> "AsyncGenerator[OracleSyncADKStore, None]":
    """Create a sync Oracle ADK store with tables created once per module."""
    store = OracleSyncADKStore(oracle_sync_config)
    await store.create_tables()
    try:
        yield store
    finally:
        _cleanup_sync_store(store, oracle_sync_config)


@pytest.fixture
async def oracle_config_with_tenant_table(
    oracle_async_config: "OracleAsyncConfig",
) -> "AsyncGenerator[OracleAsyncConfig, None]":
    """Create a tenants table for FK testing."""
    async with oracle_async_config.provide_connection() as conn:
        cursor = conn.cursor()
        await cursor.execute(
            """
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE tenants (
                    id NUMBER(10) PRIMARY KEY,
                    name VARCHAR2(128) NOT NULL
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;
            """
        )
        await cursor.execute("INSERT INTO tenants (id, name) VALUES (1, 'Tenant A')")
        await cursor.execute("INSERT INTO tenants (id, name) VALUES (2, 'Tenant B')")
        await conn.commit()

    try:
        yield oracle_async_config
    finally:
        async with oracle_async_config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                await cursor.execute(
                    """
                    BEGIN
                        EXECUTE IMMEDIATE 'DROP TABLE tenants';
                    EXCEPTION
                        WHEN OTHERS THEN
                            IF SQLCODE != -942 THEN
                                RAISE;
                            END IF;
                    END;
                    """
                )
                await conn.commit()
            except Exception:
                pass


@pytest.fixture
async def oracle_store_with_fk(
    oracle_config_with_tenant_table: "OracleAsyncConfig",
) -> "AsyncGenerator[OracleAsyncADKStore, None]":
    """Create an async Oracle ADK store with owner_id FK column."""
    config_with_extension = OracleAsyncConfig(
        connection_config=oracle_config_with_tenant_table.connection_config,
        extension_config={"adk": {"owner_id_column": "tenant_id NUMBER(10) NOT NULL REFERENCES tenants(id)"}},
    )
    store = OracleAsyncADKStore(config_with_extension)
    await store.create_tables()
    try:
        yield store
    finally:
        await _cleanup_async_store(store, config_with_extension)


@pytest.fixture
async def oracle_config_with_users_table(
    oracle_sync_config: "OracleSyncConfig",
) -> "AsyncGenerator[OracleSyncConfig, None]":
    """Create a users table for FK testing."""
    with oracle_sync_config.provide_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE users (
                    id NUMBER(19) PRIMARY KEY,
                    username VARCHAR2(128) NOT NULL
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;
            """
        )
        cursor.execute("INSERT INTO users (id, username) VALUES (100, 'alice')")
        cursor.execute("INSERT INTO users (id, username) VALUES (200, 'bob')")
        conn.commit()

    try:
        yield oracle_sync_config
    finally:
        with oracle_sync_config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    BEGIN
                        EXECUTE IMMEDIATE 'DROP TABLE users';
                    EXCEPTION
                        WHEN OTHERS THEN
                            IF SQLCODE != -942 THEN
                                RAISE;
                            END IF;
                    END;
                    """
                )
                conn.commit()
            except Exception:
                pass


@pytest.fixture
async def oracle_store_sync_with_fk(
    oracle_config_with_users_table: "OracleSyncConfig",
) -> "AsyncGenerator[OracleSyncADKStore, None]":
    """Create a sync Oracle ADK store with owner_id FK column."""
    config_with_extension = OracleSyncConfig(
        connection_config=oracle_config_with_users_table.connection_config,
        extension_config={"adk": {"owner_id_column": "owner_id NUMBER(19) REFERENCES users(id) ON DELETE CASCADE"}},
    )
    store = OracleSyncADKStore(config_with_extension)
    _cleanup_sync_store(store, config_with_extension)
    await store.create_tables()
    try:
        yield store
    finally:
        _cleanup_sync_store(store, config_with_extension)


async def test_state_lob_deserialization(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test state CLOB/BLOB is correctly deserialized."""
    session_id = _unique_session_id("lob-session")
    app_name = "test-app"
    user_id = "user-123"
    state = {"large_field": "x" * 10000, "nested": {"data": [1, 2, 3]}}

    session = await oracle_async_store.create_session(session_id, app_name, user_id, state)
    assert session["state"] == state

    retrieved = await oracle_async_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == state
    assert retrieved["state"]["large_field"] == "x" * 10000


async def test_event_json_lob_deserialization(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test event_json CLOB is correctly deserialized."""
    session_id = _unique_session_id("event-lob")
    app_name = "test-app"
    user_id = "user-123"

    await oracle_async_store.create_session(session_id, app_name, user_id, {})

    content = {"message": "x" * 5000, "data": {"nested": True}}
    event_data = {
        "content": content,
        "app_name": app_name,
        "user_id": user_id,
        "grounding_metadata": {"sources": ["a" * 1000, "b" * 1000]},
        "custom_metadata": {"tags": ["tag1", "tag2"], "priority": "high"},
    }

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "assistant",
        "timestamp": datetime.now(timezone.utc),
        "event_json": event_data,
    }

    await oracle_async_store.append_event(event_record)

    events = await oracle_async_store.get_events(session_id)
    assert len(events) == 1
    # event_json contains all the data
    retrieved_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    assert retrieved_data["content"] == content
    assert retrieved_data["grounding_metadata"] == {"sources": ["a" * 1000, "b" * 1000]}
    assert retrieved_data["custom_metadata"] == {"tags": ["tag1", "tag2"], "priority": "high"}


async def test_event_json_storage(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test event_json blob is correctly stored and retrieved."""
    session_id = _unique_session_id("event-json")
    app_name = "test-app"
    user_id = "user-123"

    await oracle_async_store.create_session(session_id, app_name, user_id, {})

    event_data = {"function": "test_func", "args": {"param": "value"}, "result": 42}

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "user",
        "timestamp": datetime.now(timezone.utc),
        "event_json": event_data,
    }

    await oracle_async_store.append_event(event_record)

    events = await oracle_async_store.get_events(session_id)
    assert len(events) == 1
    retrieved_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    assert retrieved_data == event_data


async def test_state_lob_deserialization_sync(oracle_sync_store: "OracleSyncADKStore") -> None:
    """Test state CLOB/BLOB is correctly deserialized in sync mode."""
    session_id = _unique_session_id("lob-session-sync")
    app_name = "test-app"
    user_id = "user-123"
    state = {"large_field": "y" * 10000, "nested": {"data": [4, 5, 6]}}

    session = await oracle_sync_store.create_session(session_id, app_name, user_id, state)
    assert session["state"] == state

    retrieved = await oracle_sync_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == state


async def test_event_record_5_column_contract(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test the new 5-column EventRecord contract with append_event."""
    session_id = _unique_session_id("5col-session")
    app_name = "test-app"
    user_id = "user-123"

    await oracle_async_store.create_session(session_id, app_name, user_id, {})

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "inv-001",
        "author": "assistant",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"content": {"text": "Hello"}, "partial": True, "turn_complete": False, "interrupted": True},
    }

    await oracle_async_store.append_event(event_record)

    events = await oracle_async_store.get_events(session_id)
    assert len(events) == 1
    assert events[0]["session_id"] == session_id
    assert events[0]["invocation_id"] == "inv-001"
    assert events[0]["author"] == "assistant"

    retrieved_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    assert retrieved_data["partial"] is True
    assert retrieved_data["turn_complete"] is False
    assert retrieved_data["interrupted"] is True


async def test_event_with_none_values(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test event with minimal event_json content."""
    session_id = _unique_session_id("none-session")
    app_name = "test-app"
    user_id = "user-123"

    await oracle_async_store.create_session(session_id, app_name, user_id, {})

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "user",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"app_name": app_name},
    }

    await oracle_async_store.append_event(event_record)

    events = await oracle_async_store.get_events(session_id)
    assert len(events) == 1


async def test_create_session_with_owner_id(oracle_store_with_fk: "OracleAsyncADKStore") -> None:
    """Test creating session with owner_id parameter."""
    session_id = _unique_session_id("fk-session")
    app_name = "test-app"
    user_id = "user-123"
    state = {"data": "test"}
    tenant_id = 1

    session = await oracle_store_with_fk.create_session(session_id, app_name, user_id, state, owner_id=tenant_id)
    assert session["id"] == session_id
    assert session["state"] == state


async def test_owner_id_constraint_validation(oracle_store_with_fk: "OracleAsyncADKStore") -> None:
    """Test FK constraint is enforced (invalid FK should fail)."""
    session_id = _unique_session_id("fk-invalid")
    app_name = "test-app"
    user_id = "user-123"
    state = {"data": "test"}
    invalid_tenant_id = 9999

    with pytest.raises(oracledb.IntegrityError):
        await oracle_store_with_fk.create_session(session_id, app_name, user_id, state, owner_id=invalid_tenant_id)


async def test_create_session_without_owner_id_when_required(oracle_store_with_fk: "OracleAsyncADKStore") -> None:
    """Test creating session without owner_id when column has NOT NULL."""
    session_id = _unique_session_id("fk-missing")
    app_name = "test-app"
    user_id = "user-123"
    state = {"data": "test"}

    with pytest.raises(oracledb.IntegrityError):
        await oracle_store_with_fk.create_session(session_id, app_name, user_id, state, owner_id=None)


async def test_fk_column_name_parsing(oracle_async_config: "OracleAsyncConfig") -> None:
    """Test owner_id_column_name is correctly parsed from DDL."""
    config_with_extension = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config,
        extension_config={"adk": {"owner_id_column": "account_id NUMBER(19) REFERENCES accounts(id)"}},
    )
    store = OracleAsyncADKStore(config_with_extension)
    assert store.owner_id_column_name == "account_id"
    assert store.owner_id_column_ddl == "account_id NUMBER(19) REFERENCES accounts(id)"

    config_with_extension_two = OracleAsyncConfig(
        connection_config=oracle_async_config.connection_config,
        extension_config={"adk": {"owner_id_column": "org_uuid RAW(16) REFERENCES organizations(id)"}},
    )
    store_two = OracleAsyncADKStore(config_with_extension_two)
    assert store_two.owner_id_column_name == "org_uuid"


async def test_json_storage_type_detection(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test JSON storage type is detected correctly."""
    detector = cast("Any", oracle_async_store)
    storage_type = await detector._detect_json_storage_type()

    assert storage_type in ["json", "blob_json", "blob_plain"]


async def test_json_fields_stored_and_retrieved(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test JSON fields use appropriate CLOB/BLOB/JSON storage."""
    session_id = _unique_session_id("json-session")
    app_name = "test-app"
    user_id = "user-123"
    state = {
        "complex": {
            "nested": {"deep": {"structure": "value"}},
            "array": [1, 2, 3, {"key": "value"}],
            "unicode": "\u65e5\u672c\u8a9e\u30c6\u30b9\u30c8",
            "special_chars": "test@example.com | value > 100",
        }
    }

    session = await oracle_async_store.create_session(session_id, app_name, user_id, state)
    assert session["state"] == state

    retrieved = await oracle_async_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == state
    assert retrieved["state"]["complex"]["unicode"] == "\u65e5\u672c\u8a9e\u30c6\u30b9\u30c8"


async def test_create_session_with_owner_id_sync(oracle_store_sync_with_fk: "OracleSyncADKStore") -> None:
    """Test creating session with owner_id in sync mode."""
    session_id = _unique_session_id("sync-fk")
    app_name = "test-app"
    user_id = "alice"
    state = {"data": "sync test"}
    owner_id = 100

    session = await oracle_store_sync_with_fk.create_session(session_id, app_name, user_id, state, owner_id=owner_id)
    assert session["id"] == session_id
    assert session["state"] == state

    retrieved = await oracle_store_sync_with_fk.get_session(session_id)
    assert retrieved is not None
    assert retrieved["id"] == session_id
