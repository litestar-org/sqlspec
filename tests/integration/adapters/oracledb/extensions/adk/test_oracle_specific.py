"""Oracle-specific ADK store tests for LOB handling, JSON types, and FK columns."""

import pickle
from collections.abc import AsyncGenerator, Generator
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
def oracle_sync_store(oracle_sync_config: "OracleSyncConfig") -> "Generator[OracleSyncADKStore, None, None]":
    """Create a sync Oracle ADK store with tables created once per module."""
    store = OracleSyncADKStore(oracle_sync_config)
    store.create_tables()
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
def oracle_config_with_users_table(oracle_sync_config: "OracleSyncConfig") -> "Generator[OracleSyncConfig, None, None]":
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
def oracle_store_sync_with_fk(
    oracle_config_with_users_table: "OracleSyncConfig",
) -> "Generator[OracleSyncADKStore, None, None]":
    """Create a sync Oracle ADK store with owner_id FK column."""
    config_with_extension = OracleSyncConfig(
        connection_config=oracle_config_with_users_table.connection_config,
        extension_config={"adk": {"owner_id_column": "owner_id NUMBER(19) REFERENCES users(id) ON DELETE CASCADE"}},
    )
    store = OracleSyncADKStore(config_with_extension)
    _cleanup_sync_store(store, config_with_extension)
    store.create_tables()
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


async def test_event_content_lob_deserialization(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test event content CLOB is correctly deserialized."""
    session_id = _unique_session_id("event-lob")
    app_name = "test-app"
    user_id = "user-123"

    await oracle_async_store.create_session(session_id, app_name, user_id, {})

    content = {"message": "x" * 5000, "data": {"nested": True}}
    grounding_metadata = {"sources": ["a" * 1000, "b" * 1000]}
    custom_metadata = {"tags": ["tag1", "tag2"], "priority": "high"}

    event_record: EventRecord = {
        "id": "event-1",
        "session_id": session_id,
        "app_name": app_name,
        "user_id": user_id,
        "author": "assistant",
        "actions": pickle.dumps([{"name": "test", "args": {}}]),
        "content": content,
        "grounding_metadata": grounding_metadata,
        "custom_metadata": custom_metadata,
        "timestamp": datetime.now(timezone.utc),
        "partial": False,
        "turn_complete": True,
        "interrupted": False,
        "error_code": None,
        "error_message": None,
        "invocation_id": "",
        "branch": None,
        "long_running_tool_ids_json": None,
    }

    await oracle_async_store.append_event(event_record)

    events = await oracle_async_store.get_events(session_id)
    assert len(events) == 1
    assert events[0]["content"] == content
    assert events[0]["grounding_metadata"] == grounding_metadata
    assert events[0]["custom_metadata"] == custom_metadata


async def test_actions_blob_handling(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test actions BLOB is correctly read and unpickled."""
    session_id = _unique_session_id("actions-blob")
    app_name = "test-app"
    user_id = "user-123"

    await oracle_async_store.create_session(session_id, app_name, user_id, {})

    test_actions = [{"function": "test_func", "args": {"param": "value"}, "result": 42}]
    actions_bytes = pickle.dumps(test_actions)

    event_record: EventRecord = {
        "id": "event-actions",
        "session_id": session_id,
        "app_name": app_name,
        "user_id": user_id,
        "author": "user",
        "actions": actions_bytes,
        "content": None,
        "grounding_metadata": None,
        "custom_metadata": None,
        "timestamp": datetime.now(timezone.utc),
        "partial": None,
        "turn_complete": None,
        "interrupted": None,
        "error_code": None,
        "error_message": None,
        "invocation_id": "",
        "branch": None,
        "long_running_tool_ids_json": None,
    }

    await oracle_async_store.append_event(event_record)

    events = await oracle_async_store.get_events(session_id)
    assert len(events) == 1
    assert events[0]["actions"] == actions_bytes
    unpickled = pickle.loads(events[0]["actions"])
    assert unpickled == test_actions


def test_state_lob_deserialization_sync(oracle_sync_store: "OracleSyncADKStore") -> None:
    """Test state CLOB/BLOB is correctly deserialized in sync mode."""
    session_id = _unique_session_id("lob-session-sync")
    app_name = "test-app"
    user_id = "user-123"
    state = {"large_field": "y" * 10000, "nested": {"data": [4, 5, 6]}}

    session = oracle_sync_store.create_session(session_id, app_name, user_id, state)
    assert session["state"] == state

    retrieved = oracle_sync_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == state


async def test_boolean_fields_conversion(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test partial, turn_complete, interrupted converted to NUMBER(1)."""
    session_id = _unique_session_id("bool-session")
    app_name = "test-app"
    user_id = "user-123"

    await oracle_async_store.create_session(session_id, app_name, user_id, {})

    event_record: EventRecord = {
        "id": "bool-event-1",
        "session_id": session_id,
        "app_name": app_name,
        "user_id": user_id,
        "author": "assistant",
        "actions": b"",
        "content": None,
        "grounding_metadata": None,
        "custom_metadata": None,
        "timestamp": datetime.now(timezone.utc),
        "partial": True,
        "turn_complete": False,
        "interrupted": True,
        "error_code": None,
        "error_message": None,
        "invocation_id": "",
        "branch": None,
        "long_running_tool_ids_json": None,
    }

    await oracle_async_store.append_event(event_record)

    events = await oracle_async_store.get_events(session_id)
    assert len(events) == 1
    assert events[0]["partial"] is True
    assert events[0]["turn_complete"] is False
    assert events[0]["interrupted"] is True


async def test_boolean_fields_none_values(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test None values for boolean fields."""
    session_id = _unique_session_id("bool-none-session")
    app_name = "test-app"
    user_id = "user-123"

    await oracle_async_store.create_session(session_id, app_name, user_id, {})

    event_record: EventRecord = {
        "id": "bool-event-none",
        "session_id": session_id,
        "app_name": app_name,
        "user_id": user_id,
        "author": "user",
        "actions": b"",
        "content": None,
        "grounding_metadata": None,
        "custom_metadata": None,
        "timestamp": datetime.now(timezone.utc),
        "partial": None,
        "turn_complete": None,
        "interrupted": None,
        "error_code": None,
        "error_message": None,
        "invocation_id": "",
        "branch": None,
        "long_running_tool_ids_json": None,
    }

    await oracle_async_store.append_event(event_record)

    events = await oracle_async_store.get_events(session_id)
    assert len(events) == 1
    assert events[0]["partial"] is None
    assert events[0]["turn_complete"] is None
    assert events[0]["interrupted"] is None


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

    assert storage_type in ["json", "blob_json", "clob_json", "blob_plain"]


async def test_json_fields_stored_and_retrieved(oracle_async_store: "OracleAsyncADKStore") -> None:
    """Test JSON fields use appropriate CLOB/BLOB/JSON storage."""
    session_id = _unique_session_id("json-session")
    app_name = "test-app"
    user_id = "user-123"
    state = {
        "complex": {
            "nested": {"deep": {"structure": "value"}},
            "array": [1, 2, 3, {"key": "value"}],
            "unicode": "こんにちは世界",
            "special_chars": "test@example.com | value > 100",
        }
    }

    session = await oracle_async_store.create_session(session_id, app_name, user_id, state)
    assert session["state"] == state

    retrieved = await oracle_async_store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["state"] == state
    assert retrieved["state"]["complex"]["unicode"] == "こんにちは世界"


def test_create_session_with_owner_id_sync(oracle_store_sync_with_fk: "OracleSyncADKStore") -> None:
    """Test creating session with owner_id in sync mode."""
    session_id = _unique_session_id("sync-fk")
    app_name = "test-app"
    user_id = "alice"
    state = {"data": "sync test"}
    owner_id = 100

    session = oracle_store_sync_with_fk.create_session(session_id, app_name, user_id, state, owner_id=owner_id)
    assert session["id"] == session_id
    assert session["state"] == state

    retrieved = oracle_store_sync_with_fk.get_session(session_id)
    assert retrieved is not None
    assert retrieved["id"] == session_id
