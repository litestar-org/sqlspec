"""Oracle-specific ADK store tests for LOB handling, JSON types, and FK columns.

Tests verify:
- LOB reading works correctly (Oracle returns LOB objects)
- JSON/CLOB types used optimally based on Oracle version
- NUMBER(1) boolean conversion
- user_fk_column support with Oracle NUMBER FK
- FK constraint validation
"""

import pickle
from datetime import datetime, timezone

import pytest

from sqlspec.adapters.oracledb.adk import OracleAsyncADKStore, OracleSyncADKStore

pytestmark = [pytest.mark.xdist_group("oracle"), pytest.mark.oracledb, pytest.mark.integration]


@pytest.mark.oracledb
class TestOracleAsyncLOBHandling:
    """Test LOB reading in async store."""

    @pytest.fixture()
    async def oracle_store_async(self, oracle_async_config):
        """Create async Oracle ADK store."""
        store = OracleAsyncADKStore(oracle_async_config)
        await store.create_tables()
        yield store
        async with oracle_async_config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()

    async def test_state_lob_deserialization(self, oracle_store_async):
        """Test state CLOB/BLOB is correctly deserialized."""
        session_id = "lob-test-session"
        app_name = "test-app"
        user_id = "user-123"
        state = {"large_field": "x" * 10000, "nested": {"data": [1, 2, 3]}}

        session = await oracle_store_async.create_session(session_id, app_name, user_id, state)
        assert session["state"] == state

        retrieved = await oracle_store_async.get_session(session_id)
        assert retrieved is not None
        assert retrieved["state"] == state
        assert retrieved["state"]["large_field"] == "x" * 10000

    async def test_event_content_lob_deserialization(self, oracle_store_async):
        """Test event content CLOB is correctly deserialized."""
        from sqlspec.extensions.adk._types import EventRecord

        session_id = "event-lob-session"
        app_name = "test-app"
        user_id = "user-123"

        await oracle_store_async.create_session(session_id, app_name, user_id, {})

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
            "invocation_id": None,
            "branch": None,
            "long_running_tool_ids_json": None,
        }

        await oracle_store_async.append_event(event_record)

        events = await oracle_store_async.get_events(session_id)
        assert len(events) == 1
        assert events[0]["content"] == content
        assert events[0]["grounding_metadata"] == grounding_metadata
        assert events[0]["custom_metadata"] == custom_metadata

    async def test_actions_blob_handling(self, oracle_store_async):
        """Test actions BLOB is correctly read and unpickled."""
        from sqlspec.extensions.adk._types import EventRecord

        session_id = "actions-blob-session"
        app_name = "test-app"
        user_id = "user-123"

        await oracle_store_async.create_session(session_id, app_name, user_id, {})

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
            "invocation_id": None,
            "branch": None,
            "long_running_tool_ids_json": None,
        }

        await oracle_store_async.append_event(event_record)

        events = await oracle_store_async.get_events(session_id)
        assert len(events) == 1
        assert events[0]["actions"] == actions_bytes
        unpickled = pickle.loads(events[0]["actions"])
        assert unpickled == test_actions


@pytest.mark.oracledb
class TestOracleSyncLOBHandling:
    """Test LOB reading in sync store."""

    @pytest.fixture()
    def oracle_store_sync(self, oracle_sync_config):
        """Create sync Oracle ADK store."""
        store = OracleSyncADKStore(oracle_sync_config)
        store.create_tables()
        yield store
        with oracle_sync_config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():
                try:
                    cursor.execute(stmt)
                except Exception:
                    pass
            conn.commit()

    def test_state_lob_deserialization_sync(self, oracle_store_sync):
        """Test state CLOB/BLOB is correctly deserialized in sync mode."""
        session_id = "lob-test-session-sync"
        app_name = "test-app"
        user_id = "user-123"
        state = {"large_field": "y" * 10000, "nested": {"data": [4, 5, 6]}}

        session = oracle_store_sync.create_session(session_id, app_name, user_id, state)
        assert session["state"] == state

        retrieved = oracle_store_sync.get_session(session_id)
        assert retrieved is not None
        assert retrieved["state"] == state


@pytest.mark.oracledb
class TestOracleBooleanConversion:
    """Test NUMBER(1) boolean conversion."""

    @pytest.fixture()
    async def oracle_store_async(self, oracle_async_config):
        """Create async Oracle ADK store."""
        store = OracleAsyncADKStore(oracle_async_config)
        await store.create_tables()
        yield store
        async with oracle_async_config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()

    async def test_boolean_fields_conversion(self, oracle_store_async):
        """Test partial, turn_complete, interrupted converted to NUMBER(1)."""
        from sqlspec.extensions.adk._types import EventRecord

        session_id = "bool-session"
        app_name = "test-app"
        user_id = "user-123"

        await oracle_store_async.create_session(session_id, app_name, user_id, {})

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
            "invocation_id": None,
            "branch": None,
            "long_running_tool_ids_json": None,
        }

        await oracle_store_async.append_event(event_record)

        events = await oracle_store_async.get_events(session_id)
        assert len(events) == 1
        assert events[0]["partial"] is True
        assert events[0]["turn_complete"] is False
        assert events[0]["interrupted"] is True

    async def test_boolean_fields_none_values(self, oracle_store_async):
        """Test None values for boolean fields."""
        from sqlspec.extensions.adk._types import EventRecord

        session_id = "bool-none-session"
        app_name = "test-app"
        user_id = "user-123"

        await oracle_store_async.create_session(session_id, app_name, user_id, {})

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
            "invocation_id": None,
            "branch": None,
            "long_running_tool_ids_json": None,
        }

        await oracle_store_async.append_event(event_record)

        events = await oracle_store_async.get_events(session_id)
        assert len(events) == 1
        assert events[0]["partial"] is None
        assert events[0]["turn_complete"] is None
        assert events[0]["interrupted"] is None


@pytest.mark.oracledb
class TestOracleUserFKColumn:
    """Test user_fk_column support with Oracle NUMBER FK."""

    @pytest.fixture()
    async def oracle_config_with_tenant_table(self, oracle_async_config):
        """Create tenant table for FK testing."""
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

        yield oracle_async_config

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

    @pytest.fixture()
    async def oracle_store_with_fk(self, oracle_config_with_tenant_table):
        """Create async Oracle ADK store with user_fk_column."""
        store = OracleAsyncADKStore(
            oracle_config_with_tenant_table, user_fk_column="tenant_id NUMBER(10) NOT NULL REFERENCES tenants(id)"
        )
        await store.create_tables()
        yield store
        async with oracle_config_with_tenant_table.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()

    async def test_create_session_with_user_fk(self, oracle_store_with_fk):
        """Test creating session with user_fk parameter."""
        session_id = "fk-session-1"
        app_name = "test-app"
        user_id = "user-123"
        state = {"data": "test"}
        tenant_id = 1

        session = await oracle_store_with_fk.create_session(session_id, app_name, user_id, state, user_fk=tenant_id)
        assert session["id"] == session_id
        assert session["state"] == state

    async def test_user_fk_constraint_validation(self, oracle_store_with_fk):
        """Test FK constraint is enforced (invalid FK should fail)."""
        import oracledb

        session_id = "fk-invalid-session"
        app_name = "test-app"
        user_id = "user-123"
        state = {"data": "test"}
        invalid_tenant_id = 9999

        with pytest.raises(oracledb.IntegrityError):
            await oracle_store_with_fk.create_session(session_id, app_name, user_id, state, user_fk=invalid_tenant_id)

    async def test_create_session_without_user_fk_when_required(self, oracle_store_with_fk):
        """Test creating session without user_fk when column has NOT NULL."""
        import oracledb

        session_id = "fk-missing-session"
        app_name = "test-app"
        user_id = "user-123"
        state = {"data": "test"}

        with pytest.raises(oracledb.IntegrityError):
            await oracle_store_with_fk.create_session(session_id, app_name, user_id, state, user_fk=None)

    async def test_fk_column_name_parsing(self, oracle_async_config):
        """Test _user_fk_column_name is correctly parsed from DDL."""
        store = OracleAsyncADKStore(oracle_async_config, user_fk_column="account_id NUMBER(19) REFERENCES accounts(id)")
        assert store.user_fk_column_name == "account_id"
        assert store.user_fk_column_ddl == "account_id NUMBER(19) REFERENCES accounts(id)"

        store2 = OracleAsyncADKStore(
            oracle_async_config, user_fk_column="org_uuid RAW(16) REFERENCES organizations(id)"
        )
        assert store2.user_fk_column_name == "org_uuid"


@pytest.mark.oracledb
class TestOracleJSONStorageTypes:
    """Test JSON storage type detection and usage."""

    @pytest.fixture()
    async def oracle_store_async(self, oracle_async_config):
        """Create async Oracle ADK store."""
        store = OracleAsyncADKStore(oracle_async_config)
        await store.create_tables()
        yield store
        async with oracle_async_config.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():
                try:
                    await cursor.execute(stmt)
                except Exception:
                    pass
            await conn.commit()

    async def test_json_storage_type_detection(self, oracle_store_async):
        """Test JSON storage type is detected correctly."""
        storage_type = await oracle_store_async._detect_json_storage_type()

        assert storage_type in ["json", "blob_json", "clob_json", "blob_plain"]

    async def test_json_fields_stored_and_retrieved(self, oracle_store_async):
        """Test JSON fields use appropriate CLOB/BLOB/JSON storage."""
        session_id = "json-test-session"
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

        session = await oracle_store_async.create_session(session_id, app_name, user_id, state)
        assert session["state"] == state

        retrieved = await oracle_store_async.get_session(session_id)
        assert retrieved is not None
        assert retrieved["state"] == state
        assert retrieved["state"]["complex"]["unicode"] == "こんにちは世界"


@pytest.mark.oracledb
class TestOracleSyncUserFKColumn:
    """Test user_fk_column support in sync store."""

    @pytest.fixture()
    def oracle_config_with_users_table(self, oracle_sync_config):
        """Create users table for FK testing."""
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

        yield oracle_sync_config

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

    @pytest.fixture()
    def oracle_store_sync_with_fk(self, oracle_config_with_users_table):
        """Create sync Oracle ADK store with user_fk_column."""
        store = OracleSyncADKStore(
            oracle_config_with_users_table, user_fk_column="owner_id NUMBER(19) REFERENCES users(id) ON DELETE CASCADE"
        )
        store.create_tables()
        yield store
        with oracle_config_with_users_table.provide_connection() as conn:
            cursor = conn.cursor()
            for stmt in store._get_drop_tables_sql():
                try:
                    cursor.execute(stmt)
                except Exception:
                    pass
            conn.commit()

    def test_create_session_with_user_fk_sync(self, oracle_store_sync_with_fk):
        """Test creating session with user_fk in sync mode."""
        session_id = "sync-fk-session"
        app_name = "test-app"
        user_id = "alice"
        state = {"data": "sync test"}
        owner_id = 100

        session = oracle_store_sync_with_fk.create_session(session_id, app_name, user_id, state, user_fk=owner_id)
        assert session["id"] == session_id
        assert session["state"] == state

        retrieved = oracle_store_sync_with_fk.get_session(session_id)
        assert retrieved is not None
        assert retrieved["id"] == session_id
