"""Integration tests for Psqlpy ADK store owner_id_column feature."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.psqlpy.adk.store import PsqlpyADKStore
from sqlspec.adapters.psqlpy.config import PsqlpyConfig

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.postgres, pytest.mark.integration]


@pytest.fixture
async def psqlpy_store_with_fk(psqlpy_config: PsqlpyConfig) -> "AsyncGenerator[PsqlpyADKStore, None]":
    """Create Psqlpy ADK store with owner_id_column configured."""
    psqlpy_config.extension_config = {
        "adk": {
            "session_table": "test_sessions_fk",
            "events_table": "test_events_fk",
            "owner_id_column": "tenant_id INTEGER NOT NULL",
        }
    }
    store = PsqlpyADKStore(psqlpy_config)
    await store.create_tables()
    yield store

    async with psqlpy_config.provide_connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_events_fk CASCADE", [])
        await conn.execute("DROP TABLE IF EXISTS test_sessions_fk CASCADE", [])


async def test_store_owner_id_column_initialization(psqlpy_store_with_fk: PsqlpyADKStore) -> None:
    """Test that owner_id_column is properly initialized."""
    assert psqlpy_store_with_fk.owner_id_column_ddl == "tenant_id INTEGER NOT NULL"
    assert psqlpy_store_with_fk.owner_id_column_name == "tenant_id"


async def test_store_inherits_owner_id_column(psqlpy_config: PsqlpyConfig) -> None:
    """Test that store correctly inherits owner_id_column from base class."""
    psqlpy_config.extension_config = {
        "adk": {
            "session_table": "test_inherit",
            "events_table": "test_events_inherit",
            "owner_id_column": "org_id UUID",
        }
    }
    store = PsqlpyADKStore(psqlpy_config)

    assert hasattr(store, "_owner_id_column_ddl")
    assert hasattr(store, "_owner_id_column_name")
    assert store.owner_id_column_ddl == "org_id UUID"
    assert store.owner_id_column_name == "org_id"


async def test_store_without_owner_id_column(psqlpy_config: PsqlpyConfig) -> None:
    """Test that store works without owner_id_column (default behavior)."""
    psqlpy_config.extension_config = {"adk": {"session_table": "test_no_fk", "events_table": "test_events_no_fk"}}
    store = PsqlpyADKStore(psqlpy_config)

    assert store.owner_id_column_ddl is None
    assert store.owner_id_column_name is None


async def test_create_session_with_owner_id(psqlpy_store_with_fk: PsqlpyADKStore) -> None:
    """Test creating a session with owner_id value."""
    session_id = "session-001"
    app_name = "test-app"
    user_id = "user-001"
    state = {"key": "value"}
    tenant_id = 42

    session = await psqlpy_store_with_fk.create_session(
        session_id=session_id, app_name=app_name, user_id=user_id, state=state, owner_id=tenant_id
    )

    assert session["id"] == session_id
    assert session["app_name"] == app_name
    assert session["user_id"] == user_id
    assert session["state"] == state


async def test_table_has_owner_id_column(psqlpy_store_with_fk: PsqlpyADKStore) -> None:
    """Test that the created table includes the owner_id_column."""
    config = psqlpy_store_with_fk.config

    async with config.provide_connection() as conn:
        result = await conn.fetch(
            """
            SELECT
                a.attname::text AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relname = $1 AND n.nspname = 'public' AND a.attname = $2 AND a.attnum > 0 AND NOT a.attisdropped
            """,
            ["test_sessions_fk", "tenant_id"],
        )
        rows = result.result() if result else []

        assert len(rows) == 1
        row = rows[0]
        assert row["column_name"] == "tenant_id"
        assert row["data_type"] == "integer"
        assert row["is_nullable"] == "NO"


async def test_create_multiple_sessions_with_different_tenants(psqlpy_store_with_fk: PsqlpyADKStore) -> None:
    """Test creating multiple sessions with different tenant_id values."""
    session1 = await psqlpy_store_with_fk.create_session(
        session_id="session-tenant-1", app_name="test-app", user_id="user-001", state={"key": "value1"}, owner_id=1
    )

    session2 = await psqlpy_store_with_fk.create_session(
        session_id="session-tenant-2", app_name="test-app", user_id="user-002", state={"key": "value2"}, owner_id=2
    )

    assert session1["id"] == "session-tenant-1"
    assert session1["user_id"] == "user-001"
    assert session1["state"] == {"key": "value1"}

    assert session2["id"] == "session-tenant-2"
    assert session2["user_id"] == "user-002"
    assert session2["state"] == {"key": "value2"}
