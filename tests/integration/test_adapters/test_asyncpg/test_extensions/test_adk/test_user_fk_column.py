"""Tests for AsyncPG ADK store user_fk_column support."""

import asyncpg
import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.asyncpg, pytest.mark.integration]


@pytest.fixture
async def asyncpg_config_for_fk(postgres_service):
    """Create AsyncPG config for FK tests."""
    return AsyncpgConfig(
        pool_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        }
    )


@pytest.fixture
async def tenants_table(asyncpg_config_for_fk):
    """Create a tenants table for FK testing."""
    async with asyncpg_config_for_fk.provide_connection() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tenants (
                id INTEGER PRIMARY KEY,
                name VARCHAR(128) NOT NULL
            )
        """)
        await conn.execute("INSERT INTO tenants (id, name) VALUES (1, 'Tenant A')")
        await conn.execute("INSERT INTO tenants (id, name) VALUES (2, 'Tenant B')")
        await conn.execute("INSERT INTO tenants (id, name) VALUES (3, 'Tenant C')")

    yield

    async with asyncpg_config_for_fk.provide_connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS adk_events CASCADE")
        await conn.execute("DROP TABLE IF EXISTS adk_sessions CASCADE")
        await conn.execute("DROP TABLE IF EXISTS tenants CASCADE")


@pytest.fixture
async def users_table(asyncpg_config_for_fk):
    """Create a users table for FK testing with UUID."""
    async with asyncpg_config_for_fk.provide_connection() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email VARCHAR(255) NOT NULL UNIQUE
            )
        """)
        await conn.execute(
            "INSERT INTO users (id, email) VALUES ('550e8400-e29b-41d4-a716-446655440000', 'user1@example.com')"
        )
        await conn.execute(
            "INSERT INTO users (id, email) VALUES ('550e8400-e29b-41d4-a716-446655440001', 'user2@example.com')"
        )

    yield

    async with asyncpg_config_for_fk.provide_connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS adk_events CASCADE")
        await conn.execute("DROP TABLE IF EXISTS adk_sessions CASCADE")
        await conn.execute("DROP TABLE IF EXISTS users CASCADE")


@pytest.mark.asyncio
async def test_store_without_user_fk_column(asyncpg_config_for_fk):
    """Test creating store without user_fk_column works as before."""
    store = AsyncpgADKStore(asyncpg_config_for_fk)
    await store.create_tables()

    session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"})

    assert session["id"] == "session-1"
    assert session["app_name"] == "app-1"
    assert session["user_id"] == "user-1"
    assert session["state"] == {"data": "test"}

    async with asyncpg_config_for_fk.provide_connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS adk_events CASCADE")
        await conn.execute("DROP TABLE IF EXISTS adk_sessions CASCADE")


@pytest.mark.asyncio
async def test_create_tables_with_user_fk_column(asyncpg_config_for_fk, tenants_table):
    """Test that DDL includes user FK column when configured."""
    store = AsyncpgADKStore(
        asyncpg_config_for_fk, user_fk_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
    )
    await store.create_tables()

    async with asyncpg_config_for_fk.provide_connection() as conn:
        result = await conn.fetchrow("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'adk_sessions' AND column_name = 'tenant_id'
        """)

        assert result is not None
        assert result["column_name"] == "tenant_id"
        assert result["data_type"] == "integer"
        assert result["is_nullable"] == "NO"


@pytest.mark.asyncio
async def test_create_session_with_user_fk(asyncpg_config_for_fk, tenants_table):
    """Test creating session with user FK value."""
    store = AsyncpgADKStore(
        asyncpg_config_for_fk, user_fk_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
    )
    await store.create_tables()

    session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, user_fk=1)

    assert session["id"] == "session-1"
    assert session["app_name"] == "app-1"
    assert session["user_id"] == "user-1"
    assert session["state"] == {"data": "test"}

    async with asyncpg_config_for_fk.provide_connection() as conn:
        result = await conn.fetchrow("SELECT tenant_id FROM adk_sessions WHERE id = $1", "session-1")
        assert result["tenant_id"] == 1


@pytest.mark.asyncio
async def test_create_session_without_user_fk_when_configured(asyncpg_config_for_fk, tenants_table):
    """Test that creating session without user_fk when configured uses original SQL."""
    store = AsyncpgADKStore(asyncpg_config_for_fk, user_fk_column="tenant_id INTEGER REFERENCES tenants(id)")
    await store.create_tables()

    session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"})

    assert session["id"] == "session-1"


@pytest.mark.asyncio
async def test_fk_constraint_enforcement_not_null(asyncpg_config_for_fk, tenants_table):
    """Test that FK constraint prevents invalid references when NOT NULL."""
    store = AsyncpgADKStore(asyncpg_config_for_fk, user_fk_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id)")
    await store.create_tables()

    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await store.create_session("session-invalid", "app-1", "user-1", {"data": "test"}, user_fk=999)


@pytest.mark.asyncio
async def test_cascade_delete_behavior(asyncpg_config_for_fk, tenants_table):
    """Test that CASCADE DELETE removes sessions when tenant deleted."""
    store = AsyncpgADKStore(
        asyncpg_config_for_fk, user_fk_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
    )
    await store.create_tables()

    await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, user_fk=1)
    await store.create_session("session-2", "app-1", "user-2", {"data": "test"}, user_fk=1)
    await store.create_session("session-3", "app-1", "user-3", {"data": "test"}, user_fk=2)

    session = await store.get_session("session-1")
    assert session is not None

    async with asyncpg_config_for_fk.provide_connection() as conn:
        await conn.execute("DELETE FROM tenants WHERE id = 1")

    session1 = await store.get_session("session-1")
    session2 = await store.get_session("session-2")
    session3 = await store.get_session("session-3")

    assert session1 is None
    assert session2 is None
    assert session3 is not None


@pytest.mark.asyncio
async def test_nullable_user_fk_column(asyncpg_config_for_fk, tenants_table):
    """Test nullable FK column allows NULL values."""
    store = AsyncpgADKStore(
        asyncpg_config_for_fk, user_fk_column="tenant_id INTEGER REFERENCES tenants(id) ON DELETE SET NULL"
    )
    await store.create_tables()

    session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"})

    assert session is not None

    async with asyncpg_config_for_fk.provide_connection() as conn:
        result = await conn.fetchrow("SELECT tenant_id FROM adk_sessions WHERE id = $1", "session-1")
        assert result["tenant_id"] is None


@pytest.mark.asyncio
async def test_set_null_on_delete_behavior(asyncpg_config_for_fk, tenants_table):
    """Test that ON DELETE SET NULL sets FK to NULL when parent deleted."""
    store = AsyncpgADKStore(
        asyncpg_config_for_fk, user_fk_column="tenant_id INTEGER REFERENCES tenants(id) ON DELETE SET NULL"
    )
    await store.create_tables()

    await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, user_fk=1)

    async with asyncpg_config_for_fk.provide_connection() as conn:
        result = await conn.fetchrow("SELECT tenant_id FROM adk_sessions WHERE id = $1", "session-1")
        assert result["tenant_id"] == 1

        await conn.execute("DELETE FROM tenants WHERE id = 1")

        result = await conn.fetchrow("SELECT tenant_id FROM adk_sessions WHERE id = $1", "session-1")
        assert result["tenant_id"] is None


@pytest.mark.asyncio
async def test_uuid_user_fk_column(asyncpg_config_for_fk, users_table):
    """Test FK column with UUID type."""
    store = AsyncpgADKStore(
        asyncpg_config_for_fk, user_fk_column="account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"
    )
    await store.create_tables()

    import uuid

    user_uuid = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")

    session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, user_fk=user_uuid)

    assert session is not None

    async with asyncpg_config_for_fk.provide_connection() as conn:
        result = await conn.fetchrow("SELECT account_id FROM adk_sessions WHERE id = $1", "session-1")
        assert result["account_id"] == user_uuid


@pytest.mark.asyncio
async def test_deferrable_initially_deferred_fk(asyncpg_config_for_fk, tenants_table):
    """Test DEFERRABLE INITIALLY DEFERRED FK constraint."""
    store = AsyncpgADKStore(
        asyncpg_config_for_fk,
        user_fk_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) DEFERRABLE INITIALLY DEFERRED",
    )
    await store.create_tables()

    session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, user_fk=1)

    assert session is not None


@pytest.mark.asyncio
async def test_backwards_compatibility_without_user_fk(asyncpg_config_for_fk):
    """Test that existing code without user_fk parameter still works."""
    store = AsyncpgADKStore(asyncpg_config_for_fk)
    await store.create_tables()

    session1 = await store.create_session("session-1", "app-1", "user-1", {"data": "test"})
    session2 = await store.create_session("session-2", "app-1", "user-2", {"data": "test2"})

    assert session1["id"] == "session-1"
    assert session2["id"] == "session-2"

    sessions = await store.list_sessions("app-1", "user-1")
    assert len(sessions) == 1
    assert sessions[0]["id"] == "session-1"

    async with asyncpg_config_for_fk.provide_connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS adk_events CASCADE")
        await conn.execute("DROP TABLE IF EXISTS adk_sessions CASCADE")


@pytest.mark.asyncio
async def test_user_fk_column_name_property(asyncpg_config_for_fk, tenants_table):
    """Test that user_fk_column_name property is correctly set."""
    store = AsyncpgADKStore(asyncpg_config_for_fk, user_fk_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id)")

    assert store.user_fk_column_name == "tenant_id"
    assert store.user_fk_column_ddl == "tenant_id INTEGER NOT NULL REFERENCES tenants(id)"


@pytest.mark.asyncio
async def test_user_fk_column_name_none_when_not_configured(asyncpg_config_for_fk):
    """Test that user_fk_column properties are None when not configured."""
    store = AsyncpgADKStore(asyncpg_config_for_fk)

    assert store.user_fk_column_name is None
    assert store.user_fk_column_ddl is None


@pytest.mark.asyncio
async def test_multiple_sessions_same_tenant(asyncpg_config_for_fk, tenants_table):
    """Test creating multiple sessions for the same tenant."""
    store = AsyncpgADKStore(
        asyncpg_config_for_fk, user_fk_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
    )
    await store.create_tables()

    for i in range(5):
        await store.create_session(f"session-{i}", "app-1", f"user-{i}", {"session_num": i}, user_fk=1)

    async with asyncpg_config_for_fk.provide_connection() as conn:
        result = await conn.fetch("SELECT id FROM adk_sessions WHERE tenant_id = $1 ORDER BY id", 1)
        assert len(result) == 5
        assert [r["id"] for r in result] == [f"session-{i}" for i in range(5)]


@pytest.mark.asyncio
async def test_user_fk_with_custom_table_names(asyncpg_config_for_fk, tenants_table):
    """Test user_fk_column with custom table names."""
    store = AsyncpgADKStore(
        asyncpg_config_for_fk,
        session_table="custom_sessions",
        events_table="custom_events",
        user_fk_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id)",
    )
    await store.create_tables()

    session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, user_fk=1)

    assert session is not None

    async with asyncpg_config_for_fk.provide_connection() as conn:
        result = await conn.fetchrow("SELECT tenant_id FROM custom_sessions WHERE id = $1", "session-1")
        assert result["tenant_id"] == 1

        await conn.execute("DROP TABLE IF EXISTS custom_events CASCADE")
        await conn.execute("DROP TABLE IF EXISTS custom_sessions CASCADE")
