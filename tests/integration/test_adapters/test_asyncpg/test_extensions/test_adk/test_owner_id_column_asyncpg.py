"""Tests for AsyncPG ADK store owner_id_column support."""

from collections.abc import AsyncGenerator
from typing import Any, cast

import asyncpg
import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
from sqlspec.config import ADKConfig, ExtensionConfigs

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.asyncpg, pytest.mark.integration]


def _make_config_with_owner_id(
    postgres_service: Any,
    owner_id_column: "str | None" = None,
    session_table: str = "adk_sessions",
    events_table: str = "adk_events",
) -> AsyncpgConfig:
    """Helper to create config with ADK extension config."""
    extension_config = cast("ExtensionConfigs", {"adk": {"session_table": session_table, "events_table": events_table}})
    adk_settings = cast("ADKConfig", extension_config["adk"])
    if owner_id_column is not None:
        adk_settings["owner_id_column"] = owner_id_column

    return AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
            "max_size": 20,
            "min_size": 2,
        },
        extension_config=extension_config,
    )


@pytest.fixture
async def asyncpg_config_for_fk(postgres_service: Any) -> "AsyncGenerator[AsyncpgConfig, None]":
    """Create AsyncPG config for FK tests with proper pool cleanup."""
    config = _make_config_with_owner_id(postgres_service)

    try:
        yield config
    finally:
        if config.connection_instance:
            await config.close_pool()


@pytest.fixture
async def tenants_table(asyncpg_config_for_fk: AsyncpgConfig) -> "AsyncGenerator[None, None]":
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
async def users_table(asyncpg_config_for_fk: AsyncpgConfig) -> "AsyncGenerator[None, None]":
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


async def test_store_without_owner_id_column(asyncpg_config_for_fk: AsyncpgConfig) -> None:
    """Test creating store without owner_id_column works as before."""
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


async def test_create_tables_with_owner_id_column(
    asyncpg_config_for_fk: AsyncpgConfig, tenants_table: Any, postgres_service: Any
) -> None:
    """Test that DDL includes owner ID column when configured."""
    config = _make_config_with_owner_id(
        postgres_service, owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
    )
    store = AsyncpgADKStore(config)
    try:
        await store.create_tables()
        async with config.provide_connection() as conn:
            result = await conn.fetchrow("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'adk_sessions' AND column_name = 'tenant_id'
            """)

            assert result is not None
            assert result["column_name"] == "tenant_id"
            assert result["data_type"] == "integer"
            assert result["is_nullable"] == "NO"
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_create_session_with_owner_id(tenants_table: Any, postgres_service: Any) -> None:
    """Test creating session with owner ID value."""
    config = _make_config_with_owner_id(
        postgres_service, owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
    )
    store = AsyncpgADKStore(config)
    try:
        await store.create_tables()

        session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, owner_id=1)

        assert session["id"] == "session-1"
        assert session["app_name"] == "app-1"
        assert session["user_id"] == "user-1"
        assert session["state"] == {"data": "test"}

        async with config.provide_connection() as conn:
            result = await conn.fetchrow("SELECT tenant_id FROM adk_sessions WHERE id = $1", "session-1")
            assert result is not None
            assert result["tenant_id"] == 1
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_create_session_without_owner_id_when_configured(tenants_table: Any, postgres_service: Any) -> None:
    """Test that creating session without owner_id when configured uses original SQL."""
    config = _make_config_with_owner_id(postgres_service, owner_id_column="tenant_id INTEGER REFERENCES tenants(id)")
    store = AsyncpgADKStore(config)
    try:
        await store.create_tables()

        session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"})

        assert session["id"] == "session-1"
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_fk_constraint_enforcement_not_null(tenants_table: Any, postgres_service: Any) -> None:
    """Test that FK constraint prevents invalid references when NOT NULL."""
    config = _make_config_with_owner_id(
        postgres_service, owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id)"
    )
    store = AsyncpgADKStore(config)
    try:
        await store.create_tables()

        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await store.create_session("session-invalid", "app-1", "user-1", {"data": "test"}, owner_id=999)
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_cascade_delete_behavior(tenants_table: Any, postgres_service: Any) -> None:
    """Test that CASCADE DELETE removes sessions when tenant deleted."""
    config = _make_config_with_owner_id(
        postgres_service, owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
    )
    store = AsyncpgADKStore(config)
    try:
        await store.create_tables()

        await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, owner_id=1)
        await store.create_session("session-2", "app-1", "user-2", {"data": "test"}, owner_id=1)
        await store.create_session("session-3", "app-1", "user-3", {"data": "test"}, owner_id=2)

        session = await store.get_session("session-1")
        assert session is not None

        async with config.provide_connection() as conn:
            await conn.execute("DELETE FROM tenants WHERE id = 1")

        session1 = await store.get_session("session-1")
        session2 = await store.get_session("session-2")
        session3 = await store.get_session("session-3")

        assert session1 is None
        assert session2 is None
        assert session3 is not None
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_nullable_owner_id_column(tenants_table: Any, postgres_service: Any) -> None:
    """Test nullable FK column allows NULL values."""
    config = _make_config_with_owner_id(
        postgres_service, owner_id_column="tenant_id INTEGER REFERENCES tenants(id) ON DELETE SET NULL"
    )
    store = AsyncpgADKStore(config)
    try:
        await store.create_tables()

        session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"})

        assert session is not None

        async with config.provide_connection() as conn:
            result = await conn.fetchrow("SELECT tenant_id FROM adk_sessions WHERE id = $1", "session-1")
            assert result is not None
            assert result["tenant_id"] is None
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_set_null_on_delete_behavior(tenants_table: Any, postgres_service: Any) -> None:
    """Test that ON DELETE SET NULL sets FK to NULL when parent deleted."""
    config = _make_config_with_owner_id(
        postgres_service, owner_id_column="tenant_id INTEGER REFERENCES tenants(id) ON DELETE SET NULL"
    )
    store = AsyncpgADKStore(config)
    try:
        await store.create_tables()

        await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, owner_id=1)

        async with config.provide_connection() as conn:
            result = await conn.fetchrow("SELECT tenant_id FROM adk_sessions WHERE id = $1", "session-1")
            assert result is not None
            assert result["tenant_id"] == 1

            await conn.execute("DELETE FROM tenants WHERE id = 1")

            result = await conn.fetchrow("SELECT tenant_id FROM adk_sessions WHERE id = $1", "session-1")
            assert result is not None
            assert result["tenant_id"] is None
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_uuid_owner_id_column(users_table: Any, postgres_service: Any) -> None:
    """Test FK column with UUID type."""
    import uuid

    config = _make_config_with_owner_id(
        postgres_service, owner_id_column="account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"
    )
    store = AsyncpgADKStore(config)
    try:
        await store.create_tables()

        user_uuid = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")

        session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, owner_id=user_uuid)

        assert session is not None

        async with config.provide_connection() as conn:
            result = await conn.fetchrow("SELECT account_id FROM adk_sessions WHERE id = $1", "session-1")
            assert result is not None
            assert result["account_id"] == user_uuid
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_deferrable_initially_deferred_fk(tenants_table: Any, postgres_service: Any) -> None:
    """Test DEFERRABLE INITIALLY DEFERRED FK constraint."""
    config = _make_config_with_owner_id(
        postgres_service,
        owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) DEFERRABLE INITIALLY DEFERRED",
    )
    store = AsyncpgADKStore(config)
    try:
        await store.create_tables()

        session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, owner_id=1)

        assert session is not None
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_backwards_compatibility_without_owner_id(asyncpg_config_for_fk: AsyncpgConfig) -> None:
    """Test that existing code without owner_id parameter still works."""
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


async def test_owner_id_column_name_property(tenants_table: Any, postgres_service: Any) -> None:
    """Test that owner_id_column_name property is correctly set."""
    config = _make_config_with_owner_id(
        postgres_service, owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id)"
    )
    store = AsyncpgADKStore(config)
    try:
        assert store.owner_id_column_name == "tenant_id"
        assert store.owner_id_column_ddl == "tenant_id INTEGER NOT NULL REFERENCES tenants(id)"
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_owner_id_column_name_none_when_not_configured(asyncpg_config_for_fk: AsyncpgConfig) -> None:
    """Test that owner_id_column properties are None when not configured."""
    store = AsyncpgADKStore(asyncpg_config_for_fk)

    assert store.owner_id_column_name is None
    assert store.owner_id_column_ddl is None


async def test_multiple_sessions_same_tenant(tenants_table: Any, postgres_service: Any) -> None:
    """Test creating multiple sessions for the same tenant."""
    config = _make_config_with_owner_id(
        postgres_service, owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
    )
    store = AsyncpgADKStore(config)
    try:
        await store.create_tables()

        for i in range(5):
            await store.create_session(f"session-{i}", "app-1", f"user-{i}", {"session_num": i}, owner_id=1)

        async with config.provide_connection() as conn:
            result = await conn.fetch("SELECT id FROM adk_sessions WHERE tenant_id = $1 ORDER BY id", 1)
            assert len(result) == 5
            assert [r["id"] for r in result] == [f"session-{i}" for i in range(5)]
    finally:
        if config.connection_instance:
            await config.close_pool()


async def test_owner_id_with_custom_table_names(tenants_table: Any, postgres_service: Any) -> None:
    """Test owner_id_column with custom table names."""
    config = _make_config_with_owner_id(
        postgres_service,
        owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id)",
        session_table="custom_sessions",
        events_table="custom_events",
    )
    store = AsyncpgADKStore(config)
    try:
        await store.create_tables()

        session = await store.create_session("session-1", "app-1", "user-1", {"data": "test"}, owner_id=1)

        assert session is not None

        async with config.provide_connection() as conn:
            result = await conn.fetchrow("SELECT tenant_id FROM custom_sessions WHERE id = $1", "session-1")
            assert result is not None
            assert result["tenant_id"] == 1

            await conn.execute("DROP TABLE IF EXISTS custom_events CASCADE")
            await conn.execute("DROP TABLE IF EXISTS custom_sessions CASCADE")
    finally:
        if config.connection_instance:
            await config.close_pool()
