"""Integration tests for Psqlpy ADK store user_fk_column feature."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psqlpy.adk.store import PsqlpyADKStore
from sqlspec.adapters.psqlpy.config import PsqlpyConfig

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.postgres, pytest.mark.integration]


@pytest.fixture
async def psqlpy_store_with_fk(postgres_service: "PostgresService") -> PsqlpyADKStore:
    """Create Psqlpy ADK store with user_fk_column configured."""
    dsn = f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    config = PsqlpyConfig(pool_config={"dsn": dsn, "max_db_pool_size": 5})
    store = PsqlpyADKStore(
        config,
        session_table="test_sessions_fk",
        events_table="test_events_fk",
        user_fk_column="tenant_id INTEGER NOT NULL",
    )
    await store.create_tables()
    yield store

    async with config.provide_connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_events_fk CASCADE", [])
        await conn.execute("DROP TABLE IF EXISTS test_sessions_fk CASCADE", [])

    await config.close_pool()


async def test_store_user_fk_column_initialization(psqlpy_store_with_fk: PsqlpyADKStore) -> None:
    """Test that user_fk_column is properly initialized."""
    assert psqlpy_store_with_fk.user_fk_column_ddl == "tenant_id INTEGER NOT NULL"
    assert psqlpy_store_with_fk.user_fk_column_name == "tenant_id"


async def test_store_inherits_user_fk_column(postgres_service: "PostgresService") -> None:
    """Test that store correctly inherits user_fk_column from base class."""
    dsn = f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    config = PsqlpyConfig(pool_config={"dsn": dsn, "max_db_pool_size": 5})
    store = PsqlpyADKStore(
        config, session_table="test_inherit", events_table="test_events_inherit", user_fk_column="org_id UUID"
    )

    assert hasattr(store, "_user_fk_column_ddl")
    assert hasattr(store, "_user_fk_column_name")
    assert store.user_fk_column_ddl == "org_id UUID"
    assert store.user_fk_column_name == "org_id"

    await config.close_pool()


async def test_store_without_user_fk_column(postgres_service: "PostgresService") -> None:
    """Test that store works without user_fk_column (default behavior)."""
    dsn = f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    config = PsqlpyConfig(pool_config={"dsn": dsn, "max_db_pool_size": 5})
    store = PsqlpyADKStore(config, session_table="test_no_fk", events_table="test_events_no_fk")

    assert store.user_fk_column_ddl is None
    assert store.user_fk_column_name is None

    await config.close_pool()


async def test_create_session_with_user_fk(psqlpy_store_with_fk: PsqlpyADKStore) -> None:
    """Test creating a session with user_fk value."""
    session_id = "session-001"
    app_name = "test-app"
    user_id = "user-001"
    state = {"key": "value"}
    tenant_id = 42

    session = await psqlpy_store_with_fk.create_session(
        session_id=session_id, app_name=app_name, user_id=user_id, state=state, user_fk=tenant_id
    )

    assert session["id"] == session_id
    assert session["app_name"] == app_name
    assert session["user_id"] == user_id
    assert session["state"] == state


async def test_table_has_user_fk_column(psqlpy_store_with_fk: PsqlpyADKStore) -> None:
    """Test that the created table includes the user_fk_column."""
    config = psqlpy_store_with_fk.config

    async with config.provide_connection() as conn:
        result = await conn.fetch(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = $1 AND column_name = $2
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
        session_id="session-tenant-1", app_name="test-app", user_id="user-001", state={"key": "value1"}, user_fk=1
    )

    session2 = await psqlpy_store_with_fk.create_session(
        session_id="session-tenant-2", app_name="test-app", user_id="user-002", state={"key": "value2"}, user_fk=2
    )

    assert session1["id"] == "session-tenant-1"
    assert session1["user_id"] == "user-001"
    assert session1["state"] == {"key": "value1"}

    assert session2["id"] == "session-tenant-2"
    assert session2["user_id"] == "user-002"
    assert session2["state"] == {"key": "value2"}
