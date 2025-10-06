"""Integration tests for Psycopg ADK store user_fk_column feature."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psycopg.adk.store import PsycopgAsyncADKStore, PsycopgSyncADKStore
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService

pytestmark = [pytest.mark.postgres, pytest.mark.integration]


@pytest.fixture
async def psycopg_async_store_with_fk(postgres_service: "PostgresService"):
    """Create Psycopg async ADK store with user_fk_column configured."""
    config = PsycopgAsyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    store = PsycopgAsyncADKStore(
        config,
        session_table="test_sessions_fk",
        events_table="test_events_fk",
        user_fk_column="tenant_id INTEGER NOT NULL",
    )
    await store.create_tables()
    yield store

    async with config.provide_connection() as conn, conn.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS test_events_fk CASCADE")
        await cur.execute("DROP TABLE IF EXISTS test_sessions_fk CASCADE")

    if config.pool_instance:
        await config.close_pool()


@pytest.fixture
def psycopg_sync_store_with_fk(postgres_service: "PostgresService"):
    """Create Psycopg sync ADK store with user_fk_column configured."""
    config = PsycopgSyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    store = PsycopgSyncADKStore(
        config,
        session_table="test_sessions_sync_fk",
        events_table="test_events_sync_fk",
        user_fk_column="account_id VARCHAR(64) NOT NULL",
    )
    store.create_tables()
    yield store

    with config.provide_connection() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_events_sync_fk CASCADE")
        cur.execute("DROP TABLE IF EXISTS test_sessions_sync_fk CASCADE")

    if config.pool_instance:
        config.close_pool()


async def test_async_store_user_fk_column_initialization(psycopg_async_store_with_fk: PsycopgAsyncADKStore) -> None:
    """Test that user_fk_column is properly initialized in async store."""
    assert psycopg_async_store_with_fk.user_fk_column_ddl == "tenant_id INTEGER NOT NULL"
    assert psycopg_async_store_with_fk.user_fk_column_name == "tenant_id"


def test_sync_store_user_fk_column_initialization(psycopg_sync_store_with_fk: PsycopgSyncADKStore) -> None:
    """Test that user_fk_column is properly initialized in sync store."""
    assert psycopg_sync_store_with_fk.user_fk_column_ddl == "account_id VARCHAR(64) NOT NULL"
    assert psycopg_sync_store_with_fk.user_fk_column_name == "account_id"


async def test_async_store_inherits_user_fk_column(postgres_service: "PostgresService") -> None:
    """Test that async store correctly inherits user_fk_column from base class."""
    config = PsycopgAsyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    store = PsycopgAsyncADKStore(
        config,
        session_table="test_inherit_async",
        events_table="test_events_inherit_async",
        user_fk_column="org_id UUID",
    )

    assert hasattr(store, "_user_fk_column_ddl")
    assert hasattr(store, "_user_fk_column_name")
    assert store.user_fk_column_ddl == "org_id UUID"
    assert store.user_fk_column_name == "org_id"

    if config.pool_instance:
        await config.close_pool()


def test_sync_store_inherits_user_fk_column(postgres_service: "PostgresService") -> None:
    """Test that sync store correctly inherits user_fk_column from base class."""
    config = PsycopgSyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    store = PsycopgSyncADKStore(
        config,
        session_table="test_inherit_sync",
        events_table="test_events_inherit_sync",
        user_fk_column="company_id BIGINT",
    )

    assert hasattr(store, "_user_fk_column_ddl")
    assert hasattr(store, "_user_fk_column_name")
    assert store.user_fk_column_ddl == "company_id BIGINT"
    assert store.user_fk_column_name == "company_id"

    if config.pool_instance:
        config.close_pool()


async def test_async_store_without_user_fk_column(postgres_service: "PostgresService") -> None:
    """Test that async store works without user_fk_column (default behavior)."""
    config = PsycopgAsyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    store = PsycopgAsyncADKStore(config, session_table="test_no_fk_async", events_table="test_events_no_fk_async")

    assert store.user_fk_column_ddl is None
    assert store.user_fk_column_name is None

    if config.pool_instance:
        await config.close_pool()


def test_sync_store_without_user_fk_column(postgres_service: "PostgresService") -> None:
    """Test that sync store works without user_fk_column (default behavior)."""
    config = PsycopgSyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    store = PsycopgSyncADKStore(config, session_table="test_no_fk_sync", events_table="test_events_no_fk_sync")

    assert store.user_fk_column_ddl is None
    assert store.user_fk_column_name is None

    if config.pool_instance:
        config.close_pool()


async def test_async_ddl_includes_user_fk_column(psycopg_async_store_with_fk: PsycopgAsyncADKStore) -> None:
    """Test that the DDL generation includes the user_fk_column."""
    ddl = psycopg_async_store_with_fk._get_create_sessions_table_sql()

    assert "tenant_id INTEGER NOT NULL" in ddl
    assert "test_sessions_fk" in ddl


def test_sync_ddl_includes_user_fk_column(psycopg_sync_store_with_fk: PsycopgSyncADKStore) -> None:
    """Test that the DDL generation includes the user_fk_column."""
    ddl = psycopg_sync_store_with_fk._get_create_sessions_table_sql()

    assert "account_id VARCHAR(64) NOT NULL" in ddl
    assert "test_sessions_sync_fk" in ddl
