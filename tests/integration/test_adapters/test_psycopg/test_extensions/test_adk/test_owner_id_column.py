"""Integration tests for Psycopg ADK store owner_id_column feature."""

from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING, Any

import pytest

from sqlspec.adapters.psycopg.adk.store import PsycopgAsyncADKStore, PsycopgSyncADKStore
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.postgres, pytest.mark.integration]


@pytest.fixture
async def psycopg_async_store_with_fk(postgres_service: "PostgresService") -> "AsyncGenerator[Any, None]":
    """Create Psycopg async ADK store with owner_id_column configured."""
    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        extension_config={
            "adk": {
                "session_table": "test_sessions_fk",
                "events_table": "test_events_fk",
                "owner_id_column": "tenant_id INTEGER NOT NULL",
            }
        },
    )
    store = PsycopgAsyncADKStore(config)
    await store.create_tables()
    yield store

    async with config.provide_connection() as conn, conn.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS test_events_fk CASCADE")
        await cur.execute("DROP TABLE IF EXISTS test_sessions_fk CASCADE")

    if config.connection_instance:
        await config.close_pool()


@pytest.fixture
def psycopg_sync_store_with_fk(postgres_service: "PostgresService") -> "Generator[Any, None, None]":
    """Create Psycopg sync ADK store with owner_id_column configured."""
    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        extension_config={
            "adk": {
                "session_table": "test_sessions_sync_fk",
                "events_table": "test_events_sync_fk",
                "owner_id_column": "account_id VARCHAR(64) NOT NULL",
            }
        },
    )
    store = PsycopgSyncADKStore(config)
    store.create_tables()
    yield store

    with config.provide_connection() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_events_sync_fk CASCADE")
        cur.execute("DROP TABLE IF EXISTS test_sessions_sync_fk CASCADE")

    if config.connection_instance:
        config.close_pool()


async def test_async_store_owner_id_column_initialization(psycopg_async_store_with_fk: PsycopgAsyncADKStore) -> None:
    """Test that owner_id_column is properly initialized in async store."""
    assert psycopg_async_store_with_fk.owner_id_column_ddl == "tenant_id INTEGER NOT NULL"
    assert psycopg_async_store_with_fk.owner_id_column_name == "tenant_id"


def test_sync_store_owner_id_column_initialization(psycopg_sync_store_with_fk: PsycopgSyncADKStore) -> None:
    """Test that owner_id_column is properly initialized in sync store."""
    assert psycopg_sync_store_with_fk.owner_id_column_ddl == "account_id VARCHAR(64) NOT NULL"
    assert psycopg_sync_store_with_fk.owner_id_column_name == "account_id"


async def test_async_store_inherits_owner_id_column(postgres_service: "PostgresService") -> None:
    """Test that async store correctly inherits owner_id_column from base class."""
    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        extension_config={
            "adk": {
                "session_table": "test_inherit_async",
                "events_table": "test_events_inherit_async",
                "owner_id_column": "org_id UUID",
            }
        },
    )
    store = PsycopgAsyncADKStore(config)

    assert hasattr(store, "_owner_id_column_ddl")
    assert hasattr(store, "_owner_id_column_name")
    assert store.owner_id_column_ddl == "org_id UUID"
    assert store.owner_id_column_name == "org_id"

    if config.connection_instance:
        await config.close_pool()


def test_sync_store_inherits_owner_id_column(postgres_service: "PostgresService") -> None:
    """Test that sync store correctly inherits owner_id_column from base class."""
    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        extension_config={
            "adk": {
                "session_table": "test_inherit_sync",
                "events_table": "test_events_inherit_sync",
                "owner_id_column": "company_id BIGINT",
            }
        },
    )
    store = PsycopgSyncADKStore(config)

    assert hasattr(store, "_owner_id_column_ddl")
    assert hasattr(store, "_owner_id_column_name")
    assert store.owner_id_column_ddl == "company_id BIGINT"
    assert store.owner_id_column_name == "company_id"

    if config.connection_instance:
        config.close_pool()


async def test_async_store_without_owner_id_column(postgres_service: "PostgresService") -> None:
    """Test that async store works without owner_id_column (default behavior)."""
    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        extension_config={"adk": {"session_table": "test_no_fk_async", "events_table": "test_events_no_fk_async"}},
    )
    store = PsycopgAsyncADKStore(config)

    assert store.owner_id_column_ddl is None
    assert store.owner_id_column_name is None

    if config.connection_instance:
        await config.close_pool()


def test_sync_store_without_owner_id_column(postgres_service: "PostgresService") -> None:
    """Test that sync store works without owner_id_column (default behavior)."""
    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        extension_config={"adk": {"session_table": "test_no_fk_sync", "events_table": "test_events_no_fk_sync"}},
    )
    store = PsycopgSyncADKStore(config)

    assert store.owner_id_column_ddl is None
    assert store.owner_id_column_name is None

    if config.connection_instance:
        config.close_pool()


async def test_async_ddl_includes_owner_id_column(psycopg_async_store_with_fk: PsycopgAsyncADKStore) -> None:
    """Test that the DDL generation includes the owner_id_column."""
    ddl = await psycopg_async_store_with_fk._get_create_sessions_table_sql()  # pyright: ignore[reportPrivateUsage]

    assert "tenant_id INTEGER NOT NULL" in ddl
    assert "test_sessions_fk" in ddl


def test_sync_ddl_includes_owner_id_column(psycopg_sync_store_with_fk: PsycopgSyncADKStore) -> None:
    """Test that the DDL generation includes the owner_id_column."""
    ddl = psycopg_sync_store_with_fk._get_create_sessions_table_sql()  # pyright: ignore[reportPrivateUsage]

    assert "account_id VARCHAR(64) NOT NULL" in ddl
    assert "test_sessions_sync_fk" in ddl
