"""AsyncMY ADK test fixtures."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.asyncmy.adk.store import AsyncmyADKStore


@pytest.fixture
async def asyncmy_adk_store(mysql_service: MySQLService) -> "AsyncGenerator[AsyncmyADKStore, None]":
    """Create AsyncMY ADK store with test database.

    Args:
        mysql_service: Pytest fixture providing MySQL connection config.

    Yields:
        Configured AsyncMY ADK store instance.

    Notes:
        Uses pytest-databases MySQL container for testing.
        Tables are created before test and cleaned up after.
    """
    config = AsyncmyConfig(
        pool_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": False,
            "minsize": 1,
            "maxsize": 5,
        }
    )

    store = AsyncmyADKStore(config, session_table="test_sessions", events_table="test_events")
    await store.create_tables()

    yield store

    async with config.provide_connection() as conn, conn.cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS test_events")
        await cursor.execute("DROP TABLE IF EXISTS test_sessions")
        await conn.commit()


@pytest.fixture
async def asyncmy_adk_store_with_fk(mysql_service: MySQLService) -> "AsyncGenerator[AsyncmyADKStore, None]":
    """Create AsyncMY ADK store with owner ID column.

    Args:
        mysql_service: Pytest fixture providing MySQL connection config.

    Yields:
        Configured AsyncMY ADK store with FK column.

    Notes:
        Creates a tenants table and configures FK constraint.
        Tests multi-tenant isolation and CASCADE behavior.
    """
    config = AsyncmyConfig(
        pool_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": False,
            "minsize": 1,
            "maxsize": 5,
        }
    )

    async with config.provide_connection() as conn, conn.cursor() as cursor:
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_tenants (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(128) NOT NULL UNIQUE
            ) ENGINE=InnoDB
        """)
        await cursor.execute("INSERT INTO test_tenants (name) VALUES ('tenant1'), ('tenant2')")
        await conn.commit()

    store = AsyncmyADKStore(
        config,
        session_table="test_fk_sessions",
        events_table="test_fk_events",
        owner_id_column="tenant_id BIGINT NOT NULL REFERENCES test_tenants(id) ON DELETE CASCADE",
    )
    await store.create_tables()

    yield store

    async with config.provide_connection() as conn, conn.cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS test_fk_events")
        await cursor.execute("DROP TABLE IF EXISTS test_fk_sessions")
        await cursor.execute("DROP TABLE IF EXISTS test_tenants")
        await conn.commit()
