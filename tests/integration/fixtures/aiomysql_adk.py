"""aiomysql ADK test fixtures."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql import AiomysqlConfig
from sqlspec.adapters.aiomysql._typing import AiomysqlCursor
from sqlspec.adapters.aiomysql.adk import AiomysqlADKStore
from tests.integration.fixtures.mysql import _mysql_connection_config


@pytest.fixture
async def aiomysql_adk_store(mysql_service: MySQLService) -> "AsyncGenerator[AiomysqlADKStore, None]":
    """Create aiomysql ADK store with test database.

    Args:
        mysql_service: Pytest fixture providing MySQL connection config.

    Yields:
        Configured aiomysql ADK store instance.

    Notes:
        Uses pytest-databases MySQL container for testing.
        Tables are created before test and cleaned up after.
    """
    connection_config = _mysql_connection_config(mysql_service, database_key="db")
    connection_config.update({"autocommit": False, "minsize": 1, "maxsize": 5})
    config = AiomysqlConfig(
        connection_config=connection_config,
        extension_config={"adk": {"session_table": "test_sessions", "events_table": "test_events"}},
    )

    try:
        store = AiomysqlADKStore(config)
        await store.create_tables()

        yield store

        async with config.provide_connection() as conn, AiomysqlCursor(conn) as cursor:
            await cursor.execute("DROP TABLE IF EXISTS test_events")
            await cursor.execute("DROP TABLE IF EXISTS test_sessions")
            await conn.commit()
    finally:
        pool = config.connection_instance
        if pool is not None:
            pool.close()
            await pool.wait_closed()
        config.connection_instance = None


@pytest.fixture
async def aiomysql_adk_store_with_fk(mysql_service: MySQLService) -> "AsyncGenerator[AiomysqlADKStore, None]":
    """Create aiomysql ADK store with owner ID column.

    Args:
        mysql_service: Pytest fixture providing MySQL connection config.

    Yields:
        Configured aiomysql ADK store with FK column.

    Notes:
        Creates a tenants table and configures FK constraint.
        Tests multi-tenant isolation and CASCADE behavior.
    """
    connection_config = _mysql_connection_config(mysql_service, database_key="db")
    connection_config.update({"autocommit": False, "minsize": 1, "maxsize": 5})
    config = AiomysqlConfig(
        connection_config=connection_config,
        extension_config={
            "adk": {
                "session_table": "test_fk_sessions",
                "events_table": "test_fk_events",
                "owner_id_column": "tenant_id BIGINT NOT NULL REFERENCES test_tenants(id) ON DELETE CASCADE",
            }
        },
    )

    try:
        async with config.provide_connection() as conn, AiomysqlCursor(conn) as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_tenants (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(128) NOT NULL UNIQUE
                ) ENGINE=InnoDB
            """)
            await cursor.execute("INSERT INTO test_tenants (name) VALUES ('tenant1'), ('tenant2')")
            await conn.commit()

        store = AiomysqlADKStore(config)
        await store.create_tables()

        yield store

        async with config.provide_connection() as conn, AiomysqlCursor(conn) as cursor:
            await cursor.execute("DROP TABLE IF EXISTS test_fk_events")
            await cursor.execute("DROP TABLE IF EXISTS test_fk_sessions")
            await cursor.execute("DROP TABLE IF EXISTS test_tenants")
            await conn.commit()
    finally:
        pool = config.connection_instance
        if pool is not None:
            pool.close()
            await pool.wait_closed()
        config.connection_instance = None
