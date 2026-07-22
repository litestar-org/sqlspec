"""MysqlConnector ADK test fixtures."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig
from sqlspec.adapters.mysqlconnector.adk import MysqlConnectorAsyncADKStore
from tests.integration.fixtures.mysql import _mysql_connection_config


@pytest.fixture
async def mysqlconnector_adk_store(mysql_service: MySQLService) -> "AsyncGenerator[MysqlConnectorAsyncADKStore, None]":
    """Create MysqlConnector ADK store with test database."""
    connection_config = _mysql_connection_config(mysql_service)
    connection_config.update({"autocommit": False, "use_pure": True})
    config = MysqlConnectorAsyncConfig(
        connection_config=connection_config,
        extension_config={"adk": {"session_table": "test_sessions", "events_table": "test_events"}},
    )

    try:
        store = MysqlConnectorAsyncADKStore(config)
        await store.create_tables()

        yield store

        async with config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute("DROP TABLE IF EXISTS test_events")
                await cursor.execute("DROP TABLE IF EXISTS test_sessions")
                await conn.commit()
            finally:
                await cursor.close()
    finally:
        config.connection_instance = None


@pytest.fixture
async def mysqlconnector_adk_store_with_fk(
    mysql_service: MySQLService,
) -> "AsyncGenerator[MysqlConnectorAsyncADKStore, None]":
    """Create MysqlConnector ADK store with owner ID column."""
    connection_config = _mysql_connection_config(mysql_service)
    connection_config.update({"autocommit": False, "use_pure": True})
    config = MysqlConnectorAsyncConfig(
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
        async with config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS test_tenants (
                        id BIGINT PRIMARY KEY AUTO_INCREMENT,
                        name VARCHAR(128) NOT NULL UNIQUE
                    ) ENGINE=InnoDB
                """)
                await cursor.execute("INSERT INTO test_tenants (name) VALUES ('tenant1'), ('tenant2')")
                await conn.commit()
            finally:
                await cursor.close()

        store = MysqlConnectorAsyncADKStore(config)
        await store.create_tables()

        yield store

        async with config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute("DROP TABLE IF EXISTS test_fk_events")
                await cursor.execute("DROP TABLE IF EXISTS test_fk_sessions")
                await cursor.execute("DROP TABLE IF EXISTS test_tenants")
                await conn.commit()
            finally:
                await cursor.close()
    finally:
        config.connection_instance = None
