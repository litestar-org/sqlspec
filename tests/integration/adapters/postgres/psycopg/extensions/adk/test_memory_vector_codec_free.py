"""PostgreSQL ADK vector search without pgvector client codecs."""

from datetime import datetime, timezone
from typing import Any, cast

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk import AsyncpgADKMemoryStore
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.psycopg.adk import PsycopgAsyncADKMemoryStore, PsycopgSyncADKMemoryStore
from sqlspec.extensions.adk import StoredMemory

pytestmark = [pytest.mark.xdist_group("pgvector"), pytest.mark.integration]


def _connection_config(service: PostgresService) -> dict[str, Any]:
    return {
        "host": service.host,
        "port": service.port,
        "user": service.user,
        "password": service.password,
        "dbname": service.database,
    }


def _entries() -> list[StoredMemory]:
    now = datetime.now(timezone.utc)
    return [
        cast(
            "StoredMemory",
            {
                "id": memory_id,
                "session_id": "session-1",
                "app_name": "app",
                "user_id": "user",
                "scope": "user",
                "event_id": f"event-{memory_id}",
                "author": "user",
                "timestamp": now,
                "embedding": embedding,
                "content_json": {"text": memory_id},
                "content_text": memory_id,
                "metadata_json": None,
                "inserted_at": now,
            },
        )
        for memory_id, embedding in (("nearest", [1.0, 0.0]), ("farther", [0.0, 1.0]), ("null", None))
    ]


@pytest.mark.asyncpg
async def test_asyncpg_vector_memory_without_codec(pgvector_service: PostgresService) -> None:
    config = AsyncpgConfig(
        connection_config={
            "host": pgvector_service.host,
            "port": pgvector_service.port,
            "user": pgvector_service.user,
            "password": pgvector_service.password,
            "database": pgvector_service.database,
        },
        driver_features={"enable_pgvector": False},
        extension_config={"adk": {"memory_table": "adk_memory_asyncpg_codec_free", "vector_dimensions": 2}},
    )
    store = AsyncpgADKMemoryStore(config)
    try:
        async with config.provide_connection() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        await store.create_tables()
        assert await store.insert_memory_entries(_entries()) == 3
        results = await store.search_entries("", "app", "user", embedding=[1.0, 0.0])
        assert [entry["id"] for entry in results] == ["nearest", "farther"]
    finally:
        async with config.provide_connection() as conn:
            await conn.execute("DROP TABLE IF EXISTS adk_memory_asyncpg_codec_free")
        await config.close_pool()


@pytest.mark.psycopg
async def test_psycopg_async_vector_memory_without_codec(pgvector_service: PostgresService) -> None:
    config = PsycopgAsyncConfig(
        connection_config=_connection_config(pgvector_service),
        driver_features={"enable_pgvector": False},
        extension_config={"adk": {"memory_table": "adk_memory_psycopg_async_codec_free", "vector_dimensions": 2}},
    )
    store = PsycopgAsyncADKMemoryStore(config)
    try:
        async with config.provide_connection() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        await store.create_tables()
        assert await store.insert_memory_entries(_entries()) == 3
        results = await store.search_entries("", "app", "user", embedding=[1.0, 0.0])
        assert [entry["id"] for entry in results] == ["nearest", "farther"]
    finally:
        async with config.provide_connection() as conn:
            await conn.execute("DROP TABLE IF EXISTS adk_memory_psycopg_async_codec_free")
        await config.close_pool()


@pytest.mark.psycopg
def test_psycopg_sync_vector_memory_without_codec(pgvector_service: PostgresService) -> None:
    config = PsycopgSyncConfig(
        connection_config=_connection_config(pgvector_service),
        driver_features={"enable_pgvector": False},
        extension_config={"adk": {"memory_table": "adk_memory_psycopg_sync_codec_free", "vector_dimensions": 2}},
    )
    store = PsycopgSyncADKMemoryStore(config)
    try:
        with config.provide_connection() as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        store.create_tables()
        assert store.insert_memory_entries(_entries()) == 3
        results = store.search_entries("", "app", "user", embedding=[1.0, 0.0])
        assert [entry["id"] for entry in results] == ["nearest", "farther"]
    finally:
        with config.provide_connection() as conn:
            conn.execute("DROP TABLE IF EXISTS adk_memory_psycopg_sync_codec_free")
        config.close_pool()
