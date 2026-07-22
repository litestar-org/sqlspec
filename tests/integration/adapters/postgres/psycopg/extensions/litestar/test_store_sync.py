"""Psycopg-sync-specific Litestar session-store coverage.

The shared store lifecycle (set/get, expiration, upsert, renew, cleanup, delete_all,
exists, context manager, large data) is covered by
tests/integration/adapters/_shared/suite_litestar_store_contract.py. This module keeps the
psycopg sync_to_thread concurrency behavior, which exercises the sync driver under the async
store API.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg.config import PsycopgSyncConfig
from sqlspec.adapters.psycopg.litestar.store import PsycopgSyncStore

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.psycopg, pytest.mark.integration]


@pytest.fixture(scope="module")
def psycopg_sync_store_config(postgres_service: "PostgresService") -> Generator[PsycopgSyncConfig, None, None]:
    """Module-scoped config so all store tests share one pool."""
    config = PsycopgSyncConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "dbname": postgres_service.database,
            "min_size": 1,
            "max_size": 4,
        },
        extension_config={"litestar": {"session_table": "test_psycopg_sync_sessions"}},
    )
    try:
        yield config
    finally:
        if config.connection_instance:
            config.close_pool()


@pytest.fixture
async def psycopg_sync_store(psycopg_sync_store_config: PsycopgSyncConfig) -> AsyncGenerator[PsycopgSyncStore, None]:
    """Create Psycopg sync store using the shared module-scoped config."""
    store = PsycopgSyncStore(psycopg_sync_store_config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()


async def test_sync_to_thread_concurrency(psycopg_sync_store: PsycopgSyncStore) -> None:
    """Test concurrent access via sync_to_thread wrapper.

    PostgreSQL handles concurrent writes well, so we test concurrent
    writes and reads which is a typical session store pattern.
    """
    for i in range(10):
        await psycopg_sync_store.set(f"session_{i}", f"data_{i}".encode())

    async def read_session(session_id: int) -> "bytes | None":
        return await psycopg_sync_store.get(f"session_{session_id}")

    results = await asyncio.gather(*[read_session(i) for i in range(10)])

    for i, result in enumerate(results):
        assert result == f"data_{i}".encode()
