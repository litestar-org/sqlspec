"""ADBC Litestar store residuals outside the shared store contract.

The shared Litestar store contract owns CRUD, expiration, cleanup, upsert,
renewal, large payloads, delete-all, existence checks, and context manager
behavior. This file keeps the PostgreSQL-backed ADBC `sync_to_thread`
concurrency proof, which is not a generic store contract today.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.litestar import ADBCStore

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.adbc, pytest.mark.integration]


@pytest.fixture
async def adbc_store(adbc_postgres_config: AdbcConfig) -> AsyncGenerator[ADBCStore, None]:
    """Create an ADBC store with PostgreSQL backend."""
    adbc_postgres_config.extension_config = {"litestar": {"session_table": "test_adbc_sessions"}}
    store = ADBCStore(adbc_postgres_config)
    await store.create_table()
    try:
        yield store
    finally:
        with contextlib.suppress(Exception):
            await store.delete_all()


async def test_sync_to_thread_concurrency(adbc_store: ADBCStore) -> None:
    """ADBC PostgreSQL supports concurrent store reads and writes through sync_to_thread."""

    async def write_session(session_id: int) -> None:
        await adbc_store.set(f"session_{session_id}", f"data_{session_id}".encode())

    await asyncio.gather(*[write_session(i) for i in range(10)])

    async def read_session(session_id: int) -> bytes | None:
        return await adbc_store.get(f"session_{session_id}")

    results = await asyncio.gather(*[read_session(i) for i in range(10)])

    for i, result in enumerate(results):
        assert result == f"data_{i}".encode()
