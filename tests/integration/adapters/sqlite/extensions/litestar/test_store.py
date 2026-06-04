"""SQLite-specific Litestar session-store coverage.

The shared store lifecycle (set/get, expiration, upsert, renew, cleanup, delete_all,
exists, context manager, large data) is covered by
tests/integration/adapters/contracts/test_litestar_store_contract.py. This module keeps the
SQLite sync_to_thread concurrency behavior, which exercises SQLite's write-serialization model.
"""

import asyncio
from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.sqlite.litestar.store import SQLiteStore

pytestmark = [pytest.mark.sqlite, pytest.mark.integration, pytest.mark.xdist_group("sqlite")]


@pytest.fixture
async def sqlite_store() -> AsyncGenerator[SQLiteStore, None]:
    """Create SQLite store with shared in-memory database."""
    config = SqliteConfig(
        connection_config={"database": "file:test_sessions_mem?mode=memory&cache=shared", "uri": True},
        extension_config={"litestar": {"session_table": "test_sessions"}},
    )
    store = SQLiteStore(config)
    await store.create_table()
    yield store
    await store.delete_all()


async def test_sync_to_thread_concurrency(sqlite_store: SQLiteStore) -> None:
    """Test concurrent access via sync_to_thread wrapper.

    SQLite has write serialization, so we test sequential writes
    followed by concurrent reads which is the typical session store pattern.
    """
    for i in range(10):
        await sqlite_store.set(f"session_{i}", f"data_{i}".encode())

    async def read_session(session_id: int) -> "bytes | None":
        return await sqlite_store.get(f"session_{session_id}")

    results = await asyncio.gather(*[read_session(i) for i in range(10)])

    for i, result in enumerate(results):
        assert result == f"data_{i}".encode()
