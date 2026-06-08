"""DuckDB-specific Litestar session-store coverage.

The shared store lifecycle (set/get, expiration, upsert, renew, cleanup, delete_all,
exists, context manager, large data) is covered by
tests/integration/adapters/contracts/test_litestar_store_contract.py. This module keeps the
DuckDB sync_to_thread concurrency behavior, which exercises DuckDB's write-serialization model.
"""

import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from uuid import uuid4

import pytest

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.duckdb.litestar.store import DuckdbStore

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


@pytest.fixture
async def duckdb_store(tmp_path: Path) -> AsyncGenerator[DuckdbStore, None]:
    """Create DuckDB store with temporary file-based database.

    Args:
        tmp_path: Pytest fixture providing unique temporary directory per test.

    Note:
        DuckDB in-memory databases are connection-local, not process-wide.
        Since the thread-local connection pool creates separate connection
        objects for each thread, we must use a file-based database to ensure
        all threads share the same data.

        A unique database filename per test keeps DuckDB's derived attach
        name distinct even if a prior pool has not yet released the file
        (DuckDB rejects ATTACH when any live connection already claims the
        same logical database name).
    """
    db_path = tmp_path / f"test_sessions_{uuid4().hex}.duckdb"
    config = DuckDBConfig(
        connection_config={"database": str(db_path)}, extension_config={"litestar": {"session_table": "test_sessions"}}
    )
    try:
        store = DuckdbStore(config)
        await store.create_table()
        yield store
        await store.delete_all()
    finally:
        config._close_pool()
        if db_path.exists():
            db_path.unlink()


async def test_sync_to_thread_concurrency(duckdb_store: DuckdbStore) -> None:
    """Test concurrent access via sync_to_thread wrapper.

    DuckDB has write serialization, so we test sequential writes
    followed by concurrent reads which is the typical session store pattern.
    """
    for i in range(10):
        await duckdb_store.set(f"session_{i}", f"data_{i}".encode())

    async def read_session(session_id: int) -> "bytes | None":
        return await duckdb_store.get(f"session_{session_id}")

    results = await asyncio.gather(*[read_session(i) for i in range(10)])

    for i, result in enumerate(results):
        assert result == f"data_{i}".encode()
