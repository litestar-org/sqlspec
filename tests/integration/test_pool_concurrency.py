from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.duckdb import DuckDBConfig

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService

    from sqlspec.adapters.asyncpg import AsyncpgPool
    from sqlspec.adapters.duckdb import DuckDBConnectionPool


@pytest.mark.asyncio
async def test_asyncpg_pool_concurrency(postgres_service: PostgresService) -> None:
    """Verify that multiple concurrent calls to provide_pool result in a single pool."""
    config_params = {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "database": postgres_service.database,
    }
    # Initialize with connection_instance=None explicitly just to be sure
    config = AsyncpgConfig(connection_config=config_params, connection_instance=None)

    async def get_pool() -> AsyncpgPool:
        # Artificial delay to ensure tasks overlap in checking connection_instance
        # This simulates the "check" part of check-then-act overlapping
        return await config.provide_pool()

    # Launch many tasks simultaneously
    tasks = [get_pool() for _ in range(50)]
    pools = await asyncio.gather(*tasks)

    # All pools should be the exact same object
    first_pool = pools[0]
    unique_pools = {id(p) for p in pools}

    await config.close_pool()

    assert len(unique_pools) == 1, f"Race condition detected! {len(unique_pools)} unique pools created."
    assert all(p is first_pool for p in pools)


def test_duckdb_pool_concurrency() -> None:
    """Verify that multiple concurrent calls to provide_pool result in a single pool (Sync)."""
    # Use shared memory db for valid concurrency test
    config = DuckDBConfig(connection_config={"database": ":memory:"})

    # We need to capture results from threads
    results: list[DuckDBConnectionPool | None] = [None] * 50
    exceptions: list[Exception] = []

    def get_pool(index: int) -> None:
        try:
            pool = config.provide_pool()
            results[index] = pool
        except Exception as e:
            exceptions.append(e)

    threads = [threading.Thread(target=get_pool, args=(i,)) for i in range(50)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if exceptions:
        pytest.fail(f"Exceptions in threads: {exceptions}")

    unique_pools = {id(p) for p in results if p is not None}
    config.close_pool()

    assert len(unique_pools) == 1, f"Race condition detected! {len(unique_pools)} unique DuckDB pools created."
