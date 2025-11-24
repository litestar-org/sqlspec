import pytest

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_manual_pool",)


async def test_manual_pool() -> None:

    # start-example
    import os

    import asyncpg

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    pool = await asyncpg.create_pool(
        dsn=os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db"), min_size=10, max_size=20
    )
    db = AsyncpgConfig(pool_instance=pool)
    # end-example
    assert db.pool_instance is pool
    await pool.close()
