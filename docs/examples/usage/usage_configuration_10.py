POOL_INSTANCE = 20
__all__ = ("test_manual_pool",)


def test_manual_pool() -> None:
    import os

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    pool = {"dsn": dsn, "min_size": 10, "max_size": POOL_INSTANCE}
    db = AsyncpgConfig(pool_instance=pool)
    assert db.pool_instance["max_size"] == POOL_INSTANCE
