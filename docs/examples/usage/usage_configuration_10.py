__all__ = ("test_manual_pool",)


def test_manual_pool() -> None:
    import os

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    # TODO: manually create asyncpg pool and assign to `pool_instance`.
    max_pool_size = 20
    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    pool = {"dsn": dsn, "min_size": 10, "max_size": max_pool_size}
    db = AsyncpgConfig(pool_config=pool)
    assert db.pool_config["max_size"] == max_pool_size
