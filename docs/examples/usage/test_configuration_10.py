POOL_INSTANCE = 20


def test_manual_pool() -> None:
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    pool = {"dsn": "postgresql://localhost/db", "min_size": 10, "max_size": POOL_INSTANCE}
    db = AsyncpgConfig(pool_instance=pool)
    assert db.pool_instance["max_size"] == POOL_INSTANCE
