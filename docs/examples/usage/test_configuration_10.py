import asyncpg
from sqlspec.adapters.asyncpg import AsyncpgConfig

def test_manual_pool():
    pool = {
        "dsn": "postgresql://localhost/db",
        "min_size": 10,
        "max_size": 20
    }
    db = AsyncpgConfig(pool_instance=pool)
    assert db.pool_instance["max_size"] == 20

