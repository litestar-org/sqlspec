from sqlspec.adapters.asyncpg import AsyncpgConfig

def test_asyncpg_pool_setup():
    config = AsyncpgConfig(
        pool_config={
            "dsn": "postgresql://localhost/db",
            "min_size": 10,
            "max_size": 20,
            "max_queries": 50000,
            "max_inactive_connection_lifetime": 300.0,
        }
    )
    assert config.pool_config["min_size"] == 10

