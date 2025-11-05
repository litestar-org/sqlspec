MIN_POOL_SIZE = 10


def test_asyncpg_pool_setup() -> None:
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(
        pool_config={
            "dsn": "postgresql://localhost/db",
            "min_size": 10,
            "max_size": 20,
            "max_queries": 50000,
            "max_inactive_connection_lifetime": 300.0,
        }
    )
    assert config.pool_config["min_size"] == MIN_POOL_SIZE
