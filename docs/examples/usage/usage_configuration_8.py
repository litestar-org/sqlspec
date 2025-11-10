__all__ = ("test_asyncpg_pool_setup",)


def test_asyncpg_pool_setup() -> None:
    import os

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    min_pool_size = 10
    config = AsyncpgConfig(
        pool_config={
            "dsn": dsn,
            "min_size": min_pool_size,
            "max_size": 20,
            "max_queries": 50000,
            "max_inactive_connection_lifetime": 300.0,
        }
    )
    assert config.pool_config["min_size"] == min_pool_size
