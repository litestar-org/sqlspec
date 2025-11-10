__all__ = ("test_pool_lifecycle",)


def test_pool_lifecycle() -> None:
    import os

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    db_manager = SQLSpec()
    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    asyncpg_key = db_manager.add_config(AsyncpgConfig(pool_config={"dsn": dsn}))

    asyncpg_config = db_manager.get_config(asyncpg_key)
    assert asyncpg_config.pool_config["dsn"] == dsn
