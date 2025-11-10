__all__ = ("test_pool_lifecycle",)


def test_pool_lifecycle() -> None:

    # start-example
    import os

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    db_manager = SQLSpec()
    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    asyncpg_key = db_manager.add_config(AsyncpgConfig(pool_config={"dsn": dsn}))

    asyncpg_config = db_manager.get_config(asyncpg_key)
    # end-example
    assert asyncpg_config.pool_config["dsn"] == dsn
