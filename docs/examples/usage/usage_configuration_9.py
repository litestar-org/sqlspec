import pytest

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_pool_lifecycle",)


def test_pool_lifecycle() -> None:

    # start-example
    import os

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    db_manager = SQLSpec()
    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    asyncpg_config = db_manager.add_config(AsyncpgConfig(pool_config={"dsn": dsn}))

    # The config instance is now the handle - add_config returns it directly
    # end-example
    assert asyncpg_config.pool_config["dsn"] == dsn
