import pytest

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_asyncpg_config_setup",)


def test_asyncpg_config_setup() -> None:

    # start-example
    import os

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    host = os.getenv("SQLSPEC_USAGE_PG_HOST", "localhost")
    port = int(os.getenv("SQLSPEC_USAGE_PG_PORT", "5432"))
    user = os.getenv("SQLSPEC_USAGE_PG_USER", "user")
    password = os.getenv("SQLSPEC_USAGE_PG_PASSWORD", "password")
    database = os.getenv("SQLSPEC_USAGE_PG_DATABASE", "db")
    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", f"postgresql://{user}:{password}@{host}:{port}/{database}")

    config = AsyncpgConfig(
        pool_config={
            "dsn": dsn,
            "min_size": 10,
            "max_size": 20,
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }
    )
    # end-example
    assert config.pool_config["host"] == host
