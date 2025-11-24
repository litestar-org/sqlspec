"""Connection pooling configuration example."""

import os

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_example_21_pool_config",)


def test_example_21_pool_config() -> None:
    dsn = os.environ.get("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/test")

    # start-example
    config = AsyncpgConfig(pool_config={"dsn": dsn, "min_size": 10, "max_size": 20})
    # end-example

    assert config.pool_config["min_size"] == 10
    assert config.pool_config["max_size"] == 20
