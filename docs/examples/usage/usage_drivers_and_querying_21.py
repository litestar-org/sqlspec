"""Connection pooling configuration example."""

import os

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_example_21_connection_config",)


def test_example_21_connection_config() -> None:
    dsn = os.environ.get("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/test")

    # start-example
    config = AsyncpgConfig(connection_config={"dsn": dsn, "min_size": 10, "max_size": 20})
    # end-example

    assert config.connection_config["min_size"] == 10
    assert config.connection_config["max_size"] == 20
