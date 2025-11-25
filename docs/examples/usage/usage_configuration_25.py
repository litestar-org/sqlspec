"""Test configuration example: Best practice - Use connection pooling."""

import pytest

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_connection_pooling_best_practice",)


def test_connection_pooling_best_practice() -> None:
    """Test connection pooling best practice configuration."""
    # start-example
    import os

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    config = AsyncpgConfig(pool_config={"dsn": dsn, "min_size": 10, "max_size": 20})

    # end-example
    assert config.pool_config["min_size"] == 10
    assert config.pool_config["max_size"] == 20
    assert config.supports_connection_pooling is True
