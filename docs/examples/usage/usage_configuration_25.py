"""Test configuration example: Best practice - Use connection pooling."""

import pytest

__all__ = ("test_connection_pooling_best_practice", )


MIN_POOL_SIZE = 10
MAX_POOL_SIZE = 20


@pytest.mark.skipif(
    not pytest.importorskip("asyncpg", reason="AsyncPG not installed"), reason="AsyncPG integration tests disabled"
)
def test_connection_pooling_best_practice() -> None:
    """Test connection pooling best practice configuration."""
    import os

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    config = AsyncpgConfig(pool_config={"dsn": dsn, "min_size": MIN_POOL_SIZE, "max_size": MAX_POOL_SIZE})

    assert config.pool_config["min_size"] == MIN_POOL_SIZE
    assert config.pool_config["max_size"] == MAX_POOL_SIZE
    assert config.supports_connection_pooling is True
