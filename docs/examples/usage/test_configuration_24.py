"""Test configuration example: Best practice - Use connection pooling."""

import pytest


@pytest.mark.skipif(
    not pytest.importorskip("asyncpg", reason="AsyncPG not installed"), reason="AsyncPG integration tests disabled"
)
def test_connection_pooling_best_practice() -> None:
    """Test connection pooling best practice configuration."""
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/db", "min_size": 10, "max_size": 20})

    assert config.pool_config["min_size"] == 10
    assert config.pool_config["max_size"] == 20
    assert config.supports_connection_pooling is True
