"""Test configuration example: Environment-based configuration."""

import os
from unittest.mock import patch

POSTGRES_PORT = 5433


def test_environment_based_configuration() -> None:
    """Test environment-based configuration pattern."""

    # Mock environment variables
    env_vars = {
        "DB_HOST": "testhost",
        "DB_PORT": "5433",
        "DB_USER": "testuser",
        "DB_PASSWORD": "testpass",
        "DB_NAME": "testdb",
    }

    with patch.dict(os.environ, env_vars, clear=False):
        from sqlspec.adapters.asyncpg import AsyncpgConfig

        config = AsyncpgConfig(
            pool_config={
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "5432")),
                "user": os.getenv("DB_USER"),
                "password": os.getenv("DB_PASSWORD"),
                "database": os.getenv("DB_NAME"),
            }
        )

        assert config.pool_config["host"] == "testhost"
        assert config.pool_config["port"] == POSTGRES_PORT
        assert config.pool_config["user"] == "testuser"
        assert config.pool_config["password"] == "testpass"
        assert config.pool_config["database"] == "testdb"
