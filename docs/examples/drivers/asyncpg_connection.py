"""Configure an AsyncPG connection pool for PostgreSQL."""

from sqlspec.adapters.asyncpg import AsyncpgConfig

__all__ = ("main",)


def main() -> AsyncpgConfig:
    """Construct an AsyncpgConfig with explicit pool sizing."""
    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://user:pass@localhost:5432/app", "min_size": 1, "max_size": 5}
    )
    assert "dsn" in config.connection_config
    return config


if __name__ == "__main__":
    print(main())
