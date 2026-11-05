"""Configure an AsyncPG connection pool for PostgreSQL."""

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

spec = SQLSpec()
config = spec.add_config(
    AsyncpgConfig(
        connection_config={
            "dsn": "postgresql://app:secret@localhost:5432/prod",
            "min_size": 2,
            "max_size": 20,
            "command_timeout": 30.0,
        },
        driver_features={"pooler_mode": "session", "enable_pgvector": True},
    )
)
# --8<-- [end:example]

__all__ = ("main",)


def main() -> AsyncpgConfig:
    """Construct an AsyncpgConfig with explicit pool sizing."""
    assert "dsn" in config.connection_config
    return config


if __name__ == "__main__":
    print(main())
