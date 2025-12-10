import os

from sqlspec.adapters.asyncpg import AsyncpgConfig

__all__ = ("test_extension_config",)


def test_extension_config() -> None:
    # start-example
    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    config = AsyncpgConfig(
        connection_config={"dsn": dsn},
        migration_config={
            "enabled": True,
            "script_location": "migrations",
            "include_extensions": ["litestar"],  # Enable litestar extension migrations
        },
        extension_config={"litestar": {"enable_repository_pattern": True, "enable_dto_generation": False}},
    )
    # end-example
    assert config.extension_config
