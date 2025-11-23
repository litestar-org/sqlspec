from sqlspec.adapters.asyncpg import AsyncpgConfig

__all__ = ("test_extension_config",)


def test_extension_config() -> None:
    # start-example
    config = AsyncpgConfig(
        pool_config={"dsn": "postgresql://..."},
        migration_config={
            "enabled": True,
            "script_location": "migrations",
            "include_extensions": ["litestar"],  # Enable litestar extension migrations
        },
        extension_config={"litestar": {"enable_repository_pattern": True, "enable_dto_generation": False}},
    )
    # end-example
    assert config.extension_config
