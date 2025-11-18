from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig

__all__ = ("test_multi_config", )


def test_multi_config() -> None:
    # start-example
    configs = [
        AsyncpgConfig(
            bind_key="postgres",
            pool_config={"dsn": "postgresql://..."},
            migration_config={"script_location": "migrations/postgres", "enabled": True},
        ),
        AsyncmyConfig(
            bind_key="mysql",
            pool_config={"host": "localhost", "database": "mydb"},
            migration_config={"script_location": "migrations/mysql", "enabled": True},
        ),
        AsyncpgConfig(
            bind_key="analytics",
            pool_config={"dsn": "postgresql://analytics/..."},
            migration_config={"script_location": "migrations/analytics", "enabled": True},
        ),
    ]
    # end-example
    assert isinstance(configs, list)
    assert all(hasattr(cfg, "bind_key") for cfg in configs)
