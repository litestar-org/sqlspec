from sqlspec.adapters.asyncpg import AsyncpgConfig


def test_single_and_multiple_configs() -> None:
    # start-example
    # Single config
    db_config = AsyncpgConfig(
        pool_config={"dsn": "postgresql://user:pass@localhost/mydb"},
        migration_config={"script_location": "migrations", "enabled": True},
    )

    # Multiple configs
    configs = [
        AsyncpgConfig(
            bind_key="postgres",
            pool_config={"dsn": "postgresql://..."},
            migration_config={"script_location": "migrations/postgres"},
        )
        # ... more configs
    ]

    # Callable function
    def get_configs() -> list[AsyncpgConfig]:
        return [db_config]

    # end-example
    assert isinstance(db_config, AsyncpgConfig)
    assert isinstance(configs, list)
    assert callable(get_configs)
    assert get_configs()[0] is db_config
