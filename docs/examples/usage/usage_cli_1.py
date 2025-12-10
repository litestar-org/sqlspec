from sqlspec.adapters.asyncpg import AsyncpgConfig

__all__ = ("test_single_and_multiple_configs",)


def test_single_and_multiple_configs() -> None:
    # start-example
    # Single config
    db_config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://user:pass@localhost/mydb"},
        migration_config={"script_location": "migrations", "enabled": True},
    )

    # Multiple configs
    configs = [
        AsyncpgConfig(
            bind_key="postgres",
            connection_config={"dsn": "postgresql://..."},
            migration_config={"script_location": "migrations/postgres"},
        )
        # ... more configs
    ]

    # Callable function
    def get_configs() -> list[AsyncpgConfig]:
        return [db_config]

    # Usage with CLI:
    # --config "myapp.config.db_config"                          # Single config
    # --config "myapp.config.configs"                            # Config list
    # --config "myapp.config.get_configs"                        # Callable
    # --config "myapp.config.db_config,myapp.config.configs"     # Multiple paths (comma-separated)

    # end-example
    assert isinstance(db_config, AsyncpgConfig)
    assert isinstance(configs, list)
    assert callable(get_configs)
    assert get_configs()[0] is db_config
