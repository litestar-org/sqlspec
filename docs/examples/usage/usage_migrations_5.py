from sqlspec.adapters.asyncpg import AsyncpgConfig

__all__ = ("test_config_structure",)


# start-example
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://user:pass@localhost/mydb"},
    migration_config={
        "enabled": True,
        "script_location": "migrations",
        "version_table_name": "ddl_migrations",
        "auto_sync": True,  # Enable automatic version reconciliation
    },
)
# end-example


def test_config_structure() -> None:
    # Check config attributes
    assert hasattr(config, "pool_config")
    assert hasattr(config, "migration_config")
    assert config.migration_config["enabled"] is True
    assert config.migration_config["script_location"] == "migrations"
    assert config.migration_config["version_table_name"] == "ddl_migrations"
    assert config.migration_config["auto_sync"] is True
