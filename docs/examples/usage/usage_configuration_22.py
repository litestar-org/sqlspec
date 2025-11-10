def test_basic_migration_config() -> None:
    import os

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    config = AsyncpgConfig(
        pool_config={"dsn": dsn},
        extension_config={
            "litestar": {"session_table": "custom_sessions"}  # Extension settings
        },
        migration_config={
            "script_location": "migrations",  # Migration directory
            "version_table": "alembic_version",  # Version tracking table
            "include_extensions": ["litestar"],  # Simple string list only
        },
    )
    assert config.migration_config["script_location"] == "migrations"
    assert "litestar" in config.migration_config["include_extensions"]
