"""Test configuration example: Binding multiple database configurations."""

__all__ = ("test_binding_multiple_configs", )


def test_binding_multiple_configs() -> None:
    """Test binding multiple database configurations."""
    import os
    import tempfile

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.sqlite import SqliteConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        db_manager = SQLSpec()

        # Add multiple configurations
        sqlite_key = db_manager.add_config(SqliteConfig(pool_config={"database": tmp.name}))
        dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
        asyncpg_key = db_manager.add_config(AsyncpgConfig(pool_config={"dsn": dsn}))

        # Use specific configuration
        with db_manager.provide_session(sqlite_key) as session:
            session.execute("SELECT 1")

        sqlite_config = db_manager.get_config(sqlite_key)
        pg_config = db_manager.get_config(asyncpg_key)

        assert sqlite_config.pool_config["database"] == tmp.name
        assert pg_config.pool_config["dsn"] == os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
