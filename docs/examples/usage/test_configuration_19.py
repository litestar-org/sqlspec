"""Test configuration example: Binding multiple database configurations."""


def test_binding_multiple_configs() -> None:
    """Test binding multiple database configurations."""
    import tempfile

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.sqlite import SqliteConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        spec = SQLSpec()

        # Add multiple configurations
        sqlite_db = spec.add_config(SqliteConfig(pool_config={"database": tmp.name}))
        pg_db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://..."}))

        # Use specific configuration
        with spec.provide_session(sqlite_db) as session:
            session.execute("SELECT 1")

        assert spec.configs[SqliteConfig].pool_config["database"] == tmp.name
        assert spec.configs[AsyncpgConfig].pool_config["dsn"] == "postgresql://..."
