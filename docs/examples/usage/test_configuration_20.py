"""Test configuration example: Named database bindings."""


def test_named_bindings() -> None:
    """Test named database bindings."""
    import tempfile

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.sqlite import SqliteConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        spec = SQLSpec()

        # Add with bind keys
        spec.add_config(SqliteConfig(pool_config={"database": tmp.name}, bind_key="cache_db"))
        spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://..."}, bind_key="main_db"))

        # Access by bind key
        with spec.provide_session("cache_db") as session:
            session.execute("SELECT 1")

        assert spec.configs[SqliteConfig].pool_config["database"] == tmp.name
        assert spec.configs[AsyncpgConfig].pool_config["dsn"] == "postgresql://..."
