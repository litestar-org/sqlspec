"""Test configuration example: Binding multiple database configurations."""

import tempfile


def test_binding_multiple_configs() -> None:
    """Test binding multiple database configurations."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        spec = SQLSpec()

        # Add multiple configurations
        sqlite_db = spec.add_config(SqliteConfig(pool_config={"database": tmp.name}))
        postgres_db = spec.add_config(
            AsyncpgConfig(pool_config={"dsn": "postgresql://..."})
        )

        # Use specific configuration
        with spec.provide_session(sqlite_db) as session:
            session.execute("SELECT 1")

        assert sqlite_db.pool_config["database"] == tmp.name
        assert postgres_db.pool_config["dsn"] == "postgresql://..."

