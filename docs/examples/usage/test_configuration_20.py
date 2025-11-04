"""Test configuration example: Named database bindings."""

import tempfile


def test_named_bindings() -> None:
    """Test named database bindings."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        spec = SQLSpec()

        # Add with bind keys
        cache_db = spec.add_config(
            SqliteConfig(pool_config={"database": tmp.name}), bind_key="cache_db"
        )
        main_db = spec.add_config(
            AsyncpgConfig(pool_config={"dsn": "postgresql://..."}), bind_key="main_db"
        )

        # Access by bind key
        with spec.provide_session("cache_db") as session:
            session.execute("SELECT 1")

        assert cache_db.pool_config["database"] == tmp.name
        assert main_db.pool_config["dsn"] == "postgresql://..."

