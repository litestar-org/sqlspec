"""Test configuration example: Binding multiple database configurations."""

import pytest

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_binding_multiple_configs",)


def test_binding_multiple_configs() -> None:
    """Test binding multiple database configurations."""
    # start-example
    import os
    import tempfile

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.sqlite import SqliteConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        db_manager = SQLSpec()

        # Add multiple configurations - add_config returns the config instance
        sqlite_config = db_manager.add_config(SqliteConfig(pool_config={"database": tmp.name}))
        dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
        pg_config = db_manager.add_config(AsyncpgConfig(pool_config={"dsn": dsn}))

        # Use specific configuration - pass the config instance directly
        with db_manager.provide_session(sqlite_config) as session:
            session.execute("SELECT 1")

        # end-example
        assert sqlite_config.pool_config["database"] == tmp.name
        assert pg_config.pool_config["dsn"] == os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
