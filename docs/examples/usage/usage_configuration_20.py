"""Test configuration example: Named database bindings."""

import pytest

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_named_bindings",)


def test_named_bindings() -> None:
    """Test named database bindings."""
    # start-example
    import os
    import tempfile

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.sqlite import SqliteConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        db_manager = SQLSpec()

        # Add with bind keys - add_config returns the config instance
        cache_config = db_manager.add_config(SqliteConfig(pool_config={"database": tmp.name}, bind_key="cache_db"))
        dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
        main_config = db_manager.add_config(AsyncpgConfig(pool_config={"dsn": dsn}, bind_key="main_db"))

        # Access by config instance directly
        with db_manager.provide_session(cache_config) as session:
            session.execute("SELECT 1")

        # end-example
        assert cache_config.bind_key == "cache_db"
        assert main_config.bind_key == "main_db"
