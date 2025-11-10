"""Test configuration example: Named database bindings."""

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

        # Add with bind keys
        cache_key = db_manager.add_config(SqliteConfig(pool_config={"database": tmp.name}, bind_key="cache_db"))
        dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
        main_key = db_manager.add_config(AsyncpgConfig(pool_config={"dsn": dsn}, bind_key="main_db"))

        # Access by bind key
        with db_manager.provide_session(cache_key) as session:
            session.execute("SELECT 1")

        cache_config = db_manager.get_config(cache_key)
        main_config = db_manager.get_config(main_key)

    # end-example
        assert cache_config.bind_key == "cache_db"
        assert main_config.bind_key == "main_db"

