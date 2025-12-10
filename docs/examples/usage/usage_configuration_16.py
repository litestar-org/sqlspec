"""Test configuration example: Per-instance cache configuration."""

__all__ = ("test_per_instance_cache_config",)


def test_per_instance_cache_config() -> None:
    """Test per-instance cache configuration."""
    # start-example
    import tempfile

    from sqlspec import CacheConfig, SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        # Configure cache for specific SQLSpec instance
        db_manager = SQLSpec()
        db_manager.update_cache_config(CacheConfig(sql_cache_enabled=True, sql_cache_size=500))

        # Add database config
        db = db_manager.add_config(SqliteConfig(connection_config={"database": tmp.name}))

        # Use the configured spec
        with db_manager.provide_session(db) as session:
            result = session.execute("SELECT 1")
            # end-example
            assert result is not None
