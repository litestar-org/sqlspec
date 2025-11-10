"""Test configuration example: Per-instance cache configuration."""


def test_per_instance_cache_config() -> None:
    """Test per-instance cache configuration."""
    import tempfile

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.core.cache import CacheConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        # Configure cache for specific SQLSpec instance
        db_manager = SQLSpec()
        db_manager.update_cache_config(CacheConfig(sql_cache_enabled=True, sql_cache_size=500))

        # Add database config
        db = db_manager.add_config(SqliteConfig(pool_config={"database": tmp.name}))

        # Use the configured spec
        with db_manager.provide_session(db) as session:
            result = session.execute("SELECT 1")
            assert result is not None
