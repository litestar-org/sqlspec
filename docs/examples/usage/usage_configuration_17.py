"""Test configuration example: Cache statistics tracking."""

__all__ = ("test_cache_statistics",)


def test_cache_statistics() -> None:
    """Test cache statistics tracking."""
    # start-example
    import tempfile

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.core import get_cache_statistics, log_cache_stats

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        db_manager = SQLSpec()
        db = db_manager.add_config(SqliteConfig(pool_config={"database": tmp.name}))

        # Execute some queries to generate cache activity
        with db_manager.provide_session(db) as session:
            session.execute("SELECT 1")
            session.execute("SELECT 1")  # Should hit cache

        # Get statistics
        stats = get_cache_statistics()
    # end-example
        assert isinstance(stats, dict)
        assert "multi_level" in stats

        # Log statistics (logs to configured logger)
        log_cache_stats()

