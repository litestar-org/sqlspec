"""Test configuration example: Cache statistics tracking."""

import tempfile


def test_cache_statistics() -> None:
    """Test cache statistics tracking."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.core.cache import get_cache_statistics, log_cache_stats

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        spec = SQLSpec()
        db = spec.add_config(SqliteConfig(pool_config={"database": tmp.name}))

        # Execute some queries to generate cache activity
        with spec.provide_session(db) as session:
            session.execute("SELECT 1")
            session.execute("SELECT 1")  # Should hit cache

        # Get statistics
        stats = get_cache_statistics()
        assert isinstance(stats, dict)
        assert "multi_level" in stats

        # Log statistics (logs to configured logger)
        log_cache_stats()

