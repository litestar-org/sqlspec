"""Tests for SQL statement caching."""

import threading
import time
from unittest.mock import MagicMock, patch

from sqlspec.statement.cache import SQLCache, sql_cache
from sqlspec.statement.sql import SQL, SQLConfig, _ProcessedState


class TestSQLCache:
    """Test the SQLCache implementation."""

    def test_cache_get_set(self) -> None:
        """Test basic cache get/set operations."""
        cache = SQLCache(max_size=10)

        # Create a mock processed state
        state = MagicMock(spec=_ProcessedState)

        # Set and get
        cache.set("key1", state)
        assert cache.get("key1") is state

        # Non-existent key
        assert cache.get("key2") is None

    def test_cache_eviction(self) -> None:
        """Test cache eviction when max size is reached."""
        cache = SQLCache(max_size=3)

        states = [MagicMock(spec=_ProcessedState) for _ in range(4)]

        # Fill cache
        for i in range(4):
            cache.set(f"key{i}", states[i])

        # First item should be evicted (LRU)
        assert cache.get("key0") is None
        assert cache.get("key1") is states[1]
        assert cache.get("key2") is states[2]
        assert cache.get("key3") is states[3]

    def test_lru_behavior(self) -> None:
        """Test LRU eviction behavior."""
        cache = SQLCache(max_size=3)

        states = [MagicMock(spec=_ProcessedState) for _ in range(5)]

        # Add three items
        cache.set("key0", states[0])
        cache.set("key1", states[1])
        cache.set("key2", states[2])

        # Access key0 and key1 to make them recently used
        cache.get("key0")
        cache.get("key1")

        # Add key3 - should evict key2 (least recently used)
        cache.set("key3", states[3])

        assert cache.get("key0") is states[0]  # Still in cache
        assert cache.get("key1") is states[1]  # Still in cache
        assert cache.get("key2") is None  # Evicted (LRU)
        assert cache.get("key3") is states[3]  # New item

        # Access key0 again
        cache.get("key0")

        # Add key4 - should evict key1 (now least recently used)
        cache.set("key4", states[4])

        assert cache.get("key0") is states[0]  # Still in cache (most recently accessed)
        assert cache.get("key1") is None  # Evicted
        assert cache.get("key3") is states[3]  # Still in cache
        assert cache.get("key4") is states[4]  # New item

    def test_cache_thread_safety(self) -> None:
        """Test thread-safe cache operations."""
        cache = SQLCache(max_size=100)
        results = []

        def writer(thread_id: int) -> None:
            """Write to cache from thread."""
            for i in range(10):
                state = MagicMock(spec=_ProcessedState)
                cache.set(f"thread_{thread_id}_key_{i}", state)
                time.sleep(0.001)

        def reader(thread_id: int) -> None:
            """Read from cache from thread."""
            for i in range(10):
                result = cache.get(f"thread_{thread_id}_key_{i}")
                results.append(result is not None)
                time.sleep(0.001)

        # Start threads
        threads = []
        for i in range(5):
            t1 = threading.Thread(target=writer, args=(i,))
            t2 = threading.Thread(target=reader, args=(i,))
            threads.extend([t1, t2])
            t1.start()
            t2.start()

        # Wait for completion
        for t in threads:
            t.join()

        # Should have some successful reads
        assert any(results)

    def test_cache_clear(self) -> None:
        """Test cache clearing."""
        cache = SQLCache()

        states = [MagicMock(spec=_ProcessedState) for _ in range(3)]
        for i in range(3):
            cache.set(f"key{i}", states[i])

        # Verify items exist
        assert cache.get("key0") is not None

        # Clear cache
        cache.clear()

        # All items should be gone
        assert cache.get("key0") is None
        assert cache.get("key1") is None
        assert cache.get("key2") is None


class TestSQLCaching:
    """Test SQL statement caching integration."""

    def test_sql_cache_hit(self) -> None:
        """Test cache hit for identical SQL statements."""
        config = SQLConfig(enable_caching=True)

        # Clear cache
        sql_cache.clear()

        # Create two identical SQL objects
        sql1 = SQL("SELECT * FROM users", _config=config)
        sql2 = SQL("SELECT * FROM users", _config=config)

        # Access sql property to trigger processing
        result1 = sql1.sql

        # Mock the pipeline to verify it's not called on cache hit
        with patch.object(sql2._config, "get_statement_pipeline") as mock_pipeline:
            result2 = sql2.sql

            # Pipeline should not be called due to cache hit
            mock_pipeline.assert_not_called()

        assert result1 == result2

    def test_sql_cache_miss_different_queries(self) -> None:
        """Test cache miss for different SQL queries."""
        config = SQLConfig(enable_caching=True)

        sql1 = SQL("SELECT * FROM users", _config=config)
        sql2 = SQL("SELECT * FROM products", _config=config)

        # These should have different cache keys
        assert sql1._cache_key() != sql2._cache_key()

    def test_sql_cache_miss_different_parameters(self) -> None:
        """Test cache miss for same query with different parameters."""
        config = SQLConfig(enable_caching=True)

        sql1 = SQL("SELECT * FROM users WHERE id = :id", id=1, _config=config)
        sql2 = SQL("SELECT * FROM users WHERE id = :id", id=2, _config=config)

        # Different parameters should result in different cache keys
        assert sql1._cache_key() != sql2._cache_key()

    def test_sql_cache_disabled(self) -> None:
        """Test that caching can be disabled."""
        config = SQLConfig(enable_caching=False)

        sql = SQL("SELECT * FROM users", _config=config)

        # Mock sql_cache to verify it's not accessed
        with patch("sqlspec.statement.sql.sql_cache") as mock_cache:
            _ = sql.sql

            # Cache should not be accessed
            mock_cache.get.assert_not_called()
            mock_cache.set.assert_not_called()

    def test_sql_cache_with_filters(self) -> None:
        """Test caching with filters applied."""
        config = SQLConfig(enable_caching=True)

        sql1 = SQL("SELECT * FROM users", _config=config)
        sql2 = sql1.where("active = true")

        # Different filters should result in different cache keys
        assert sql1._cache_key() != sql2._cache_key()

    def test_sql_cache_with_dialect(self) -> None:
        """Test caching with different dialects."""
        config = SQLConfig(enable_caching=True)

        sql1 = SQL("SELECT * FROM users", _dialect="mysql", _config=config)
        sql2 = SQL("SELECT * FROM users", _dialect="postgres", _config=config)

        # Different dialects should result in different cache keys
        assert sql1._cache_key() != sql2._cache_key()

    def test_cache_key_generation(self) -> None:
        """Test cache key generation includes all relevant state."""
        config = SQLConfig(enable_caching=True)

        # Test with positional parameters
        sql1 = SQL("SELECT * FROM users WHERE id = ?", 1, _config=config)
        key1 = sql1._cache_key()
        assert "sql:" in key1

        # Test with named parameters
        sql2 = SQL("SELECT * FROM users WHERE id = :id", id=1, _config=config)
        key2 = sql2._cache_key()
        assert key1 != key2  # Different parameter styles

        # Test with mixed parameters
        sql3 = SQL("SELECT * FROM users WHERE id = ? AND name = :name", 1, name="test", _config=config)
        key3 = sql3._cache_key()
        assert key3 != key1
        assert key3 != key2
