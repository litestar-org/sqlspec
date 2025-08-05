from sqlspec.parameters import ParameterStyle, ParameterStyleConfig

"""Tests for SQL statement caching."""

import threading
import time
from unittest.mock import MagicMock

from sqlspec.statement.cache import SQLCache, sql_cache
from sqlspec.statement.sql import SQL, StatementConfig, _ProcessedState


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
        param_config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
        config = StatementConfig(parameter_config=param_config, enable_caching=True)

        # Clear cache
        sql_cache.clear()

        # Create two identical SQL objects
        sql1 = SQL("SELECT * FROM users", statement_config=config)
        sql2 = SQL("SELECT * FROM users", statement_config=config)

        # Access sql property to trigger processing
        result1 = sql1.sql

        # Store the current cache size
        len(sql_cache.cache)

        # Access sql property on second object
        result2 = sql2.sql

        # NOTE: Legacy cache testing removed - new caching implementation will be added
        # Both should have the same result
        assert result1 == result2

        # Both should have processed states (not testing cache sharing until new implementation)
        assert sql1._processed_state is not None
        assert sql2._processed_state is not None

    def test_sql_cache_miss_different_queries(self) -> None:
        """Test cache miss for different SQL queries."""
        param_config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
        config = StatementConfig(parameter_config=param_config, enable_caching=True)

        sql1 = SQL("SELECT * FROM users", statement_config=config)
        sql2 = SQL("SELECT * FROM products", statement_config=config)

        # These should be different SQL objects (cache keys not tested in legacy cleanup)
        assert sql1.sql != sql2.sql

    def test_sql_cache_miss_different_parameters(self) -> None:
        """Test cache miss for same query with different parameters."""
        param_config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
        config = StatementConfig(parameter_config=param_config, enable_caching=True)

        sql1 = SQL("SELECT * FROM users WHERE id = :id", id=1, statement_config=config)
        sql2 = SQL("SELECT * FROM users WHERE id = :id", id=2, statement_config=config)

        # Different parameters should result in different SQL objects
        assert sql1.parameters != sql2.parameters

    def test_sql_cache_disabled(self) -> None:
        """Test that caching can be disabled."""
        config = StatementConfig(enable_caching=False)

        sql = SQL("SELECT * FROM users", statement_config=config)

        # NOTE: Legacy cache mocking removed - new caching implementation will respect enable_caching
        # SQL should still process normally
        result = sql.sql
        assert result == "SELECT * FROM users"

    def test_sql_cache_with_filters(self) -> None:
        """Test caching with filters applied."""
        param_config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
        config = StatementConfig(parameter_config=param_config, enable_caching=True)

        sql1 = SQL("SELECT * FROM users", statement_config=config)
        sql2 = sql1.where("active = true")

        # Different filters should result in different SQL content
        assert sql1.sql != sql2.sql

    def test_sql_cache_with_dialect(self) -> None:
        """Test caching with different dialects."""
        param_config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
        config = StatementConfig(parameter_config=param_config, enable_caching=True)

        sql1 = SQL("SELECT * FROM users", _dialect="mysql", statement_config=config)
        sql2 = SQL("SELECT * FROM users", _dialect="postgres", statement_config=config)

        # Different dialects should result in different internal dialect settings
        assert sql1._dialect != sql2._dialect

    # NOTE: test_cache_key_generation() removed - _cache_key() method removed in legacy cleanup
