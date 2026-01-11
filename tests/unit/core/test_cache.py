# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for the SQLSpec cache system.

This module tests the caching system. Tests cover:

1. CacheKey - Immutable cache keys
2. CacheStats - Cache statistics tracking and monitoring
3. LRUCache - Main LRU cache implementation with TTL support
4. NamespacedCache - Namespace-based cache with zero-copy views
5. Cache management functions - Global cache management and configuration
6. Thread safety - Concurrent access and operations
7. Performance characteristics - O(1) operations and memory efficiency

The cache system provides thread-safe caching with LRU eviction,
TTL-based expiration, and statistics tracking for monitoring
across the entire SQLSpec system.
"""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.core import (
    CacheConfig,
    CacheKey,
    CacheStats,
    LRUCache,
    NamespacedCache,
    clear_all_caches,
    get_cache,
    get_cache_config,
    get_cache_statistics,
    get_cache_stats,
    get_default_cache,
    log_cache_stats,
    reset_cache_stats,
    update_cache_config,
)

pytestmark = pytest.mark.xdist_group("core")


def test_cache_key_creation_and_immutability() -> None:
    """Test CacheKey creation and immutable behavior."""
    key_data = ("test", "key", 123)
    cache_key = CacheKey(key_data)

    assert cache_key.key_data == key_data
    assert isinstance(cache_key.key_data, tuple)

    original_data = cache_key.key_data
    assert original_data == key_data
    assert cache_key.key_data is original_data


def test_cache_key_hashing_consistency() -> None:
    """Test that CacheKey hashing is consistent and cached."""
    key_data = ("test", "hash", 456)
    cache_key1 = CacheKey(key_data)
    cache_key2 = CacheKey(key_data)

    assert hash(cache_key1) == hash(cache_key2)

    assert hash(cache_key1) == hash(cache_key1)


def test_cache_key_equality_comparison() -> None:
    """Test CacheKey equality comparison with short-circuit evaluation."""
    key_data1 = ("test", "equality", 789)
    key_data2 = ("test", "equality", 789)
    key_data3 = ("different", "key", 789)

    cache_key1 = CacheKey(key_data1)
    cache_key2 = CacheKey(key_data2)
    cache_key3 = CacheKey(key_data3)

    assert cache_key1 == cache_key2
    assert cache_key1 is not cache_key2

    assert cache_key1 != cache_key3

    assert cache_key1 != "not_a_cache_key"
    assert cache_key1 != 123


def test_cache_key_string_representation() -> None:
    """Test CacheKey string representation."""
    key_data = ("test", "repr", 999)
    cache_key = CacheKey(key_data)

    repr_str = repr(cache_key)
    assert "CacheKey" in repr_str
    assert str(key_data) in repr_str


def test_cache_stats_initialization() -> None:
    """Test CacheStats initialization with zero values."""
    stats = CacheStats()

    assert stats.hits == 0
    assert stats.misses == 0
    assert stats.evictions == 0
    assert stats.total_operations == 0
    assert stats.memory_usage == 0


def test_cache_stats_hit_rate_calculation() -> None:
    """Test hit rate and miss rate calculations."""
    stats = CacheStats()

    assert stats.hit_rate == 0.0
    assert stats.miss_rate == 100.0

    stats.record_hit()
    stats.record_hit()
    stats.record_miss()

    assert stats.hits == 2
    assert stats.misses == 1
    assert stats.total_operations == 3
    assert stats.hit_rate == pytest.approx(66.67, rel=1e-2)
    assert stats.miss_rate == pytest.approx(33.33, rel=1e-2)


def test_cache_stats_operations_recording() -> None:
    """Test recording of cache operations."""
    stats = CacheStats()

    stats.record_hit()
    stats.record_hit()
    assert stats.hits == 2
    assert stats.total_operations == 2

    stats.record_miss()
    assert stats.misses == 1
    assert stats.total_operations == 3

    stats.record_eviction()
    assert stats.evictions == 1
    assert stats.total_operations == 3


def test_cache_stats_reset() -> None:
    """Test resetting cache statistics."""
    stats = CacheStats()

    stats.record_hit()
    stats.record_miss()
    stats.record_eviction()

    stats.reset()
    assert stats.hits == 0
    assert stats.misses == 0
    assert stats.evictions == 0
    assert stats.total_operations == 0
    assert stats.memory_usage == 0


def test_cache_stats_string_representation() -> None:
    """Test CacheStats string representation."""
    stats = CacheStats()
    stats.record_hit()
    stats.record_miss()

    repr_str = repr(stats)
    assert "CacheStats" in repr_str
    assert "hit_rate=" in repr_str
    assert "hits=1" in repr_str
    assert "misses=1" in repr_str


def test_lru_cache_initialization() -> None:
    """Test LRUCache initialization with default parameters."""
    cache = LRUCache()

    assert cache.size() == 0
    assert cache.is_empty() is True
    assert len(cache) == 0


def test_lru_cache_basic_operations() -> None:
    """Test basic cache operations - get, put, delete."""
    cache = LRUCache(max_size=3)
    key1 = CacheKey(("test", 1))
    key2 = CacheKey(("test", 2))

    cache.put(key1, "value1")
    assert cache.get(key1) == "value1"
    assert cache.size() == 1
    assert not cache.is_empty()

    assert cache.get(key2) is None

    assert cache.delete(key1) is True
    assert cache.get(key1) is None
    assert cache.delete(key1) is False
    assert cache.size() == 0


def test_lru_cache_lru_eviction() -> None:
    """Test LRU eviction policy when cache exceeds max size."""
    cache = LRUCache(max_size=2)
    key1 = CacheKey(("test", 1))
    key2 = CacheKey(("test", 2))
    key3 = CacheKey(("test", 3))

    cache.put(key1, "value1")
    cache.put(key2, "value2")
    assert cache.size() == 2

    cache.put(key3, "value3")
    assert cache.size() == 2
    assert cache.get(key1) is None
    assert cache.get(key2) == "value2"
    assert cache.get(key3) == "value3"


def test_lru_cache_lru_ordering() -> None:
    """Test that LRU ordering is maintained correctly."""
    cache = LRUCache(max_size=3)
    key1 = CacheKey(("test", 1))
    key2 = CacheKey(("test", 2))
    key3 = CacheKey(("test", 3))
    key4 = CacheKey(("test", 4))

    cache.put(key1, "value1")
    cache.put(key2, "value2")
    cache.put(key3, "value3")

    cache.get(key1)

    cache.put(key4, "value4")

    assert cache.get(key1) == "value1"
    assert cache.get(key2) is None
    assert cache.get(key3) == "value3"
    assert cache.get(key4) == "value4"


def test_lru_cache_update_existing_key() -> None:
    """Test updating value for existing cache key."""
    cache = LRUCache()
    key = CacheKey(("test", "update"))

    cache.put(key, "original")
    assert cache.get(key) == "original"
    assert cache.size() == 1

    cache.put(key, "updated")
    assert cache.get(key) == "updated"
    assert cache.size() == 1


def test_lru_cache_ttl_expiration() -> None:
    """Test TTL-based cache expiration."""
    cache = LRUCache(max_size=10, ttl_seconds=1)
    key = CacheKey(("test", "ttl"))

    cache.put(key, "expires_soon")
    assert cache.get(key) == "expires_soon"
    assert key in cache

    time.sleep(1.1)

    assert cache.get(key) is None
    assert key not in cache


def test_lru_cache_contains_operation() -> None:
    """Test __contains__ operation with TTL consideration."""
    cache = LRUCache(ttl_seconds=1)
    key = CacheKey(("test", "contains"))

    assert key not in cache

    cache.put(key, "test_value")
    assert key in cache

    time.sleep(1.1)
    assert key not in cache


def test_lru_cache_clear_operation() -> None:
    """Test clearing all cache entries."""
    cache = LRUCache()
    key1 = CacheKey(("test", 1))
    key2 = CacheKey(("test", 2))

    cache.put(key1, "value1")
    cache.put(key2, "value2")
    assert cache.size() == 2

    cache.clear()
    assert cache.size() == 0
    assert cache.is_empty()
    assert cache.get(key1) is None
    assert cache.get(key2) is None


def test_lru_cache_statistics_tracking() -> None:
    """Test cache statistics tracking during operations."""
    cache = LRUCache(max_size=2)
    key1 = CacheKey(("test", 1))
    key2 = CacheKey(("test", 2))
    key3 = CacheKey(("test", 3))

    stats = cache.get_stats()
    assert stats.hits == 0
    assert stats.misses == 0

    cache.get(key1)
    stats = cache.get_stats()
    assert stats.misses == 1
    assert stats.hits == 0

    cache.put(key1, "value1")
    cache.get(key1)
    stats = cache.get_stats()
    assert stats.hits == 1
    assert stats.misses == 1

    cache.put(key2, "value2")
    cache.put(key3, "value3")
    stats = cache.get_stats()
    assert stats.evictions == 1


def test_namespaced_cache_statement_operations() -> None:
    """Test NamespacedCache statement namespace operations."""
    cache = get_cache()

    cache_key = "SELECT * FROM users WHERE id = ?"
    compiled_sql = "SELECT * FROM users WHERE id = $1"
    parameters = ["param1"]
    cache_value = (compiled_sql, parameters)

    cache.put_statement(cache_key, cache_value)

    result = cache.get_statement(cache_key)
    assert result is not None
    assert result[0] == compiled_sql
    assert result[1] == parameters

    cache.delete_statement(cache_key)
    assert cache.get_statement(cache_key) is None


def test_namespaced_cache_expression_operations() -> None:
    """Test NamespacedCache expression namespace operations."""
    cache = get_cache()

    sql = "SELECT * FROM users WHERE id = 1"
    dialect = "postgresql"
    cache_key = f"{sql}::{dialect}"
    mock_expression = MagicMock()
    mock_expression.sql.return_value = sql

    cache.put_expression(cache_key, mock_expression)

    result = cache.get_expression(cache_key)
    assert result is mock_expression

    result_missing = cache.get_expression("missing_key")
    assert result_missing is None


def test_namespaced_cache_additional_namespaces() -> None:
    """Test NamespacedCache optimized, builder, and file namespaces."""
    cache = get_cache()

    cache_key = "shared-key"
    optimized_value = MagicMock()
    builder_value = MagicMock()
    file_value = MagicMock()

    cache.put_optimized(cache_key, optimized_value)
    cache.put_builder(cache_key, builder_value)
    cache.put_file(cache_key, file_value)

    assert cache.get_optimized(cache_key) is optimized_value
    assert cache.get_builder(cache_key) is builder_value
    assert cache.get_file(cache_key) is file_value

    cache.delete_optimized(cache_key)
    assert cache.get_optimized(cache_key) is None
    assert cache.get_builder(cache_key) is builder_value
    assert cache.get_file(cache_key) is file_value


def test_namespaced_cache_respects_config_flags() -> None:
    """Test that cache config flags disable namespaces."""
    original_config = get_cache_config()

    try:
        update_cache_config(
            CacheConfig(sql_cache_enabled=False, fragment_cache_enabled=False, optimized_cache_enabled=False)
        )
        cache = get_cache()

        cache.put_statement("stmt", "value")
        cache.put_builder("builder", "value")
        cache.put_expression("expr", "value")
        cache.put_file("file", "value")
        cache.put_optimized("opt", "value")

        assert cache.get_statement("stmt") is None
        assert cache.get_builder("builder") is None
        assert cache.get_expression("expr") is None
        assert cache.get_file("file") is None
        assert cache.get_optimized("opt") is None
    finally:
        update_cache_config(original_config)


def test_namespaced_cache_respects_compiled_flag() -> None:
    """Test that compiled_cache_enabled disables all namespaces."""
    original_config = get_cache_config()

    try:
        update_cache_config(CacheConfig(compiled_cache_enabled=False))
        cache = get_cache()

        cache.put_statement("stmt", "value")
        cache.put_expression("expr", "value")
        cache.put_builder("builder", "value")
        cache.put_file("file", "value")
        cache.put_optimized("opt", "value")

        assert cache.get_statement("stmt") is None
        assert cache.get_expression("expr") is None
        assert cache.get_builder("builder") is None
        assert cache.get_file("file") is None
        assert cache.get_optimized("opt") is None
    finally:
        update_cache_config(original_config)


def test_get_default_cache_singleton() -> None:
    """Test that get_default_cache returns the same instance."""
    cache1 = get_default_cache()
    cache2 = get_default_cache()

    assert cache1 is cache2
    assert isinstance(cache1, LRUCache)


def test_get_cache_singleton() -> None:
    """Test that get_cache returns the same instance."""
    cache1 = get_cache()
    cache2 = get_cache()

    assert cache1 is cache2
    assert isinstance(cache1, NamespacedCache)


def test_clear_all_caches_function() -> None:
    """Test clearing all global cache instances."""

    default_cache = get_default_cache()
    multi_cache = get_cache()

    test_key = CacheKey(("test",))
    default_cache.put(test_key, "test_value")
    multi_cache.put_statement("key1", "value1")

    assert default_cache.size() > 0
    assert multi_cache.get_statement("key1") == "value1"

    clear_all_caches()

    assert default_cache.size() == 0
    assert multi_cache.get_statement("key1") is None


def test_get_cache_statistics_function() -> None:
    """Test getting statistics from all cache instances."""

    get_default_cache()
    get_cache()

    stats_dict = get_cache_statistics()

    assert isinstance(stats_dict, dict)
    assert "default" in stats_dict
    assert "namespaced" in stats_dict

    for stats in stats_dict.values():
        assert isinstance(stats, CacheStats)


def test_cache_config_initialization() -> None:
    """Test CacheConfig initialization with defaults."""
    config = CacheConfig()

    assert config.compiled_cache_enabled is True
    assert config.sql_cache_enabled is True
    assert config.fragment_cache_enabled is True
    assert config.optimized_cache_enabled is True
    assert config.sql_cache_size == 1000
    assert config.fragment_cache_size == 5000
    assert config.optimized_cache_size == 2000


def test_cache_config_custom_values() -> None:
    """Test CacheConfig with custom values."""
    config = CacheConfig(sql_cache_enabled=False, fragment_cache_size=10000, optimized_cache_enabled=False)

    assert config.sql_cache_enabled is False
    assert config.fragment_cache_size == 10000
    assert config.optimized_cache_enabled is False

    assert config.compiled_cache_enabled is True
    assert config.sql_cache_size == 1000


def test_get_cache_config_singleton() -> None:
    """Test that get_cache_config returns the same instance."""
    config1 = get_cache_config()
    config2 = get_cache_config()

    assert config1 is config2
    assert isinstance(config1, CacheConfig)


def test_update_cache_config_function() -> None:
    """Test updating global cache configuration."""
    original_config = get_cache_config()

    try:
        new_config = CacheConfig(sql_cache_size=9999, fragment_cache_enabled=False)

        update_cache_config(new_config)

        current_config = get_cache_config()
        assert current_config is new_config
        assert current_config.sql_cache_size == 9999
        assert current_config.fragment_cache_enabled is False

    finally:
        update_cache_config(original_config)


def test_namespaced_cache_namespace_isolation() -> None:
    """Test that different namespaces in NamespacedCache are isolated."""
    cache = get_cache()

    cache.put_statement("key1", "value1")
    cache.put_expression("key1", "value2")
    cache.put_builder("key1", "value3")

    assert cache.get_statement("key1") == "value1"
    assert cache.get_expression("key1") == "value2"
    assert cache.get_builder("key1") == "value3"

    cache.delete_statement("key1")
    assert cache.get_statement("key1") is None
    assert cache.get_expression("key1") == "value2"
    assert cache.get_builder("key1") == "value3"


def test_get_cache_stats_aggregation() -> None:
    """Test cache statistics aggregation."""
    reset_cache_stats()

    stats = get_cache_stats()
    assert isinstance(stats, dict)
    assert "default" in stats
    assert "namespaced" in stats


def test_reset_cache_stats_function() -> None:
    """Test resetting all cache statistics."""
    default_cache = get_default_cache()
    multi_cache = get_cache()

    test_key = CacheKey(("test",))
    default_cache.get(test_key)
    multi_cache.get_statement("key")

    reset_cache_stats()

    default_stats = default_cache.get_stats()
    multi_stats = multi_cache.get_stats()

    assert default_stats.hits == 0
    assert default_stats.misses == 0
    assert multi_stats.hits == 0
    assert multi_stats.misses == 0


def test_log_cache_stats_function() -> None:
    """Test logging cache statistics."""
    with patch("sqlspec.core.cache.get_logger") as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        log_cache_stats()

        mock_get_logger.assert_called_once_with("sqlspec.cache")
        mock_logger.log.assert_called_once()


def test_namespaced_cache_interface() -> None:
    """Test namespaced cache interface."""
    cache = get_cache()
    cache_key = "test_cache_key"
    cache_value = ("SELECT * FROM users WHERE id = $1", [1])

    cache.put_statement(cache_key, cache_value, "postgres")

    result = cache.get_statement(cache_key, "postgres")
    assert result == cache_value

    result_none = cache.get_statement("non_existent_key", "postgres")
    assert result_none is None


def test_lru_cache_thread_safety() -> None:
    """Test LRUCache thread safety with concurrent operations."""
    cache = LRUCache(max_size=100)
    results = []
    errors = []

    def worker(thread_id: int) -> None:
        try:
            for i in range(50):
                key = CacheKey((thread_id, i))
                cache.put(key, thread_id * 1000 + i)
                value = cache.get(key)
                results.append(value)
        except Exception as e:
            errors.append(e)

    threads = []
    for tid in range(5):
        thread = threading.Thread(target=worker, args=(tid,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    assert len(errors) == 0, f"Thread safety errors: {errors}"
    assert len(results) > 0


def test_cache_statistics_thread_safety() -> None:
    """Test cache statistics thread safety."""
    cache = LRUCache()
    errors = []

    def stats_worker() -> None:
        try:
            for i in range(100):
                key = CacheKey((f"thread_stats_{i}",))
                cache.get(key)
                cache.put(key, f"value_{i}")
                cache.get(key)
        except Exception as e:
            errors.append(e)

    threads = []
    for _ in range(3):
        thread = threading.Thread(target=stats_worker)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    assert len(errors) == 0

    stats = cache.get_stats()
    assert stats.hits > 0
    assert stats.misses > 0
    assert stats.total_operations > 0


def test_cache_key_performance_with_large_data() -> None:
    """Test CacheKey performance with large key data."""
    large_key_data = tuple(range(1000))
    cache_key = CacheKey(large_key_data)

    assert cache_key.key_data == large_key_data
    assert isinstance(hash(cache_key), int)


def test_lru_cache_zero_max_size() -> None:
    """Test LRUCache with zero max size (no caching)."""
    cache = LRUCache(max_size=0)
    key = CacheKey(("test",))

    cache.put(key, "test_value")

    assert cache.get(key) is None
    assert cache.size() == 0


def test_lru_cache_very_short_ttl() -> None:
    """Test LRUCache with very short TTL."""
    cache = LRUCache(ttl_seconds=1)
    key = CacheKey(("test", "short_ttl"))

    cache.put(key, "expires_quickly")
    assert cache.get(key) == "expires_quickly"

    time.sleep(1.1)

    assert cache.get(key) is None


@pytest.mark.parametrize("cache_size,num_items", [(10, 15), (100, 50), (1, 10)])
def test_lru_cache_various_sizes(cache_size: int, num_items: int) -> None:
    """Test LRUCache with various size configurations."""
    cache = LRUCache(max_size=cache_size)

    for i in range(num_items):
        key = CacheKey((i,))
        cache.put(key, i)

    assert cache.size() <= cache_size

    if num_items > cache_size:
        assert cache.size() == cache_size

        early_key = CacheKey((0,))
        assert cache.get(early_key) is None


def test_cache_with_none_values() -> None:
    """Test cache behavior with None values."""
    cache = LRUCache()
    key = CacheKey(("none_test",))

    cache.put(key, None)

    result = cache.get(key)
    assert result is None
    assert key in cache

    missing_key = CacheKey(("not_in_cache",))
    missing_result = cache.get(missing_key)
    assert missing_result is None
    assert missing_key not in cache
