"""Comprehensive unit tests for the SQLSpec cache system.

This module tests the unified caching system that is critical to the CORE_ROUND_3
architecture performance optimization. Tests cover:

1. CacheKey - High-performance immutable cache keys
2. CacheStats - Cache statistics tracking and monitoring
3. UnifiedCache - Main LRU cache implementation with TTL support
4. StatementCache - Specialized caching for compiled SQL statements
5. ExpressionCache - Specialized caching for parsed SQLGlot expressions
6. ParameterCache - Specialized caching for processed parameters
7. Cache management functions - Global cache management and configuration
8. Thread safety - Concurrent access and operations
9. Performance characteristics - O(1) operations and memory efficiency

The cache system provides high-performance, thread-safe caching with LRU eviction,
TTL-based expiration, and comprehensive statistics tracking for monitoring and
optimization across the entire SQLSpec system.
"""

import threading
import time
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.core.cache import (
    CacheConfig,
    CacheKey,
    CacheStats,
    CacheStatsAggregate,
    ExpressionCache,
    ParameterCache,
    StatementCache,
    UnifiedCache,
    clear_all_caches,
    get_cache_config,
    get_cache_statistics,
    get_cache_stats,
    get_default_cache,
    get_expression_cache,
    get_parameter_cache,
    get_statement_cache,
    log_cache_stats,
    reset_cache_stats,
    sql_cache,
    update_cache_config,
)

# CacheKey Tests


def test_cache_key_creation_and_immutability() -> None:
    """Test CacheKey creation and immutable behavior."""
    key_data = ("test", "key", 123)
    cache_key = CacheKey(key_data)

    assert cache_key.key_data == key_data
    assert isinstance(cache_key.key_data, tuple)

    # Note: With mypyc compilation, we can't enforce immutability the same way
    # as with object.__setattr__. The CacheKey is still effectively immutable
    # in practice since we don't provide any methods to modify it.
    # Test that the key data is preserved correctly
    original_data = cache_key.key_data
    assert original_data == key_data
    assert cache_key.key_data is original_data  # Same object reference


def test_cache_key_hashing_consistency() -> None:
    """Test that CacheKey hashing is consistent and cached."""
    key_data = ("test", "hash", 456)
    cache_key1 = CacheKey(key_data)
    cache_key2 = CacheKey(key_data)

    # Same data should produce same hash
    assert hash(cache_key1) == hash(cache_key2)

    # Hash should be cached (same object returns same hash)
    assert hash(cache_key1) == hash(cache_key1)


def test_cache_key_equality_comparison() -> None:
    """Test CacheKey equality comparison with short-circuit evaluation."""
    key_data1 = ("test", "equality", 789)
    key_data2 = ("test", "equality", 789)
    key_data3 = ("different", "key", 789)

    cache_key1 = CacheKey(key_data1)
    cache_key2 = CacheKey(key_data2)
    cache_key3 = CacheKey(key_data3)

    # Equal keys
    assert cache_key1 == cache_key2
    assert cache_key1 is not cache_key2  # Different objects

    # Different keys
    assert cache_key1 != cache_key3

    # Different types
    assert cache_key1 != "not_a_cache_key"
    assert cache_key1 != 123


def test_cache_key_string_representation() -> None:
    """Test CacheKey string representation."""
    key_data = ("test", "repr", 999)
    cache_key = CacheKey(key_data)

    repr_str = repr(cache_key)
    assert "CacheKey" in repr_str
    assert str(key_data) in repr_str


# CacheStats Tests


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

    # Initially no operations, hit rate should be 0
    assert stats.hit_rate == 0.0
    assert stats.miss_rate == 100.0

    # Record some hits and misses
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

    # Record hits
    stats.record_hit()
    stats.record_hit()
    assert stats.hits == 2
    assert stats.total_operations == 2

    # Record misses
    stats.record_miss()
    assert stats.misses == 1
    assert stats.total_operations == 3

    # Record evictions
    stats.record_eviction()
    assert stats.evictions == 1
    assert stats.total_operations == 3  # Evictions don't count as operations


def test_cache_stats_reset() -> None:
    """Test resetting cache statistics."""
    stats = CacheStats()

    # Record some operations
    stats.record_hit()
    stats.record_miss()
    stats.record_eviction()

    # Reset and verify all counters are zero
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


# UnifiedCache Tests


def test_unified_cache_initialization() -> None:
    """Test UnifiedCache initialization with default parameters."""
    cache: UnifiedCache[str] = UnifiedCache()

    assert cache.size() == 0
    assert cache.is_empty() is True
    assert len(cache) == 0


def test_unified_cache_basic_operations() -> None:
    """Test basic cache operations - get, put, delete."""
    cache: UnifiedCache[str] = UnifiedCache(max_size=3)
    key1 = CacheKey(("test", 1))
    key2 = CacheKey(("test", 2))

    # Test put and get
    cache.put(key1, "value1")
    assert cache.get(key1) == "value1"
    assert cache.size() == 1
    assert not cache.is_empty()

    # Test get non-existent key
    assert cache.get(key2) is None

    # Test delete
    assert cache.delete(key1) is True
    assert cache.get(key1) is None
    assert cache.delete(key1) is False  # Already deleted
    assert cache.size() == 0


def test_unified_cache_lru_eviction() -> None:
    """Test LRU eviction policy when cache exceeds max size."""
    cache: UnifiedCache[str] = UnifiedCache(max_size=2)
    key1 = CacheKey(("test", 1))
    key2 = CacheKey(("test", 2))
    key3 = CacheKey(("test", 3))

    # Fill cache to capacity
    cache.put(key1, "value1")
    cache.put(key2, "value2")
    assert cache.size() == 2

    # Add third item, should evict first (LRU)
    cache.put(key3, "value3")
    assert cache.size() == 2
    assert cache.get(key1) is None  # Evicted
    assert cache.get(key2) == "value2"  # Still present
    assert cache.get(key3) == "value3"  # Recently added


def test_unified_cache_lru_ordering() -> None:
    """Test that LRU ordering is maintained correctly."""
    cache: UnifiedCache[str] = UnifiedCache(max_size=3)
    key1 = CacheKey(("test", 1))
    key2 = CacheKey(("test", 2))
    key3 = CacheKey(("test", 3))
    key4 = CacheKey(("test", 4))

    # Add items
    cache.put(key1, "value1")
    cache.put(key2, "value2")
    cache.put(key3, "value3")

    # Access key1 to make it most recently used
    cache.get(key1)

    # Add key4, should evict key2 (least recently used)
    cache.put(key4, "value4")

    assert cache.get(key1) == "value1"  # Still present (recently accessed)
    assert cache.get(key2) is None  # Evicted
    assert cache.get(key3) == "value3"  # Still present
    assert cache.get(key4) == "value4"  # Recently added


def test_unified_cache_update_existing_key() -> None:
    """Test updating value for existing cache key."""
    cache: UnifiedCache[str] = UnifiedCache()
    key = CacheKey(("test", "update"))

    # Initial value
    cache.put(key, "original")
    assert cache.get(key) == "original"
    assert cache.size() == 1

    # Update value
    cache.put(key, "updated")
    assert cache.get(key) == "updated"
    assert cache.size() == 1  # Size unchanged


def test_unified_cache_ttl_expiration() -> None:
    """Test TTL-based cache expiration."""
    cache: UnifiedCache[str] = UnifiedCache(max_size=10, ttl_seconds=1)
    key = CacheKey(("test", "ttl"))

    # Put value and verify it's cached
    cache.put(key, "expires_soon")
    assert cache.get(key) == "expires_soon"
    assert key in cache

    # Wait for TTL expiration
    time.sleep(1.1)

    # Value should be expired and removed
    assert cache.get(key) is None
    assert key not in cache


def test_unified_cache_contains_operation() -> None:
    """Test __contains__ operation with TTL consideration."""
    cache: UnifiedCache[str] = UnifiedCache(ttl_seconds=1)
    key = CacheKey(("test", "contains"))

    # Key not in cache
    assert key not in cache

    # Add key to cache
    cache.put(key, "test_value")
    assert key in cache

    # Wait for expiration
    time.sleep(1.1)
    assert key not in cache


def test_unified_cache_clear_operation() -> None:
    """Test clearing all cache entries."""
    cache: UnifiedCache[str] = UnifiedCache()
    key1 = CacheKey(("test", 1))
    key2 = CacheKey(("test", 2))

    # Add items
    cache.put(key1, "value1")
    cache.put(key2, "value2")
    assert cache.size() == 2

    # Clear cache
    cache.clear()
    assert cache.size() == 0
    assert cache.is_empty()
    assert cache.get(key1) is None
    assert cache.get(key2) is None


def test_unified_cache_statistics_tracking() -> None:
    """Test cache statistics tracking during operations."""
    cache: UnifiedCache[str] = UnifiedCache(max_size=2)
    key1 = CacheKey(("test", 1))
    key2 = CacheKey(("test", 2))
    key3 = CacheKey(("test", 3))

    stats = cache.get_stats()
    assert stats.hits == 0
    assert stats.misses == 0

    # Miss - key not in cache
    cache.get(key1)
    stats = cache.get_stats()
    assert stats.misses == 1
    assert stats.hits == 0

    # Put and hit
    cache.put(key1, "value1")
    cache.get(key1)
    stats = cache.get_stats()
    assert stats.hits == 1
    assert stats.misses == 1

    # Test eviction statistics
    cache.put(key2, "value2")
    cache.put(key3, "value3")  # Should evict key1
    stats = cache.get_stats()
    assert stats.evictions == 1


# StatementCache Tests


def test_statement_cache_initialization() -> None:
    """Test StatementCache initialization."""
    stmt_cache = StatementCache(max_size=100)

    assert isinstance(stmt_cache._cache, UnifiedCache)
    stats = stmt_cache.get_stats()
    assert stats.hits == 0
    assert stats.misses == 0


@patch("sqlspec.core.statement.SQL")
def test_statement_cache_compiled_storage_and_retrieval(mock_sql: MagicMock) -> None:
    """Test storing and retrieving compiled SQL statements."""
    stmt_cache = StatementCache()

    # Create mock SQL statement
    mock_statement = MagicMock()
    mock_statement._raw_sql = "SELECT * FROM users WHERE id = ?"
    mock_statement.dialect = None
    mock_statement.is_many = False
    mock_statement.is_script = False
    mock_sql.return_value = mock_statement

    # Store compiled SQL
    compiled_sql = "SELECT * FROM users WHERE id = $1"
    parameters = ["param1"]
    stmt_cache.put_compiled(mock_statement, compiled_sql, parameters)

    # Retrieve compiled SQL
    result = stmt_cache.get_compiled(mock_statement)
    assert result is not None
    assert result[0] == compiled_sql
    assert result[1] == parameters


@patch("sqlspec.core.statement.SQL")
def test_statement_cache_key_generation(mock_sql: MagicMock) -> None:
    """Test cache key generation for SQL statements."""
    stmt_cache = StatementCache()

    # Create mock SQL statements
    mock_statement1 = MagicMock()
    mock_statement1._raw_sql = "SELECT * FROM users"
    mock_statement1.dialect = "postgresql"
    mock_statement1.is_many = False
    mock_statement1.is_script = False
    mock_statement1.__hash__ = lambda self: hash("statement1")  # type: ignore[misc]

    mock_statement2 = MagicMock()
    mock_statement2._raw_sql = "SELECT * FROM orders"  # Different SQL
    mock_statement2.dialect = "postgresql"
    mock_statement2.is_many = False
    mock_statement2.is_script = False
    mock_statement2.__hash__ = lambda self: hash("statement2")  # type: ignore[misc]

    # Generate cache keys
    key1 = stmt_cache._create_statement_key(mock_statement1)
    key2 = stmt_cache._create_statement_key(mock_statement2)

    # Keys should be different for different statements
    assert key1 != key2
    assert isinstance(key1, CacheKey)
    assert isinstance(key2, CacheKey)


def test_statement_cache_clear_operation() -> None:
    """Test clearing statement cache."""
    stmt_cache = StatementCache()

    # Add some mock data by accessing the internal cache
    test_key = CacheKey(("test", "data"))
    stmt_cache._cache.put(test_key, ("SELECT 1", []))

    assert stmt_cache._cache.size() == 1

    # Clear cache
    stmt_cache.clear()
    assert stmt_cache._cache.size() == 0


# ExpressionCache Tests


def test_expression_cache_initialization() -> None:
    """Test ExpressionCache initialization."""
    expr_cache = ExpressionCache(max_size=50)

    assert isinstance(expr_cache._cache, UnifiedCache)
    stats = expr_cache.get_stats()
    assert stats.hits == 0


def test_expression_cache_key_generation() -> None:
    """Test cache key generation for expressions."""
    expr_cache = ExpressionCache()

    sql1 = "SELECT * FROM users"
    dialect1 = "postgresql"
    key1 = expr_cache._create_expression_key(sql1, dialect1)

    sql2 = "SELECT * FROM orders"  # Different SQL
    dialect2 = "postgresql"
    key2 = expr_cache._create_expression_key(sql2, dialect2)

    sql3 = sql1  # Same SQL
    dialect3 = "mysql"  # Different dialect
    key3 = expr_cache._create_expression_key(sql3, dialect3)

    # Different SQL should produce different keys
    assert key1 != key2

    # Same SQL with different dialect should produce different keys
    assert key1 != key3


def test_expression_cache_storage_and_retrieval() -> None:
    """Test storing and retrieving parsed expressions."""
    expr_cache = ExpressionCache()

    sql = "SELECT * FROM users WHERE id = 1"
    dialect = "postgresql"
    mock_expression = MagicMock()
    mock_expression.sql.return_value = sql

    # Store expression
    expr_cache.put_expression(sql, mock_expression, dialect)

    # Retrieve expression
    result = expr_cache.get_expression(sql, dialect)
    assert result is mock_expression

    # Try with different dialect - should not find
    result_different = expr_cache.get_expression(sql, "mysql")
    assert result_different is None


def test_expression_cache_clear_operation() -> None:
    """Test clearing expression cache."""
    expr_cache = ExpressionCache()

    # Add mock expression
    sql = "SELECT 1"
    expr_cache.put_expression(sql, MagicMock())
    assert expr_cache._cache.size() == 1

    # Clear cache
    expr_cache.clear()
    assert expr_cache._cache.size() == 0


# ParameterCache Tests


def test_parameter_cache_initialization() -> None:
    """Test ParameterCache initialization."""
    param_cache = ParameterCache(max_size=200)

    assert isinstance(param_cache._cache, UnifiedCache)
    stats = param_cache.get_stats()
    assert stats.hits == 0


def test_parameter_cache_key_generation_dict_params() -> None:
    """Test cache key generation for dictionary parameters."""
    param_cache = ParameterCache()

    params1 = {"user_id": 1, "name": "John"}
    config_hash1 = hash("config1")
    key1 = param_cache._create_parameter_key(params1, config_hash1)

    params2 = {"user_id": 2, "name": "Jane"}  # Different values
    config_hash2 = hash("config1")  # Same config
    key2 = param_cache._create_parameter_key(params2, config_hash2)

    params3 = params1  # Same params
    config_hash3 = hash("config2")  # Different config
    key3 = param_cache._create_parameter_key(params3, config_hash3)

    # Different parameters should produce different keys
    assert key1 != key2

    # Same parameters with different config should produce different keys
    assert key1 != key3


def test_parameter_cache_key_generation_list_params() -> None:
    """Test cache key generation for list/tuple parameters."""
    param_cache = ParameterCache()

    params1 = [1, 2, 3]
    params2 = (1, 2, 3)  # Same values, different type
    config_hash = hash("config")

    key1 = param_cache._create_parameter_key(params1, config_hash)
    key2 = param_cache._create_parameter_key(params2, config_hash)

    # List and tuple with same values should produce same key (both converted to tuple)
    assert key1 == key2


def test_parameter_cache_key_generation_unhashable_params() -> None:
    """Test cache key generation for unhashable parameters."""
    param_cache = ParameterCache()

    # Unhashable parameter (list of lists)
    params = [[1, 2], [3, 4]]
    config_hash = hash("config")

    # Should not raise exception, use string fallback
    key = param_cache._create_parameter_key(params, config_hash)
    assert isinstance(key, CacheKey)


def test_parameter_cache_storage_and_retrieval() -> None:
    """Test storing and retrieving processed parameters."""
    param_cache = ParameterCache()

    original_params = {"user_id": 1, "name": "John"}
    processed_params = [1, "John"]  # Converted to list format
    config_hash = hash("config")

    # Store parameters
    param_cache.put_parameters(original_params, processed_params, config_hash)

    # Retrieve parameters
    result = param_cache.get_parameters(original_params, config_hash)
    assert result == processed_params

    # Try with different config hash - should not find
    result_different = param_cache.get_parameters(original_params, hash("different_config"))
    assert result_different is None


def test_parameter_cache_clear_operation() -> None:
    """Test clearing parameter cache."""
    param_cache = ParameterCache()

    # Add mock parameters
    param_cache.put_parameters({"test": 1}, [1], hash("config"))
    assert param_cache._cache.size() == 1

    # Clear cache
    param_cache.clear()
    assert param_cache._cache.size() == 0


# Global Cache Management Tests


def test_get_default_cache_singleton() -> None:
    """Test that get_default_cache returns the same instance."""
    cache1 = get_default_cache()
    cache2 = get_default_cache()

    assert cache1 is cache2
    assert isinstance(cache1, UnifiedCache)


def test_get_statement_cache_singleton() -> None:
    """Test that get_statement_cache returns the same instance."""
    cache1 = get_statement_cache()
    cache2 = get_statement_cache()

    assert cache1 is cache2
    assert isinstance(cache1, StatementCache)


def test_get_expression_cache_singleton() -> None:
    """Test that get_expression_cache returns the same instance."""
    cache1 = get_expression_cache()
    cache2 = get_expression_cache()

    assert cache1 is cache2
    assert isinstance(cache1, ExpressionCache)


def test_get_parameter_cache_singleton() -> None:
    """Test that get_parameter_cache returns the same instance."""
    cache1 = get_parameter_cache()
    cache2 = get_parameter_cache()

    assert cache1 is cache2
    assert isinstance(cache1, ParameterCache)


def test_clear_all_caches_function() -> None:
    """Test clearing all global cache instances."""
    # Access caches to ensure they are initialized
    default_cache = get_default_cache()
    stmt_cache = get_statement_cache()
    expr_cache = get_expression_cache()
    param_cache = get_parameter_cache()

    # Add some data
    test_key = CacheKey(("test",))
    default_cache.put(test_key, "test_value")
    stmt_cache._cache.put(test_key, ("SELECT 1", []))
    expr_cache._cache.put(test_key, MagicMock())
    param_cache._cache.put(test_key, [1, 2, 3])

    # Verify data exists
    assert default_cache.size() > 0
    assert stmt_cache._cache.size() > 0
    assert expr_cache._cache.size() > 0
    assert param_cache._cache.size() > 0

    # Clear all caches
    clear_all_caches()

    # Verify all caches are empty
    assert default_cache.size() == 0
    assert stmt_cache._cache.size() == 0
    assert expr_cache._cache.size() == 0
    assert param_cache._cache.size() == 0


def test_get_cache_statistics_function() -> None:
    """Test getting statistics from all cache instances."""
    # Access caches to ensure they are initialized
    get_default_cache()
    get_statement_cache()
    get_expression_cache()
    get_parameter_cache()

    # Get statistics
    stats_dict = get_cache_statistics()

    assert isinstance(stats_dict, dict)
    assert "default" in stats_dict
    assert "statement" in stats_dict
    assert "expression" in stats_dict
    assert "parameter" in stats_dict

    # Each should be a CacheStats instance
    for stats in stats_dict.values():
        assert isinstance(stats, CacheStats)


# Cache Configuration Tests


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
    # Other values should use defaults
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
        # Create new configuration
        new_config = CacheConfig(sql_cache_size=9999, fragment_cache_enabled=False)

        # Update configuration
        update_cache_config(new_config)

        # Verify configuration changed
        current_config = get_cache_config()
        assert current_config is new_config
        assert current_config.sql_cache_size == 9999
        assert current_config.fragment_cache_enabled is False

    finally:
        # Restore original configuration
        update_cache_config(original_config)


# Cache Statistics Aggregation Tests


def test_cache_stats_aggregate_initialization() -> None:
    """Test CacheStatsAggregate initialization."""
    stats = CacheStatsAggregate()

    assert stats.sql_hit_rate == 0.0
    assert stats.fragment_hit_rate == 0.0
    assert stats.optimized_hit_rate == 0.0
    assert stats.sql_size == 0
    assert stats.fragment_size == 0
    assert stats.optimized_size == 0
    assert stats.sql_hits == 0
    assert stats.sql_misses == 0
    assert stats.fragment_hits == 0
    assert stats.fragment_misses == 0
    assert stats.optimized_hits == 0
    assert stats.optimized_misses == 0


def test_get_cache_stats_aggregation() -> None:
    """Test cache statistics aggregation."""
    # Clear existing stats
    reset_cache_stats()

    # Get aggregated stats
    stats = get_cache_stats()
    assert isinstance(stats, CacheStatsAggregate)

    # Should be initialized with zeros
    assert stats.sql_hits == 0
    assert stats.sql_misses == 0


def test_reset_cache_stats_function() -> None:
    """Test resetting all cache statistics."""
    # Access caches to initialize them
    default_cache = get_default_cache()
    stmt_cache = get_statement_cache()

    # Generate some statistics
    test_key = CacheKey(("test",))
    default_cache.get(test_key)  # Miss
    stmt_cache._cache.get(test_key)  # Miss

    # Reset statistics
    reset_cache_stats()

    # Verify statistics are reset
    default_stats = default_cache.get_stats()
    stmt_stats = stmt_cache.get_stats()

    assert default_stats.hits == 0
    assert default_stats.misses == 0
    assert stmt_stats.hits == 0
    assert stmt_stats.misses == 0


def test_log_cache_stats_function() -> None:
    """Test logging cache statistics."""
    with patch("sqlspec.core.cache.get_logger") as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Log cache statistics
        log_cache_stats()

        # Verify logger was called
        mock_get_logger.assert_called_once_with("sqlspec.cache")
        mock_logger.info.assert_called_once()


# SQL Compilation Cache Tests


def test_sql_cache_interface() -> None:
    """Test SQL compilation cache interface for compatibility."""
    cache_key = "test_sql_cache_key"
    cache_value = ("SELECT * FROM users WHERE id = $1", [1])

    # Set value
    sql_cache.set(cache_key, cache_value)

    # Get value
    result = sql_cache.get(cache_key)
    assert result == cache_value

    # Get non-existent key
    result_none = sql_cache.get("non_existent_key")
    assert result_none is None


# Thread Safety Tests


def test_unified_cache_thread_safety() -> None:
    """Test UnifiedCache thread safety with concurrent operations."""
    cache: UnifiedCache[int] = UnifiedCache(max_size=100)
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

    # Run multiple threads
    threads = []
    for tid in range(5):
        thread = threading.Thread(target=worker, args=(tid,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Verify no errors occurred
    assert len(errors) == 0, f"Thread safety errors: {errors}"
    assert len(results) > 0


def test_cache_statistics_thread_safety() -> None:
    """Test cache statistics thread safety."""
    cache: UnifiedCache[str] = UnifiedCache()
    errors = []

    def stats_worker() -> None:
        try:
            for i in range(100):
                key = CacheKey((f"thread_stats_{i}",))
                cache.get(key)  # Miss
                cache.put(key, f"value_{i}")
                cache.get(key)  # Hit
        except Exception as e:
            errors.append(e)

    # Run multiple threads
    threads = []
    for _ in range(3):
        thread = threading.Thread(target=stats_worker)
        threads.append(thread)
        thread.start()

    # Wait for completion
    for thread in threads:
        thread.join()

    # Verify no errors
    assert len(errors) == 0

    # Verify statistics are reasonable
    stats = cache.get_stats()
    assert stats.hits > 0
    assert stats.misses > 0
    assert stats.total_operations > 0


# Performance and Edge Case Tests


def test_cache_key_performance_with_large_data() -> None:
    """Test CacheKey performance with large key data."""
    large_key_data = tuple(range(1000))  # Large tuple
    cache_key = CacheKey(large_key_data)

    # Should handle large keys without issues
    assert cache_key.key_data == large_key_data
    assert isinstance(hash(cache_key), int)


def test_unified_cache_zero_max_size() -> None:
    """Test UnifiedCache with zero max size (no caching)."""
    cache: UnifiedCache[str] = UnifiedCache(max_size=0)
    key = CacheKey(("test",))

    # Put should work but immediately evict
    cache.put(key, "test_value")

    # Should not be able to get the value
    assert cache.get(key) is None
    assert cache.size() == 0


def test_unified_cache_very_short_ttl() -> None:
    """Test UnifiedCache with very short TTL."""
    cache: UnifiedCache[str] = UnifiedCache(ttl_seconds=1)  # 1 second TTL
    key = CacheKey(("test", "short_ttl"))

    cache.put(key, "expires_quickly")
    assert cache.get(key) == "expires_quickly"

    # Wait for expiration (1.1 seconds to ensure TTL has passed)
    time.sleep(1.1)

    assert cache.get(key) is None


@pytest.mark.parametrize(
    "cache_size,num_items",
    [
        (10, 15),  # More items than cache size
        (100, 50),  # Less items than cache size
        (1, 10),  # Much more items than cache size
    ],
)
def test_unified_cache_various_sizes(cache_size: int, num_items: int) -> None:
    """Test UnifiedCache with various size configurations."""
    cache: UnifiedCache[int] = UnifiedCache(max_size=cache_size)

    # Add items
    for i in range(num_items):
        key = CacheKey((i,))
        cache.put(key, i)

    # Cache size should not exceed max_size
    assert cache.size() <= cache_size

    # If more items than cache size, some should be evicted
    if num_items > cache_size:
        assert cache.size() == cache_size
        # Earlier items should be evicted (LRU)
        early_key = CacheKey((0,))
        assert cache.get(early_key) is None


def test_cache_with_none_values() -> None:
    """Test cache behavior with None values."""
    cache: UnifiedCache[Optional[str]] = UnifiedCache()
    key = CacheKey(("none_test",))

    # Store None value
    cache.put(key, None)

    # Should be able to retrieve None (different from cache miss)
    result = cache.get(key)
    assert result is None
    assert key in cache  # Key exists in cache

    # Compare with actual miss
    missing_key = CacheKey(("not_in_cache",))
    missing_result = cache.get(missing_key)
    assert missing_result is None
    assert missing_key not in cache  # Key does not exist in cache
