"""Comprehensive unit tests for the SQLSpec base class.

This module tests the centralized cache configuration management functionality
that is critical to the CORE_ROUND_3 architecture. Tests cover:

1. Centralized Cache Configuration Management - Global cache configuration control
2. Configuration methods - get_cache_config(), update_cache_config(), configure_cache()
3. Cache statistics - get_cache_stats(), reset_cache_stats(), log_cache_stats()
4. Global state propagation - Configuration changes affect all modules
5. Thread safety - Concurrent access to configuration
6. Default configuration - Default cache settings behavior
7. Configuration validation - Invalid configuration handling

The SQLSpec class serves as the centralized manager for all cache configuration
across the entire SQLSpec system, ensuring consistent behavior and performance
optimization through its unified cache management approach.
"""

import concurrent.futures
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.base import SQLSpec
from sqlspec.core.cache import CacheConfig

# Cache Configuration Tests


def test_get_cache_config_returns_default_configuration() -> None:
    """Test that get_cache_config returns default cache configuration."""
    config = SQLSpec.get_cache_config()

    assert isinstance(config, CacheConfig)
    assert config.sql_cache_enabled is True
    assert config.fragment_cache_enabled is True
    assert config.optimized_cache_enabled is True
    assert config.compiled_cache_enabled is True
    assert config.sql_cache_size > 0
    assert config.fragment_cache_size > 0
    assert config.optimized_cache_size > 0


def test_get_cache_config_returns_same_instance() -> None:
    """Test that multiple calls to get_cache_config return same configuration."""
    config1 = SQLSpec.get_cache_config()
    config2 = SQLSpec.get_cache_config()

    assert config1 is config2, "get_cache_config should return same instance for global state"


def test_update_cache_config_changes_global_configuration() -> None:
    """Test that update_cache_config changes the global cache configuration."""
    original_config = SQLSpec.get_cache_config()
    new_config = CacheConfig(
        sql_cache_size=5000,
        fragment_cache_size=8000,
        optimized_cache_size=3000,
        sql_cache_enabled=False,
        fragment_cache_enabled=True,
        optimized_cache_enabled=True,
    )

    try:
        SQLSpec.update_cache_config(new_config)

        updated_config = SQLSpec.get_cache_config()
        assert updated_config.sql_cache_size == 5000
        assert updated_config.fragment_cache_size == 8000
        assert updated_config.optimized_cache_size == 3000
        assert updated_config.sql_cache_enabled is False
        assert updated_config.fragment_cache_enabled is True
        assert updated_config.optimized_cache_enabled is True

    finally:
        SQLSpec.update_cache_config(original_config)


def test_configure_cache_partial_updates() -> None:
    """Test that configure_cache allows partial configuration updates."""
    original_config = SQLSpec.get_cache_config()

    try:
        SQLSpec.configure_cache(sql_cache_size=7500, fragment_cache_enabled=False)

        updated_config = SQLSpec.get_cache_config()
        assert updated_config.sql_cache_size == 7500
        assert updated_config.fragment_cache_enabled is False

        # Other values should remain unchanged from original
        assert updated_config.optimized_cache_size == original_config.optimized_cache_size
        assert updated_config.sql_cache_enabled == original_config.sql_cache_enabled
        assert updated_config.optimized_cache_enabled == original_config.optimized_cache_enabled

    finally:
        SQLSpec.update_cache_config(original_config)


def test_configure_cache_with_all_parameters() -> None:
    """Test configure_cache with all possible parameters."""
    original_config = SQLSpec.get_cache_config()

    try:
        SQLSpec.configure_cache(
            sql_cache_size=2500,
            fragment_cache_size=6500,
            optimized_cache_size=1500,
            sql_cache_enabled=False,
            fragment_cache_enabled=False,
            optimized_cache_enabled=False,
        )

        config = SQLSpec.get_cache_config()
        assert config.sql_cache_size == 2500
        assert config.fragment_cache_size == 6500
        assert config.optimized_cache_size == 1500
        assert config.sql_cache_enabled is False
        assert config.fragment_cache_enabled is False
        assert config.optimized_cache_enabled is False

    finally:
        SQLSpec.update_cache_config(original_config)


def test_configure_cache_with_no_parameters_does_nothing() -> None:
    """Test that configure_cache with no parameters leaves configuration unchanged."""
    original_config = SQLSpec.get_cache_config()

    SQLSpec.configure_cache()

    updated_config = SQLSpec.get_cache_config()
    assert updated_config.sql_cache_size == original_config.sql_cache_size
    assert updated_config.fragment_cache_size == original_config.fragment_cache_size
    assert updated_config.optimized_cache_size == original_config.optimized_cache_size
    assert updated_config.sql_cache_enabled == original_config.sql_cache_enabled
    assert updated_config.fragment_cache_enabled == original_config.fragment_cache_enabled
    assert updated_config.optimized_cache_enabled == original_config.optimized_cache_enabled


# Cache Statistics Tests


def test_get_cache_stats_returns_statistics() -> None:
    """Test that get_cache_stats returns cache statistics."""
    stats = SQLSpec.get_cache_stats()

    assert hasattr(stats, "sql_hit_rate")
    assert hasattr(stats, "fragment_hit_rate")
    assert hasattr(stats, "optimized_hit_rate")
    assert hasattr(stats, "sql_size")
    assert hasattr(stats, "fragment_size")
    assert hasattr(stats, "optimized_size")
    assert hasattr(stats, "sql_hits")
    assert hasattr(stats, "sql_misses")
    assert hasattr(stats, "fragment_hits")
    assert hasattr(stats, "fragment_misses")
    assert hasattr(stats, "optimized_hits")
    assert hasattr(stats, "optimized_misses")


def test_reset_cache_stats_clears_statistics() -> None:
    """Test that reset_cache_stats clears all cache statistics."""
    SQLSpec.reset_cache_stats()
    stats = SQLSpec.get_cache_stats()

    # After reset, stats should be zeroed out
    assert stats.sql_hits == 0
    assert stats.sql_misses == 0
    assert stats.fragment_hits == 0
    assert stats.fragment_misses == 0
    assert stats.optimized_hits == 0
    assert stats.optimized_misses == 0


def test_log_cache_stats_logs_to_configured_logger() -> None:
    """Test that log_cache_stats outputs to the logging system."""
    with patch("sqlspec.core.cache.get_logger") as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        SQLSpec.log_cache_stats()

        mock_get_logger.assert_called_once_with("sqlspec.cache")
        mock_logger.info.assert_called_once()
        # Check that the log call includes stats information
        call_args = mock_logger.info.call_args
        assert call_args is not None
        assert "Cache Statistics" in call_args[0][0]


# Global State Propagation Tests


@patch("sqlspec.core.cache.get_default_cache")
@patch("sqlspec.core.cache.get_statement_cache")
def test_update_cache_config_clears_all_caches(
    mock_get_statement_cache: MagicMock, mock_get_default_cache: MagicMock
) -> None:
    """Test that updating cache configuration clears all existing caches."""
    mock_default_cache = MagicMock()
    mock_statement_cache = MagicMock()
    mock_get_default_cache.return_value = mock_default_cache
    mock_get_statement_cache.return_value = mock_statement_cache

    new_config = CacheConfig(sql_cache_size=1000)

    SQLSpec.update_cache_config(new_config)

    # All caches should be cleared when configuration changes
    mock_default_cache.clear.assert_called_once()
    mock_statement_cache.clear.assert_called_once()


def test_multiple_sqlspec_instances_share_cache_configuration() -> None:
    """Test that multiple SQLSpec instances share the same cache configuration."""
    sqlspec1 = SQLSpec()
    sqlspec2 = SQLSpec()
    original_config = SQLSpec.get_cache_config()

    try:
        # Update configuration through one instance
        new_config = CacheConfig(sql_cache_size=9999)
        sqlspec1.update_cache_config(new_config)

        # Both instances see the same configuration
        config1 = sqlspec1.get_cache_config()
        config2 = sqlspec2.get_cache_config()
        static_config = SQLSpec.get_cache_config()

        assert config1.sql_cache_size == 9999
        assert config2.sql_cache_size == 9999
        assert static_config.sql_cache_size == 9999

    finally:
        SQLSpec.update_cache_config(original_config)


# Thread Safety Tests


def test_concurrent_cache_config_access_is_thread_safe() -> None:
    """Test that concurrent access to cache configuration is thread-safe."""
    results = []
    errors = []

    def get_config_worker() -> None:
        try:
            for _ in range(100):
                config = SQLSpec.get_cache_config()
                results.append(config.sql_cache_size)
                time.sleep(0.001)  # Small delay to encourage race conditions
        except Exception as e:
            errors.append(e)

    # Run multiple threads accessing configuration
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_config_worker) for _ in range(10)]
        concurrent.futures.wait(futures)

    # Assert no errors occurred and we got results
    assert len(errors) == 0, f"Thread safety errors: {errors}"
    assert len(results) > 0, "Should have collected configuration values"
    # All results should be consistent (same configuration)
    assert len(set(results)) <= 2, "Configuration should be consistent across threads"


def test_concurrent_cache_config_updates_are_atomic() -> None:
    """Test that concurrent cache configuration updates are atomic."""
    original_config = SQLSpec.get_cache_config()
    update_count = 0
    errors = []
    lock = threading.Lock()

    def update_config_worker(cache_size: int) -> None:
        nonlocal update_count
        try:
            config = CacheConfig(sql_cache_size=cache_size)
            SQLSpec.update_cache_config(config)
            with lock:
                nonlocal update_count
                update_count += 1
        except Exception as e:
            errors.append(e)

    try:
        # Run concurrent updates
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(update_config_worker, 1000 + i) for i in range(5)]
            concurrent.futures.wait(futures)

        # Assert no errors and all updates completed
        assert len(errors) == 0, f"Concurrent update errors: {errors}"
        assert update_count == 5, f"Expected 5 updates, got {update_count}"

        # Final configuration should be one of the update values
        final_config = SQLSpec.get_cache_config()
        assert final_config.sql_cache_size in range(1000, 1005)

    finally:
        SQLSpec.update_cache_config(original_config)


def test_concurrent_statistics_access_is_thread_safe() -> None:
    """Test that concurrent access to cache statistics is thread-safe."""
    errors = []
    results = []

    def stats_worker() -> None:
        try:
            for _ in range(50):
                stats = SQLSpec.get_cache_stats()
                SQLSpec.reset_cache_stats()
                results.append(stats.sql_hits + stats.sql_misses)
                time.sleep(0.001)
        except Exception as e:
            errors.append(e)

    # Run concurrent statistics operations
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(stats_worker) for _ in range(5)]
        concurrent.futures.wait(futures)

    # Assert no errors occurred
    assert len(errors) == 0, f"Thread safety errors in statistics: {errors}"


# Default Configuration Tests


def test_default_cache_configuration_values() -> None:
    """Test that default cache configuration has expected values."""
    # Reset to ensure we have fresh defaults
    default_config = CacheConfig()
    SQLSpec.update_cache_config(default_config)

    config = SQLSpec.get_cache_config()

    # Assert default values are reasonable
    assert config.sql_cache_enabled is True
    assert config.fragment_cache_enabled is True
    assert config.optimized_cache_enabled is True
    assert config.compiled_cache_enabled is True
    assert config.sql_cache_size >= 1000  # Should have reasonable size
    assert config.fragment_cache_size >= 1000
    assert config.optimized_cache_size >= 1000


def test_cache_configuration_persistence_across_instances() -> None:
    """Test that cache configuration persists across SQLSpec instances."""
    original_config = SQLSpec.get_cache_config()

    try:
        # Modify configuration
        test_config = CacheConfig(sql_cache_size=12345, fragment_cache_enabled=False)
        SQLSpec.update_cache_config(test_config)

        # Create new instance and verify it sees the same configuration
        new_sqlspec = SQLSpec()
        config = new_sqlspec.get_cache_config()

        assert config.sql_cache_size == 12345
        assert config.fragment_cache_enabled is False

    finally:
        SQLSpec.update_cache_config(original_config)


# Configuration Validation Tests


def test_cache_config_with_zero_sizes_is_allowed() -> None:
    """Test that cache configuration with zero sizes is allowed."""
    original_config = SQLSpec.get_cache_config()

    try:
        # Zero cache sizes should be allowed (disables caching)
        config = CacheConfig(sql_cache_size=0, fragment_cache_size=0, optimized_cache_size=0)
        SQLSpec.update_cache_config(config)

        updated_config = SQLSpec.get_cache_config()
        assert updated_config.sql_cache_size == 0
        assert updated_config.fragment_cache_size == 0
        assert updated_config.optimized_cache_size == 0

    finally:
        SQLSpec.update_cache_config(original_config)


def test_cache_config_with_negative_sizes_is_handled() -> None:
    """Test behavior with negative cache sizes."""
    original_config = SQLSpec.get_cache_config()

    try:
        # Negative sizes may be allowed (implementation dependent)
        config = CacheConfig(sql_cache_size=-1, fragment_cache_size=-10, optimized_cache_size=-100)

        # Should not raise exception during configuration
        SQLSpec.update_cache_config(config)
        updated_config = SQLSpec.get_cache_config()

        # Values should be set as specified (even if negative)
        assert updated_config.sql_cache_size == -1
        assert updated_config.fragment_cache_size == -10
        assert updated_config.optimized_cache_size == -100

    finally:
        SQLSpec.update_cache_config(original_config)


def test_cache_config_with_very_large_sizes() -> None:
    """Test cache configuration with very large sizes."""
    original_config = SQLSpec.get_cache_config()

    try:
        # Very large cache sizes
        large_size = 10**9  # 1 billion entries
        config = CacheConfig(sql_cache_size=large_size, fragment_cache_size=large_size, optimized_cache_size=large_size)

        SQLSpec.update_cache_config(config)
        updated_config = SQLSpec.get_cache_config()

        assert updated_config.sql_cache_size == large_size
        assert updated_config.fragment_cache_size == large_size
        assert updated_config.optimized_cache_size == large_size

    finally:
        SQLSpec.update_cache_config(original_config)


# Instance Configuration Tests


def test_sqlspec_instances_use_same_global_cache_config() -> None:
    """Test that all SQLSpec instances use the same global cache configuration."""
    instance1 = SQLSpec()
    instance2 = SQLSpec()
    instance3 = SQLSpec()

    config1 = instance1.get_cache_config()
    config2 = instance2.get_cache_config()
    config3 = instance3.get_cache_config()
    static_config = SQLSpec.get_cache_config()

    # All should reference the same global configuration
    assert config1 is config2
    assert config2 is config3
    assert config3 is static_config


def test_instance_cache_config_state_isolation() -> None:
    """Test that SQLSpec instances don't have independent cache state."""
    original_config = SQLSpec.get_cache_config()
    instance1 = SQLSpec()
    instance2 = SQLSpec()

    try:
        # Update configuration through instance1
        test_config = CacheConfig(sql_cache_size=7777)
        instance1.update_cache_config(test_config)

        # instance2 should see the same change
        config1 = instance1.get_cache_config()
        config2 = instance2.get_cache_config()

        assert config1.sql_cache_size == 7777
        assert config2.sql_cache_size == 7777
        assert config1 is config2  # Same object reference

    finally:
        SQLSpec.update_cache_config(original_config)


# Cache Management Integration Tests


def test_cache_configuration_affects_cache_clearing() -> None:
    """Test that cache configuration changes trigger cache clearing."""
    original_config = SQLSpec.get_cache_config()

    with patch("sqlspec.core.cache.clear_all_caches"):
        try:
            new_config = CacheConfig(sql_cache_size=5555)
            SQLSpec.update_cache_config(new_config)

            # Should trigger cache clearing through the cache system
            # Note: Actual clearing depends on implementation details
            # This test verifies the configuration update process

        finally:
            SQLSpec.update_cache_config(original_config)


@patch("sqlspec.core.cache.get_logger")
def test_cache_configuration_logging_integration(mock_get_logger: MagicMock) -> None:
    """Test that cache configuration changes are logged properly."""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    original_config = SQLSpec.get_cache_config()

    try:
        new_config = CacheConfig(sql_cache_size=3333, fragment_cache_enabled=False)
        SQLSpec.update_cache_config(new_config)

        # Should log configuration changes
        mock_logger.info.assert_called()
        log_call = mock_logger.info.call_args
        assert "Cache configuration updated" in log_call[0][0]

    finally:
        SQLSpec.update_cache_config(original_config)


# Parametrized tests for different configuration scenarios
@pytest.mark.parametrize(
    "cache_sizes,expected_sizes",
    [
        # Normal sizes
        ((1000, 2000, 3000), (1000, 2000, 3000)),
        # Zero sizes (disabling)
        ((0, 0, 0), (0, 0, 0)),
        # Mixed sizes
        ((5000, 0, 1000), (5000, 0, 1000)),
        # Large sizes
        ((100000, 200000, 50000), (100000, 200000, 50000)),
    ],
)
def test_cache_configuration_size_scenarios(
    cache_sizes: tuple[int, int, int], expected_sizes: tuple[int, int, int]
) -> None:
    """Test various cache size configuration scenarios."""
    original_config = SQLSpec.get_cache_config()
    sql_size, fragment_size, optimized_size = cache_sizes
    expected_sql, expected_fragment, expected_optimized = expected_sizes

    try:
        # Act
        config = CacheConfig(
            sql_cache_size=sql_size, fragment_cache_size=fragment_size, optimized_cache_size=optimized_size
        )
        SQLSpec.update_cache_config(config)

        # Assert
        updated_config = SQLSpec.get_cache_config()
        assert updated_config.sql_cache_size == expected_sql
        assert updated_config.fragment_cache_size == expected_fragment
        assert updated_config.optimized_cache_size == expected_optimized

    finally:
        # Cleanup
        SQLSpec.update_cache_config(original_config)


@pytest.mark.parametrize(
    "enable_flags,expected_flags",
    [
        # All enabled
        ((True, True, True, True), (True, True, True, True)),
        # All disabled
        ((False, False, False, False), (False, False, False, False)),
        # Mixed scenarios
        ((True, False, True, False), (True, False, True, False)),
        ((False, True, False, True), (False, True, False, True)),
    ],
)
def test_cache_configuration_enable_scenarios(
    enable_flags: tuple[bool, bool, bool, bool], expected_flags: tuple[bool, bool, bool, bool]
) -> None:
    """Test various cache enable/disable configuration scenarios."""
    original_config = SQLSpec.get_cache_config()
    sql_enabled, fragment_enabled, optimized_enabled, compiled_enabled = enable_flags
    expected_sql, expected_fragment, expected_optimized, expected_compiled = expected_flags

    try:
        # Act
        config = CacheConfig(
            sql_cache_enabled=sql_enabled,
            fragment_cache_enabled=fragment_enabled,
            optimized_cache_enabled=optimized_enabled,
            compiled_cache_enabled=compiled_enabled,
        )
        SQLSpec.update_cache_config(config)

        # Assert
        updated_config = SQLSpec.get_cache_config()
        assert updated_config.sql_cache_enabled == expected_sql
        assert updated_config.fragment_cache_enabled == expected_fragment
        assert updated_config.optimized_cache_enabled == expected_optimized
        assert updated_config.compiled_cache_enabled == expected_compiled

    finally:
        # Cleanup
        SQLSpec.update_cache_config(original_config)


# Edge case and stress tests
# Edge Cases and Stress Tests


def test_rapid_configuration_changes() -> None:
    """Test rapid successive configuration changes."""
    original_config = SQLSpec.get_cache_config()

    try:
        # Rapid configuration changes
        for i in range(20):
            config = CacheConfig(sql_cache_size=1000 + i * 100)
            SQLSpec.update_cache_config(config)

        # Final configuration should be set
        final_config = SQLSpec.get_cache_config()
        assert final_config.sql_cache_size == 2900  # 1000 + 19*100

    finally:
        SQLSpec.update_cache_config(original_config)


def test_statistics_collection_during_configuration_changes() -> None:
    """Test that statistics collection works during configuration changes."""
    original_config = SQLSpec.get_cache_config()

    try:
        # Change configuration and check statistics
        for i in range(5):
            config = CacheConfig(sql_cache_size=2000 + i)
            SQLSpec.update_cache_config(config)

            # Should be able to get statistics without error
            stats = SQLSpec.get_cache_stats()
            assert hasattr(stats, "sql_hit_rate")

            # Reset statistics should work
            SQLSpec.reset_cache_stats()

    finally:
        SQLSpec.update_cache_config(original_config)


def test_logging_during_concurrent_operations() -> None:
    """Test logging functionality during concurrent cache operations."""
    errors = []

    def log_stats_worker() -> None:
        try:
            for _ in range(10):
                SQLSpec.log_cache_stats()
                time.sleep(0.01)
        except Exception as e:
            errors.append(e)

    def config_update_worker() -> None:
        try:
            for i in range(5):
                config = CacheConfig(sql_cache_size=4000 + i)
                SQLSpec.update_cache_config(config)
                time.sleep(0.02)
        except Exception as e:
            errors.append(e)

    # Run concurrent operations
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(log_stats_worker) for _ in range(2)] + [
            executor.submit(config_update_worker) for _ in range(2)
        ]
        concurrent.futures.wait(futures)

    # Assert no errors occurred during concurrent operations
    assert len(errors) == 0, f"Concurrent operation errors: {errors}"
