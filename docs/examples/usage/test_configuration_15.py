"""Test configuration example: Global cache configuration."""


def test_global_cache_config() -> None:
    """Test global cache configuration."""
    from sqlspec.core.cache import CacheConfig, update_cache_config

    cache_config = CacheConfig(
        compiled_cache_enabled=True,  # Cache compiled SQL
        sql_cache_enabled=True,  # Cache SQL strings
        fragment_cache_enabled=True,  # Cache SQL fragments
        optimized_cache_enabled=True,  # Cache optimized AST
        sql_cache_size=1000,  # Maximum cached SQL items
    )

    # Update global cache configuration
    update_cache_config(cache_config)

    # Verify config applied
    assert cache_config.sql_cache_enabled is True
    assert cache_config.sql_cache_size == 1000
