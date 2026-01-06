"""Test configuration example: Global cache configuration."""

__all__ = ("test_global_cache_config",)


def test_global_cache_config() -> None:
    """Test global cache configuration."""
    # start-example
    from sqlspec.core.cache import CacheConfig, update_cache_config

    cache_config = CacheConfig(
        compiled_cache_enabled=True,  # Cache compiled SQL
        sql_cache_enabled=True,  # Cache statements/builders
        fragment_cache_enabled=True,  # Cache expressions/files
        optimized_cache_enabled=True,  # Cache optimized AST
        sql_cache_size=1000,  # Maximum cached SQL items
    )

    # Update global cache configuration
    update_cache_config(cache_config)

    # Verify config applied
    # end-example
    assert cache_config.sql_cache_enabled is True
    assert cache_config.sql_cache_size == 1000
