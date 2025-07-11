"""Cache configuration and monitoring for SQLSpec.

This module provides configuration options and monitoring utilities
for the various caching layers in SQLSpec.
"""

from dataclasses import dataclass
from typing import Any, Optional

__all__ = ("CacheConfig", "CacheStats", "get_cache_stats", "reset_cache_stats")


@dataclass
class CacheConfig:
    """Configuration for SQLSpec caching layers."""

    # SQL statement cache settings
    sql_cache_size: int = 1000
    sql_cache_enabled: bool = True

    # AST fragment cache settings
    fragment_cache_size: int = 5000
    fragment_cache_enabled: bool = True

    # Optimized expression cache settings
    optimized_cache_size: int = 500
    optimized_cache_enabled: bool = True

    # Cache monitoring
    enable_stats: bool = True
    stats_interval: int = 3600  # Log stats every hour

    # Cache eviction policies
    eviction_policy: str = "lru"  # Options: "lru", "lfu", "fifo"

    # Memory limits (in MB)
    max_memory_usage: Optional[int] = None

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.sql_cache_size < 0:
            msg = "sql_cache_size must be non-negative"
            raise ValueError(msg)
        if self.fragment_cache_size < 0:
            msg = "fragment_cache_size must be non-negative"
            raise ValueError(msg)
        if self.optimized_cache_size < 0:
            msg = "optimized_cache_size must be non-negative"
            raise ValueError(msg)
        if self.eviction_policy not in {"lru", "lfu", "fifo"}:
            msg = f"Invalid eviction_policy: {self.eviction_policy}"
            raise ValueError(msg)


@dataclass
class CacheStats:
    """Statistics for cache performance monitoring."""

    # SQL cache stats
    sql_hits: int = 0
    sql_misses: int = 0
    sql_evictions: int = 0
    sql_size: int = 0

    # Fragment cache stats
    fragment_hits: int = 0
    fragment_misses: int = 0
    fragment_evictions: int = 0
    fragment_size: int = 0

    # Optimized cache stats
    optimized_hits: int = 0
    optimized_misses: int = 0
    optimized_evictions: int = 0
    optimized_size: int = 0

    # Timing stats (in seconds)
    avg_cache_lookup_time: float = 0.0
    avg_parse_time: float = 0.0
    avg_optimize_time: float = 0.0

    @property
    def sql_hit_rate(self) -> float:
        """Calculate SQL cache hit rate."""
        total = self.sql_hits + self.sql_misses
        return self.sql_hits / total if total > 0 else 0.0

    @property
    def fragment_hit_rate(self) -> float:
        """Calculate fragment cache hit rate."""
        total = self.fragment_hits + self.fragment_misses
        return self.fragment_hits / total if total > 0 else 0.0

    @property
    def optimized_hit_rate(self) -> float:
        """Calculate optimized expression cache hit rate."""
        total = self.optimized_hits + self.optimized_misses
        return self.optimized_hits / total if total > 0 else 0.0

    @property
    def overall_hit_rate(self) -> float:
        """Calculate overall cache hit rate across all caches."""
        total_hits = self.sql_hits + self.fragment_hits + self.optimized_hits
        total_accesses = (
            self.sql_hits
            + self.sql_misses
            + self.fragment_hits
            + self.fragment_misses
            + self.optimized_hits
            + self.optimized_misses
        )
        return total_hits / total_accesses if total_accesses > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert stats to dictionary for logging/monitoring."""
        return {
            "sql_cache": {
                "hits": self.sql_hits,
                "misses": self.sql_misses,
                "hit_rate": self.sql_hit_rate,
                "evictions": self.sql_evictions,
                "size": self.sql_size,
            },
            "fragment_cache": {
                "hits": self.fragment_hits,
                "misses": self.fragment_misses,
                "hit_rate": self.fragment_hit_rate,
                "evictions": self.fragment_evictions,
                "size": self.fragment_size,
            },
            "optimized_cache": {
                "hits": self.optimized_hits,
                "misses": self.optimized_misses,
                "hit_rate": self.optimized_hit_rate,
                "evictions": self.optimized_evictions,
                "size": self.optimized_size,
            },
            "performance": {
                "avg_cache_lookup_time_ms": self.avg_cache_lookup_time * 1000,
                "avg_parse_time_ms": self.avg_parse_time * 1000,
                "avg_optimize_time_ms": self.avg_optimize_time * 1000,
            },
            "overall": {
                "hit_rate": self.overall_hit_rate,
                "total_size": self.sql_size + self.fragment_size + self.optimized_size,
            },
        }


# Global cache configuration
_cache_config = CacheConfig()

# Global cache statistics
_cache_stats = CacheStats()


def get_cache_config() -> CacheConfig:
    """Get the current cache configuration."""
    return _cache_config


def update_cache_config(config: CacheConfig) -> None:
    """Update the cache configuration.

    Note: This will clear all existing caches.
    """
    global _cache_config
    _cache_config = config

    # Apply new configuration to caches
    from sqlspec.statement.cache import ast_fragment_cache, expression_cache, sql_cache

    if config.sql_cache_enabled:
        sql_cache.max_size = config.sql_cache_size
    else:
        sql_cache.clear()

    if config.fragment_cache_enabled:
        ast_fragment_cache.max_size = config.fragment_cache_size
    else:
        ast_fragment_cache.clear()

    if config.optimized_cache_enabled:
        expression_cache.max_size = config.optimized_cache_size
    else:
        expression_cache.clear()


def get_cache_stats() -> CacheStats:
    """Get current cache statistics."""
    from sqlspec.statement.cache import ast_fragment_cache, expression_cache, sql_cache

    # Update sizes
    _cache_stats.sql_size = sql_cache.size
    _cache_stats.fragment_size = ast_fragment_cache.size
    _cache_stats.optimized_size = expression_cache.size

    # Update fragment cache stats
    if hasattr(ast_fragment_cache, "_hit_count"):
        _cache_stats.fragment_hits = ast_fragment_cache._hit_count
        _cache_stats.fragment_misses = ast_fragment_cache._miss_count

    return _cache_stats


def reset_cache_stats() -> None:
    """Reset all cache statistics."""
    global _cache_stats
    _cache_stats = CacheStats()

    # Reset fragment cache stats
    from sqlspec.statement.cache import ast_fragment_cache

    if hasattr(ast_fragment_cache, "_hit_count"):
        ast_fragment_cache._hit_count = 0
        ast_fragment_cache._miss_count = 0


def log_cache_stats() -> None:
    """Log current cache statistics."""
    from sqlspec.utils.logging import get_logger

    logger = get_logger("sqlspec.cache")
    stats = get_cache_stats()

    logger.info("Cache Statistics", extra=stats.to_dict())
