"""Caching system for SQL statement processing.

This module provides a caching system with LRU eviction and TTL support for
SQL statement processing and SQLGlot expression caching.

Components:
- CacheKey: Immutable cache key
- LRUCache: LRU + TTL cache implementation
- NamespacedCache: Namespace-aware cache wrapper for statement processing
"""

import logging
import threading
import time
from typing import TYPE_CHECKING, Any, Final

from mypy_extensions import mypyc_attr
from typing_extensions import TypeVar

from sqlspec.core.pipeline import (
    configure_statement_pipeline_cache,
    get_statement_pipeline_metrics,
    reset_statement_pipeline_cache,
)
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.type_guards import has_field_name, has_filter_attributes

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    import sqlglot.expressions as exp


__all__ = (
    "CacheKey",
    "CacheStats",
    "CachedStatement",
    "FiltersView",
    "LRUCache",
    "NamespacedCache",
    "canonicalize_filters",
    "create_cache_key",
    "get_cache",
    "get_cache_config",
    "get_cache_instances",
    "get_default_cache",
    "get_pipeline_metrics",
    "reset_pipeline_registry",
    "set_cache_instances",
)

logger = get_logger("sqlspec.cache")

T = TypeVar("T")
CacheValueT = TypeVar("CacheValueT")


DEFAULT_MAX_SIZE: Final = 10000
DEFAULT_TTL_SECONDS: Final = 3600
CACHE_STATS_UPDATE_INTERVAL: Final = 100


CACHE_KEY_SLOTS: Final = ("_hash", "_key_data")
CACHE_NODE_SLOTS: Final = ("key", "value", "prev", "next", "timestamp", "access_count")
LRU_CACHE_SLOTS: Final = ("_cache", "_lock", "_max_size", "_ttl", "_head", "_tail", "_stats")
CACHE_STATS_SLOTS: Final = ("hits", "misses", "evictions", "total_operations", "memory_usage")


@mypyc_attr(allow_interpreted_subclasses=False)
class CacheKey:
    """Immutable cache key.

    Args:
        key_data: Tuple of hashable values that uniquely identify the cached item
    """

    __slots__ = ("_hash", "_key_data")

    def __init__(self, key_data: "tuple[Any, ...]") -> None:
        """Initialize cache key.

        Args:
            key_data: Tuple of hashable values for the cache key
        """
        self._key_data = key_data
        self._hash = hash(key_data)

    @property
    def key_data(self) -> "tuple[Any, ...]":
        """Get the key data tuple."""
        return self._key_data

    def __hash__(self) -> int:
        """Return cached hash value."""
        return self._hash

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if type(other) is not CacheKey:
            return False
        other_key = other
        if self._hash != other_key._hash:
            return False
        return self._key_data == other_key._key_data

    def __repr__(self) -> str:
        """String representation of the cache key."""
        return f"CacheKey({self._key_data!r})"


@mypyc_attr(allow_interpreted_subclasses=False)
class CacheStats:
    """Cache statistics tracking.

    Tracks cache metrics including hit rates, evictions, and memory usage.
    """

    __slots__ = CACHE_STATS_SLOTS

    def __init__(self) -> None:
        """Initialize cache statistics."""
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.total_operations = 0
        self.memory_usage = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate as percentage."""
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0

    @property
    def miss_rate(self) -> float:
        """Calculate cache miss rate as percentage."""
        return 100.0 - self.hit_rate

    def record_hit(self) -> None:
        """Record a cache hit."""
        self.hits += 1
        self.total_operations += 1

    def record_miss(self) -> None:
        """Record a cache miss."""
        self.misses += 1
        self.total_operations += 1

    def record_eviction(self) -> None:
        """Record a cache eviction."""
        self.evictions += 1

    def reset(self) -> None:
        """Reset all statistics."""
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.total_operations = 0
        self.memory_usage = 0

    def __repr__(self) -> str:
        """String representation of cache statistics."""
        return (
            f"CacheStats(hit_rate={self.hit_rate:.1f}%, "
            f"hits={self.hits}, misses={self.misses}, "
            f"evictions={self.evictions}, ops={self.total_operations})"
        )


@mypyc_attr(allow_interpreted_subclasses=False)
class CacheNode:
    """Internal cache node for LRU linked list implementation."""

    __slots__ = CACHE_NODE_SLOTS

    def __init__(self, key: CacheKey, value: Any) -> None:
        """Initialize cache node.

        Args:
            key: Cache key for this node
            value: Cached value
        """
        self.key = key
        self.value = value
        self.prev: CacheNode | None = None
        self.next: CacheNode | None = None
        self.timestamp = time.time()
        self.access_count = 1


@mypyc_attr(allow_interpreted_subclasses=False)
class LRUCache:
    """Cache with LRU eviction and TTL support.

    Args:
        max_size: Maximum number of items to cache (LRU eviction when exceeded)
        ttl_seconds: Time-to-live in seconds (None for no expiration)
    """

    __slots__ = LRU_CACHE_SLOTS

    def __init__(self, max_size: int = DEFAULT_MAX_SIZE, ttl_seconds: int | None = DEFAULT_TTL_SECONDS) -> None:
        """Initialize LRU cache.

        Args:
            max_size: Maximum number of cache entries
            ttl_seconds: Time-to-live in seconds (None for no expiration)
        """
        self._cache: dict[CacheKey, CacheNode] = {}
        self._lock = threading.RLock()
        self._max_size = max_size
        self._ttl = ttl_seconds
        self._stats = CacheStats()

        self._head = CacheNode(CacheKey(()), None)
        self._tail = CacheNode(CacheKey(()), None)
        self._head.next = self._tail
        self._tail.prev = self._head

    def get(self, key: CacheKey) -> Any | None:
        """Get value from cache.

        Args:
            key: Cache key to lookup

        Returns:
            Cached value or None if not found or expired
        """
        with self._lock:
            node = self._cache.get(key)
            if node is None:
                self._stats.record_miss()
                if logger.isEnabledFor(logging.DEBUG):
                    log_with_context(logger, logging.DEBUG, "cache.miss", cache_size=len(self._cache))
                return None

            ttl = self._ttl
            if ttl is not None:
                current_time = time.time()
                if (current_time - node.timestamp) > ttl:
                    self._remove_node(node)
                    del self._cache[key]
                    self._stats.record_miss()
                    self._stats.record_eviction()
                    if logger.isEnabledFor(logging.DEBUG):
                        log_with_context(
                            logger, logging.DEBUG, "cache.evict", cache_size=len(self._cache), reason="expired"
                        )
                    return None

            self._move_to_head(node)
            node.access_count += 1
            self._stats.record_hit()
            if logger.isEnabledFor(logging.DEBUG):
                log_with_context(logger, logging.DEBUG, "cache.hit", cache_size=len(self._cache))
            return node.value

    def put(self, key: CacheKey, value: Any) -> None:
        """Put value in cache.

        Args:
            key: Cache key
            value: Value to cache
        """
        with self._lock:
            existing_node = self._cache.get(key)
            if existing_node is not None:
                existing_node.value = value
                existing_node.timestamp = time.time()
                existing_node.access_count += 1
                self._move_to_head(existing_node)
                return

            new_node = CacheNode(key, value)
            self._cache[key] = new_node
            self._add_to_head(new_node)

            if len(self._cache) > self._max_size:
                tail_node = self._tail.prev
                if tail_node is not None and tail_node is not self._head:
                    self._remove_node(tail_node)
                    del self._cache[tail_node.key]
                    self._stats.record_eviction()
                    if logger.isEnabledFor(logging.DEBUG):
                        log_with_context(
                            logger, logging.DEBUG, "cache.evict", cache_size=len(self._cache), reason="max_size"
                        )

    def delete(self, key: CacheKey) -> bool:
        """Delete entry from cache.

        Args:
            key: Cache key to delete

        Returns:
            True if key was found and deleted, False otherwise
        """
        with self._lock:
            node: CacheNode | None = self._cache.get(key)
            if node is None:
                return False

            self._remove_node(node)
            del self._cache[key]
            return True

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._head.next = self._tail
            self._tail.prev = self._head
            self._stats.reset()

    def size(self) -> int:
        """Get current cache size."""
        return len(self._cache)

    def is_empty(self) -> bool:
        """Check if cache is empty."""
        return not self._cache

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._stats

    def _add_to_head(self, node: CacheNode) -> None:
        """Add node to head of list."""
        node.prev = self._head
        head_next: CacheNode | None = self._head.next
        node.next = head_next
        if head_next is not None:
            head_next.prev = node
        self._head.next = node

    def _remove_node(self, node: CacheNode) -> None:
        """Remove node from linked list."""
        node_prev: CacheNode | None = node.prev
        node_next: CacheNode | None = node.next
        if node_prev is not None:
            node_prev.next = node_next
        if node_next is not None:
            node_next.prev = node_prev

    def _move_to_head(self, node: CacheNode) -> None:
        """Move node to head of list."""
        self._remove_node(node)
        self._add_to_head(node)

    def __len__(self) -> int:
        """Get current cache size."""
        return len(self._cache)

    def __contains__(self, key: CacheKey) -> bool:
        """Check if key exists in cache."""
        with self._lock:
            node = self._cache.get(key)
            if node is None:
                return False

            ttl = self._ttl
            return not (ttl is not None and time.time() - node.timestamp > ttl)


_default_cache: LRUCache | None = None
_cache_lock = threading.Lock()


def get_default_cache() -> LRUCache:
    """Get the default LRU cache instance.

    Returns:
        Singleton default cache instance
    """
    global _default_cache
    if _default_cache is None:
        with _cache_lock:
            if _default_cache is None:
                config = get_cache_config()
                _default_cache = LRUCache(config.sql_cache_size)
    return _default_cache


def get_cache_instances() -> "tuple[LRUCache | None, NamespacedCache | None]":
    """Return the current cache instances.

    Returns:
        Tuple of (default_cache, namespaced_cache).
    """
    return _default_cache, _namespaced_cache


def set_cache_instances(default_cache: "LRUCache | None", namespaced_cache: "NamespacedCache | None") -> None:
    """Replace cache instances (used by tests and diagnostics).

    Args:
        default_cache: Default cache instance or None.
        namespaced_cache: Namespaced cache instance or None.
    """
    global _default_cache, _namespaced_cache
    _default_cache = default_cache
    _namespaced_cache = namespaced_cache


def clear_all_caches() -> None:
    """Clear all cache instances."""
    if _default_cache is not None:
        _default_cache.clear()
    cache = get_cache()
    cache.clear()
    reset_statement_pipeline_cache()


def get_cache_statistics() -> "dict[str, CacheStats]":
    """Get statistics from all cache instances.

    Returns:
        Dictionary mapping cache type to statistics
    """
    stats: dict[str, CacheStats] = {}
    default_cache = get_default_cache()
    stats["default"] = default_cache.get_stats()
    cache = get_cache()
    stats["namespaced"] = cache.get_stats()
    return stats


_global_cache_config: "CacheConfig | None" = None


@mypyc_attr(allow_interpreted_subclasses=False)
class CacheConfig:
    """Global cache configuration for SQLSpec."""

    def __init__(
        self,
        *,
        compiled_cache_enabled: bool = True,
        sql_cache_enabled: bool = True,
        fragment_cache_enabled: bool = True,
        optimized_cache_enabled: bool = True,
        sql_cache_size: int = 1000,
        fragment_cache_size: int = 5000,
        optimized_cache_size: int = 2000,
    ) -> None:
        """Initialize cache configuration.

        Args:
            compiled_cache_enabled: Master switch for namespaced caches and compiled SQL caching.
            sql_cache_enabled: Enable statement and builder caching.
            fragment_cache_enabled: Enable expression, parameter, and file caching.
            optimized_cache_enabled: Enable optimized expression caching.
            sql_cache_size: Maximum statement/builder cache entries.
            fragment_cache_size: Maximum expression/parameter/file cache entries.
            optimized_cache_size: Maximum optimized cache entries.
        """
        self.compiled_cache_enabled = compiled_cache_enabled
        self.sql_cache_enabled = sql_cache_enabled
        self.fragment_cache_enabled = fragment_cache_enabled
        self.optimized_cache_enabled = optimized_cache_enabled
        self.sql_cache_size = sql_cache_size
        self.fragment_cache_size = fragment_cache_size
        self.optimized_cache_size = optimized_cache_size


def get_cache_config() -> CacheConfig:
    """Get the global cache configuration.

    Returns:
        Current global cache configuration instance
    """
    global _global_cache_config
    if _global_cache_config is None:
        _global_cache_config = CacheConfig()
        _configure_pipeline_cache(_global_cache_config)
    return _global_cache_config


def _configure_pipeline_cache(config: "CacheConfig") -> None:
    compiled_cache_enabled = config.compiled_cache_enabled and config.sql_cache_enabled
    fragment_cache_enabled = config.compiled_cache_enabled and config.fragment_cache_enabled
    cache_size = config.sql_cache_size if compiled_cache_enabled else 0
    parse_cache_size = config.fragment_cache_size if fragment_cache_enabled else 0
    configure_statement_pipeline_cache(
        cache_size=cache_size, parse_cache_size=parse_cache_size, cache_enabled=compiled_cache_enabled
    )


def update_cache_config(config: CacheConfig) -> None:
    """Update the global cache configuration.

    Clears all existing caches when configuration changes.

    Args:
        config: New cache configuration to apply globally
    """
    logger = get_logger("sqlspec.cache")
    log_with_context(
        logger,
        logging.DEBUG,
        "cache.config.updated",
        compiled_cache_enabled=config.compiled_cache_enabled,
        sql_cache_enabled=config.sql_cache_enabled,
        fragment_cache_enabled=config.fragment_cache_enabled,
        optimized_cache_enabled=config.optimized_cache_enabled,
        sql_cache_size=config.sql_cache_size,
        fragment_cache_size=config.fragment_cache_size,
        optimized_cache_size=config.optimized_cache_size,
    )

    global _default_cache, _global_cache_config, _namespaced_cache
    _global_cache_config = config

    _configure_pipeline_cache(config)

    if _default_cache is not None:
        _default_cache.clear()
    if _namespaced_cache is not None:
        _namespaced_cache.clear()
    _default_cache = None
    _namespaced_cache = None

    log_with_context(
        logger,
        logging.DEBUG,
        "cache.config.cleared",
        compiled_cache_enabled=config.compiled_cache_enabled,
        sql_cache_enabled=config.sql_cache_enabled,
        fragment_cache_enabled=config.fragment_cache_enabled,
        optimized_cache_enabled=config.optimized_cache_enabled,
    )


def get_cache_stats() -> "dict[str, CacheStats]":
    """Get cache statistics from all caches.

    Returns:
        Dictionary of cache statistics
    """
    return get_cache_statistics()


def reset_cache_stats() -> None:
    """Reset all cache statistics."""
    clear_all_caches()


def log_cache_stats() -> None:
    """Log cache statistics."""
    logger = get_logger("sqlspec.cache")
    stats = get_cache_stats()
    stats_summary = {
        name: {
            "hits": stat.hits,
            "misses": stat.misses,
            "evictions": stat.evictions,
            "total_operations": stat.total_operations,
            "memory_usage": stat.memory_usage,
        }
        for name, stat in stats.items()
    }
    log_with_context(logger, logging.DEBUG, "cache.stats", stats=stats_summary)


@mypyc_attr(allow_interpreted_subclasses=False)
class CachedStatement:
    """Immutable cached statement result.

    This class stores compiled SQL and parameters in an immutable format
    that can be safely shared between different parts of the system without
    risk of mutation. Tuple parameters ensure no copying is needed.
    """

    __slots__ = ("compiled_sql", "expression", "parameters")

    def __init__(
        self,
        compiled_sql: str,
        parameters: "tuple[Any, ...] | dict[str, Any] | None",
        expression: "exp.Expression | None",
    ) -> None:
        self.compiled_sql = compiled_sql
        self.parameters = parameters
        self.expression = expression

    def __repr__(self) -> str:
        return (
            "CachedStatement("
            f"compiled_sql={self.compiled_sql!r}, "
            f"parameters={self.parameters!r}, "
            f"expression={self.expression!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CachedStatement):
            return False
        return (
            self.compiled_sql == other.compiled_sql
            and self.parameters == other.parameters
            and self.expression == other.expression
        )

    def __hash__(self) -> int:
        return hash((self.compiled_sql, self.parameters, self.expression))


def create_cache_key(namespace: str, key: str, dialect: str | None = None) -> str:
    """Create optimized cache key using string concatenation.

    Args:
        namespace: Cache namespace name.
        key: Base cache key.
        dialect: SQL dialect (optional).

    Returns:
        Optimized cache key string.
    """
    return f"{namespace}:{dialect or 'default'}:{key}"


def _sql_cache_enabled(config: "CacheConfig") -> bool:
    return config.sql_cache_enabled


def _sql_cache_size(config: "CacheConfig") -> int:
    return config.sql_cache_size


def _fragment_cache_enabled(config: "CacheConfig") -> bool:
    return config.fragment_cache_enabled


def _fragment_cache_size(config: "CacheConfig") -> int:
    return config.fragment_cache_size


def _optimized_cache_enabled(config: "CacheConfig") -> bool:
    return config.optimized_cache_enabled


def _optimized_cache_size(config: "CacheConfig") -> int:
    return config.optimized_cache_size


NAMESPACED_CACHE_CONFIG: "dict[str, tuple[Callable[[CacheConfig], bool], Callable[[CacheConfig], int]]]" = {
    "statement": (_sql_cache_enabled, _sql_cache_size),
    "builder": (_sql_cache_enabled, _sql_cache_size),
    "expression": (_fragment_cache_enabled, _fragment_cache_size),
    "file": (_fragment_cache_enabled, _fragment_cache_size),
    "optimized": (_optimized_cache_enabled, _optimized_cache_size),
}


@mypyc_attr(allow_interpreted_subclasses=False)
class NamespacedCache:
    """Single cache with namespace isolation.

    Uses per-namespace LRU caches sized by CacheConfig to keep memory usage
    predictable while avoiding stringly-typed cache access.
    """

    __slots__ = ("_caches", "_config")

    def __init__(self, config: "CacheConfig | None" = None, ttl_seconds: int | None = DEFAULT_TTL_SECONDS) -> None:
        """Initialize namespaced cache.

        Args:
            config: Cache configuration to apply.
            ttl_seconds: Time-to-live in seconds (None for no expiration).
        """
        self._config = config or get_cache_config()
        self._caches = self._build_caches(self._config, ttl_seconds)

    @staticmethod
    def _build_caches(config: "CacheConfig", ttl_seconds: int | None) -> "dict[str, LRUCache]":
        caches: dict[str, LRUCache] = {}
        for namespace, (_, size_getter) in NAMESPACED_CACHE_CONFIG.items():
            size = size_getter(config)
            caches[namespace] = LRUCache(size, ttl_seconds)
        return caches

    def _is_enabled(self, namespace: str) -> bool:
        if not self._config.compiled_cache_enabled:
            return False
        enabled_getter = NAMESPACED_CACHE_CONFIG[namespace][0]
        return bool(enabled_getter(self._config))

    def _get(self, namespace: str, key: str, dialect: str | None = None) -> Any | None:
        """Get cached value by namespace.

        Args:
            namespace: Cache namespace.
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            Cached value or None if not found.
        """
        if not self._is_enabled(namespace):
            return None
        cache = self._caches[namespace]
        full_key = create_cache_key(namespace, key, dialect)
        cache_key = CacheKey((full_key,))
        return cache.get(cache_key)

    def _put(self, namespace: str, key: str, value: Any, dialect: str | None = None) -> None:
        """Put cached value by namespace.

        Args:
            namespace: Cache namespace.
            key: Cache key.
            value: Value to cache.
            dialect: Optional SQL dialect.
        """
        if not self._is_enabled(namespace):
            return
        cache = self._caches[namespace]
        full_key = create_cache_key(namespace, key, dialect)
        cache_key = CacheKey((full_key,))
        cache.put(cache_key, value)

    def _delete(self, namespace: str, key: str, dialect: str | None = None) -> bool:
        """Delete cached value by namespace.

        Args:
            namespace: Cache namespace.
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            True when the key was found and deleted.
        """
        if not self._is_enabled(namespace):
            return False
        cache = self._caches[namespace]
        full_key = create_cache_key(namespace, key, dialect)
        cache_key = CacheKey((full_key,))
        return cache.delete(cache_key)

    def get_statement(self, key: str, dialect: str | None = None) -> Any | None:
        """Get cached statement data.

        Args:
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            Cached value or None if not found.
        """
        return self._get("statement", key, dialect)

    def put_statement(self, key: str, value: Any, dialect: str | None = None) -> None:
        """Cache compiled statement data.

        Args:
            key: Cache key.
            value: Value to cache.
            dialect: Optional SQL dialect.
        """
        self._put("statement", key, value, dialect)

    def delete_statement(self, key: str, dialect: str | None = None) -> bool:
        """Delete cached statement data.

        Args:
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            True when the key was found and deleted.
        """
        return self._delete("statement", key, dialect)

    def get_expression(self, key: str, dialect: str | None = None) -> Any | None:
        """Get cached expression data.

        Args:
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            Cached value or None if not found.
        """
        return self._get("expression", key, dialect)

    def put_expression(self, key: str, value: Any, dialect: str | None = None) -> None:
        """Cache parsed expression data.

        Args:
            key: Cache key.
            value: Value to cache.
            dialect: Optional SQL dialect.
        """
        self._put("expression", key, value, dialect)

    def delete_expression(self, key: str, dialect: str | None = None) -> bool:
        """Delete cached expression data.

        Args:
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            True when the key was found and deleted.
        """
        return self._delete("expression", key, dialect)

    def get_optimized(self, key: str, dialect: str | None = None) -> Any | None:
        """Get cached optimized expression data.

        Args:
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            Cached value or None if not found.
        """
        return self._get("optimized", key, dialect)

    def put_optimized(self, key: str, value: Any, dialect: str | None = None) -> None:
        """Cache optimized expression data.

        Args:
            key: Cache key.
            value: Value to cache.
            dialect: Optional SQL dialect.
        """
        self._put("optimized", key, value, dialect)

    def delete_optimized(self, key: str, dialect: str | None = None) -> bool:
        """Delete cached optimized expression data.

        Args:
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            True when the key was found and deleted.
        """
        return self._delete("optimized", key, dialect)

    def get_builder(self, key: str, dialect: str | None = None) -> Any | None:
        """Get cached builder statement data.

        Args:
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            Cached value or None if not found.
        """
        return self._get("builder", key, dialect)

    def put_builder(self, key: str, value: Any, dialect: str | None = None) -> None:
        """Cache builder statement data.

        Args:
            key: Cache key.
            value: Value to cache.
            dialect: Optional SQL dialect.
        """
        self._put("builder", key, value, dialect)

    def delete_builder(self, key: str, dialect: str | None = None) -> bool:
        """Delete cached builder statement data.

        Args:
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            True when the key was found and deleted.
        """
        return self._delete("builder", key, dialect)

    def get_file(self, key: str, dialect: str | None = None) -> Any | None:
        """Get cached SQL file data.

        Args:
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            Cached value or None if not found.
        """
        return self._get("file", key, dialect)

    def put_file(self, key: str, value: Any, dialect: str | None = None) -> None:
        """Cache SQL file data.

        Args:
            key: Cache key.
            value: Value to cache.
            dialect: Optional SQL dialect.
        """
        self._put("file", key, value, dialect)

    def delete_file(self, key: str, dialect: str | None = None) -> bool:
        """Delete cached SQL file data.

        Args:
            key: Cache key.
            dialect: Optional SQL dialect.

        Returns:
            True when the key was found and deleted.
        """
        return self._delete("file", key, dialect)

    def clear(self) -> None:
        """Clear all cache entries."""
        for cache in self._caches.values():
            cache.clear()

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        aggregated = CacheStats()
        for cache in self._caches.values():
            stats = cache.get_stats()
            aggregated.hits += stats.hits
            aggregated.misses += stats.misses
            aggregated.evictions += stats.evictions
            aggregated.total_operations += stats.total_operations
            aggregated.memory_usage += stats.memory_usage
        return aggregated


_namespaced_cache: NamespacedCache | None = None


def get_cache() -> NamespacedCache:
    """Get the namespaced cache instance.

    Returns:
        Singleton namespaced cache instance
    """
    global _namespaced_cache
    if _namespaced_cache is None:
        with _cache_lock:
            if _namespaced_cache is None:
                _namespaced_cache = NamespacedCache(get_cache_config())
    return _namespaced_cache


@mypyc_attr(allow_interpreted_subclasses=False)
class Filter:
    """Immutable filter that can be safely shared."""

    __slots__ = ("field_name", "operation", "value")

    def __init__(self, field_name: str, operation: str, value: Any) -> None:
        if not field_name:
            msg = "Field name cannot be empty"
            raise ValueError(msg)
        if not operation:
            msg = "Operation cannot be empty"
            raise ValueError(msg)
        self.field_name = field_name
        self.operation = operation
        self.value = value

    def __repr__(self) -> str:
        return f"Filter(field_name={self.field_name!r}, operation={self.operation!r}, value={self.value!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Filter):
            return False
        return self.field_name == other.field_name and self.operation == other.operation and self.value == other.value

    def __hash__(self) -> int:
        return hash((self.field_name, self.operation, self.value))


def canonicalize_filters(filters: "list[Filter]") -> "tuple[Filter, ...]":
    """Create canonical representation of filters for cache keys.

    Args:
        filters: List of filters to canonicalize

    Returns:
        Tuple of unique filters sorted by field_name, operation, then value
    """
    if not filters:
        return ()

    # Deduplicate and sort for canonical representation
    unique_filters = set(filters)
    return tuple(sorted(unique_filters, key=_filter_sort_key))


def _filter_sort_key(filter_obj: "Filter") -> "tuple[str, str, str]":
    return filter_obj.field_name, filter_obj.operation, str(filter_obj.value)


@mypyc_attr(allow_interpreted_subclasses=False)
class FiltersView:
    """Read-only view of filters without copying.

    Provides zero-copy access to filters with methods for querying,
    iteration, and canonical representation generation.
    """

    __slots__ = ("_filters_ref",)

    def __init__(self, filters: "list[Any]") -> None:
        """Initialize filters view.

        Args:
            filters: List of filters (will be referenced, not copied)
        """
        self._filters_ref = filters

    def __len__(self) -> int:
        """Get number of filters."""
        return len(self._filters_ref)

    def __iter__(self) -> "Iterator[Any]":
        """Iterate over filters."""
        return iter(self._filters_ref)

    def get_by_field(self, field_name: str) -> "list[Any]":
        """Get all filters for a specific field.

        Args:
            field_name: Field name to filter by

        Returns:
            List of filters matching the field name
        """
        return [f for f in self._filters_ref if has_field_name(f) and f.field_name == field_name]

    def has_field(self, field_name: str) -> bool:
        """Check if any filter exists for a field.

        Args:
            field_name: Field name to check

        Returns:
            True if field has filters
        """
        return any(has_field_name(f) and f.field_name == field_name for f in self._filters_ref)

    def to_canonical(self) -> "tuple[Any, ...]":
        """Create canonical representation for cache keys.

        Returns:
            Canonical tuple representation of filters
        """
        # Convert to Filter objects if needed, then canonicalize
        filter_objects = []
        for f in self._filters_ref:
            if isinstance(f, Filter):
                filter_objects.append(f)
            elif has_filter_attributes(f):
                filter_objects.append(Filter(f.field_name, f.operation, f.value))

        return canonicalize_filters(filter_objects)


def get_pipeline_metrics() -> "list[dict[str, Any]]":
    """Return metrics for the shared statement pipeline cache when enabled."""

    return get_statement_pipeline_metrics()


def reset_pipeline_registry() -> None:
    """Clear shared statement pipeline caches and metrics."""

    reset_statement_pipeline_cache()
