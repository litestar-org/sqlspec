"""Caching system for SQL statement processing.

This module provides a caching system with LRU eviction and TTL support for
SQL statement processing, parameter processing, and expression caching.

Components:
- CacheKey: Immutable cache key
- UnifiedCache: Cache implementation with LRU eviction and TTL
- StatementCache: Cache for compiled SQL statements
- ExpressionCache: Cache for parsed expressions
- ParameterCache: Cache for processed parameters
"""

import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Optional

from mypy_extensions import mypyc_attr
from typing_extensions import TypeVar

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterator

    import sqlglot.expressions as exp


__all__ = (
    "CacheKey",
    "CacheStats",
    "CachedStatement",
    "FiltersView",
    "MultiLevelCache",
    "ParametersView",
    "UnifiedCache",
    "canonicalize_filters",
    "create_cache_key",
    "get_cache",
    "get_cache_config",
    "get_default_cache",
)

T = TypeVar("T")
CacheValueT = TypeVar("CacheValueT")


DEFAULT_MAX_SIZE: Final = 10000
DEFAULT_TTL_SECONDS: Final = 3600
CACHE_STATS_UPDATE_INTERVAL: Final = 100


CACHE_KEY_SLOTS: Final = ("_hash", "_key_data")
CACHE_NODE_SLOTS: Final = ("key", "value", "prev", "next", "timestamp", "access_count")
UNIFIED_CACHE_SLOTS: Final = ("_cache", "_lock", "_max_size", "_ttl", "_head", "_tail", "_stats")
CACHE_STATS_SLOTS: Final = ("hits", "misses", "evictions", "total_operations", "memory_usage")


@mypyc_attr(allow_interpreted_subclasses=False)
class CacheKey:
    """Immutable cache key.

    Args:
        key_data: Tuple of hashable values that uniquely identify the cached item
    """

    __slots__ = ("_hash", "_key_data")

    def __init__(self, key_data: tuple[Any, ...]) -> None:
        """Initialize cache key.

        Args:
            key_data: Tuple of hashable values for the cache key
        """
        self._key_data = key_data
        self._hash = hash(key_data)

    @property
    def key_data(self) -> tuple[Any, ...]:
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
        self.prev: Optional[CacheNode] = None
        self.next: Optional[CacheNode] = None
        self.timestamp = time.time()
        self.access_count = 1


@mypyc_attr(allow_interpreted_subclasses=False)
class UnifiedCache:
    """Cache with LRU eviction and TTL support.

    Args:
        max_size: Maximum number of items to cache (LRU eviction when exceeded)
        ttl_seconds: Time-to-live in seconds (None for no expiration)
    """

    __slots__ = UNIFIED_CACHE_SLOTS

    def __init__(self, max_size: int = DEFAULT_MAX_SIZE, ttl_seconds: Optional[int] = DEFAULT_TTL_SECONDS) -> None:
        """Initialize unified cache.

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

    def get(self, key: CacheKey) -> Optional[Any]:
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
                return None

            ttl = self._ttl
            if ttl is not None:
                current_time = time.time()
                if (current_time - node.timestamp) > ttl:
                    self._remove_node(node)
                    del self._cache[key]
                    self._stats.record_miss()
                    self._stats.record_eviction()
                    return None

            self._move_to_head(node)
            node.access_count += 1
            self._stats.record_hit()
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

    def delete(self, key: CacheKey) -> bool:
        """Delete entry from cache.

        Args:
            key: Cache key to delete

        Returns:
            True if key was found and deleted, False otherwise
        """
        with self._lock:
            node: Optional[CacheNode] = self._cache.get(key)
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
        head_next: Optional[CacheNode] = self._head.next
        node.next = head_next
        if head_next is not None:
            head_next.prev = node
        self._head.next = node

    def _remove_node(self, node: CacheNode) -> None:
        """Remove node from linked list."""
        node_prev: Optional[CacheNode] = node.prev
        node_next: Optional[CacheNode] = node.next
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


_default_cache: Optional[UnifiedCache] = None
_cache_lock = threading.Lock()


def get_default_cache() -> UnifiedCache:
    """Get the default unified cache instance.

    Returns:
        Singleton default cache instance
    """
    global _default_cache
    if _default_cache is None:
        with _cache_lock:
            if _default_cache is None:
                _default_cache = UnifiedCache()
    return _default_cache


def clear_all_caches() -> None:
    """Clear all cache instances."""
    if _default_cache is not None:
        _default_cache.clear()
    cache = get_cache()
    cache.clear()


def get_cache_statistics() -> dict[str, CacheStats]:
    """Get statistics from all cache instances.

    Returns:
        Dictionary mapping cache type to statistics
    """
    stats = {}
    if _default_cache is not None:
        stats["default"] = _default_cache.get_stats()
    cache = get_cache()
    stats["multi_level"] = cache.get_stats()
    return stats


_global_cache_config: "Optional[CacheConfig]" = None


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
            compiled_cache_enabled: Enable compiled SQL caching
            sql_cache_enabled: Enable SQL statement caching
            fragment_cache_enabled: Enable AST fragment caching
            optimized_cache_enabled: Enable optimized expression caching
            sql_cache_size: Maximum SQL cache entries
            fragment_cache_size: Maximum fragment cache entries
            optimized_cache_size: Maximum optimized cache entries
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
    return _global_cache_config


def update_cache_config(config: CacheConfig) -> None:
    """Update the global cache configuration.

    Clears all existing caches when configuration changes.

    Args:
        config: New cache configuration to apply globally
    """
    logger = get_logger("sqlspec.cache")
    logger.info("Cache configuration updated: %s", config)

    global _global_cache_config
    _global_cache_config = config

    unified_cache = get_default_cache()
    unified_cache.clear()
    cache = get_cache()
    cache.clear()

    logger = get_logger("sqlspec.cache")
    logger.info(
        "Cache configuration updated - all caches cleared",
        extra={
            "compiled_cache_enabled": config.compiled_cache_enabled,
            "sql_cache_enabled": config.sql_cache_enabled,
            "fragment_cache_enabled": config.fragment_cache_enabled,
            "optimized_cache_enabled": config.optimized_cache_enabled,
        },
    )


def get_cache_stats() -> dict[str, CacheStats]:
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
    logger.info("Cache Statistics: %s", stats)


@mypyc_attr(allow_interpreted_subclasses=False)
class ParametersView:
    """Read-only view of parameters without copying.

    Provides read-only access to parameters without making copies,
    enabling zero-copy parameter access patterns.
    """

    __slots__ = ("_named_ref", "_positional_ref")

    def __init__(self, positional: list[Any], named: dict[str, Any]) -> None:
        """Initialize parameters view.

        Args:
            positional: List of positional parameters (will be referenced, not copied)
            named: Dictionary of named parameters (will be referenced, not copied)
        """
        self._positional_ref = positional
        self._named_ref = named

    def get_positional(self, index: int) -> Any:
        """Get positional parameter by index.

        Args:
            index: Parameter index

        Returns:
            Parameter value
        """
        return self._positional_ref[index]

    def get_named(self, key: str) -> Any:
        """Get named parameter by key.

        Args:
            key: Parameter name

        Returns:
            Parameter value
        """
        return self._named_ref[key]

    def has_named(self, key: str) -> bool:
        """Check if named parameter exists.

        Args:
            key: Parameter name

        Returns:
            True if parameter exists
        """
        return key in self._named_ref

    @property
    def positional_count(self) -> int:
        """Number of positional parameters."""
        return len(self._positional_ref)

    @property
    def named_count(self) -> int:
        """Number of named parameters."""
        return len(self._named_ref)


@mypyc_attr(allow_interpreted_subclasses=False)
@dataclass(frozen=True)
class CachedStatement:
    """Immutable cached statement result.

    This class stores compiled SQL and parameters in an immutable format
    that can be safely shared between different parts of the system without
    risk of mutation. Tuple parameters ensure no copying is needed.
    """

    compiled_sql: str
    parameters: Optional[tuple[Any, ...]]  # None allowed for static script compilation
    expression: Optional["exp.Expression"]

    def get_parameters_view(self) -> "ParametersView":
        """Get read-only parameter view.

        Returns:
            View object that provides read-only access to parameters
        """
        if self.parameters is None:
            return ParametersView([], {})
        return ParametersView(list(self.parameters), {})


def create_cache_key(level: str, key: str, dialect: Optional[str] = None) -> str:
    """Create optimized cache key using string concatenation.

    Args:
        level: Cache level (statement, expression, parameter)
        key: Base cache key
        dialect: SQL dialect (optional)

    Returns:
        Optimized cache key string
    """
    return f"{level}:{dialect or 'default'}:{key}"


@mypyc_attr(allow_interpreted_subclasses=False)
class MultiLevelCache:
    """Single cache with namespace isolation - no connection pool complexity."""

    __slots__ = ("_cache",)

    def __init__(self, max_size: int = DEFAULT_MAX_SIZE, ttl_seconds: Optional[int] = DEFAULT_TTL_SECONDS) -> None:
        """Initialize multi-level cache.

        Args:
            max_size: Maximum number of cache entries
            ttl_seconds: Time-to-live in seconds (None for no expiration)
        """
        self._cache = UnifiedCache(max_size, ttl_seconds)

    def get(self, level: str, key: str, dialect: Optional[str] = None) -> Optional[Any]:
        """Get value from cache with level and dialect namespace.

        Args:
            level: Cache level (e.g., "statement", "expression", "parameter")
            key: Cache key
            dialect: SQL dialect (optional)

        Returns:
            Cached value or None if not found
        """
        full_key = create_cache_key(level, key, dialect)
        cache_key = CacheKey((full_key,))
        return self._cache.get(cache_key)

    def put(self, level: str, key: str, value: Any, dialect: Optional[str] = None) -> None:
        """Put value in cache with level and dialect namespace.

        Args:
            level: Cache level (e.g., "statement", "expression", "parameter")
            key: Cache key
            value: Value to cache
            dialect: SQL dialect (optional)
        """
        full_key = create_cache_key(level, key, dialect)
        cache_key = CacheKey((full_key,))
        self._cache.put(cache_key, value)

    def delete(self, level: str, key: str, dialect: Optional[str] = None) -> bool:
        """Delete entry from cache.

        Args:
            level: Cache level
            key: Cache key to delete
            dialect: SQL dialect (optional)

        Returns:
            True if key was found and deleted, False otherwise
        """
        full_key = create_cache_key(level, key, dialect)
        cache_key = CacheKey((full_key,))
        return self._cache.delete(cache_key)

    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._cache.get_stats()


_multi_level_cache: Optional[MultiLevelCache] = None


def get_cache() -> MultiLevelCache:
    """Get the multi-level cache instance.

    Returns:
        Singleton multi-level cache instance
    """
    global _multi_level_cache
    if _multi_level_cache is None:
        with _cache_lock:
            if _multi_level_cache is None:
                _multi_level_cache = MultiLevelCache()
    return _multi_level_cache


@dataclass(frozen=True)
class Filter:
    """Immutable filter that can be safely shared."""

    field_name: str
    operation: str
    value: Any

    def __post_init__(self) -> None:
        """Validate filter parameters."""
        if not self.field_name:
            msg = "Field name cannot be empty"
            raise ValueError(msg)
        if not self.operation:
            msg = "Operation cannot be empty"
            raise ValueError(msg)


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
    return tuple(sorted(unique_filters, key=lambda f: (f.field_name, f.operation, str(f.value))))


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
        return [f for f in self._filters_ref if hasattr(f, "field_name") and f.field_name == field_name]

    def has_field(self, field_name: str) -> bool:
        """Check if any filter exists for a field.

        Args:
            field_name: Field name to check

        Returns:
            True if field has filters
        """
        return any(hasattr(f, "field_name") and f.field_name == field_name for f in self._filters_ref)

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
            elif hasattr(f, "field_name") and hasattr(f, "operation") and hasattr(f, "value"):
                filter_objects.append(Filter(f.field_name, f.operation, f.value))

        return canonicalize_filters(filter_objects)
