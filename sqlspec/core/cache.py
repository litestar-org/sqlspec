"""Caching system with unified cache management.

This module provides a caching system with LRU eviction and TTL support for
SQL statement processing, parameter processing, and expression caching.

Components:
- CacheKey: Immutable cache key with optimized hashing
- UnifiedCache: Main cache implementation with LRU eviction and TTL
- StatementCache: Specialized cache for compiled SQL statements
- ExpressionCache: Specialized cache for parsed SQLGlot expressions
- ParameterCache: Specialized cache for processed parameters

Features:
- LRU caching with configurable size and TTL
- Thread-safe cache operations for concurrent access
- Cached hash values to avoid recomputation
- O(1) cache lookup and insertion operations
"""

import threading
import time
from typing import TYPE_CHECKING, Any, Generic, Optional

from mypy_extensions import mypyc_attr
from typing_extensions import TypeVar

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    import sqlglot.expressions as exp

    from sqlspec.core.statement import SQL

__all__ = (
    "CacheKey",
    "CacheStats",
    "ExpressionCache",
    "ParameterCache",
    "StatementCache",
    "UnifiedCache",
    "get_cache_config",
    "get_default_cache",
    "get_expression_cache",
    "get_parameter_cache",
    "get_statement_cache",
    "sql_cache",
)

T = TypeVar("T")
CacheValueT = TypeVar("CacheValueT")

# Cache configuration constants
DEFAULT_MAX_SIZE = 10000  # LRU cache size limit
DEFAULT_TTL_SECONDS = 3600  # 1 hour default TTL
CACHE_STATS_UPDATE_INTERVAL = 100  # Update stats every N operations

# Cache slots - optimized structure
CACHE_KEY_SLOTS = ("_hash", "_key_data")
CACHE_NODE_SLOTS = ("key", "value", "prev", "next", "timestamp", "access_count")
UNIFIED_CACHE_SLOTS = ("_cache", "_lock", "_max_size", "_ttl", "_head", "_tail", "_stats")
CACHE_STATS_SLOTS = ("hits", "misses", "evictions", "total_operations", "memory_usage")


@mypyc_attr(allow_interpreted_subclasses=True)
class CacheKey:
    """Immutable cache key with optimized hashing.

    This class provides an immutable cache key for consistent cache operations
    across all cache types.

    Features:
    - Cached hash value to avoid recomputation
    - Immutable design for safe sharing across threads
    - Fast equality comparison with short-circuit evaluation

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
        """Equality comparison with short-circuit evaluation."""
        if type(other) is not CacheKey:
            return False
        other_key = other  # type: CacheKey
        if self._hash != other_key._hash:
            return False
        return self._key_data == other_key._key_data

    def __repr__(self) -> str:
        """String representation of the cache key."""
        return f"CacheKey({self._key_data!r})"


@mypyc_attr(allow_interpreted_subclasses=True)
class CacheStats:
    """Cache statistics tracking.

    Tracks cache performance metrics including hit rates, evictions,
    and memory usage.
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


@mypyc_attr(allow_interpreted_subclasses=True)
class CacheNode:
    """Internal cache node for LRU linked list implementation.

    This class represents a node in the doubly-linked list used for
    LRU cache implementation with O(1) operations.
    """

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


@mypyc_attr(allow_interpreted_subclasses=True)
class UnifiedCache(Generic[CacheValueT]):
    """Cache with LRU eviction and TTL support.

    This class provides a thread-safe cache implementation with LRU eviction
    and time-based expiration.

    Features:
    - O(1) cache lookup, insertion, and deletion operations
    - LRU eviction policy with configurable size limits
    - TTL-based expiration for cache entries
    - Thread-safe operations
    - Statistics tracking

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

    def get(self, key: CacheKey) -> Optional[CacheValueT]:
        """Get value from cache with LRU update.

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

            # Optimize TTL check with early variable assignment
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
            return node.value  # type: ignore[no-any-return]

    def put(self, key: CacheKey, value: CacheValueT) -> None:
        """Put value in cache with LRU management.

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

            # Optimize size check with cached length
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
        return len(self._cache) == 0

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._stats

    def _add_to_head(self, node: CacheNode) -> None:
        """Add node after head."""
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
        """Move existing node to head."""
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

            # Optimize TTL check
            ttl = self._ttl
            return not (ttl is not None and time.time() - node.timestamp > ttl)


@mypyc_attr(allow_interpreted_subclasses=False)
class StatementCache:
    """Specialized cache for compiled SQL statements.

    Caches compiled SQL statements and their execution parameters.
    """

    def __init__(self, max_size: int = DEFAULT_MAX_SIZE) -> None:
        """Initialize statement cache.

        Args:
            max_size: Maximum number of statements to cache
        """
        self._cache: UnifiedCache[tuple[str, Any]] = UnifiedCache(max_size)

    def get_compiled(self, statement: "SQL") -> Optional[tuple[str, Any]]:
        """Get compiled SQL and parameters from cache.

        Args:
            statement: SQL statement to lookup

        Returns:
            Tuple of (compiled_sql, parameters) or None if not cached
        """
        cache_key = self._create_statement_key(statement)
        return self._cache.get(cache_key)

    def put_compiled(self, statement: "SQL", compiled_sql: str, parameters: Any) -> None:
        """Cache compiled SQL and parameters.

        Args:
            statement: Original SQL statement
            compiled_sql: Compiled SQL string
            parameters: Processed parameters
        """
        cache_key = self._create_statement_key(statement)
        self._cache.put(cache_key, (compiled_sql, parameters))

    def _create_statement_key(self, statement: "SQL") -> CacheKey:
        """Create cache key for SQL statement.

        Args:
            statement: SQL statement

        Returns:
            Cache key for the statement
        """
        # Create key from SQL text, parameters, and configuration
        key_data = (
            "statement",
            statement._raw_sql,
            hash(statement),  # Includes parameters and flags
            str(statement.dialect) if statement.dialect else None,
            statement.is_many,
            statement.is_script,
        )
        return CacheKey(key_data)

    def clear(self) -> None:
        """Clear statement cache."""
        self._cache.clear()

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._cache.get_stats()


@mypyc_attr(allow_interpreted_subclasses=False)
class ExpressionCache:
    """Specialized cache for parsed SQLGlot expressions.

    Caches parsed SQLGlot expressions to avoid redundant parsing operations.
    """

    def __init__(self, max_size: int = DEFAULT_MAX_SIZE) -> None:
        """Initialize expression cache.

        Args:
            max_size: Maximum number of expressions to cache
        """
        self._cache: UnifiedCache[exp.Expression] = UnifiedCache(max_size)

    def get_expression(self, sql: str, dialect: Optional[str] = None) -> "Optional[exp.Expression]":
        """Get parsed expression from cache.

        Args:
            sql: SQL string
            dialect: SQL dialect

        Returns:
            Parsed expression or None if not cached
        """
        cache_key = self._create_expression_key(sql, dialect)
        return self._cache.get(cache_key)

    def put_expression(self, sql: str, expression: "exp.Expression", dialect: Optional[str] = None) -> None:
        """Cache parsed expression.

        Args:
            sql: SQL string
            expression: Parsed SQLGlot expression
            dialect: SQL dialect
        """
        cache_key = self._create_expression_key(sql, dialect)
        self._cache.put(cache_key, expression)

    def _create_expression_key(self, sql: str, dialect: Optional[str]) -> CacheKey:
        """Create cache key for expression.

        Args:
            sql: SQL string
            dialect: SQL dialect

        Returns:
            Cache key for the expression
        """
        key_data = ("expression", sql, dialect)
        return CacheKey(key_data)

    def clear(self) -> None:
        """Clear expression cache."""
        self._cache.clear()

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._cache.get_stats()


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterCache:
    """Specialized cache for processed parameters.

    Caches processed parameter transformations.
    """

    def __init__(self, max_size: int = DEFAULT_MAX_SIZE) -> None:
        """Initialize parameter cache.

        Args:
            max_size: Maximum number of parameter sets to cache
        """
        self._cache: UnifiedCache[Any] = UnifiedCache(max_size)

    def get_parameters(self, original_params: Any, config_hash: int) -> Optional[Any]:
        """Get processed parameters from cache.

        Args:
            original_params: Original parameters
            config_hash: Hash of parameter processing configuration

        Returns:
            Processed parameters or None if not cached
        """
        cache_key = self._create_parameter_key(original_params, config_hash)
        return self._cache.get(cache_key)

    def put_parameters(self, original_params: Any, processed_params: Any, config_hash: int) -> None:
        """Cache processed parameters.

        Args:
            original_params: Original parameters
            processed_params: Processed parameters
            config_hash: Hash of parameter processing configuration
        """
        cache_key = self._create_parameter_key(original_params, config_hash)
        self._cache.put(cache_key, processed_params)

    def _create_parameter_key(self, params: Any, config_hash: int) -> CacheKey:
        """Create cache key for parameters.

        Args:
            params: Parameters to cache
            config_hash: Configuration hash

        Returns:
            Cache key for the parameters
        """
        # Create stable key from parameters and configuration
        try:
            # Optimize type checking order
            param_key: tuple[Any, ...]
            if isinstance(params, dict):
                param_key = tuple(sorted(params.items()))
            elif isinstance(params, (list, tuple)):
                param_key = tuple(params)
            else:
                param_key = (params,)

            return CacheKey(("parameters", param_key, config_hash))
        except (TypeError, ValueError):
            # Fallback for unhashable types
            param_key_fallback = (str(params), type(params).__name__)
            return CacheKey(("parameters", param_key_fallback, config_hash))

    def clear(self) -> None:
        """Clear parameter cache."""
        self._cache.clear()

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._cache.get_stats()


_default_cache: Optional[UnifiedCache[Any]] = None
_statement_cache: Optional[StatementCache] = None
_expression_cache: Optional[ExpressionCache] = None
_parameter_cache: Optional[ParameterCache] = None
_cache_lock = threading.Lock()


def get_default_cache() -> UnifiedCache[Any]:
    """Get the default unified cache instance.

    Returns:
        Singleton default cache instance
    """
    global _default_cache
    if _default_cache is None:
        with _cache_lock:
            if _default_cache is None:
                _default_cache = UnifiedCache[Any]()
    return _default_cache


def get_statement_cache() -> StatementCache:
    """Get the statement cache instance.

    Returns:
        Singleton statement cache instance
    """
    global _statement_cache
    if _statement_cache is None:
        with _cache_lock:
            if _statement_cache is None:
                _statement_cache = StatementCache()
    return _statement_cache


def get_expression_cache() -> ExpressionCache:
    """Get the expression cache instance.

    Returns:
        Singleton expression cache instance
    """
    global _expression_cache
    if _expression_cache is None:
        with _cache_lock:
            if _expression_cache is None:
                _expression_cache = ExpressionCache()
    return _expression_cache


def get_parameter_cache() -> ParameterCache:
    """Get the parameter cache instance.

    Returns:
        Singleton parameter cache instance
    """
    global _parameter_cache
    if _parameter_cache is None:
        with _cache_lock:
            if _parameter_cache is None:
                _parameter_cache = ParameterCache()
    return _parameter_cache


def clear_all_caches() -> None:
    """Clear all cache instances."""
    if _default_cache is not None:
        _default_cache.clear()
    if _statement_cache is not None:
        _statement_cache.clear()
    if _expression_cache is not None:
        _expression_cache.clear()
    if _parameter_cache is not None:
        _parameter_cache.clear()


def get_cache_statistics() -> dict[str, CacheStats]:
    """Get statistics from all cache instances.

    Returns:
        Dictionary mapping cache type to statistics
    """
    stats = {}
    if _default_cache is not None:
        stats["default"] = _default_cache.get_stats()
    if _statement_cache is not None:
        stats["statement"] = _statement_cache.get_stats()
    if _expression_cache is not None:
        stats["expression"] = _expression_cache.get_stats()
    if _parameter_cache is not None:
        stats["parameter"] = _parameter_cache.get_stats()
    return stats


_global_cache_config: "Optional[CacheConfig]" = None


@mypyc_attr(allow_interpreted_subclasses=True)
class CacheConfig:
    """Global cache configuration for SQLSpec.

    Controls caching behavior across the SQLSpec system.
    """

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

    This clears all existing caches when configuration changes to ensure
    consistency with the new settings.

    Args:
        config: New cache configuration to apply globally
    """
    logger = get_logger("sqlspec.cache")
    logger.info("Cache configuration updated: %s", config)

    global _global_cache_config
    _global_cache_config = config

    unified_cache = get_default_cache()
    unified_cache.clear()
    statement_cache = get_statement_cache()
    statement_cache.clear()

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


@mypyc_attr(allow_interpreted_subclasses=True)
class CacheStatsAggregate:
    """Aggregated cache statistics from all cache instances."""

    __slots__ = (
        "fragment_capacity",
        "fragment_hit_rate",
        "fragment_hits",
        "fragment_misses",
        "fragment_size",
        "optimized_capacity",
        "optimized_hit_rate",
        "optimized_hits",
        "optimized_misses",
        "optimized_size",
        "sql_capacity",
        "sql_hit_rate",
        "sql_hits",
        "sql_misses",
        "sql_size",
    )

    def __init__(self) -> None:
        """Initialize aggregated cache statistics."""
        self.sql_hit_rate = 0.0
        self.fragment_hit_rate = 0.0
        self.optimized_hit_rate = 0.0
        self.sql_size = 0
        self.fragment_size = 0
        self.optimized_size = 0
        self.sql_capacity = 0
        self.fragment_capacity = 0
        self.optimized_capacity = 0
        self.sql_hits = 0
        self.sql_misses = 0
        self.fragment_hits = 0
        self.fragment_misses = 0
        self.optimized_hits = 0
        self.optimized_misses = 0


def get_cache_stats() -> CacheStatsAggregate:
    """Get current cache statistics from all caches.

    Returns:
        Combined cache statistics object
    """
    stats_dict = get_cache_statistics()
    stats = CacheStatsAggregate()

    for cache_name, cache_stats in stats_dict.items():
        hits = cache_stats.hits
        misses = cache_stats.misses
        size = 0

        if "sql" in cache_name.lower():
            stats.sql_hits += hits
            stats.sql_misses += misses
            stats.sql_size += size
        elif "fragment" in cache_name.lower():
            stats.fragment_hits += hits
            stats.fragment_misses += misses
            stats.fragment_size += size
        elif "optimized" in cache_name.lower():
            stats.optimized_hits += hits
            stats.optimized_misses += misses
            stats.optimized_size += size

    sql_total = stats.sql_hits + stats.sql_misses
    if sql_total > 0:
        stats.sql_hit_rate = stats.sql_hits / sql_total

    fragment_total = stats.fragment_hits + stats.fragment_misses
    if fragment_total > 0:
        stats.fragment_hit_rate = stats.fragment_hits / fragment_total

    optimized_total = stats.optimized_hits + stats.optimized_misses
    if optimized_total > 0:
        stats.optimized_hit_rate = stats.optimized_hits / optimized_total

    return stats


def reset_cache_stats() -> None:
    """Reset all cache statistics."""
    clear_all_caches()


def log_cache_stats() -> None:
    """Log current cache statistics using the configured logger."""
    logger = get_logger("sqlspec.cache")
    stats = get_cache_stats()
    logger.info("Cache Statistics: %s", stats)


@mypyc_attr(allow_interpreted_subclasses=False)
class SQLCompilationCache:
    """Wrapper around StatementCache for compatibility."""

    __slots__ = ("_statement_cache", "_unified_cache")

    def __init__(self) -> None:
        self._statement_cache = get_statement_cache()
        self._unified_cache = get_default_cache()

    def get(self, cache_key: str) -> Optional[tuple[str, Any]]:
        """Get cached compiled SQL and parameters."""
        key = CacheKey((cache_key,))
        return self._unified_cache.get(key)

    def set(self, cache_key: str, value: tuple[str, Any]) -> None:
        """Set cached compiled SQL and parameters."""
        key = CacheKey((cache_key,))
        self._unified_cache.put(key, value)


sql_cache = SQLCompilationCache()
