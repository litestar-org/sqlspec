# Caching & Hashing

```python
from sqlspec.core import (
    CacheConfig,
    CachedStatement,
    CacheKey,
    CacheStats,
    LRUCache,
    NamespacedCache,
    clear_all_caches,
    create_cache_key,
    get_cache,
    get_cache_config,
    get_cache_statistics,
    hash_expression,
    hash_sql_statement,
    log_cache_stats,
    reset_stats_only,
    update_cache_config,
)
```

Multi-tier LRU caching and deterministic structural hashing for SQL statements, SQLGlot AST expressions, and parameter bindings. See the [Performance Guide](../../learn/performance.md) for tuning guidance.

## Cache Configuration & Statistics

::: sqlspec.core.cache.CacheConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.core.cache.CacheStats
    options:
      heading_level: 3
      members: false

::: sqlspec.core.cache.CachedStatement
    options:
      heading_level: 3
      members: false

::: sqlspec.core.cache.CacheKey
    options:
      heading_level: 3
      members: false

## Cache Implementations

::: sqlspec.core.cache.LRUCache
    options:
      heading_level: 3
      members:
        - get
        - put
        - delete
        - clear
        - size
        - get_stats

::: sqlspec.core.cache.NamespacedCache
    options:
      heading_level: 3
      members: false

## Cache Management & Hashing Functions

::: sqlspec.core.cache.get_cache_config
    options:
      heading_level: 4

::: sqlspec.core.cache.update_cache_config
    options:
      heading_level: 4

::: sqlspec.core.cache.get_cache_statistics
    options:
      heading_level: 4

::: sqlspec.core.cache.clear_all_caches
    options:
      heading_level: 4

::: sqlspec.core.cache.reset_stats_only
    options:
      heading_level: 4

::: sqlspec.core.cache.log_cache_stats
    options:
      heading_level: 4

::: sqlspec.core.hashing.hash_sql_statement
    options:
      heading_level: 4

::: sqlspec.core.hashing.hash_expression
    options:
      heading_level: 4

::: sqlspec.core.hashing.hash_parameters
    options:
      heading_level: 4
