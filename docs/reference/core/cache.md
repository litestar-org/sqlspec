# Caching & Hashing

```python
from sqlspec.core import (
    CacheConfig,
    CachedQuery,
    CachedStatement,
    CacheKey,
    CacheStats,
    LRUCache,
    NamespacedCache,
    QueryCache,
    StatementCacheHolder,
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

Coordinated L1/L2 LRU caching (`StatementCacheHolder`, `QueryCache`, `NamespacedCache`) and deterministic structural hashing for SQL statements, SQLGlot AST expressions, and parameter bindings. See the [Performance Guide](../../learn/performance.md) for tuning guidance.

## Per-Config Fast-Path Cache (`L1`)

::: sqlspec.core.cache.StatementCacheHolder
    options:
      heading_level: 3

::: sqlspec.core.cache.QueryCache
    options:
      heading_level: 3

::: sqlspec.core.cache.CachedQuery
    options:
      heading_level: 3

## Cache Configuration & Statistics (`L2`)

::: sqlspec.core.cache.CacheConfig
    options:
      heading_level: 3

::: sqlspec.core.cache.CacheStats
    options:
      heading_level: 3
      members:
        - record_hit
        - record_miss
        - record_eviction
        - reset

::: sqlspec.core.cache.CachedStatement
    options:
      heading_level: 3

::: sqlspec.core.cache.CacheKey
    options:
      heading_level: 3

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
      members:
        - get_statement
        - put_statement
        - get_expression
        - put_expression
        - clear_namespace

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
