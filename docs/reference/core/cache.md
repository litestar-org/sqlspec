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
    hash_parameters,
    hash_sql_statement,
    log_cache_stats,
    reset_stats_only,
    update_cache_config,
)
```

Coordinated L1/L2 LRU caching (`StatementCacheHolder`, `QueryCache`, `NamespacedCache`) and deterministic structural hashing for SQL statements, SQLGlot AST expressions, and parameter bindings. See the [Performance Guide](../../learn/performance.md) for tuning guidance.

## Per-Config Fast-Path Cache

::: sqlspec.core.cache.StatementCacheHolder
    options:
      heading_level: 3
      members: false

::: sqlspec.core.cache.QueryCache
    options:
      heading_level: 3
      members: false

::: sqlspec.core.cache.CachedQuery
    options:
      heading_level: 3
      members: false

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
      members:
        - get_statement
        - put_statement
        - get_expression
        - put_expression
        - clear_namespace

## Cache Management & Hashing Functions

::: sqlspec.core.cache.get_cache
    options:
      heading_level: 4

::: sqlspec.core.cache.create_cache_key
    options:
      heading_level: 4

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
