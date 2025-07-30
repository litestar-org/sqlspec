# SQLSpec Cache Configuration Guide

SQLSpec provides a three-tier caching system that delivers up to 90% performance improvements for common query patterns through the single-pass pipeline architecture.

## Three-Tier Cache System

The SQLSpec architecture includes three complementary caching layers:

1. **Base Statement Cache**: Processed SQL objects and pipeline results
2. **Filter Result Cache**: Applied filter transformations and compositions
3. **Optimized Expression Cache**: SQLGlot optimization results with AST sub-expression caching

## Cache Layers

### 1. Global Cache Configuration

Controls the size and behavior of all statement caches globally:

```python
from sqlspec.statement.cache import CacheConfig, update_cache_config

# Configure cache sizes
config = CacheConfig(
    sql_cache_size=2000,           # Processed SQL statements
    fragment_cache_size=10000,     # AST fragments (WHERE, JOIN, etc.)
    optimized_cache_size=3000,     # Optimized expressions
    sql_cache_enabled=True,        # Enable/disable SQL cache
    fragment_cache_enabled=True,   # Enable/disable fragment cache
    optimized_cache_enabled=True,  # Enable/disable optimization cache
)
update_cache_config(config)

# Disable specific caches by setting size to 0
minimal_config = CacheConfig(
    sql_cache_size=0,              # Disables SQL cache
    fragment_cache_size=5000,      # Fragment cache still active
    optimized_cache_size=0,        # Disables optimization cache
)
update_cache_config(minimal_config)
```

### 2. Driver-Level Configuration

Set default caching behavior for all statements executed by a driver:

```python
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.statement.sql import StatementConfig

# Configure driver with custom statement defaults
config = SqliteConfig(
    connection_config={"database": "mydb.db"},
    statement_config=StatementConfig(
        enable_caching=True,        # Enable statement caching
        enable_parsing=True,        # Enable SQL parsing
        enable_validation=True,     # Enable validation
    ),
    adapter_cache_size=1000,        # Driver's compiled SQL cache
)

# Create driver with configuration
with config.provide_session() as driver:
    # All statements use the driver's default config
    result = driver.execute("SELECT * FROM users")
```

### 3. Statement-Level Override

Override caching for specific statements:

```python
from sqlspec.statement.sql import SQL, StatementConfig

# Method 1: Create SQL object with custom config
sql = SQL(
    "SELECT * FROM users WHERE id = ?",
    config=StatementConfig(enable_caching=False)  # Disable caching for this statement
)
result = driver.execute(sql, (123,))

# Method 2: Override at execution time
result = driver.execute(
    "SELECT * FROM products",
    _config=StatementConfig(enable_caching=False)  # Override driver's default
)

# Method 3: Using query builders
from sqlspec.statement.builder import Select

query = Select("id", "name").from_("users").where("active = ?", True)
result = driver.execute(
    query,
    _config=StatementConfig(enable_caching=True)  # Force caching even if driver default is False
)
```

## Caching Behavior

### What Gets Cached?

1. **SQL Cache**: Stores fully processed SQL statements with their parameters
2. **Fragment Cache**: Stores parsed AST fragments (WHERE clauses, JOINs, subqueries)
3. **Optimized Expression Cache**: Stores optimized/simplified expressions
4. **Base Statement Cache**: Stores parsed base SQL before modifications
5. **Filter Cache**: Stores results of applying filters to statements

### Cache Keys

- SQL statements are cached based on:
    - Raw SQL text
    - Dialect
    - Parameter styles
    - Applied filters

### Performance Considerations

```python
from sqlspec.statement.cache import get_cache_stats, log_cache_stats

# Monitor cache performance
stats = get_cache_stats()
print(f"SQL Cache Hit Rate: {stats.sql_hit_rate:.2%}")
print(f"Fragment Cache Hit Rate: {stats.fragment_hit_rate:.2%}")

# Log detailed statistics
log_cache_stats()
```

## Best Practices

### 1. Development vs Production

```python
# Development: Disable caching for easier debugging
dev_config = StatementConfig(enable_caching=False)

# Production: Enable all caches with appropriate sizes
prod_config = CacheConfig(
    sql_cache_size=5000,
    fragment_cache_size=20000,
    optimized_cache_size=5000,
)
```

### 2. Memory-Constrained Environments

```python
# Reduce cache sizes for low-memory environments
low_memory_config = CacheConfig(
    sql_cache_size=100,
    fragment_cache_size=500,
    optimized_cache_size=100,
)
```

### 3. High-Performance Scenarios

```python
# Maximize cache sizes for read-heavy workloads
high_perf_config = CacheConfig(
    sql_cache_size=10000,
    fragment_cache_size=50000,
    optimized_cache_size=10000,
)

# Pre-warm caches with common queries
common_queries = [
    "SELECT * FROM users WHERE active = ?",
    "SELECT id, name FROM products ORDER BY created_at DESC LIMIT ?",
]
for query in common_queries:
    driver.execute(query, (True,))  # Warm up the cache
```

### 4. Selective Caching

```python
# Cache only expensive queries
def should_cache(sql: str) -> bool:
    # Cache complex queries with JOINs, CTEs, or aggregations
    complex_keywords = ["JOIN", "WITH", "GROUP BY", "HAVING"]
    return any(keyword in sql.upper() for keyword in complex_keywords)

# Use conditional caching
sql = "SELECT COUNT(*) FROM orders JOIN users ON orders.user_id = users.id"
config = StatementConfig(enable_caching=should_cache(sql))
result = driver.execute(sql, _config=config)
```

## Monitoring and Debugging

```python
from sqlspec.statement.cache import (
    get_cache_config,
    get_cache_stats,
    reset_cache_stats,
    sql_cache,
    ast_fragment_cache,
)

# Check current configuration
config = get_cache_config()
print(f"SQL Cache Size: {config.sql_cache_size}")
print(f"SQL Cache Enabled: {config.sql_cache_enabled}")

# Monitor cache usage
stats = get_cache_stats()
print(f"SQL Cache: {stats.sql_size}/{sql_cache.max_size} entries")
print(f"Hit Rate: {stats.sql_hit_rate:.2%}")

# Reset statistics for benchmarking
reset_cache_stats()

# Direct cache inspection (for debugging)
print(f"SQL Cache has {sql_cache.size} entries")
print(f"Fragment Cache has {ast_fragment_cache.size} entries")
```
