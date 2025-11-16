# Performance Optimization Patterns

Guide to optimizing SQLSpec performance.

## Connection Pooling

**Essential for production - reuse connections:**

```python
config = AsyncpgConfig(
    pool_config={
        "dsn": "postgresql://localhost/db",
        "min_size": 10,  # Keep 10 connections ready
        "max_size": 20,  # Allow up to 20 total
        "max_inactive_connection_lifetime": 300,  # Close idle after 5 min
        "max_queries": 50000,  # Recycle after 50k queries
        "timeout": 60.0,  # Connection acquisition timeout
    }
)
```

**Sizing Guidelines:**
- **min_size**: Core * 2 (e.g., 8 cores → 16 connections)
- **max_size**: min_size * 2 (allow bursts)
- **Web apps**: 10-20 connections typical
- **Background workers**: 2-5 connections per worker

## Statement Caching

**Cache parsed SQL statements:**

```python
from sqlspec.core import update_cache_config, CacheConfig

# Global cache
update_cache_config(CacheConfig(
    enabled=True,
    maxsize=1000,  # Cache up to 1000 statements
))

# Per-instance cache
spec = SQLSpec()
spec._instance_cache_config = CacheConfig(enabled=True, maxsize=500)
```

**Benefits:**
- Avoid re-parsing identical SQL
- 10-30% speedup for repeated queries
- Memory overhead: ~1KB per cached statement

## Batch Operations

**Use execute_many() for bulk inserts:**

```python
# ✅ GOOD - Single round-trip
users = [("Alice", "alice@example.com"), ("Bob", "bob@example.com")]
session.execute_many(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    users
)

# ❌ BAD - N round-trips
for name, email in users:
    session.execute("INSERT INTO users (name, email) VALUES (?, ?)", name, email)
```

**Speedup:** 5-10x faster for batch inserts

## Statement Stacks

**Reduce round-trips with pipelined execution:**

```python
from sqlspec import StatementStack

stack = (
    StatementStack()
    .push_execute("INSERT INTO audit_log (message) VALUES ($1)", ("login",))
    .push_execute("UPDATE users SET last_login = NOW() WHERE id = $1", (user_id,))
    .push_execute("SELECT permissions FROM user_permissions WHERE user_id = $1", (user_id,))
)

# Single round-trip (if adapter supports native pipeline)
results = await session.execute_stack(stack)
```

**Native support:** PostgreSQL (asyncpg), Oracle
**Speedup:** 2-3x for multiple operations

## Apache Arrow Export

**Efficient for large result sets:**

```python
# ✅ GOOD - Columnar format
result = await session.select_to_arrow("SELECT * FROM users")
df = result.to_pandas()  # Efficient conversion

# ❌ LESS EFFICIENT - Row-by-row
result = await session.execute("SELECT * FROM users")
df = pd.DataFrame(result.all())
```

**Benefits:**
- 2-5x faster for large result sets (>10K rows)
- Lower memory overhead
- Native support: ADBC, DuckDB, BigQuery

## Disable Validation for Trusted Queries

**Skip security checks for performance-critical code:**

```python
from sqlspec.core import StatementConfig

statement_config = StatementConfig(
    enable_validation=False,  # Skip syntax validation
    enable_transformations=False,  # Skip SQL transformations
    enable_security_checks=False,  # Skip injection detection
)

# Use for trusted, performance-critical queries
result = session.execute(
    "SELECT * FROM users WHERE id = ?",
    user_id,
    statement_config=statement_config
)
```

**Speedup:** 5-15% for simple queries

## Use Scalar Methods Directly

**Avoid unnecessary data conversion:**

```python
# ✅ GOOD - Direct scalar access
count = session.select_value("SELECT COUNT(*) FROM users")

# ❌ LESS EFFICIENT - Extra conversion
result = session.execute("SELECT COUNT(*) FROM users")
count = result.scalar()
```

## Optimize Queries

**Database-level optimization:**

1. **Use indexes:**
   ```sql
   CREATE INDEX idx_users_email ON users(email);
   CREATE INDEX idx_users_status_created ON users(status, created_at);
   ```

2. **Select only needed columns:**
   ```python
   # ✅ GOOD
   users = session.execute("SELECT id, name FROM users").all()

   # ❌ BAD
   users = session.execute("SELECT * FROM users").all()
   ```

3. **Use LIMIT for pagination:**
   ```python
   users = session.execute(
       "SELECT * FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?",
       page_size, offset
   ).all()
   ```

4. **Use EXPLAIN to analyze:**
   ```python
   explain = session.execute("EXPLAIN ANALYZE SELECT * FROM users WHERE status = ?", "active").all()
   ```

## Async Over Sync

**Async adapters handle concurrency better:**

```python
# ✅ GOOD - Async for web apps
config = AsyncpgConfig(pool_config={...})

# ❌ LESS EFFICIENT - Sync blocks threads
config = PsycopgConfig(pool_config={...})
```

**Async benefits:**
- Handle 1000s of concurrent requests
- Better resource utilization
- Non-blocking I/O

## Connection Reuse

**Reuse connections within request:**

```python
# ✅ GOOD - Framework plugin caches session per request
@get("/user-data")
async def get_user_data(db_session: AsyncpgDriver):
    user = await db_session.select_one("SELECT * FROM users WHERE id = ?", 1)
    posts = await db_session.execute("SELECT * FROM posts WHERE user_id = ?", 1).all()
    # Same connection reused

# ❌ BAD - Multiple context managers
@get("/user-data")
async def get_user_data():
    async with spec.provide_session(db) as session1:
        user = await session1.select_one("SELECT * FROM users WHERE id = ?", 1)
    async with spec.provide_session(db) as session2:
        posts = await session2.execute("SELECT * FROM posts WHERE user_id = ?", 1).all()
    # Two different connections
```

## Adapter Selection for Performance

**Choose adapter based on use case:**

| Use Case | Best Adapter | Reason |
|----------|-------------|---------|
| High-throughput PostgreSQL | `psqlpy` | Rust-based, fastest |
| General PostgreSQL async | `asyncpg` | Mature, excellent performance |
| Analytics queries | `duckdb` | Columnar, OLAP-optimized |
| Embedded high-perf | `duckdb` | In-process, no network |
| Arrow ecosystem | `adbc` or `duckdb` | Zero-copy Arrow |
| Oracle extreme scale | `oracledb` async | Native async driver |

## Monitor and Profile

**Identify bottlenecks:**

```python
import time

start = time.time()
result = await session.execute("SELECT * FROM users")
duration = time.time() - start
print(f"Query took {duration:.3f}s")

# Use observability middleware
config = AsyncpgConfig(
    pool_config={...},
    extension_config={
        "litestar": {
            "enable_correlation_middleware": True  # Request tracking
        }
    }
)
```

## Performance Checklist

- [ ] Connection pooling enabled
- [ ] Statement caching enabled
- [ ] Using batch operations for bulk inserts
- [ ] Using async adapter for web apps
- [ ] Indexes on frequently queried columns
- [ ] SELECT only needed columns
- [ ] Using LIMIT for pagination
- [ ] Using Arrow for large result sets
- [ ] Validation disabled for trusted queries
- [ ] Monitoring query performance

## Common Performance Anti-Patterns

### Anti-Pattern: N+1 Queries

```python
# ❌ BAD - N+1 queries
users = await session.execute("SELECT * FROM users").all()
for user in users:
    posts = await session.execute(
        "SELECT * FROM posts WHERE user_id = ?", user["id"]
    ).all()

# ✅ GOOD - Single JOIN query
data = await session.execute("""
    SELECT u.*, p.*
    FROM users u
    LEFT JOIN posts p ON p.user_id = u.id
""").all()
```

### Anti-Pattern: Loading All Data

```python
# ❌ BAD - Load everything
all_users = await session.execute("SELECT * FROM users").all()
active_users = [u for u in all_users if u["status"] == "active"]

# ✅ GOOD - Filter in SQL
active_users = await session.execute(
    "SELECT * FROM users WHERE status = ?", "active"
).all()
```

### Anti-Pattern: Missing Indexes

```python
# ❌ BAD - No index on email (full table scan)
user = await session.select_one(
    "SELECT * FROM users WHERE email = ?", email
)

# ✅ GOOD - Add index
await session.execute("CREATE INDEX idx_users_email ON users(email)")
user = await session.select_one(
    "SELECT * FROM users WHERE email = ?", email
)
```

## Benchmark Results

Typical performance characteristics (relative to baseline):

- **Connection pooling**: 10-20x improvement
- **Statement caching**: 1.1-1.3x improvement
- **Batch operations**: 5-10x improvement
- **Statement stacks**: 2-3x improvement
- **Arrow export**: 2-5x improvement (large datasets)
- **Async vs sync**: 2-10x concurrent throughput
