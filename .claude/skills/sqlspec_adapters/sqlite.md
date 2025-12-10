# SQLite Adapter Skill

**Adapter:** SQLite (Sync, Embedded RDBMS)
**Category:** Database Adapter
**Status:** Active

## Description

Expert guidance for using SQLSpec's SQLite adapter for synchronous database operations. SQLite is a lightweight, serverless, self-contained SQL database engine embedded directly in your application.

SQLite provides ACID transactions, full SQL support, and thread-local connection pooling for safe multi-threaded access. Ideal for embedded databases, local caching, testing, and applications where simplicity and zero-configuration are priorities.

## When to Use SQLite

- **Embedded applications** - No server setup, single file database
- **Testing** - Fast, isolated test databases per thread
- **Local caching** - Store application state locally
- **Mobile/desktop apps** - Embedded database for offline-first apps
- **Prototyping** - Quick iteration without infrastructure
- **Small-scale web apps** - Low-traffic applications (reads < 100K/day)
- **Configuration storage** - Structured config instead of JSON/YAML
- **Development** - Local development without PostgreSQL/MySQL

## Configuration

```python
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriverFeatures

config = SqliteConfig(
    connection_config={
        # Database path
        "database": "app.db",  # File-based database
        # OR: "file:memory_{uuid}?mode=memory&cache=private",  # Default
        # OR: "/path/to/data.db",  # Absolute path

        # Connection settings
        "timeout": 5.0,  # Lock timeout in seconds
        "detect_types": 0,  # sqlite3.PARSE_DECLTYPES | PARSE_COLNAMES
        "isolation_level": None,  # None = autocommit, "DEFERRED" | "IMMEDIATE" | "EXCLUSIVE"
        "check_same_thread": False,  # Allow cross-thread access (pooling handles safety)
        "cached_statements": 128,  # Statement cache size
        "uri": True,  # Enable URI mode (auto-enabled for file: URIs)
    },
    driver_features=SqliteDriverFeatures(
        # Custom type adapters (default: True)
        enable_custom_adapters=True,

        # JSON serialization
        json_serializer=custom_json_encoder,  # Defaults to to_json
        json_deserializer=custom_json_decoder,  # Defaults to from_json
    ),
)
```

## Parameter Style

**Positional**: `?` (positional parameters)

```python
# Single parameter
result = session.execute(
    "SELECT * FROM users WHERE id = ?",
    user_id
)

# Multiple parameters
result = session.execute(
    "SELECT * FROM users WHERE status = ? AND age > ?",
    "active", 18
)

# Named parameters NOT supported by default - use positional
result = session.execute(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    "Alice", "alice@example.com"
)
```

## Thread-Local Pooling

### Per-Thread Connections

```python
# SQLite uses thread-local connections for safety
config = SqliteConfig(
    connection_config={
        "database": "app.db",
        "check_same_thread": False,  # Pool handles thread safety
    }
)

# Each thread gets its own connection
import concurrent.futures

def process_user(user_id):
    with config.provide_session() as session:
        # Thread-local connection
        return session.execute(
            "SELECT * FROM users WHERE id = ?", user_id
        ).all()

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(process_user, range(100)))
```

### Unique Memory Databases

```python
# Default: Unique memory DB per instance
config = SqliteConfig()  # Uses file:memory_{uuid}?mode=memory&cache=private

# Each config gets isolated memory database
config1 = SqliteConfig()
config2 = SqliteConfig()

with config1.provide_session() as session1:
    session1.execute("CREATE TABLE users (id INTEGER)")

with config2.provide_session() as session2:
    # Different database - users table doesn't exist here
    session2.execute("CREATE TABLE products (id INTEGER)")
```

## Custom Type Adapters

### JSON Support

```python
config = SqliteConfig(
    driver_features={
        "enable_custom_adapters": True,  # Default
        "json_serializer": to_json,  # Custom if needed
        "json_deserializer": from_json,
    }
)

# JSON columns automatically serialized/deserialized
session.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        metadata TEXT  -- Stores JSON
    )
""")

session.execute(
    "INSERT INTO users (id, metadata) VALUES (?, ?)",
    1, {"role": "admin", "tags": ["python", "sql"]}
)

result = session.execute("SELECT metadata FROM users WHERE id = ?", 1).one()
metadata = result["metadata"]  # Automatically deserialized dict
```

### UUID Support

```python
from uuid import uuid4

# UUIDs automatically converted to strings
config = SqliteConfig(
    driver_features={"enable_custom_adapters": True}
)

user_id = uuid4()
session.execute(
    "INSERT INTO users (id, name) VALUES (?, ?)",
    user_id, "Alice"
)

# UUID strings automatically converted back to UUID objects
result = session.execute(
    "SELECT id FROM users WHERE name = ?", "Alice"
).one()
assert isinstance(result["id"], uuid.UUID)
```

### Datetime Support

```python
from datetime import datetime

# Datetimes automatically serialized as ISO 8601 strings
config = SqliteConfig(
    driver_features={"enable_custom_adapters": True}
)

now = datetime.now()
session.execute(
    "INSERT INTO events (timestamp, event) VALUES (?, ?)",
    now, "user_login"
)

# Strings automatically converted back to datetime objects
result = session.execute(
    "SELECT timestamp FROM events WHERE event = ?", "user_login"
).one()
assert isinstance(result["timestamp"], datetime)
```

## URI Mode

### Memory Databases

```python
# Shared memory database (multiple connections see same data)
config = SqliteConfig(
    connection_config={
        "database": "file:memdb1?mode=memory&cache=shared",
        "uri": True,
    }
)

# Private memory database (isolated)
config = SqliteConfig(
    connection_config={
        "database": "file:memdb2?mode=memory&cache=private",
        "uri": True,
    }
)
```

### Read-Only Databases

```python
config = SqliteConfig(
    connection_config={
        "database": "file:app.db?mode=ro",
        "uri": True,
    }
)

# All writes will fail
with config.provide_session() as session:
    result = session.execute("SELECT * FROM users").all()  # OK
    session.execute("INSERT INTO users VALUES (1, 'Alice')")  # Raises error
```

## Arrow Integration

### Native Arrow Export

```python
# Export query results to Arrow (zero-copy when possible)
result = session.execute("SELECT * FROM large_table")
arrow_table = result.to_arrow()

# Use with pandas
df = arrow_table.to_pandas()

# Use with polars
import polars as pl
polars_df = pl.from_arrow(arrow_table)
```

### Native Arrow Import

```python
import pyarrow as pa

# Create Arrow table
data = pa.table({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
})

# Import to SQLite
session.execute("CREATE TABLE users (id INTEGER, name TEXT)")
# Use storage adapter for bulk import
from sqlspec import StorageConfig
storage = StorageConfig(config)
storage.import_arrow("users", data)
```

## Performance Features

### Statement Caching

```python
config = SqliteConfig(
    connection_config={
        "cached_statements": 256,  # Cache 256 prepared statements
    }
)

# Repeated queries use cached statements
for user_id in range(1000):
    session.execute(
        "SELECT * FROM users WHERE id = ?", user_id
    ).one()
```

### Transactions

```python
# Manual transaction control
session.execute("BEGIN IMMEDIATE")
try:
    session.execute("INSERT INTO users (name) VALUES (?)", "Alice")
    session.execute("INSERT INTO audit_log (action) VALUES (?)", "user_created")
    session.execute("COMMIT")
except Exception:
    session.execute("ROLLBACK")
    raise

# Context manager (autocommit disabled)
config = SqliteConfig(
    connection_config={
        "isolation_level": "DEFERRED",  # Enable transaction mode
    }
)
```

### Indexes

```python
# Create indexes for query performance
session.execute("""
    CREATE INDEX idx_users_email ON users(email)
""")

session.execute("""
    CREATE INDEX idx_users_status_created ON users(status, created_at)
""")

# Query uses index
result = session.execute(
    "SELECT * FROM users WHERE email = ?", "alice@example.com"
).one()
```

## Best Practices

1. **Use file databases for persistence** - Avoid `:memory:` for production data
2. **Enable custom adapters** - Default `True` for JSON/UUID/datetime support
3. **Set appropriate timeout** - Default 5s, increase for write-heavy workloads
4. **Use transactions explicitly** - Set `isolation_level` for ACID guarantees
5. **Create indexes** - Essential for query performance on large tables
6. **Use URI mode** - Enables advanced features (read-only, shared cache)
7. **Cache statements** - Increase `cached_statements` for repeated queries
8. **Thread-local pooling** - Pool handles thread safety automatically
9. **Regular VACUUM** - Reclaim space after large deletes
10. **Use PRAGMA settings** - Tune `journal_mode`, `synchronous`, `cache_size`

## Common Issues

### "Database is locked"

Increase timeout or use WAL mode:
```python
config = SqliteConfig(
    connection_config={
        "timeout": 30.0,  # Wait up to 30s for locks
    }
)

# Enable WAL mode for better concurrency
with config.provide_session() as session:
    session.execute("PRAGMA journal_mode=WAL")
```

### "Cannot use multiple connections"

SQLite file databases support multiple readers, one writer:
```python
# Use WAL mode for concurrent reads and writes
session.execute("PRAGMA journal_mode=WAL")
session.execute("PRAGMA synchronous=NORMAL")
```

### "Type adapter not working"

Ensure custom adapters enabled:
```python
config = SqliteConfig(
    driver_features={
        "enable_custom_adapters": True,  # Must be True
    }
)
```

### "URI mode not detected"

Explicitly enable URI mode:
```python
config = SqliteConfig(
    connection_config={
        "database": "file:app.db?mode=ro",
        "uri": True,  # Required
    }
)
```

### "Performance degradation"

Regular maintenance:
```python
# Rebuild indexes and reclaim space
session.execute("VACUUM")
session.execute("ANALYZE")

# Optimize settings
session.execute("PRAGMA journal_mode=WAL")
session.execute("PRAGMA synchronous=NORMAL")
session.execute("PRAGMA cache_size=-64000")  # 64MB cache
session.execute("PRAGMA temp_store=MEMORY")
```

## Performance Benchmarks

Compared to other embedded databases:

- **SQLite**: Baseline (excellent read performance)
- **DuckDB**: 10-100x faster for analytics (OLAP optimized)
- **PostgreSQL**: 2-3x faster writes (network overhead negligible for local)

SQLite performance characteristics:
- Reads: Excellent (100K+ reads/sec)
- Writes: Moderate (10-50K writes/sec with WAL)
- Concurrency: Limited (multiple readers, single writer)
- File size: Efficient (compact storage)

Best for:
- Read-heavy workloads
- Embedded applications
- Testing environments
- Single-user applications

Not ideal for:
- Heavy write concurrency (use PostgreSQL)
- Multi-user web applications (use PostgreSQL/MySQL)
- Analytics workloads (use DuckDB)
