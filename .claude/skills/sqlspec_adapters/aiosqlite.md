# AioSQLite Adapter Skill

**Adapter:** SQLite (Async, Embedded RDBMS)
**Category:** Database Adapter
**Status:** Active

## Description

Expert guidance for using SQLSpec's AioSQLite adapter for asynchronous SQLite operations. AioSQLite wraps the standard sqlite3 module with async/await support, enabling non-blocking database operations in async web frameworks.

Ideal for async web applications (Litestar, FastAPI, Starlette) that need embedded databases, local caching, or isolated test databases without blocking the event loop.

## When to Use AioSQLite

- **Async web applications** - Litestar, FastAPI, Starlette, Sanic
- **Non-blocking I/O** - Avoid blocking event loop with sync SQLite
- **Testing async code** - Fast, isolated test databases
- **Local caching** - Async cache backend for web apps
- **Session storage** - Store user sessions in embedded database
- **Job queues** - Lightweight task queue with SQLite backend
- **Prototyping** - Quick async app development without PostgreSQL
- **Serverless functions** - Embedded database in Lambda/Cloud Functions

## Configuration

```python
from sqlspec.adapters.aiosqlite import (
    AiosqliteConfig,
    AiosqliteDriverFeatures,
)

config = AiosqliteConfig(
    pool_config={
        # Database path
        "database": "file::memory:?cache=shared",  # Default shared memory
        # OR: "app.db",  # File-based database
        # OR: "/path/to/data.db",  # Absolute path

        # Connection settings (same as sync SQLite)
        "timeout": 5.0,  # Lock timeout in seconds
        "detect_types": 0,  # sqlite3.PARSE_DECLTYPES | PARSE_COLNAMES
        "isolation_level": None,  # None = autocommit
        "check_same_thread": False,  # aiosqlite handles thread safety
        "cached_statements": 128,  # Statement cache size
        "uri": True,  # Enable URI mode (auto-enabled for file: URIs)

        # Async pool settings
        "pool_size": 5,  # Number of connections in pool
        "connect_timeout": 30.0,  # Connection acquisition timeout
        "idle_timeout": 86400.0,  # Close idle connections after 24h
        "operation_timeout": 10.0,  # Query execution timeout
    },
    driver_features=AiosqliteDriverFeatures(
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
result = await session.execute(
    "SELECT * FROM users WHERE id = ?",
    user_id
)

# Multiple parameters
result = await session.execute(
    "SELECT * FROM users WHERE status = ? AND age > ?",
    "active", 18
)

# Named parameters NOT supported - use positional
result = await session.execute(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    "Alice", "alice@example.com"
)
```

## Async Connection Pooling

### Connection Pool Management

```python
# Configure async pool
config = AiosqliteConfig(
    pool_config={
        "database": "app.db",
        "pool_size": 10,  # 10 concurrent connections
        "connect_timeout": 30.0,  # Wait up to 30s for connection
        "idle_timeout": 3600.0,  # Close idle after 1h
        "operation_timeout": 10.0,  # Query timeout
    }
)

# Pool created lazily on first use
async with config.provide_session() as session:
    result = await session.execute("SELECT * FROM users").all()

# Cleanup pool on shutdown
await config.close_pool()
```

### Shared Cache Memory Database

```python
# Shared memory database (default)
config = AiosqliteConfig(
    pool_config={
        "database": "file::memory:?cache=shared",  # All connections see same data
        "uri": True,
    }
)

# Multiple connections share same memory database
async def query1():
    async with config.provide_session() as session:
        await session.execute("CREATE TABLE users (id INTEGER)")

async def query2():
    async with config.provide_session() as session:
        # Can see users table created in query1
        await session.execute("SELECT * FROM users")

await asyncio.gather(query1(), query2())
```

## Custom Type Adapters

### JSON Support

```python
config = AiosqliteConfig(
    driver_features={
        "enable_custom_adapters": True,  # Default
        "json_serializer": to_json,
        "json_deserializer": from_json,
    }
)

# JSON columns automatically serialized/deserialized
await session.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        metadata TEXT  -- Stores JSON
    )
""")

await session.execute(
    "INSERT INTO users (id, metadata) VALUES (?, ?)",
    1, {"role": "admin", "tags": ["python", "async"]}
)

result = await session.execute(
    "SELECT metadata FROM users WHERE id = ?", 1
).one()
metadata = result["metadata"]  # Automatically deserialized dict
```

### UUID Support

```python
from uuid import uuid4

# UUIDs automatically converted to/from strings
config = AiosqliteConfig(
    driver_features={"enable_custom_adapters": True}
)

user_id = uuid4()
await session.execute(
    "INSERT INTO users (id, name) VALUES (?, ?)",
    user_id, "Alice"
)

result = await session.execute(
    "SELECT id FROM users WHERE name = ?", "Alice"
).one()
assert isinstance(result["id"], uuid.UUID)
```

### Datetime Support

```python
from datetime import datetime

# Datetimes automatically serialized as ISO 8601 strings
now = datetime.now()
await session.execute(
    "INSERT INTO events (timestamp, event) VALUES (?, ?)",
    now, "user_login"
)

result = await session.execute(
    "SELECT timestamp FROM events WHERE event = ?", "user_login"
).one()
assert isinstance(result["timestamp"], datetime)
```

## Async Framework Integration

### Litestar

```python
from litestar import Litestar
from litestar.contrib.sqlspec import SQLSpecConfig, SQLSpecPlugin

sqlspec_config = SQLSpecConfig(
    configs=[
        AiosqliteConfig(
            pool_config={"database": "app.db", "pool_size": 10},
            extension_config={
                "litestar": {
                    "commit_mode": "autocommit",
                    "session_key": "db",
                }
            }
        )
    ]
)

app = Litestar(
    route_handlers=[...],
    plugins=[SQLSpecPlugin(config=sqlspec_config)],
)

# Use in route handlers
from litestar import get
from sqlspec.adapters.aiosqlite import AiosqliteDriver

@get("/users/{user_id:int}")
async def get_user(user_id: int, db: AiosqliteDriver) -> dict:
    result = await db.execute(
        "SELECT * FROM users WHERE id = ?", user_id
    ).one()
    return result
```

### FastAPI

```python
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager

config = AiosqliteConfig(
    pool_config={"database": "app.db"}
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    yield
    # Shutdown
    await config.close_pool()

app = FastAPI(lifespan=lifespan)

async def get_db():
    async with config.provide_session() as session:
        yield session

@app.get("/users/{user_id}")
async def get_user(user_id: int, db = Depends(get_db)):
    result = await db.execute(
        "SELECT * FROM users WHERE id = ?", user_id
    ).one()
    return result
```

### Starlette

```python
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse

config = AiosqliteConfig(
    pool_config={"database": "app.db"},
    extension_config={
        "starlette": {
            "commit_mode": "autocommit",
            "session_key": "db",
        }
    }
)

app = Starlette()

@app.route("/users/{user_id:int}")
async def get_user(request: Request):
    db = request.state.db
    result = await db.execute(
        "SELECT * FROM users WHERE id = ?",
        request.path_params["user_id"]
    ).one()
    return JSONResponse(result)
```

## Arrow Integration

### Native Arrow Export

```python
# Export query results to Arrow (zero-copy when possible)
result = await session.execute("SELECT * FROM large_table")
arrow_table = result.to_arrow()

# Use with pandas
df = arrow_table.to_pandas()

# Use with polars
import polars as pl
polars_df = pl.from_arrow(arrow_table)
```

## Performance Features

### Connection Pool Tuning

```python
# Optimize pool for workload
config = AiosqliteConfig(
    pool_config={
        "database": "app.db",
        "pool_size": 20,  # High concurrency
        "connect_timeout": 60.0,  # Long timeout for slow startup
        "idle_timeout": 1800.0,  # Keep connections for 30min
        "operation_timeout": 30.0,  # Long-running queries
    }
)
```

### WAL Mode for Concurrency

```python
config = AiosqliteConfig(
    pool_config={"database": "app.db"}
)

# Enable WAL mode on startup
async with config.provide_session() as session:
    await session.execute("PRAGMA journal_mode=WAL")
    await session.execute("PRAGMA synchronous=NORMAL")
    await session.execute("PRAGMA cache_size=-64000")  # 64MB cache
```

### Statement Caching

```python
config = AiosqliteConfig(
    pool_config={
        "cached_statements": 256,  # Cache 256 prepared statements
    }
)

# Repeated queries use cached statements
for user_id in range(1000):
    await session.execute(
        "SELECT * FROM users WHERE id = ?", user_id
    ).one()
```

## Best Practices

1. **Use shared cache** - Default `file::memory:?cache=shared` for async concurrency
2. **Enable custom adapters** - Default `True` for JSON/UUID/datetime support
3. **Set pool size** - Match to expected concurrent requests (default: 5)
4. **Enable WAL mode** - Better concurrency for file-based databases
5. **Close pool on shutdown** - Call `await config.close_pool()` in cleanup
6. **Use appropriate timeouts** - Balance responsiveness vs. query complexity
7. **Create indexes** - Essential for query performance
8. **Avoid blocking operations** - Use async/await throughout
9. **Test with realistic concurrency** - Simulate production load
10. **Monitor pool usage** - Check connection acquisition times

## Common Issues

### "Database is locked"

Enable WAL mode or increase timeout:
```python
config = AiosqliteConfig(
    pool_config={
        "timeout": 30.0,  # Wait longer for locks
    }
)

# Enable WAL mode
async with config.provide_session() as session:
    await session.execute("PRAGMA journal_mode=WAL")
```

### "Pool exhausted"

Increase pool size:
```python
config = AiosqliteConfig(
    pool_config={
        "pool_size": 20,  # More connections
        "connect_timeout": 60.0,  # Wait longer
    }
)
```

### "Operation timeout"

Increase operation timeout for slow queries:
```python
config = AiosqliteConfig(
    pool_config={
        "operation_timeout": 30.0,  # 30s for slow queries
    }
)
```

### "Pool not closed"

Ensure cleanup on shutdown:
```python
# Litestar - automatic cleanup
# FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await config.close_pool()

# Starlette
@app.on_event("shutdown")
async def shutdown():
    await config.close_pool()
```

### "Type adapter not working"

Ensure custom adapters enabled:
```python
config = AiosqliteConfig(
    driver_features={
        "enable_custom_adapters": True,  # Must be True
    }
)
```

## Performance Benchmarks

Compared to other async database adapters:

- **AioSQLite**: Baseline (async wrapper around sync sqlite3)
- **AsyncPG**: 5-10x faster (native async PostgreSQL protocol)
- **Psycopg (async)**: 3-5x faster (native async PostgreSQL)

AioSQLite performance characteristics:
- Reads: Good (10-50K reads/sec)
- Writes: Moderate (5-20K writes/sec with WAL)
- Concurrency: Limited by GIL (thread pool executor)
- Latency: Low (in-process, no network)

Best for:
- Embedded async applications
- Low-traffic async web apps (< 1K concurrent users)
- Testing async code
- Local caching layers

Not ideal for:
- High-concurrency web apps (use AsyncPG/PostgreSQL)
- Heavy write workloads (use PostgreSQL/MySQL)
- Analytics workloads (use DuckDB)
- Production systems with strict SLAs (use PostgreSQL)

Use AsyncPG/Psycopg for production async web applications requiring high concurrency.
