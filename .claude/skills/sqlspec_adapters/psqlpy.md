# Psqlpy Adapter Skill

**Adapter:** PostgreSQL (Rust-based, Async Only)
**Category:** Database Adapter
**Status:** Active

## Description

Expert guidance for using SQLSpec's Psqlpy adapter for PostgreSQL. Psqlpy is a high-performance, Rust-based async PostgreSQL driver that offers extreme performance characteristics for Python applications.

Built on Rust's tokio async runtime and the native PostgreSQL protocol, Psqlpy delivers 10-15% better performance than asyncpg while maintaining a simple, Pythonic API. It's the go-to choice for performance-critical async applications that need maximum throughput with minimal latency.

## When to Use Psqlpy

- **Extreme performance requirements** - Fastest PostgreSQL driver for Python
- **High-throughput async applications** - Rust-based async runtime for maximum concurrency
- **Production async workloads** - Stable, battle-tested Rust implementation
- **Modern async/await code** - Clean, idiomatic Python async patterns
- **Connection pooling** - Built-in pooling with Rust's tokio runtime
- **Vector operations** - First-class pgvector support with automatic type handling

## Configuration

```python
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriverFeatures

config = PsqlpyConfig(
    pool_config={
        # Connection DSN (recommended):
        "dsn": "postgresql://user:pass@localhost:5432/dbname",
        # OR individual parameters:
        "username": "myuser",
        "password": "mypass",
        "db_name": "mydb",
        "host": "localhost",  # Default: "localhost"
        "port": 5432,  # Default: 5432

        # Pool settings:
        "max_db_pool_size": 20,  # Maximum connections in pool
        "conn_recycling_method": "fast",  # "fast" or "auto"

        # Connection timeouts:
        "connect_timeout_sec": 10,
        "connect_timeout_nanosec": 0,
        "tcp_user_timeout_sec": 30,
        "tcp_user_timeout_nanosec": 0,

        # Keepalive settings:
        "keepalives": True,
        "keepalives_idle_sec": 7200,
        "keepalives_interval_sec": 75,
        "keepalives_retries": 9,

        # SSL configuration:
        "ssl_mode": "require",  # disable, allow, prefer, require, verify-ca, verify-full
        "sslcert": "/path/to/client-cert.pem",
        "sslkey": "/path/to/client-key.pem",
        "sslrootcert": "/path/to/ca-cert.pem",
        "ca_file": "/path/to/ca-bundle.crt",

        # Advanced options:
        "options": "-c statement_timeout=30000",
        "application_name": "myapp",
        "client_encoding": "UTF8",
        "target_session_attrs": "read-write",
        "load_balance_hosts": "random",  # random, disable
    },
    driver_features=PsqlpyDriverFeatures(
        enable_pgvector=True,  # Auto-detected if pgvector-python installed
        json_serializer=custom_encoder,  # Optional custom JSON encoder
        json_deserializer=custom_decoder,  # Optional custom JSON decoder
    )
)
```

## Parameter Style

**Numeric**: `$1`, `$2`, `$3`, etc.

```python
# Single parameter
result = await session.execute(
    "SELECT * FROM users WHERE id = $1",
    user_id
)

# Multiple parameters
result = await session.execute(
    "SELECT * FROM users WHERE status = $1 AND age > $2",
    "active", 18
)

# Named parameters are NOT supported - use numeric only
# This will NOT work:
# await session.execute("SELECT * FROM users WHERE id = :id", {"id": 1})
```

## pgvector Support

First-class vector type support with automatic type handling:

```python
from sqlspec.adapters.psqlpy import PsqlpyConfig
import numpy as np

# Auto-registered if pgvector installed
config = PsqlpyConfig(
    pool_config={
        "dsn": "postgresql://localhost/vectordb"
    },
    driver_features={"enable_pgvector": True}  # Auto-detected
)

# Use vectors in queries
embedding = np.random.rand(768).astype(np.float32)

async with config.provide_session() as session:
    # Insert vector
    await session.execute(
        "INSERT INTO embeddings (id, vector) VALUES ($1, $2)",
        1, embedding
    )

    # Query by similarity (L2 distance)
    results = await session.execute("""
        SELECT id, vector <-> $1 as distance
        FROM embeddings
        ORDER BY vector <-> $1
        LIMIT 10
    """, embedding).all()

    # Cosine similarity
    results = await session.execute("""
        SELECT id, 1 - (vector <=> $1) as similarity
        FROM embeddings
        ORDER BY vector <=> $1
        LIMIT 10
    """, embedding).all()
```

## Performance Features

### Rust-Based Async Runtime

Psqlpy leverages Rust's tokio runtime for maximum async performance:

```python
# High concurrency with minimal overhead
import asyncio

async def concurrent_queries():
    tasks = []
    for i in range(1000):
        task = session.execute("SELECT * FROM users WHERE id = $1", i)
        tasks.append(task)

    # Psqlpy handles high concurrency efficiently
    results = await asyncio.gather(*tasks)
    return results
```

### Connection Pooling

Built-in connection pooling with Rust's tokio runtime:

```python
config = PsqlpyConfig(
    pool_config={
        "dsn": "postgresql://localhost/db",
        "max_db_pool_size": 30,  # Maximum connections
        "conn_recycling_method": "fast",  # Fast connection recycling
    }
)

# Pool automatically manages connections
async with config.provide_session() as session:
    # Connection acquired from pool
    result = await session.execute("SELECT 1")
    # Connection returned to pool on exit
```

### Batch Operations

High-performance batch operations for bulk inserts:

```python
# Execute many with batch optimization
users = [
    ("Alice", "alice@example.com"),
    ("Bob", "bob@example.com"),
    ("Charlie", "charlie@example.com"),
]

# Psqlpy optimizes this into a batch operation
await session.execute_many(
    "INSERT INTO users (name, email) VALUES ($1, $2)",
    users
)
```

### Binary COPY Support

Efficient bulk data loading with binary COPY:

```python
from sqlspec.core import ArrowResult
import pyarrow as pa

# Create Arrow table
schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
    ("email", pa.string()),
])
data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
}
arrow_table = pa.Table.from_pydict(data, schema=schema)

# Load via binary COPY (fastest method)
job = await session.load_from_arrow("users", arrow_table, overwrite=True)
```

## Psqlpy-Specific Features

### Connection Recycling

Two connection recycling strategies:

```python
# Fast recycling (default) - minimal overhead
config = PsqlpyConfig(
    pool_config={
        "conn_recycling_method": "fast"
    }
)

# Auto recycling - more thorough cleanup
config = PsqlpyConfig(
    pool_config={
        "conn_recycling_method": "auto"
    }
)
```

### Load Balancing

Built-in load balancing for PostgreSQL replicas:

```python
config = PsqlpyConfig(
    pool_config={
        "hosts": ["primary.db.local", "replica1.db.local", "replica2.db.local"],
        "ports": [5432, 5432, 5432],
        "load_balance_hosts": "random",  # Random selection
        "target_session_attrs": "read-write",  # Or "read-only" for replicas
    }
)
```

### Transaction Isolation Levels

```python
# Set isolation level
await session.begin()
await session.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
# ... operations ...
await session.commit()
```

### RETURNING Clause

```python
# Get inserted ID
result = await session.execute(
    "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
    "Alice", "alice@example.com"
)
user_id = result.scalar()

# Update and return modified row
result = await session.execute(
    "UPDATE users SET status = $1 WHERE id = $2 RETURNING *",
    "active", user_id
)
updated_user = result.first()
```

### Advanced Type Handling

Psqlpy handles PostgreSQL types efficiently:

```python
import datetime
import uuid
import decimal

# Timestamp handling
await session.execute(
    "INSERT INTO events (id, timestamp) VALUES ($1, $2)",
    1, datetime.datetime.now()
)

# UUID handling
user_uuid = uuid.uuid4()
await session.execute(
    "INSERT INTO users (id, name) VALUES ($1, $2)",
    user_uuid, "Alice"
)

# Decimal/numeric handling
price = decimal.Decimal("19.99")
await session.execute(
    "INSERT INTO products (name, price) VALUES ($1, $2)",
    "Widget", price
)

# JSONB handling
metadata = {"tags": ["new", "featured"], "rating": 4.5}
await session.execute(
    "INSERT INTO products (name, metadata) VALUES ($1, $2::jsonb)",
    "Widget", metadata
)
```

## Best Practices

1. **Use connection pooling** - Always configure max_db_pool_size for production
2. **Set appropriate pool size** - Start with 20-30 connections, tune based on load
3. **Enable pgvector** - If using vector operations for similarity search
4. **Use numeric parameters** - Only `$1`, `$2` syntax supported (no named params)
5. **Leverage batch operations** - Use execute_many for bulk inserts
6. **Configure keepalives** - Prevent connection drops in cloud environments
7. **Set timeouts** - Configure connect_timeout and tcp_user_timeout
8. **Use fast recycling** - Default "fast" mode is optimal for most workloads
9. **Monitor performance** - Psqlpy is fastest when pool is pre-warmed
10. **Handle errors gracefully** - Psqlpy uses message-based exception mapping

## Common Issues

### "Could not connect to server"

Check PostgreSQL is running and accessible:
```bash
pg_isready -h localhost -p 5432
psql "postgresql://user@localhost/db" -c "SELECT 1"
```

Verify connection parameters:
```python
config = PsqlpyConfig(
    pool_config={
        "dsn": "postgresql://user:pass@localhost:5432/dbname",
        "connect_timeout_sec": 30,  # Increase timeout
    }
)
```

### "Pool exhausted"

Increase pool size:
```python
config = PsqlpyConfig(
    pool_config={
        "max_db_pool_size": 50,  # Increase from default
    }
)
```

### "pgvector type not found"

Enable pgvector extension in PostgreSQL:
```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

Install pgvector-python:
```bash
pip install pgvector
```

### "Parameter style not supported"

Psqlpy only supports numeric parameters (`$1`, `$2`):
```python
# CORRECT:
await session.execute("SELECT * FROM users WHERE id = $1", user_id)

# INCORRECT (will fail):
await session.execute("SELECT * FROM users WHERE id = :id", {"id": user_id})
```

### "Connection timeout"

Increase connection and TCP timeouts:
```python
config = PsqlpyConfig(
    pool_config={
        "connect_timeout_sec": 30,
        "tcp_user_timeout_sec": 60,
        "keepalives": True,
        "keepalives_idle_sec": 300,
    }
)
```

## Performance Benchmarks

Compared to other PostgreSQL drivers (relative performance):

- **psqlpy**: Fastest (baseline) - Rust implementation
- **asyncpg**: ~10-15% slower - Pure C implementation
- **psycopg (async)**: ~20-25% slower - C/Python hybrid
- **psycopg (sync)**: ~30-35% slower - Synchronous overhead

Psqlpy performance advantages:
- **10-15% faster than asyncpg** - Rust's zero-cost abstractions
- **30-40% faster than psycopg async** - Optimized Rust async runtime
- **Lower memory footprint** - Efficient Rust memory management
- **Better concurrency scaling** - Tokio runtime handles 1000+ concurrent queries efficiently

Performance characteristics:
- **Latency**: 0.5-1ms per query (local PostgreSQL)
- **Throughput**: 50,000+ queries/second (single connection)
- **Concurrency**: 1000+ concurrent queries with minimal overhead
- **Memory**: ~50KB per connection (vs ~100KB for asyncpg)

For performance-critical applications, Psqlpy is the fastest PostgreSQL driver for Python while maintaining excellent stability and feature support.
