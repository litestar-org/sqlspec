# AsyncPG Adapter Skill

**Adapter:** PostgreSQL (Async, High Performance)
**Category:** Database Adapter
**Status:** Active

## Description

Expert guidance for using SQLSpec's AsyncPG adapter for PostgreSQL. AsyncPG is the fastest and most mature async PostgreSQL driver for Python.

## When to Use AsyncPG

- **Async web applications** (Litestar, FastAPI, Starlette)
- **High-performance requirements** (fastest PostgreSQL driver)
- **Modern async/await code**
- **Connection pooling** (excellent pool management)
- **PostgreSQL-specific features** (LISTEN/NOTIFY, COPY, etc.)

## Configuration

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriverFeatures

config = AsyncpgConfig(
    pool_config={
        "dsn": "postgresql://user:pass@localhost:5432/dbname",
        # OR individual parameters:
        "host": "localhost",
        "port": 5432,
        "user": "myuser",
        "password": "mypass",
        "database": "mydb",
        # Pool settings:
        "min_size": 10,
        "max_size": 20,
        "max_queries": 50000,
        "max_inactive_connection_lifetime": 300.0,
        "timeout": 60.0,
    },
    driver_features=AsyncpgDriverFeatures(
        enable_pgvector=True,  # Auto-detected
        enable_json_codecs=True,  # Default: True
        json_serializer=custom_encoder,  # Optional
        json_deserializer=custom_decoder,  # Optional
        # Google Cloud connectors (mutually exclusive):
        enable_cloud_sql=False,
        cloud_sql_instance="project:region:instance",
        cloud_sql_enable_iam_auth=False,
        cloud_sql_ip_type="PRIVATE",  # or "PUBLIC", "PSC"
        enable_alloydb=False,
        alloydb_instance_uri="projects/.../instances/...",
        alloydb_enable_iam_auth=False,
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
```

## Google Cloud Integration

### Cloud SQL with IAM Authentication

```python
config = AsyncpgConfig(
    pool_config={
        "user": "my-service-account@project.iam",
        "database": "mydb",
    },
    driver_features={
        "enable_cloud_sql": True,
        "cloud_sql_instance": "my-project:us-central1:my-instance",
        "cloud_sql_enable_iam_auth": True,
        "cloud_sql_ip_type": "PRIVATE",
    }
)
```

### AlloyDB

```python
config = AsyncpgConfig(
    pool_config={
        "user": "my-service-account@project.iam",
        "database": "mydb",
    },
    driver_features={
        "enable_alloydb": True,
        "alloydb_instance_uri": "projects/my-project/locations/us-central1/clusters/my-cluster/instances/my-instance",
        "alloydb_enable_iam_auth": True,
    }
)
```

## pgvector Support

Automatic vector type support when `pgvector-python` installed:

```python
from pgvector.asyncpg import register_vector

# Auto-registered if pgvector installed
config = AsyncpgConfig(
    pool_config={...},
    driver_features={"enable_pgvector": True}  # Auto-detected
)

# Use vectors in queries
import numpy as np

embedding = np.random.rand(768).astype(np.float32)

await session.execute(
    "INSERT INTO embeddings (id, vector) VALUES ($1, $2)",
    1, embedding
)

# Query by similarity
results = await session.execute("""
    SELECT id, 1 - (vector <=> $1) as similarity
    FROM embeddings
    ORDER BY vector <=> $1
    LIMIT 10
""", embedding).all()
```

## Performance Features

### Native Pipeline Support

```python
from sqlspec import StatementStack

# Execute in single round-trip using native pipeline
stack = (
    StatementStack()
    .push_execute("INSERT INTO audit_log (message) VALUES ($1)", ("login",))
    .push_execute("UPDATE users SET last_login = NOW() WHERE id = $1", (user_id,))
    .push_execute("SELECT permissions FROM user_permissions WHERE user_id = $1", (user_id,))
)

results = await session.execute_stack(stack)
```

### Connection Pooling

```python
config = AsyncpgConfig(
    pool_config={
        "dsn": "postgresql://localhost/db",
        "min_size": 10,  # Keep 10 connections ready
        "max_size": 20,  # Allow up to 20 total
        "max_queries": 50000,  # Recycle after 50k queries
        "max_inactive_connection_lifetime": 300,  # Close idle after 5 min
        "timeout": 60.0,  # Acquisition timeout
    }
)
```

### COPY Operations (Bulk Import)

```python
# Bulk insert using COPY (fastest method)
import io

data = io.StringIO()
for user in users:
    data.write(f"{user['name']}\t{user['email']}\n")
data.seek(0)

await session.connection.copy_to_table(
    "users",
    source=data,
    columns=["name", "email"],
    format="text"
)
```

## PostgreSQL-Specific Features

### LISTEN/NOTIFY

```python
async def listen_for_notifications():
    async with config.provide_connection() as connection:
        await connection.add_listener("channel_name", callback)
        # Keep connection alive
        await asyncio.sleep(3600)

def callback(connection, pid, channel, payload):
    print(f"Received: {payload}")
```

### Transaction Isolation Levels

```python
# Serializable transactions
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
```

## Best Practices

1. **Use connection pooling** - Essential for production
2. **Set appropriate pool size** - Start with min=10, max=20
3. **Enable pgvector** - If using vector operations
4. **Use statement stacks** - Reduce round-trips for multiple operations
5. **Leverage COPY** - For bulk inserts (10-100x faster)
6. **Monitor pool health** - Check connection reuse
7. **Use numeric parameters** - `$1`, `$2` syntax
8. **Enable JSON codecs** - For JSONB columns

## Common Issues

### "Could not connect to server"

Check PostgreSQL is running and accessible:
```bash
pg_isready -h localhost -p 5432
```

### "Pool is exhausted"

Increase pool size or reduce connection lifetime:
```python
config = AsyncpgConfig(
    pool_config={
        "max_size": 40,  # Increase
        "timeout": 120.0,  # Longer timeout
    }
)
```

### "pgvector type not found"

Enable pgvector extension in PostgreSQL:
```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

If error persists, gracefully degrades - check logs for DEBUG message.

## Performance Benchmarks

Compared to other PostgreSQL drivers:
- **asyncpg**: Fastest (baseline)
- **psycopg (async)**: ~20-30% slower
- **psqlpy**: ~10-15% faster (Rust-based)

For most applications, AsyncPG provides excellent performance/maturity balance.
