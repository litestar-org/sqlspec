# Psycopg Adapter Skill

**Adapter:** PostgreSQL (Psycopg3, Sync & Async)
**Category:** Database Adapter
**Status:** Active

## Description

Expert guidance for using SQLSpec's Psycopg adapter for PostgreSQL. Psycopg 3 is the most feature-rich and versatile PostgreSQL driver for Python, offering both synchronous and asynchronous support with excellent production stability.

Psycopg 3 combines battle-tested reliability with modern features like connection pooling, pipeline mode for batched operations, native PostgreSQL COPY support, and comprehensive type handling. It's the ideal choice for applications requiring flexibility between sync and async patterns or needing PostgreSQL's advanced features.

## When to Use Psycopg

- **Dual sync/async codebases** - Same adapter for both patterns
- **Production stability** - Mature, widely deployed, enterprise-ready
- **PostgreSQL-specific features** - Full LISTEN/NOTIFY, COPY, prepared statements
- **Gradual async migration** - Use sync initially, migrate to async incrementally
- **Connection pooling** - Production-grade pool with extensive configuration
- **Framework integration** - Works with Flask (sync) and Litestar/FastAPI (async)

## Configuration

### Async Configuration

```python
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgDriverFeatures

config = PsycopgAsyncConfig(
    pool_config={
        # Connection string (recommended):
        "conninfo": "postgresql://user:pass@localhost:5432/dbname",
        # OR individual parameters:
        "host": "localhost",
        "port": 5432,
        "user": "myuser",
        "password": "mypass",
        "dbname": "mydb",

        # SSL settings:
        "sslmode": "require",  # disable, allow, prefer, require, verify-ca, verify-full
        "sslcert": "/path/to/client-cert.pem",
        "sslkey": "/path/to/client-key.pem",
        "sslrootcert": "/path/to/ca-cert.pem",

        # Connection options:
        "connect_timeout": 10,
        "options": "-c statement_timeout=30000",
        "application_name": "myapp",

        # Pool settings:
        "min_size": 4,  # Default: 4
        "max_size": 20,  # Default: None (unlimited)
        "timeout": 30.0,  # Default: 30.0 seconds
        "max_waiting": 0,  # Default: 0 (unlimited queue)
        "max_lifetime": 3600.0,  # Default: 3600.0 seconds (1 hour)
        "max_idle": 600.0,  # Default: 600.0 seconds (10 minutes)
        "reconnect_timeout": 300.0,  # Default: 300.0 seconds (5 minutes)
        "num_workers": 3,  # Default: 3 background workers

        # Autocommit mode:
        "autocommit": False,  # Default: False
    },
    driver_features=PsycopgDriverFeatures(
        enable_pgvector=True,  # Auto-detected if pgvector-python installed
        json_serializer=custom_encoder,  # Optional custom JSON encoder
        json_deserializer=custom_decoder,  # Optional custom JSON decoder
    )
)
```

### Sync Configuration

```python
from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgDriverFeatures

config = PsycopgSyncConfig(
    pool_config={
        "conninfo": "postgresql://user:pass@localhost:5432/dbname",
        "min_size": 4,
        "max_size": 20,
        "timeout": 30.0,
        "max_lifetime": 3600.0,
        "autocommit": False,
    },
    driver_features=PsycopgDriverFeatures(
        enable_pgvector=True,
        json_serializer=custom_encoder,
    )
)
```

## Parameter Style

**Positional PyFormat**: `%s` (default) or **Named PyFormat**: `%(name)s`

```python
# Positional parameters (default)
result = await session.execute(
    "SELECT * FROM users WHERE id = %s",
    user_id
)

# Multiple positional parameters
result = await session.execute(
    "SELECT * FROM users WHERE status = %s AND age > %s",
    "active", 18
)

# Named parameters (pyformat)
result = await session.execute(
    "SELECT * FROM users WHERE status = %(status)s AND age > %(age)s",
    {"status": "active", "age": 18}
)

# Numeric parameters (also supported)
result = await session.execute(
    "SELECT * FROM users WHERE id = $1",
    user_id
)
```

## pgvector Support

Automatic vector type support when `pgvector-python` installed:

```python
from sqlspec.adapters.psycopg import PsycopgAsyncConfig

# Auto-registered if pgvector installed
config = PsycopgAsyncConfig(
    pool_config={
        "conninfo": "postgresql://localhost/vectordb"
    },
    driver_features={"enable_pgvector": True}  # Auto-detected
)

# Use vectors in queries
import numpy as np

embedding = np.random.rand(768).astype(np.float32)

async with config.provide_session() as session:
    # Insert vector
    await session.execute(
        "INSERT INTO embeddings (id, vector) VALUES (%s, %s)",
        1, embedding
    )

    # Query by similarity (cosine distance)
    results = await session.execute("""
        SELECT id, 1 - (vector <=> %s) as similarity
        FROM embeddings
        ORDER BY vector <=> %s
        LIMIT 10
    """, embedding, embedding).all()
```

## Performance Features

### Native Pipeline Support

Psycopg 3 supports native PostgreSQL pipeline mode for batched operations:

```python
from sqlspec import StatementStack

# Execute in single round-trip using native pipeline
stack = (
    StatementStack()
    .push_execute("INSERT INTO audit_log (message) VALUES (%s)", ("login",))
    .push_execute("UPDATE users SET last_login = NOW() WHERE id = %s", (user_id,))
    .push_execute("SELECT permissions FROM user_permissions WHERE user_id = %s", (user_id,))
)

# Single network round-trip with pipeline mode
results = await session.execute_stack(stack)
```

### Connection Pooling

Production-grade connection pooling with extensive configuration:

```python
config = PsycopgAsyncConfig(
    pool_config={
        "conninfo": "postgresql://localhost/db",
        "min_size": 10,  # Keep 10 connections ready
        "max_size": 40,  # Allow up to 40 total
        "max_lifetime": 3600.0,  # Recycle connections after 1 hour
        "max_idle": 600.0,  # Close idle connections after 10 minutes
        "timeout": 60.0,  # Connection acquisition timeout
        "num_workers": 3,  # Background pool maintenance workers
    }
)
```

### COPY Operations (Bulk Import/Export)

High-performance bulk data transfer using PostgreSQL COPY:

```python
# Bulk insert using COPY FROM STDIN (fastest method)
import io

data = io.StringIO()
for user in users:
    data.write(f"{user['name']}\t{user['email']}\n")
data.seek(0)

async with session.with_cursor(session.connection) as cursor:
    async with cursor.copy("COPY users (name, email) FROM STDIN") as copy:
        await copy.write(data.getvalue().encode())

# Bulk export using COPY TO STDOUT
output = []
async with cursor.copy("COPY users TO STDOUT") as copy:
    async for row in copy:
        output.append(row.decode())
```

## Psycopg-Specific Features

### LISTEN/NOTIFY

```python
# Async listener
async def listen_for_notifications():
    async with config.provide_connection() as connection:
        await connection.execute("LISTEN channel_name")

        # Process notifications
        async for notify in connection.notifies():
            print(f"Received: {notify.payload}")

# Synchronous listener
def listen_sync():
    with config.provide_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("LISTEN channel_name")

        for notify in connection.notifies():
            print(f"Received: {notify.payload}")
```

### Transaction Isolation Levels

```python
# Async transactions with isolation level
await session.begin()
await session.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
# ... operations ...
await session.commit()

# Sync transactions
session.begin()
session.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
# ... operations ...
session.commit()
```

### RETURNING Clause

```python
# Get inserted ID
result = await session.execute(
    "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id",
    "Alice", "alice@example.com"
)
user_id = result.scalar()

# Update and return modified row
result = await session.execute(
    "UPDATE users SET status = %s WHERE id = %s RETURNING *",
    "active", user_id
)
updated_user = result.first()
```

### Prepared Statements

```python
# Psycopg automatically uses prepared statements for repeated queries
for user_id in user_ids:
    # First call prepares, subsequent calls reuse
    result = await session.execute(
        "SELECT * FROM users WHERE id = %s",
        user_id
    )
```

### Binary Parameters

```python
# Binary data handling
binary_data = b'\x89PNG\r\n\x1a\n...'

await session.execute(
    "INSERT INTO files (name, data) VALUES (%s, %s)",
    "image.png", binary_data
)
```

## Best Practices

1. **Use connection pooling** - Essential for production (sync and async)
2. **Set appropriate pool size** - Start with min=10, max=20, tune based on load
3. **Enable pgvector** - If using vector operations for similarity search
4. **Use pipeline mode** - Reduce round-trips for multiple independent operations
5. **Leverage COPY** - For bulk inserts (10-100x faster than individual INSERTs)
6. **Monitor pool health** - Track connection reuse, idle time, and acquisition time
7. **Use parameter binding** - Always use `%s` syntax, never string formatting
8. **Set connection lifetime** - Prevent long-lived connection issues with max_lifetime
9. **Configure timeouts** - Set connect_timeout and statement_timeout in options
10. **Use autocommit wisely** - Disable for transactional workloads, enable for read-only

## Common Issues

### "Could not connect to server"

Check PostgreSQL is running and accessible:
```bash
pg_isready -h localhost -p 5432
psql "postgresql://user@localhost/db" -c "SELECT 1"
```

Verify firewall rules and PostgreSQL listen_addresses:
```bash
# In postgresql.conf
listen_addresses = '*'  # or specific IP
```

### "Pool is exhausted"

Increase pool size or reduce connection lifetime:
```python
config = PsycopgAsyncConfig(
    pool_config={
        "max_size": 50,  # Increase from default
        "timeout": 120.0,  # Longer acquisition timeout
        "max_waiting": 100,  # Allow more queued requests
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

If error persists, check logs for DEBUG message about graceful degradation.

### "SSL connection required"

Configure SSL in connection string:
```python
config = PsycopgAsyncConfig(
    pool_config={
        "conninfo": "postgresql://user@host/db?sslmode=require",
        # OR:
        "sslmode": "require",
        "sslrootcert": "/path/to/ca-cert.pem",
    }
)
```

## Performance Benchmarks

Compared to other PostgreSQL drivers (relative performance):

- **asyncpg**: ~10-20% faster (pure C implementation)
- **psycopg (async)**: Baseline (excellent performance)
- **psycopg (sync)**: ~5-10% slower than async
- **psqlpy**: ~5-10% faster (Rust-based)

Psycopg 3 offers the best balance of:
- **Feature completeness** - Most comprehensive PostgreSQL feature support
- **Stability** - Mature, widely deployed, enterprise-tested
- **Flexibility** - Sync and async in same adapter
- **Performance** - Fast enough for 99% of applications

For most applications, Psycopg provides excellent performance with superior feature support and stability compared to alternatives.
