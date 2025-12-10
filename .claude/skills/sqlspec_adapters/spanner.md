# Spanner Adapter Skill

**Adapter:** Google Cloud Spanner (Sync)
**Category:** Database Adapter
**Status:** Active

## Description

Expert guidance for using SQLSpec's Spanner adapter for Google Cloud Spanner. Spanner is a globally distributed, horizontally scalable database with strong consistency guarantees.

## When to Use Spanner

- **Global distribution** - Multi-region deployments with strong consistency
- **Horizontal scalability** - Automatic sharding and scaling
- **Financial/critical workloads** - ACID transactions at scale
- **Agent deployments** - ADK integration for AI agent session/event storage
- **Interleaved data** - Parent-child relationships with physical co-location

## Configuration

```python
from sqlspec.adapters.spanner import SpannerSyncConfig

config = SpannerSyncConfig(
    connection_config={
        "project": "my-gcp-project",
        "instance_id": "my-instance",
        "database_id": "my-database",
        # Optional pool settings:
        "pool_type": FixedSizePool,  # or PingingPool
        "min_sessions": 5,
        "max_sessions": 20,
        "ping_interval": 300,  # For PingingPool
    },
    # Optional: Custom credentials
    # credentials=credentials_object,
    # client_options={"api_endpoint": "localhost:9010"},  # Emulator
)
```

### With Emulator (Local Development)

```python
from google.auth.credentials import AnonymousCredentials

config = SpannerSyncConfig(
    connection_config={
        "project": "test-project",
        "instance_id": "test-instance",
        "database_id": "test-database",
        "credentials": AnonymousCredentials(),
        "client_options": {"api_endpoint": "localhost:9010"},
    }
)
```

## Parameter Style

**Named with @**: `@param_name`

```python
# Single parameter
result = session.execute(
    "SELECT * FROM users WHERE id = @id",
    {"id": "user-123"}
)

# Multiple parameters
result = session.execute(
    "SELECT * FROM users WHERE status = @status AND age > @min_age",
    {"status": "active", "min_age": 18}
)
```

## Custom SQLGlot Dialects

Spanner adapter includes two custom dialects:

### GoogleSQL (spanner)

Default dialect for standard Spanner SQL:

```python
# Supports INTERLEAVE, ROW DELETION POLICY
ddl = """
CREATE TABLE orders (
    customer_id STRING(36) NOT NULL,
    order_id STRING(36) NOT NULL,
    total NUMERIC
) PRIMARY KEY (customer_id, order_id),
  INTERLEAVE IN PARENT customers ON DELETE CASCADE
"""
```

### PostgreSQL Mode (spangres)

For Spanner PostgreSQL interface:

```python
from sqlspec.adapters.spanner.dialect import SpangresDialect
# Uses PostgreSQL-compatible syntax with Spanner-specific features
```

## Interleaved Tables

Physical co-location of parent-child rows for performance:

```python
# Parent table
ddl_parent = """
CREATE TABLE customers (
    customer_id STRING(36) NOT NULL,
    name STRING(100)
) PRIMARY KEY (customer_id)
"""

# Child table interleaved with parent
ddl_child = """
CREATE TABLE orders (
    customer_id STRING(36) NOT NULL,
    order_id STRING(36) NOT NULL,
    total NUMERIC
) PRIMARY KEY (customer_id, order_id),
  INTERLEAVE IN PARENT customers ON DELETE CASCADE
"""
```

Benefits:
- Automatic co-location of related data
- Efficient joins between parent and child
- Cascading deletes for data integrity

## TTL Policies (Row Deletion)

Automatic row expiration:

```python
ddl = """
CREATE TABLE events (
    id STRING(36) NOT NULL,
    data JSON,
    created_at TIMESTAMP NOT NULL
) PRIMARY KEY (id),
  ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))
"""
```

## Litestar Integration

Session store for Litestar applications:

```python
from litestar import Litestar
from litestar.middleware.session import SessionMiddleware
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.spanner.litestar import SpannerSyncStore

config = SpannerSyncConfig(
    connection_config={
        "project": "my-project",
        "instance_id": "my-instance",
        "database_id": "my-database",
    },
    extension_config={
        "litestar": {
            "table_name": "sessions",
            "shard_count": 10,  # For high throughput
        }
    },
)

store = SpannerSyncStore(config)

app = Litestar(
    middleware=[SessionMiddleware(backend=store)],
)
```

### Session Store Features

- **Sharding** - Distribute sessions across shards for write throughput
- **TTL Support** - Automatic session expiration via Spanner TTL
- **Commit Timestamps** - Automatic created_at/updated_at tracking

## ADK Integration

Session and event storage for AI agents:

```python
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.spanner.adk import SpannerADKStore

config = SpannerSyncConfig(
    connection_config={
        "project": "my-project",
        "instance_id": "my-instance",
        "database_id": "my-database",
    },
    extension_config={
        "adk": {
            "sessions_table": "adk_sessions",
            "events_table": "adk_events",
        }
    },
)

store = SpannerADKStore(config)

# Create session
session = store.create_session(app_name="my-agent", user_id="user-123")

# Add event (stored in interleaved table)
store.add_event(session.id, {"type": "tool_call", "tool": "search"})

# List events (efficient due to interleaving)
events = store.list_events(session.id)
```

### ADK Store Features

- **Interleaved Events** - Events table interleaved with sessions for efficiency
- **JSON State** - Session state stored as native JSON
- **Timestamp Tracking** - Automatic created_at/updated_at

## Storage Bridge

Export and import data via Arrow:

```python
# Export to storage
job = session.select_to_storage(
    "SELECT * FROM users WHERE active = @active",
    "gs://my-bucket/exports/users.parquet",
    {"active": True},
    format_hint="parquet",
)

# Load from Arrow table
import pyarrow as pa

table = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
})
job = session.load_from_arrow("scores", table, overwrite=True)
```

## Best Practices

1. **Use interleaved tables** - For parent-child relationships
2. **Avoid hotspots** - Use UUIDs or distributed keys
3. **Batch writes** - Stay under 20k mutation limit per transaction
4. **Configure TTL** - For temporary data (sessions, events, logs)
5. **Use session pooling** - Configure based on concurrency needs
6. **Prefer snapshots** - Use read-only snapshots for queries
7. **Transaction for writes** - Always use transactions for mutations

## Common Issues

### DDL Operations Fail

DDL cannot be executed through `execute()`. Use database admin API:

```python
database.update_ddl([ddl_statement])
```

### Mutation Limit Exceeded

Spanner has 20,000 mutation limit per transaction. Batch operations:

```python
# Split large inserts into batches
for batch in chunks(records, 1000):
    with session.transaction():
        for record in batch:
            session.execute(insert_sql, record)
```

### Read-Only Session Error

Default sessions are read-only snapshots. For writes:

```python
# Use transaction context
with config.provide_session(transaction=True) as session:
    session.execute("UPDATE ...")
```

### Emulator Limitations

Emulator doesn't support all features:
- Some complex queries
- Backups
- Instance/database admin operations

Test critical functionality against real Spanner instance.

## Performance Characteristics

- **Latency**: 5-10ms for simple queries (within region)
- **Throughput**: Scales horizontally with nodes
- **Consistency**: Linearizable (strongest)
- **Availability**: 99.999% SLA (multi-region)

Compared to other cloud databases:
- **vs BigQuery**: Better for OLTP, worse for analytics
- **vs Cloud SQL**: Better for global scale, higher cost
- **vs Firestore**: Better for complex queries, relational data
