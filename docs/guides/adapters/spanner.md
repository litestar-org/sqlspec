---
orphan: true
---

# Google Cloud Spanner Adapter Guide

This guide provides specific instructions for the `spanner` adapter.

## Key Information

-   **Driver:** `google-cloud-spanner`
-   **Parameter Style:** `named` with `@` prefix (e.g., `@name`)
-   **Dialect:** `spanner` (custom dialect extending BigQuery)
-   **Transactional DDL:** Not supported (DDL uses separate admin operations)

## Parameter Profile

-   **Registry Key:** `"spanner"`
-   **JSON Strategy:** `helper`
-   **Default Style:** `NAMED_AT` (parameters prefixed with `@`)

## Features

-   **Full ACID Transactions:** Spanner provides global transactions with strong consistency
-   **Interleaved Tables:** Physical co-location of parent-child rows for performance
-   **Row-Level TTL:** Automatic row expiration via TTL policies
-   **Session Pooling:** Built-in session pool management
-   **UUID Handling:** Automatic UUID-to-bytes conversion
-   **JSON Support:** Native JSON type handling

## Configuration

### Basic Usage

```python
from sqlspec.adapters.spanner import SpannerSyncConfig

config = SpannerSyncConfig(
    pool_config={
        "project": "my-project",
        "instance_id": "my-instance",
        "database_id": "my-database",
    }
)

# Read-only snapshot (default)
with config.provide_session() as session:
    result = session.select("SELECT * FROM users WHERE id = @id", {"id": "user-123"})

# Write-capable transaction
with config.provide_session(transaction=True) as session:
    session.execute("UPDATE users SET active = TRUE WHERE id = @id", {"id": "user-123"})
```

### With Emulator

For local development and testing, use the Spanner emulator:

```python
from google.auth.credentials import AnonymousCredentials

config = SpannerSyncConfig(
    pool_config={
        "project": "test-project",
        "instance_id": "test-instance",
        "database_id": "test-database",
        "credentials": AnonymousCredentials(),
        "client_options": {"api_endpoint": "localhost:9010"},
    }
)
```

### Session Pool Configuration

```python
from google.cloud.spanner_v1.pool import FixedSizePool, PingingPool

config = SpannerSyncConfig(
    pool_config={
        "project": "my-project",
        "instance_id": "my-instance",
        "database_id": "my-database",
        "pool_type": PingingPool,  # or FixedSizePool (default)
        "min_sessions": 5,
        "max_sessions": 20,
        "ping_interval": 300,  # seconds
    }
)
```

## Storage Bridge

The Spanner adapter supports the storage bridge for Arrow data import/export:

### Export to Storage

```python
# Export query results to Parquet
job = session.select_to_storage(
    "SELECT * FROM users WHERE active = @active",
    "gs://my-bucket/exports/users.parquet",
    {"active": True},
    format_hint="parquet",
)
print(f"Exported {job.telemetry['rows_processed']} rows")
```

### Load from Arrow

```python
import pyarrow as pa

# Create Arrow table
table = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [95, 87, 92],
})

# Load into Spanner table
job = session.load_from_arrow("scores", table, overwrite=True)
print(f"Loaded {job.telemetry['rows_processed']} rows")
```

### Load from Storage

```python
# Load from Parquet file
job = session.load_from_storage(
    "users",
    "gs://my-bucket/imports/users.parquet",
    file_format="parquet",
    overwrite=True,
)
```

## Interleaved Tables

Spanner supports interleaved tables for physically co-locating parent and child rows. The custom dialect supports this syntax:

```python
# DDL with INTERLEAVE clause (execute via database.update_ddl)
ddl = """
CREATE TABLE orders (
    customer_id STRING(36) NOT NULL,
    order_id STRING(36) NOT NULL,
    total NUMERIC,
    created_at TIMESTAMP
) PRIMARY KEY (customer_id, order_id),
  INTERLEAVE IN PARENT customers ON DELETE CASCADE
"""
```

Interleaved tables provide:
- Automatic co-location of related data
- Efficient joins between parent and child tables
- Cascading deletes for data integrity

## TTL Policies (GoogleSQL)

Spanner supports row-level TTL (row deletion policy):

```python
# DDL with TTL policy
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

Use the Spanner session store for Litestar applications:

```python
from litestar import Litestar
from litestar.middleware.session import SessionMiddleware
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.spanner.litestar import SpannerSyncStore

config = SpannerSyncConfig(
    pool_config={
        "project": "my-project",
        "instance_id": "my-instance",
        "database_id": "my-database",
    },
    extension_config={
        "litestar": {
            "table_name": "sessions",
            "shard_count": 10,  # Optional sharding for high throughput
        }
    },
)

store = SpannerSyncStore(config)

# Create session table (run once during setup)
# await store.create_table()

app = Litestar(
    middleware=[SessionMiddleware(backend=store)],
)

# Writes use transaction-backed sessions; reads use snapshots by default.
```

### Session Store Features

- **Sharding:** Distribute sessions across shards for high write throughput
- **TTL Support:** Automatic session expiration via Spanner TTL
- **Commit Timestamps:** Automatic tracking of created_at/updated_at

## ADK Integration

Use the Spanner ADK store for session and event management:

```python
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.spanner.adk import SpannerADKStore

config = SpannerSyncConfig(
    pool_config={
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

# Create tables (run once during setup)
# store.create_tables()

# Create session
session = store.create_session(app_name="my-app", user_id="user-123")

# Add event
store.add_event(session.id, {"type": "page_view", "path": "/home"})

# List events
events = store.list_events(session.id)
```

### ADK Store Features

- **Interleaved Events:** Events table interleaved with sessions for efficient queries
- **JSON State:** Session state stored as JSON for flexibility
- **Timestamp Tracking:** Automatic created_at/updated_at timestamps

## Common Issues

-   **DDL Operations:** DDL statements (CREATE TABLE, ALTER TABLE, etc.) cannot be executed through the driver's `execute()` method. Use `database.update_ddl()` for DDL operations.

-   **Mutation Limit:** Spanner has a 20,000 mutation limit per transaction. For bulk inserts, batch operations into multiple transactions.

-   **Read-Only Snapshots:** The default session context uses read-only snapshots. For write operations, use `database.run_in_transaction()` or configure a transaction context.

-   **Emulator Limitations:** The Spanner emulator doesn't support all features (e.g., some complex queries, backups). Test critical functionality against a real Spanner instance.

-   **`google.api_core.exceptions.AlreadyExists`:** Resource already exists. Check if the table or index already exists before creating.

-   **`google.api_core.exceptions.NotFound`:** Resource not found. Verify the instance, database, and table names are correct.

## Best Practices

1. **Use Interleaved Tables:** For parent-child relationships, interleave child tables with parents for performance.

2. **Avoid Hotspots:** Use UUIDs or other distributed keys for primary keys to avoid write hotspots.

3. **Batch Writes:** Group multiple writes into single transactions when possible, staying under the 20k mutation limit.

4. **Use TTL:** For temporary data (sessions, events), configure TTL policies for automatic cleanup.

5. **Session Pooling:** Configure session pool size based on your application's concurrency needs.
