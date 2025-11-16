---
orphan: true
---

# AsyncPG Adapter Guide

This guide provides specific instructions and best practices for working with the `asyncpg` adapter in `sqlspec`.

## Key Information

- **Driver:** `asyncpg`
- **Parameter Style:** `numeric` (e.g., `$1, $2`)

## Parameter Profile

- **Registry Key:** `"asyncpg"`
- **JSON Strategy:** `driver` (delegates JSON binding to asyncpg codecs)
- **Extras:** None (codecs registered through config init hook)

## Best Practices

- **High-Performance:** `asyncpg` is often chosen for high-performance applications due to its speed. It's a good choice for applications with a high volume of database traffic.

## Driver Features

The `asyncpg` adapter supports the following driver features:

- `json_serializer`: A function to serialize Python objects to JSON. Defaults to `sqlspec.utils.serializers.to_json`.
- `json_deserializer`: A function to deserialize JSON strings to Python objects. Defaults to `sqlspec.utils.serializers.from_json`.
- `enable_json_codecs`: A boolean to enable or disable automatic JSON/JSONB codec registration. Defaults to `True`.
- `enable_pgvector`: A boolean to enable or disable `pgvector` support. Defaults to `True` if `pgvector` is installed.
- `enable_cloud_sql`: Enable Google Cloud SQL connector integration. Defaults to `True` when `cloud-sql-python-connector` is installed.
- `cloud_sql_instance`: Cloud SQL instance connection name (format: `"project:region:instance"`). Required when `enable_cloud_sql` is `True`.
- `cloud_sql_enable_iam_auth`: Enable IAM database authentication for Cloud SQL. Defaults to `False`.
- `cloud_sql_ip_type`: IP address type for Cloud SQL connection (`"PUBLIC"`, `"PRIVATE"`, or `"PSC"`). Defaults to `"PRIVATE"`.
- `enable_alloydb`: Enable Google AlloyDB connector integration. Defaults to `True` when `cloud-alloydb-python-connector` is installed.
- `alloydb_instance_uri`: AlloyDB instance URI (format: `"projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE"`). Required when `enable_alloydb` is `True`.
- `alloydb_enable_iam_auth`: Enable IAM database authentication for AlloyDB. Defaults to `False`.
- `alloydb_ip_type`: IP address type for AlloyDB connection (`"PUBLIC"`, `"PRIVATE"`, or `"PSC"`). Defaults to `"PRIVATE"`.

## Google Cloud Integration

AsyncPG supports native integration with Google Cloud SQL and AlloyDB connectors for simplified authentication and connection management.

### Cloud SQL Connector

Connect to Cloud SQL PostgreSQL instances with automatic SSL and IAM authentication:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

db_manager = SQLSpec()

# IAM authentication (no password required)
db = db_manager.add_config(AsyncpgConfig(
    connection_config={
        "user": "my-service-account@project.iam",
        "database": "mydb",
        "min_size": 2,
        "max_size": 10,
    },
    driver_features={
        "enable_cloud_sql": True,
        "cloud_sql_instance": "my-project:us-central1:my-instance",
        "cloud_sql_enable_iam_auth": True,
        "cloud_sql_ip_type": "PRIVATE",
    }
))

async with db_manager.provide_session(db) as session:
    result = await session.select_one("SELECT current_user, version()")
    print(result)
```

Password authentication is also supported:

```python
config = AsyncpgConfig(
    connection_config={
        "user": "postgres",
        "password": "secret",
        "database": "mydb",
    },
    driver_features={
        "enable_cloud_sql": True,
        "cloud_sql_instance": "my-project:us-central1:my-instance",
        "cloud_sql_ip_type": "PUBLIC",  # Public IP for external access
    }
)
```

### AlloyDB Connector

Connect to AlloyDB instances with the same pattern:

```python
# IAM authentication
config = AsyncpgConfig(
    connection_config={
        "user": "my-service-account@project.iam",
        "database": "mydb",
    },
    driver_features={
        "enable_alloydb": True,
        "alloydb_instance_uri": "projects/my-project/locations/us-central1/clusters/my-cluster/instances/my-instance",
        "alloydb_enable_iam_auth": True,
        "alloydb_ip_type": "PRIVATE",
    }
)
```


```bash
# Install Cloud SQL connector
pip install cloud-sql-python-connector

# Install AlloyDB connector
pip install cloud-alloydb-python-connector
```

**Mutual Exclusion**: A single config can only use one connector (Cloud SQL or AlloyDB). For multiple databases, create separate configs with unique `bind_key` values.

**IP Type Selection**:

- `"PRIVATE"` (default): Connect via private VPC network
- `"PUBLIC"`: Connect via public IP address
- `"PSC"`: Connect via Private Service Connect (AlloyDB only)

**Authentication Methods**:

- IAM authentication: Set `cloud_sql_enable_iam_auth=True` or `alloydb_enable_iam_auth=True`
- Password authentication: Leave IAM flags as `False` (default) and provide password in `connection_config`

For comprehensive configuration options and troubleshooting, see the [Google Cloud Connectors Guide](/guides/cloud/google-connectors.md).

## Query Stack Support

`StatementStack` calls execute in a single transaction when `continue_on_error=False`, leveraging asyncpg's fast extended-query protocol to minimize round-trips. When you need partial success handling (`continue_on_error=True`), the adapter automatically disables the shared transaction and reports individual failures via `StackResult.error`.

- Telemetry spans (`sqlspec.stack.execute`), metrics (`stack.execute.*`), and hashed operation logging are emitted for every stack, so production monitoring captures adoption automatically.
- The pipeline path preserves `StackResult.result` for SELECT statements, so downstream helpers continue to operate on the original `SQLResult` objects.
- To force the sequential fallback (for incident response or regression tests), pass `driver_features={"stack_native_disabled": True}` to the config.

Example usage:

```python
from sqlspec import StatementStack

stack = (
    StatementStack()
    .push_execute("INSERT INTO audit_log (message) VALUES ($1)", ("login",))
    .push_execute("UPDATE users SET last_login = NOW() WHERE id = $1", (user_id,))
    .push_execute("SELECT permissions FROM user_permissions WHERE user_id = $1", (user_id,))
)

results = await asyncpg_session.execute_stack(stack)
```

If you enable `continue_on_error=True`, the adapter returns three `StackResult` objects, each recording its own `error`/`warning` state without rolling the entire stack back.

## MERGE Operations (PostgreSQL 15+)

AsyncPG supports high-performance MERGE operations for bulk upserts using PostgreSQL's native MERGE statement with `jsonb_to_recordset()`.

### Single Row Upsert

```python
from sqlspec import sql

async with config.provide_session() as session:
    query = (
        sql.merge_
        .into("products", alias="t")
        .using({"id": 1, "name": "Widget", "price": 19.99}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
        .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
    )

    result = await session.execute(query)
    print(f"Rows affected: {result.rows_affected}")
```

### Bulk Upsert (High Performance)

For 100+ rows, AsyncPG automatically uses `jsonb_to_recordset()` for optimal performance:

```python
from decimal import Decimal

products = [
    {"id": 1, "name": "Widget", "price": Decimal("19.99")},
    {"id": 2, "name": "Gadget", "price": Decimal("29.99")},
    # ... up to 1000+ rows
]

query = (
    sql.merge_
    .into("products", alias="t")
    .using(products, alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)

result = await session.execute(query)
print(f"Upserted {result.rows_affected} rows")
```

Generated SQL uses PostgreSQL's efficient JSON-based strategy:

```sql
MERGE INTO products AS t
USING (
  SELECT * FROM jsonb_to_recordset(:data) AS src(id INTEGER, name TEXT, price NUMERIC)
) AS src
ON t.id = src.id
WHEN MATCHED THEN UPDATE SET name = src.name, price = src.price
WHEN NOT MATCHED THEN INSERT (id, name, price) VALUES (src.id, src.name, src.price)
```

### Unified Upsert API

Use `sql.upsert()` for database-agnostic upsert operations:

```python
upsert_query = (
    sql.upsert("products", dialect="postgres")
    .using([{"id": 1, "name": "Widget", "price": 19.99}], alias="src")
    .on("t.id = src.id")
    .when_matched_then_update(name="src.name", price="src.price")
    .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
)
```

For comprehensive examples and migration guides, see:

- [MERGE Statement Builder Guide](/guides/builder/merge.md)
- [Unified Upsert API Guide](/guides/upsert.md)

## Event Channels

- AsyncPG enables native LISTEN/NOTIFY support automatically by setting
  `driver_features["events_backend"] = "native_postgres"` during config
  construction. Call `spec.event_channel(config)` to obtain a channel—no
  migrations are required.
- Publishing uses `connection.notify()` under the hood; consumers rely on
  `connection.add_listener()` with dedicated connections so the shared pool
  stays available for transactional work.
- Force the durable queue fallback (for deterministic testing or multi-tenant
  workloads) by overriding `driver_features["events_backend"] = "queue"`.

## Common Issues

- **`asyncpg.exceptions.PostgresSyntaxError`**: Check your SQL syntax and parameter styles. `asyncpg` uses the `$` numeric style for parameters.
- **Connection Pooling:** `asyncpg` has its own connection pool implementation. Ensure the pool settings in the `sqlspec` config are appropriate for the application's needs (e.g., `min_size`, `max_size`).
