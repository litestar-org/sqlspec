---
orphan: true
---

# Psycopg Adapter Guide

This guide provides specific instructions and best practices for working with the `psycopg` adapter in `sqlspec`.

## Key Information

-   **Driver:** `psycopg`
-   **Parameter Style:** `pyformat` (e.g., `%s`)

## Parameter Profile

-   **Registry Key:** `"psycopg"`
-   **JSON Strategy:** `helper` (shared JSON serializer wraps dict/list/tuple values before psycopg adapters run)
-   **Extras:** None (adapter-specific list/tuple converters remain in driver to preserve array semantics)

## Best Practices

-   **General Purpose:** `psycopg` is a robust, general-purpose PostgreSQL adapter. It has excellent type handling and is a good choice for a wide variety of applications.

## Driver Features

The `psycopg` adapter supports the following driver features:

-   `enable_pgvector`: A boolean to enable or disable `pgvector` support. Defaults to `True` if `pgvector` is installed.

## Query Stack Support

Psycopg 3 exposes libpq pipeline mode, and SQLSpec uses it automatically for `StatementStack` calls:

- When `psycopg.capabilities.has_pipeline()` reports support (libpq 14+), `execute_stack()` wraps all operations in `with conn.pipeline():` to reduce round-trips.
- Fail-fast stacks (`continue_on_error=False`) run inside a single transaction; continue-on-error stacks run in autocommit mode so later statements can proceed even if earlier ones fail.
- All executions emit the standard stack telemetry metrics/spans/logs so you can observe adoption in production.
- Pass `driver_features={"stack_native_disabled": True}` if you need to disable pipeline mode temporarily (the adapter will fall back to the sequential base implementation).

Example:

```python
stack = (
    StatementStack()
    .push_execute("INSERT INTO events (name, payload) VALUES (%s, %s)", ("login", payload))
    .push_execute("UPDATE users SET last_login = NOW() WHERE id = %s", (user_id,))
    .push_execute("SELECT permissions FROM user_permissions WHERE user_id = %s", (user_id,))
)

results = psycopg_session.execute_stack(stack)
```

When a statement fails and `continue_on_error=True`, its corresponding `StackResult` sets `error` while the other operations still run within the same pipeline block.

## MERGE Operations (PostgreSQL 15+)

Psycopg supports MERGE operations for bulk upserts using PostgreSQL's native MERGE statement with `jsonb_to_recordset()`.

### Single Row Upsert

```python
from sqlspec import sql

with config.provide_session() as session:
    query = (
        sql.merge_
        .into("products", alias="t")
        .using({"id": 1, "name": "Widget", "price": 19.99}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name", price="src.price")
        .when_not_matched_then_insert(id="src.id", name="src.name", price="src.price")
    )

    result = session.execute(query)
    print(f"Rows affected: {result.rows_affected}")
```

### Bulk Upsert (High Performance)

For 100+ rows, Psycopg automatically uses `jsonb_to_recordset()` for optimal performance:

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

result = session.execute(query)
print(f"Upserted {result.rows_affected} rows")
```

Generated SQL uses PostgreSQL's efficient JSON-based strategy:

```sql
MERGE INTO products AS t
USING (
  SELECT * FROM jsonb_to_recordset(%s) AS src(id INTEGER, name TEXT, price NUMERIC)
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

- Psycopg currently routes `EventChannel` through the durable queue backend by
  default (`driver_features["events_backend"] = "table_queue"`). Include the
  `events` extension migrations, then call `spec.event_channel(config)` to
  publish/consume events.
- Native LISTEN/NOTIFY support is coming soon; until then you can still
  subscribe to channels via the durable queue and rely on leases/acks for retry
  safety.
- When you migrate to the native backend, set
  `driver_features["events_backend"] = "listen_notify"` to opt in once the
  backend lands.

## Common Issues

-   **`psycopg.errors.UndefinedFunction`**: Often caused by incorrect parameter types. Ensure data being passed matches the table schema, especially for JSON/JSONB.
-   **Connection Pooling:** `psycopg` has its own connection pool implementation. Ensure the pool settings in the `sqlspec` config are appropriate for the application's needs (e.g., `min_size`, `max_size`).
