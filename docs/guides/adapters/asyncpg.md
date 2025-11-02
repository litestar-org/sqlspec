---
orphan: true
---

# AsyncPG Adapter Guide

This guide provides specific instructions and best practices for working with the `asyncpg` adapter in `sqlspec`.

## Key Information

-   **Driver:** `asyncpg`
-   **Parameter Style:** `numeric` (e.g., `$1, $2`)

## Best Practices

-   **High-Performance:** `asyncpg` is often chosen for high-performance applications due to its speed. It's a good choice for applications with a high volume of database traffic.

## Driver Features

The `asyncpg` adapter supports the following driver features:

-   `json_serializer`: A function to serialize Python objects to JSON. Defaults to `sqlspec.utils.serializers.to_json`.
-   `json_deserializer`: A function to deserialize JSON strings to Python objects. Defaults to `sqlspec.utils.serializers.from_json`.
-   `enable_json_codecs`: A boolean to enable or disable automatic JSON/JSONB codec registration. Defaults to `True`.
-   `enable_pgvector`: A boolean to enable or disable `pgvector` support. Defaults to `True` if `pgvector` is installed.

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

## Common Issues

-   **`asyncpg.exceptions.PostgresSyntaxError`**: Check your SQL syntax and parameter styles. `asyncpg` uses the `$` numeric style for parameters.
-   **Connection Pooling:** `asyncpg` has its own connection pool implementation. Ensure the pool settings in the `sqlspec` config are appropriate for the application's needs (e.g., `min_size`, `max_size`).
