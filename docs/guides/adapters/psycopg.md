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

The `psycopg` adapter configuration is managed through a `TypedDict` for clarity and type safety.

### PsycopgDriverFeatures TypedDict

```python
class PsycopgDriverFeatures(TypedDict):
    """Psycopg driver feature flags.

    enable_pgvector: Enable automatic pgvector extension support for vector similarity search.
        Requires pgvector-python package (pip install pgvector) and PostgreSQL with pgvector extension.
        Defaults to True when pgvector-python is installed.
        Provides automatic conversion between Python objects and PostgreSQL vector types.
        Enables vector similarity operations and index support.
        Set to False to disable pgvector support even when package is available.
    json_serializer: Custom JSON serializer for StatementConfig parameter handling.
    json_deserializer: Custom JSON deserializer reference stored alongside the serializer for parity with asyncpg.
    """

    enable_pgvector: NotRequired[bool]
    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
```

### Configuration and Defaults

-   **pgvector Support (`enable_pgvector`)**: This feature is **auto-enabled** if the `pgvector` Python package is installed. If you have `pgvector` installed but need to disable this feature for a specific database configuration, you can explicitly set it to `False`.

### Example Usage

To explicitly disable `pgvector` support, even if the package is installed:

```python
from sqlspec.adapters.psycopg import PsycopgAsyncConfig

config = PsycopgAsyncConfig(
    pool_config={"conninfo": "postgresql://user:pass@host:port/db"},
    driver_features={
        "enable_pgvector": False,  # Explicitly disable pgvector
    },
)
```

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

## Common Issues

-   **`psycopg.errors.UndefinedFunction`**: Often caused by incorrect parameter types. Ensure data being passed matches the table schema, especially for JSON/JSONB.
-   **Connection Pooling:** `psycopg` has its own connection pool implementation. Ensure the pool settings in the `sqlspec` config are appropriate for the application's needs (e.g., `min_size`, `max_size`).
