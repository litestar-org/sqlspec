---
orphan: true
---

# Google BigQuery Adapter Guide

This guide provides specific instructions for the `bigquery` adapter.

## Key Information

-   **Driver:** `google-cloud-bigquery`
-   **Parameter Style:** `named` (e.g., `@name`)

## Parameter Profile

-   **Registry Key:** `"bigquery"`
-   **JSON Strategy:** `helper` with `json_tuple_strategy="tuple"`
-   **Extras:** `type_coercion_overrides` keep list values intact while converting tuples to lists during binding

## Implementation Notes

-   Statement config helpers live in `sqlspec/adapters/bigquery/core.py` (builder + `bigquery_statement_config`).
-   `BigQueryConfig` applies `apply_bigquery_driver_features(...)` before creating sessions.

## Query Stack Support

-   BigQuery does **not** expose a native pipeline API, so `StatementStack` calls execute sequentially through the core driver. Because BigQuery does not offer transactional semantics, the `begin()`/`commit()` hooks are no-ops—the stack still runs each statement in order and surfaces failures via `StackResult.error`.
-   Continue-on-error mode is supported. Each failing operation records its own `StackExecutionError` while later statements continue to run, which is particularly helpful for long-running analytical batches.
-   Telemetry spans (`sqlspec.stack.execute`) and `StackExecutionMetrics` counters are emitted for every stack execution, making it easy to monitor adoption even though the adapter falls back to the sequential path.

## Best Practices

-   **Authentication:** BigQuery requires authentication with Google Cloud. For local development, the easiest way is to use the Google Cloud CLI and run `gcloud auth application-default login`.
-   **Project and Dataset:** Always specify the project and dataset in your queries or configure them in the `sqlspec` connection settings.
-   **Cost:** Be mindful that BigQuery is a cloud data warehouse and queries are billed based on the amount of data scanned. Avoid `SELECT *` on large tables. Use partitioned and clustered tables to reduce query costs.

## MERGE Operations

BigQuery supports MERGE statements for efficient upsert operations using standard SQL syntax.

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

### Bulk Upsert

BigQuery efficiently handles bulk upserts using multi-row VALUES:

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

### Unified Upsert API

Use `sql.upsert()` for database-agnostic upsert operations:

```python
upsert_query = (
    sql.upsert("products", dialect="bigquery")
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

-   **`google.api_core.exceptions.Forbidden: 403`**: Authentication or permission issue. Ensure your service account or user has the necessary BigQuery roles (e.g., `BigQuery User`, `BigQuery Data Viewer`).
-   **`google.api_core.exceptions.NotFound: 404`**: Table or dataset not found. Double-check your project ID, dataset ID, and table names in your queries.
