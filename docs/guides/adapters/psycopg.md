---
orphan: true
---

# Psycopg Adapter Guide

This guide provides specific instructions and best practices for working with the `psycopg` adapter in `sqlspec`.

## Key Information

-   **Driver:** `psycopg`
-   **Parameter Style:** `pyformat` (e.g., `%s`)

## Best Practices

-   **General Purpose:** `psycopg` is a robust, general-purpose PostgreSQL adapter. It has excellent type handling and is a good choice for a wide variety of applications.

## Driver Features

The `psycopg` adapter supports the following driver features:

-   `enable_pgvector`: A boolean to enable or disable `pgvector` support. Defaults to `True` if `pgvector` is installed.

## Common Issues

-   **`psycopg.errors.UndefinedFunction`**: Often caused by incorrect parameter types. Ensure data being passed matches the table schema, especially for JSON/JSONB.
-   **Connection Pooling:** `psycopg` has its own connection pool implementation. Ensure the pool settings in the `sqlspec` config are appropriate for the application's needs (e.g., `min_size`, `max_size`).
