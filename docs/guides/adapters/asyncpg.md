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

## Common Issues

-   **`asyncpg.exceptions.PostgresSyntaxError`**: Check your SQL syntax and parameter styles. `asyncpg` uses the `$` numeric style for parameters.
-   **Connection Pooling:** `asyncpg` has its own connection pool implementation. Ensure the pool settings in the `sqlspec` config are appropriate for the application's needs (e.g., `min_size`, `max_size`).
