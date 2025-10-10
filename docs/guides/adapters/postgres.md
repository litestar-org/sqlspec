# PostgreSQL Adapters Guide

This guide covers `psycopg`, `asyncpg`, and `psqlpy`.

## Key Information

-   **Parameter Style:**
    -   `psycopg`: `pyformat` (e.g., `%s`)
    -   `asyncpg`, `psqlpy`: `numeric` (e.g., `$1, $2`)

## Best Practices

-   **`psycopg`**: Use for robust, general-purpose PostgreSQL access. It has excellent type handling.
-   **`asyncpg`**: Often chosen for high-performance applications due to its speed.
-   **`psqlpy`**: A newer, Rust-based driver. Consider for performance-critical code where `asyncpg` might have limitations.

## Common Issues

-   **`psycopg.errors.UndefinedFunction`**: Often caused by incorrect parameter types. Ensure data being passed matches the table schema, especially for JSON/JSONB.
-   **Connection Pooling:** All three drivers have their own connection pool implementations. Ensure the pool settings in the `sqlspec` config are appropriate for the application's needs (e.g., `min_size`, `max_size`).
