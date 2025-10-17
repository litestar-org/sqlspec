---
orphan: true
---

# asyncmy Adapter Guide

This guide provides specific instructions for the `asyncmy` adapter for MySQL/MariaDB.

## Key Information

-   **Driver:** `asyncmy`
-   **Parameter Style:** `pyformat` (e.g., `%s`)

## Best Practices

-   **Async Only:** This is an asynchronous driver. Use it in `asyncio` applications.
-   **Connection Pooling:** `asyncmy` has a built-in connection pool. Configure it via the `sqlspec` config to manage connections efficiently.
-   **Character Set:** As with other MySQL drivers, ensure `utf8mb4` is used for full Unicode support.

## Common Issues

-   **Event Loop Conflicts:** In some frameworks, you might encounter event loop issues if the connection pool is not managed correctly within the application's lifecycle. Ensure the pool is created and closed at the appropriate times.
