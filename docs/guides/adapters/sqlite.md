---
orphan: true
---

# SQLite Adapters Guide

This guide covers `sqlite3` (sync) and `aiosqlite` (async).

## Key Information

-   **Driver:** `sqlite3` (built-in), `aiosqlite`
-   **Parameter Style:** `qmark` (e.g., `?`)

## Parameter Profile

-   **Registry Keys:** `"sqlite"` (sync), `"aiosqlite"` (async)
-   **JSON Strategy:** `helper` for both drivers (shared serializer handles dict/list/tuple parameters)
-   **Extras:** None (profiles apply ISO formatting for datetime/date and convert Decimal to string)

## Best Practices

-   **Use Cases:** Ideal for testing, local development, and embedded applications. Not suitable for high-concurrency production workloads.
-   **In-Memory Databases:** For tests, use `:memory:` for the database name to create a fast, temporary database.
-   **Foreign Keys:** Remember to enable foreign key support with `PRAGMA foreign_keys = ON;` if you need it, as it's off by default.

## Common Issues

-   **`sqlite3.OperationalError: database is locked`**: This occurs when multiple threads/processes try to write to the same database file simultaneously. For testing, use separate database files or in-memory databases for each test process.
