---
orphan: true
---

# aiosqlite Adapter Guide

This guide provides specific instructions for the `aiosqlite` adapter.

## Key Information

- **Driver:** `aiosqlite`
- **Parameter Style:** `qmark` (e.g., `?`)

## Parameter Profile

- **Registry Key:** `"aiosqlite"`
- **JSON Strategy:** `helper` (shared serializer handles dict/list/tuple inputs)
- **Extras:** None (profile applies bool→int and ISO datetime coercions automatically)

## Best Practices

- **Async Only:** This is an asynchronous driver for SQLite. Use it in `asyncio` applications.
- **Concurrency:** While `aiosqlite` provides async access, SQLite itself has limitations on concurrent writes. For highly concurrent applications, consider a different database like PostgreSQL.

## Common Issues

- **`sqlite3.OperationalError: database is locked`**: Same as the sync `sqlite3` driver, this occurs when multiple writers conflict. Ensure your application design avoids simultaneous writes.
