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

## Implementation Notes

- Statement config helpers live in `sqlspec/adapters/aiosqlite/core.py` (builder + `aiosqlite_statement_config`).
- `AiosqliteConfig` applies `apply_aiosqlite_driver_features(...)` before creating sessions.

## Query Stack Support

- `StatementStack` executions always use the sequential fallback – SQLite has no notion of pipelined requests – so each operation runs one after another on the same connection. When `continue_on_error=False`, SQLSpec opens a transaction (if one is not already in progress) so the entire stack commits or rolls back together. With `continue_on_error=True`, statements are committed individually after each success.
- Because pooled in-memory connections share state, prefer per-test temporary database files when running stacks under pytest-xdist (see `tests/integration/test_adapters/test_aiosqlite/test_driver.py::test_aiosqlite_statement_stack_*` for the reference pattern).

## Best Practices

- **Async Only:** This is an asynchronous driver for SQLite. Use it in `asyncio` applications.
- **Concurrency:** While `aiosqlite` provides async access, SQLite itself has limitations on concurrent writes. For highly concurrent applications, consider a different database like PostgreSQL.

## Common Issues

- **`sqlite3.OperationalError: database is locked`**: Same as the sync `sqlite3` driver, this occurs when multiple writers conflict. Ensure your application design avoids simultaneous writes.
