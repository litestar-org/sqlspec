---
orphan: true
---

# MySQL/MariaDB Adapters Guide

This guide covers `asyncmy`.

## Key Information

-   **Driver:** `asyncmy`
-   **Parameter Style:** `pyformat` (e.g., `%s`)

## Parameter Profile

-   **Registry Key:** `"asyncmy"`
-   **JSON Strategy:** `helper` (uses shared JSON serializers for dict/list/tuple)
-   **Extras:** None (native list expansion remains disabled)

## Implementation Notes

-   Statement config helpers live in `sqlspec/adapters/asyncmy/core.py` (builder + `asyncmy_statement_config`).
-   `AsyncmyConfig` applies `apply_asyncmy_driver_features(...)` before creating sessions.

## Best Practices

-   **Character Set:** Always ensure the connection character set is `utf8mb4` to support a full range of Unicode characters, including emojis.
-   **`sql_mode`:** Be aware of the server's `sql_mode`. It can affect how MySQL handles invalid data, dates, and other constraints.

## Common Issues

-   **`PyMySQL.err.OperationalError: (1366, ...)`**: Incorrect string value for a column. This is often due to character set issues. Ensure your connection and tables are using `utf8mb4`.
-   **Authentication Errors:** MySQL 8.0 and later use a different default authentication plugin (`caching_sha2_password`). If you have trouble connecting, you may need to configure the user account to use the older `mysql_native_password` plugin, though this is less secure.

## Event Channels

- Asyncmy uses the queue-backed event channel with MySQL-specific hints:
  `poll_interval` defaults to `0.25s`, leases default to `5s`, and the dequeuer
  issues `SELECT ... FOR UPDATE SKIP LOCKED` to avoid duplicate deliveries.
- Include the `events` extension migrations and call
  `spec.event_channel(config)` to publish/consume events. Override
  `extension_config["events"]` when you need different lease/poll windows.

## Query Stack Support

The MySQL wire protocol doesn't offer a pipeline/batch mode like Oracle or PostgreSQL, so `StatementStack` executions use the base sequential implementation:

- All operations run one-by-one within the usual transaction rules (fail-fast stacks open a transaction, continue-on-error stacks stay in autocommit mode).
- Telemetry spans/metrics/logs are still emitted so you can trace stack executions in production.

If you need reduced round-trips for MySQL/MariaDB, consider consolidating statements into stored procedures or batching logic within application-side transactions.
