# MySQL/MariaDB Adapters Guide

This guide covers `asyncmy`.

## Key Information

-   **Driver:** `asyncmy`
-   **Parameter Style:** `pyformat` (e.g., `%s`)

## Best Practices

-   **Character Set:** Always ensure the connection character set is `utf8mb4` to support a full range of Unicode characters, including emojis.
-   **`sql_mode`:** Be aware of the server's `sql_mode`. It can affect how MySQL handles invalid data, dates, and other constraints.

## Common Issues

-   **`PyMySQL.err.OperationalError: (1366, ...)`**: Incorrect string value for a column. This is often due to character set issues. Ensure your connection and tables are using `utf8mb4`.
-   **Authentication Errors:** MySQL 8.0 and later use a different default authentication plugin (`caching_sha2_password`). If you have trouble connecting, you may need to configure the user account to use the older `mysql_native_password` plugin, though this is less secure.
