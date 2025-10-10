# DuckDB Adapter Guide

This guide provides specific instructions for the `duckdb` adapter.

## Key Information

-   **Driver:** `duckdb`
-   **Parameter Style:** `qmark` (e.g., `?`)

## Best Practices

-   **In-Memory vs. File:** DuckDB can run entirely in-memory (`:memory:`) or with a file-based database. In-memory is great for fast, temporary analytics. File-based is for persistence.
-   **Extensions:** DuckDB has a rich ecosystem of extensions (e.g., for reading Parquet files, JSON, etc.). These can be loaded via the `sqlspec` configuration.
-   **Vectorized Execution:** DuckDB is extremely fast for analytical queries due to its vectorized execution engine. Write queries that operate on columns rather than row-by-row.

## Common Issues

-   **`duckdb.IOException`**: Usually occurs when there are issues reading a file (e.g., a Parquet or CSV file). Check file paths and permissions.
-   **Memory Management:** While fast, DuckDB can be memory-intensive. For large datasets, monitor memory usage and consider using a file-based database to allow for out-of-core processing.
