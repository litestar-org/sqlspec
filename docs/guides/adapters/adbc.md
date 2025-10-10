# ADBC Adapter Guide

This guide provides specific instructions for the `adbc` adapter.

## Key Information

- **Driver:** Arrow Database Connectivity (ADBC) drivers (e.g., `adbc_driver_postgresql`, `adbc_driver_sqlite`).
- **Parameter Style:** Varies by underlying database (e.g., `numeric` for PostgreSQL, `qmark` for SQLite).

## Best Practices

- **Arrow-Native:** The primary benefit of ADBC is its direct integration with Apache Arrow. Use it when you need to move large amounts of data efficiently between the database and data science tools like Pandas or Polars.
- **Driver Installation:** Each database requires a separate ADBC driver to be installed (e.g., `pip install adbc_driver_postgresql`).
- **Data Types:** Be aware of how database types are mapped to Arrow types. Use `Context7` to research the specific ADBC driver's documentation for type mapping details.

## Common Issues

- **`ArrowInvalid: Could not find ADBC driver`**: The required ADBC driver for your target database is not installed or not found in the system's library path.
- **Type Mismatches:** Errors related to converting database types to Arrow types. This often requires careful handling of complex types like JSON, arrays, or custom user-defined types.
