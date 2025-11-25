"""Basic Apache Arrow Integration Examples.

This example demonstrates the fundamental usage patterns for select_to_arrow()
across different database adapters and use cases.

Requirements:
    pip install sqlspec[arrow,pandas,polars]
"""

import asyncio
from pathlib import Path

__all__ = (
    "example_adbc_native",
    "example_native_only_mode",
    "example_pandas_integration",
    "example_parquet_export",
    "example_polars_integration",
    "example_postgres_conversion",
    "example_return_formats",
    "main",
)


# Example 1: Basic Arrow Query (ADBC - Native Path)
async def example_adbc_native() -> None:
    """Demonstrate ADBC native Arrow support with zero-copy performance."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.adbc import AdbcConfig

    db_manager = SQLSpec()
    adbc_db = db_manager.add_config(
        AdbcConfig(connection_config={"driver": "adbc_driver_sqlite", "uri": "file::memory:?cache=shared"})
    )

    with db_manager.provide_session(adbc_db) as session:
        # Create test table
        session.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER,
                email TEXT
            )
            """
        )

        # Insert test data
        session.execute_many(
            "INSERT INTO users (id, name, age, email) VALUES (?, ?, ?, ?)",
            [
                (1, "Alice", 30, "alice@example.com"),
                (2, "Bob", 25, "bob@example.com"),
                (3, "Charlie", 35, "charlie@example.com"),
            ],
        )

        # Native Arrow fetch - zero-copy!
        result = session.select_to_arrow("SELECT * FROM users WHERE age > :min_age", min_age=25)

        print("ADBC Native Arrow Results:")
        print(f"  Rows: {len(result)}")
        print(f"  Columns: {result.data.column_names}")
        print(f"  Schema: {result.data.schema}")
        print()

        # Iterate over results
        for row in result:
            print(f"  {row['name']}: {row['age']} years old")

    print()


# Example 2: PostgreSQL with Conversion Path
async def example_postgres_conversion() -> None:
    """Demonstrate PostgreSQL adapter with dict → Arrow conversion."""
    import os

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    dsn = os.getenv("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/db")
    db_manager = SQLSpec()
    asyncpg_db = db_manager.add_config(AsyncpgConfig(pool_config={"dsn": dsn}))

    async with db_manager.provide_session(asyncpg_db) as session:
        # Create test table with PostgreSQL-specific types
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name TEXT,
                price NUMERIC(10, 2),
                tags TEXT[]
            )
            """
        )

        # Insert test data
        await session.execute_many(
            "INSERT INTO products (name, price, tags) VALUES (:name, :price, :tags)",
            [
                {"name": "Widget", "price": 19.99, "tags": ["gadget", "tool"]},
                {"name": "Gadget", "price": 29.99, "tags": ["electronics", "new"]},
            ],
        )

        # Conversion path: dict → Arrow
        result = await session.select_to_arrow("SELECT * FROM products WHERE price < :price_limit", price_limit=25.00)

        print("PostgreSQL Conversion Path Results:")
        print(f"  Rows: {len(result)}")
        print(f"  Data: {result.to_dict()}")
        print()


# Example 3: pandas Integration
async def example_pandas_integration() -> None:
    """Demonstrate pandas integration via Arrow."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_manager = SQLSpec()
    sqlite_db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with db_manager.provide_session(sqlite_db) as session:
        # Create and populate table
        session.execute(
            """
            CREATE TABLE sales (
                id INTEGER PRIMARY KEY,
                region TEXT,
                amount REAL,
                sale_date DATE
            )
            """
        )

        session.execute_many(
            "INSERT INTO sales (id, region, amount, sale_date) VALUES (:id, :region, :amount, :sale_date)",
            [
                {"id": 1, "region": "North", "amount": 1000.00, "sale_date": "2024-01-15"},
                {"id": 2, "region": "South", "amount": 1500.00, "sale_date": "2024-01-20"},
                {"id": 3, "region": "North", "amount": 2000.00, "sale_date": "2024-02-10"},
                {"id": 4, "region": "East", "amount": 1200.00, "sale_date": "2024-02-15"},
            ],
        )

        # Query to Arrow
        result = session.select_to_arrow("SELECT * FROM sales")

        # Convert to pandas DataFrame
        df = result.to_pandas()

        print("pandas Integration:")
        print(df)
        print()
        print("Summary Statistics:")
        print(df.describe())
        print()


# Example 4: Polars Integration
async def example_polars_integration() -> None:
    """Demonstrate Polars integration via Arrow."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.duckdb import DuckDBConfig

    db_manager = SQLSpec()
    duckdb = db_manager.add_config(DuckDBConfig(pool_config={"database": ":memory:"}))

    with db_manager.provide_session(duckdb) as session:
        # Create and populate table
        session.execute(
            """
            CREATE TABLE events (
                id INTEGER PRIMARY KEY,
                event_type VARCHAR,
                user_id INTEGER,
                timestamp TIMESTAMP
            )
            """
        )

        session.execute_many(
            "INSERT INTO events (id, event_type, user_id, timestamp) VALUES (:id, :event_type, :user_id, :ts)",
            [
                {"id": 1, "event_type": "login", "user_id": 100, "ts": "2024-01-01 10:00:00"},
                {"id": 2, "event_type": "click", "user_id": 100, "ts": "2024-01-01 10:05:00"},
                {"id": 3, "event_type": "login", "user_id": 101, "ts": "2024-01-01 10:10:00"},
                {"id": 4, "event_type": "purchase", "user_id": 100, "ts": "2024-01-01 10:15:00"},
            ],
        )

        # Query to Arrow (native DuckDB path)
        result = session.select_to_arrow("SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type")

        # Convert to Polars DataFrame
        pl_df = result.to_polars()

        print("Polars Integration:")
        print(pl_df)
        print()


# Example 5: Return Format Options
async def example_return_formats() -> None:
    """Demonstrate table vs batch return formats."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.duckdb import DuckDBConfig

    db_manager = SQLSpec()
    duckdb = db_manager.add_config(DuckDBConfig(pool_config={"database": ":memory:"}))

    with db_manager.provide_session(duckdb) as session:
        # Create test data
        session.execute("CREATE TABLE items (id INTEGER, name VARCHAR, quantity INTEGER)")
        session.execute_many(
            "INSERT INTO items (id, name, quantity) VALUES (:id, :name, :qty)",
            [
                {"id": 1, "name": "Apple", "qty": 10},
                {"id": 2, "name": "Banana", "qty": 20},
                {"id": 3, "name": "Orange", "qty": 15},
            ],
        )

        # Table format (default)
        table_result = session.select_to_arrow("SELECT * FROM items", return_format="table")

        print("Table Format:")
        print(f"  Type: {type(table_result.data)}")
        print(f"  Rows: {len(table_result)}")
        print(f"  Columns: {table_result.data.column_names}")
        print()

        # Batch format
        batch_result = session.select_to_arrow("SELECT * FROM items", return_format="batch")

        print("Batch Format:")
        print(f"  Type: {type(batch_result.data)}")
        print(f"  Rows: {len(batch_result)}")
        print()


# Example 6: Export to Parquet
async def example_parquet_export() -> None:
    """Demonstrate exporting Arrow results to Parquet."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.duckdb import DuckDBConfig

    db_manager = SQLSpec()
    duckdb = db_manager.add_config(DuckDBConfig(pool_config={"database": ":memory:"}))

    with db_manager.provide_session(duckdb) as session:
        # Create and populate table
        session.execute(
            """
            CREATE TABLE logs (
                id INTEGER,
                timestamp TIMESTAMP,
                level VARCHAR,
                message VARCHAR
            )
            """
        )

        session.execute_many(
            "INSERT INTO logs (id, timestamp, level, message) VALUES (:id, :ts, :level, :message)",
            [
                {"id": 1, "ts": "2024-01-01 10:00:00", "level": "INFO", "message": "Application started"},
                {"id": 2, "ts": "2024-01-01 10:05:00", "level": "WARN", "message": "High memory usage"},
                {"id": 3, "ts": "2024-01-01 10:10:00", "level": "ERROR", "message": "Database connection failed"},
            ],
        )

        # Query to Arrow
        result = session.select_to_arrow("SELECT * FROM logs")

        # Export to Parquet using the storage bridge
        output_path = Path("/tmp/arrow_basic_usage_logs.parquet")
        telemetry = result.write_to_storage_sync(str(output_path), format_hint="parquet")

        print("Parquet Export:")
        print(f"  Exported to: {output_path}")
        print(f"  Rows: {telemetry['rows_processed']}")
        print(f"  Bytes processed: {telemetry['bytes_processed']}")
        print(f"  File size: {output_path.stat().st_size} bytes")
        print()


# Example 7: Native-Only Mode
async def example_native_only_mode() -> None:
    """Demonstrate native-only mode enforcement."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.sqlite import SqliteConfig

    # ADBC has native Arrow support
    db_manager = SQLSpec()
    adbc_sqlite = db_manager.add_config(AdbcConfig(connection_config={"uri": "sqlite://:memory:"}))

    with db_manager.provide_session(adbc_sqlite) as session:
        session.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        session.execute("INSERT INTO test VALUES (1, 'test')")

        # This works - ADBC has native support
        result = session.select_to_arrow("SELECT * FROM test", native_only=True)
        print("Native-only mode (ADBC): Success")
        print(f"  Rows: {len(result)}")
        print()

    # SQLite does not have native Arrow support
    sqlite_db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with db_manager.provide_session(sqlite_db) as session:
        session.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        session.execute("INSERT INTO test VALUES (1, 'test')")
        result = session.select_to_arrow("SELECT * FROM test", native_only=True)


# Run all examples
async def main() -> None:
    """Run all examples."""
    print("=" * 60)
    print("Apache Arrow Integration Examples")
    print("=" * 60)
    print()

    print("Example 1: ADBC Native Arrow")
    print("-" * 60)
    await example_adbc_native()

    # print("Example 2: PostgreSQL Conversion Path")
    # print("-" * 60)
    # await example_postgres_conversion()  # Requires PostgreSQL

    print("Example 3: pandas Integration")
    print("-" * 60)
    await example_pandas_integration()

    print("Example 4: Polars Integration")
    print("-" * 60)
    await example_polars_integration()

    print("Example 5: Return Format Options")
    print("-" * 60)
    await example_return_formats()

    print("Example 6: Parquet Export")
    print("-" * 60)
    await example_parquet_export()

    print("Example 7: Native-Only Mode")
    print("-" * 60)
    await example_native_only_mode()

    print("=" * 60)
    print("All examples completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
