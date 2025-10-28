"""Basic Apache Arrow Integration Examples.

This example demonstrates the fundamental usage patterns for select_to_arrow()
across different database adapters and use cases.

Requirements:
    pip install sqlspec[arrow,pandas,polars]
"""

import asyncio
from pathlib import Path

__all__ = ("example_adbc_native", "example_native_only_mode", "example_pandas_integration", "example_parquet_export", "example_polars_integration", "example_postgres_conversion", "example_return_formats", "main", )


# Example 1: Basic Arrow Query (ADBC - Native Path)
async def example_adbc_native() -> None:
    """Demonstrate ADBC native Arrow support with zero-copy performance."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.adbc import AdbcConfig

    sql = SQLSpec()
    config = AdbcConfig(connection_config={"driver": "adbc_driver_sqlite", "uri": "file::memory:?cache=shared"})
    sql.add_config(config)

    with config.provide_session() as session:
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
        result = session.select_to_arrow("SELECT * FROM users WHERE age > ?", (25,))

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
    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    sql = SQLSpec()
    config = AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/test"})
    sql.add_config(config)

    async with config.provide_session() as session:
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
        await session.execute(
            "INSERT INTO products (name, price, tags) VALUES ($1, $2, $3)",
            [("Widget", 19.99, ["gadget", "tool"]), ("Gadget", 29.99, ["electronics", "new"])],
            many=True,
        )

        # Conversion path: dict → Arrow
        result = await session.select_to_arrow("SELECT * FROM products WHERE price < $1", (25.00,))

        print("PostgreSQL Conversion Path Results:")
        print(f"  Rows: {len(result)}")
        print(f"  Data: {result.to_dict()}")
        print()


# Example 3: pandas Integration
async def example_pandas_integration() -> None:
    """Demonstrate pandas integration via Arrow."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    sql = SQLSpec()
    config = SqliteConfig(pool_config={"database": ":memory:"})
    sql.add_config(config)

    with config.provide_session() as session:
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

        session.execute(
            "INSERT INTO sales VALUES (?, ?, ?, ?)",
            [
                (1, "North", 1000.00, "2024-01-15"),
                (2, "South", 1500.00, "2024-01-20"),
                (3, "North", 2000.00, "2024-02-10"),
                (4, "East", 1200.00, "2024-02-15"),
            ],
            many=True,
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

    sql = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})
    sql.add_config(config)

    with config.provide_session() as session:
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

        session.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            [
                (1, "login", 100, "2024-01-01 10:00:00"),
                (2, "click", 100, "2024-01-01 10:05:00"),
                (3, "login", 101, "2024-01-01 10:10:00"),
                (4, "purchase", 100, "2024-01-01 10:15:00"),
            ],
            many=True,
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

    sql = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})
    sql.add_config(config)

    with config.provide_session() as session:
        # Create test data
        session.execute("CREATE TABLE items (id INTEGER, name VARCHAR, quantity INTEGER)")
        session.execute(
            "INSERT INTO items VALUES (?, ?, ?)", [(1, "Apple", 10), (2, "Banana", 20), (3, "Orange", 15)], many=True
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
    import pyarrow.parquet as pq

    from sqlspec import SQLSpec
    from sqlspec.adapters.duckdb import DuckDBConfig

    sql = SQLSpec()
    config = DuckDBConfig(pool_config={"database": ":memory:"})
    sql.add_config(config)

    with config.provide_session() as session:
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

        session.execute(
            "INSERT INTO logs VALUES (?, ?, ?, ?)",
            [
                (1, "2024-01-01 10:00:00", "INFO", "Application started"),
                (2, "2024-01-01 10:05:00", "WARN", "High memory usage"),
                (3, "2024-01-01 10:10:00", "ERROR", "Database connection failed"),
            ],
            many=True,
        )

        # Query to Arrow
        result = session.select_to_arrow("SELECT * FROM logs")

        # Export to Parquet
        output_path = Path("/tmp/logs.parquet")
        pq.write_table(result.data, output_path)

        print("Parquet Export:")
        print(f"  Exported to: {output_path}")
        print(f"  File size: {output_path.stat().st_size} bytes")
        print()


# Example 7: Native-Only Mode
async def example_native_only_mode() -> None:
    """Demonstrate native-only mode enforcement."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.sqlite import SqliteConfig

    # ADBC has native Arrow support
    sql = SQLSpec()
    config = AdbcConfig(connection_config={"uri": "sqlite://:memory:"})
    sql.add_config(config)

    with config.provide_session() as session:
        session.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        session.execute("INSERT INTO test VALUES (1, 'test')")

        # This works - ADBC has native support
        result = session.select_to_arrow("SELECT * FROM test", native_only=True)
        print("Native-only mode (ADBC): Success")
        print(f"  Rows: {len(result)}")
        print()

    # SQLite does not have native Arrow support
    sqlite_config = SqliteConfig(pool_config={"database": ":memory:"})
    sql.add_config(sqlite_config)

    with sqlite_config.provide_session() as session:
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
