"""Example demonstrating the high-level service layer.

This example shows how to use the DatabaseService and AsyncDatabaseService
to wrap database drivers with instrumentation and convenience methods.
"""

import asyncio

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.service import AsyncDatabaseService, DatabaseService
from sqlspec.statement import sql
from sqlspec.utils.correlation import correlation_context

__all__ = ("async_service_example", "main", "sync_service_example")


def sync_service_example() -> None:
    """Demonstrate synchronous database service usage."""
    # Create SQLSpec instance with SQLite
    spec = SQLSpec()
    config = SqliteConfig(database=":memory:")
    spec.add_config(config)

    # Get a driver and wrap it with service
    with spec.get_driver(SqliteConfig) as driver:
        # Create service with the driver
        service = DatabaseService(driver)

        # Use correlation context for request tracking
        with correlation_context() as correlation_id:
            print(f"Request correlation ID: {correlation_id}")

            # Create a table
            service.execute("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL
                )
            """)

            # Insert data using convenience method
            service.insert("INSERT INTO users (name, email) VALUES (?, ?)", parameters=("Alice", "alice@example.com"))

            # Insert multiple rows
            service.execute_many(
                "INSERT INTO users (name, email) VALUES (?, ?)",
                parameters=[("Bob", "bob@example.com"), ("Charlie", "charlie@example.com")],
            )

            # Select all users
            users = service.select("SELECT * FROM users")
            print(f"All users: {users}")

            # Select one user
            alice = service.select_one("SELECT * FROM users WHERE name = ?", parameters=["Alice"])
            print(f"Alice: {alice}")

            # Select one or none (no match)
            nobody = service.select_one_or_none("SELECT * FROM users WHERE name = ?", parameters=["Nobody"])
            print(f"Nobody: {nobody}")

            # Select scalar value
            user_count = service.select_value("SELECT COUNT(*) FROM users")
            print(f"User count: {user_count}")

            # Update with convenience method
            result = service.update(
                "UPDATE users SET email = ? WHERE name = ?", parameters=("alice.doe@example.com", "Alice")
            )
            print(f"Updated {result.rowcount} rows")

            # Delete with convenience method
            result = service.delete("DELETE FROM users WHERE name = ?", parameters=["Charlie"])
            print(f"Deleted {result.rowcount} rows")

            # Use query builder with service
            query = sql.select("*").from_("users").where("email LIKE ?")
            matching_users = service.select(query, parameters=["%@example.com%"])
            print(f"Matching users: {matching_users}")


async def async_service_example() -> None:
    """Demonstrate asynchronous database service usage."""
    # Create SQLSpec instance with AIOSQLite
    spec = SQLSpec()
    config = AiosqliteConfig(database=":memory:")
    conf = spec.add_config(config)

    # Get an async driver and wrap it with service
    async with spec.get_session(conf) as driver:
        # Create async service with the driver
        service = AsyncDatabaseService(driver)

        # Use correlation context for request tracking
        with correlation_context() as correlation_id:
            print(f"\nAsync request correlation ID: {correlation_id}")

            # Create a table
            await service.execute("""
                CREATE TABLE products (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    price REAL NOT NULL
                )
            """)

            # Insert data using convenience method
            await service.insert("INSERT INTO products (name, price) VALUES (?, ?)", parameters=("Laptop", 999.99))

            # Insert multiple rows
            await service.execute_many(
                "INSERT INTO products (name, price) VALUES (?, ?)",
                parameters=[("Mouse", 29.99), ("Keyboard", 79.99), ("Monitor", 299.99)],
            )

            # Select all products
            products = await service.select("SELECT * FROM products ORDER BY price")
            print(f"All products: {products}")

            # Select one product
            laptop = await service.select_one("SELECT * FROM products WHERE name = ?", parameters=["Laptop"])
            print(f"Laptop: {laptop}")

            # Select scalar value
            avg_price = await service.select_value("SELECT AVG(price) FROM products")
            print(f"Average price: ${avg_price:.2f}")

            # Update with convenience method
            result = await service.update("UPDATE products SET price = price * 0.9 WHERE price > ?", parameters=[100.0])
            print(f"Applied 10% discount to {result.rowcount} expensive products")

            # Use query builder with async service
            query = sql.select("name", "price").from_("products").where("price < ?").order_by("price")
            cheap_products = await service.select(query, parameters=[100.0])
            print(f"Cheap products: {cheap_products}")


def main() -> None:
    """Run both sync and async examples."""
    print("=== Synchronous Service Example ===")
    sync_service_example()

    print("\n=== Asynchronous Service Example ===")
    asyncio.run(async_service_example())


if __name__ == "__main__":
    main()
