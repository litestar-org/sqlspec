# type: ignore
"""Example demonstrating direct driver usage with query mixins.

This example shows how to use drivers directly with their built-in query
mixin functionality for common database operations.
"""

import asyncio

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.builder import Select
from sqlspec.statement.filters import LimitOffsetFilter
from sqlspec.utils.correlation import correlation_context

__all__ = ("async_driver_example", "main", "sync_driver_example")


def sync_driver_example() -> None:
    """Demonstrate synchronous database driver usage with query mixins."""
    # Create SQLSpec instance with SQLite
    spec = SQLSpec()
    config = SqliteConfig(database=":memory:")
    spec.add_config(config)

    # Get a driver directly (drivers now have built-in query methods)
    with spec.provide_session(config) as driver, correlation_context() as correlation_id:
        print(f"Request correlation ID: {correlation_id}")

        # Create a table
        driver.execute("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL
                )
            """)

        # Insert data
        driver.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Alice", "alice@example.com"))

        # Insert multiple rows
        driver.execute_many(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            [("Bob", "bob@example.com"), ("Charlie", "charlie@example.com")],
        )

        # Select all users using query mixin
        users = driver.select("SELECT * FROM users")
        print(f"All users: {users}")

        # Select one user using query mixin
        alice = driver.select_one("SELECT * FROM users WHERE name = ?", "Alice")
        print(f"Alice: {alice}")

        # Select one or none (no match) using query mixin
        nobody = driver.select_one_or_none("SELECT * FROM users WHERE name = ?", "Nobody")
        print(f"Nobody: {nobody}")

        # Select scalar value using query mixin
        user_count = driver.select_value("SELECT COUNT(*) FROM users")
        print(f"User count: {user_count}")

        # Update
        result = driver.execute("UPDATE users SET email = ? WHERE name = ?", ("alice.doe@example.com", "Alice"))
        print(f"Updated {result.rows_affected} rows")

        # Delete
        result = driver.execute("DELETE FROM users WHERE name = ?", "Charlie")
        print(f"Deleted {result.rows_affected} rows")

        # Use query builder with driver
        query = Select("*").from_("users").where("email LIKE ?")
        matching_users = driver.select(query, "%@example.com%")
        print(f"Matching users: {matching_users}")

        # Demonstrate pagination with query mixin
        paginated = driver.paginate("SELECT * FROM users ORDER BY id", limit=1, offset=0)
        print(f"Page 1: {paginated.items}, Total: {paginated.total}")


async def async_driver_example() -> None:
    """Demonstrate asynchronous database driver usage with query mixins."""
    # Create SQLSpec instance with AIOSQLite
    spec = SQLSpec()
    config = AiosqliteConfig(database=":memory:")
    conf = spec.add_config(config)

    # Get an async driver directly (drivers now have built-in query methods)
    async with spec.provide_session(conf) as driver:
        with correlation_context() as correlation_id:
            print(f"\nAsync request correlation ID: {correlation_id}")

            # Create a table
            await driver.execute("""
                CREATE TABLE products (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    price REAL NOT NULL
                )
            """)

            # Insert data
            await driver.execute("INSERT INTO products (name, price) VALUES (?, ?)", ("Laptop", 999.99))

            # Insert multiple rows
            await driver.execute_many(
                "INSERT INTO products (name, price) VALUES (?, ?)",
                [("Mouse", 29.99), ("Keyboard", 79.99), ("Monitor", 299.99)],
            )

            # Select all products using query mixin
            products = await driver.select("SELECT * FROM products ORDER BY price")
            print(f"All products: {products}")

            # Select one product using query mixin
            laptop = await driver.select_one("SELECT * FROM products WHERE name = ?", "Laptop")
            print(f"Laptop: {laptop}")

            # Select scalar value using query mixin
            avg_price = await driver.select_value("SELECT AVG(price) FROM products")
            print(f"Average price: ${avg_price:.2f}")

            # Update
            result = await driver.execute("UPDATE products SET price = price * 0.9 WHERE price > ?", 100.0)
            print(f"Applied 10% discount to {result.rows_affected} expensive products")

            # Use query builder with async driver
            query = Select("name", "price").from_("products").where("price < ?").order_by("price")
            cheap_products = await driver.select(query, 100.0)
            print(f"Cheap products: {cheap_products}")

            # Demonstrate pagination with query mixin
            paginated = await driver.paginate(
                "SELECT * FROM products ORDER BY price", LimitOffsetFilter(limit=2, offset=1)
            )
            print(f"Products page 2: {len(paginated.items)} items, Total: {paginated.total}")


def main() -> None:
    """Run both sync and async examples."""
    print("=== Synchronous Driver Example ===")
    sync_driver_example()

    print("\n=== Asynchronous Driver Example ===")
    asyncio.run(async_driver_example())


if __name__ == "__main__":
    main()
