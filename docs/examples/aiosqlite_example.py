# type: ignore
"""Example demonstrating AIOSQLite driver usage with query mixins.

This example shows how to use the AIOSQLite driver directly with its built-in query
mixin functionality for common database operations.
"""

import asyncio

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.builder import Select

__all__ = ("aiosqlite_example", "main")


async def aiosqlite_example() -> None:
    """Demonstrate asynchronous database driver usage with query mixins."""
    # Create SQLSpec instance with AIOSQLite
    spec = SQLSpec()
    config = AiosqliteConfig(pool_config={"database": ":memory:"})
    conf = spec.add_config(config)

    # Get an async driver directly (drivers now have built-in query methods)
    async with spec.provide_session(conf) as driver:
        # Create a table
        await driver.execute("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL
            )
        """)

        # Insert data
        await driver.execute("INSERT INTO products (name, price) VALUES (?, ?)", "Laptop", 999.99)

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

        # Demonstrate pagination
        page_products = await driver.select("SELECT * FROM products ORDER BY price LIMIT ? OFFSET ?", 2, 1)
        total_count = await driver.select_value("SELECT COUNT(*) FROM products")
        print(f"Products page 2: {len(page_products)} items, Total: {total_count}")


def main() -> None:
    """Run AIOSQLite example."""
    print("=== AIOSQLite Driver Example ===")
    asyncio.run(aiosqlite_example())
    print("✅ AIOSQLite example completed successfully!")


if __name__ == "__main__":
    main()
