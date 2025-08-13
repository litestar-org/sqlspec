"""Example demonstrating asyncpg driver usage with query mixins.

This example shows how to use the asyncpg driver with the development PostgreSQL
container started by `make infra-up`.
"""

import asyncio

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.builder import Select

__all__ = ("asyncpg_example", "main")


async def asyncpg_example() -> None:
    """Demonstrate asyncpg database driver usage with query mixins."""
    # Create SQLSpec instance with PostgreSQL (connects to dev container)
    spec = SQLSpec()
    config = AsyncpgConfig(
        pool_config={
            "host": "localhost",
            "port": 5433,
            "user": "postgres",
            "password": "postgres",
            "database": "postgres",
        }
    )
    spec.add_config(config)

    # Get a driver directly (drivers now have built-in query methods)
    async with spec.provide_session(config) as driver:
        # Create a table
        await driver.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                category TEXT
            )
        """)

        # Clean up any existing data
        await driver.execute("TRUNCATE TABLE products RESTART IDENTITY")

        # Insert data
        await driver.execute(
            "INSERT INTO products (name, price, category) VALUES ($1, $2, $3)", "Laptop", 999.99, "Electronics"
        )

        # Insert multiple rows
        await driver.execute_many(
            "INSERT INTO products (name, price, category) VALUES ($1, $2, $3)",
            [
                ("Mouse", 29.99, "Electronics"),
                ("Keyboard", 79.99, "Electronics"),
                ("Monitor", 299.99, "Electronics"),
                ("Coffee Mug", 12.50, "Office"),
            ],
        )

        # Select all products using query mixin
        products = await driver.select("SELECT * FROM products ORDER BY price")
        print(f"All products: {products}")

        # Select one product using query mixin
        laptop = await driver.select_one("SELECT * FROM products WHERE name = $1", "Laptop")
        print(f"Laptop: {laptop}")

        # Select one or none (no match) using query mixin
        nothing = await driver.select_one_or_none("SELECT * FROM products WHERE name = $1", "Nothing")
        print(f"Nothing: {nothing}")

        # Select scalar value using query mixin
        avg_price = await driver.select_value("SELECT AVG(price) FROM products")
        print(f"Average price: ${avg_price:.2f}")

        # Update
        result = await driver.execute("UPDATE products SET price = price * 0.9 WHERE price > $1", 100.0)
        print(f"Applied 10% discount to {result.rows_affected} expensive products")

        # Delete
        result = await driver.execute("DELETE FROM products WHERE category = $1", "Office")
        print(f"Deleted {result.rows_affected} office products")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = Select("*").from_("products").where("category = $1")
        electronics = await driver.select(query, "Electronics")
        print(f"Electronics: {electronics}")

        # Query builder with LIKE operator
        query = Select("name", "price").from_("products").where("name LIKE $1").order_by("price")
        m_products = await driver.select(query, "M%")
        print(f"Products starting with M: {m_products}")

        # Demonstrate pagination
        page_products = await driver.select("SELECT * FROM products ORDER BY price LIMIT $1 OFFSET $2", 2, 1)
        total_count = await driver.select_value("SELECT COUNT(*) FROM products")
        print(f"Page 2: {page_products}, Total: {total_count}")


def main() -> None:
    """Run asyncpg example."""
    print("=== asyncpg Driver Example ===")
    try:
        asyncio.run(asyncpg_example())
        print("✅ asyncpg example completed successfully!")
    except Exception as e:
        print(f"❌ asyncpg example failed: {e}")
        print("Make sure PostgreSQL is running with: make infra-up")


if __name__ == "__main__":
    main()
