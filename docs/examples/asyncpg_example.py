# /// script
# dependencies = [
#   "sqlspec[asyncpg]",
#   "rich",
# ]
# requires-python = ">=3.10"
# ///
"""Example demonstrating asyncpg driver usage with query mixins.

This example shows how to use the asyncpg driver with the development PostgreSQL
container started by `make infra-up`.
"""

import asyncio

from rich import print

from sqlspec import SQLSpec, sql
from sqlspec.adapters.asyncpg import AsyncpgConfig

__all__ = ("asyncpg_example", "main")


async def asyncpg_example() -> None:
    """Demonstrate asyncpg database driver usage with query mixins."""
    # Create SQLSpec instance with PostgreSQL (connects to dev container)
    spec = SQLSpec()
    db = spec.add_config(
        AsyncpgConfig(
            pool_config={
                "host": "localhost",
                "port": 5433,
                "user": "postgres",
                "password": "postgres",
                "database": "postgres",
            }
        )
    )

    # Get a driver directly (drivers now have built-in query methods)
    async with spec.provide_session(db) as driver:
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
        print(f"[cyan]All products:[/cyan] {products}")

        # Select one product using query mixin
        laptop = await driver.select_one("SELECT * FROM products WHERE name = $1", "Laptop")
        print(f"[cyan]Laptop:[/cyan] {laptop}")

        # Select one or none (no match) using query mixin
        nothing = await driver.select_one_or_none("SELECT * FROM products WHERE name = $1", "Nothing")
        print(f"[cyan]Nothing:[/cyan] {nothing}")

        # Select scalar value using query mixin
        avg_price = await driver.select_value("SELECT AVG(price) FROM products")
        print(f"[cyan]Average price:[/cyan] ${avg_price:.2f}")

        # Update
        result = await driver.execute("UPDATE products SET price = price * 0.9 WHERE price > $1", 100.0)
        print(f"[yellow]Applied 10% discount to {result.rows_affected} expensive products[/yellow]")

        # Delete
        result = await driver.execute("DELETE FROM products WHERE category = $1", "Office")
        print(f"[yellow]Deleted {result.rows_affected} office products[/yellow]")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = sql.select("*").from_("products").where("category = $1")
        electronics = await driver.select(query, "Electronics")
        print(f"[cyan]Electronics:[/cyan] {electronics}")

        # Query builder with LIKE operator
        query = sql.select("name", "price").from_("products").where("name LIKE $1").order_by("price")
        m_products = await driver.select(query, "M%")
        print(f"[cyan]Products starting with M:[/cyan] {m_products}")

        # Demonstrate pagination
        page_products = await driver.select("SELECT * FROM products ORDER BY price LIMIT $1 OFFSET $2", 2, 1)
        total_count = await driver.select_value("SELECT COUNT(*) FROM products")
        print(f"[cyan]Page 2:[/cyan] {page_products}[cyan], Total:[/cyan] {total_count}")


def main() -> None:
    """Run asyncpg example."""
    print("[bold blue]=== asyncpg Driver Example ===[/bold blue]")
    try:
        asyncio.run(asyncpg_example())
        print("[green]✅ asyncpg example completed successfully![/green]")
    except Exception as e:
        print(f"[red]❌ asyncpg example failed: {e}[/red]")
        print("[yellow]Make sure PostgreSQL is running with: make infra-up[/yellow]")


if __name__ == "__main__":
    main()
