# type: ignore
# /// script
# dependencies = [
#   "sqlspec[aiosqlite]",
#   "rich",
# ]
# requires-python = ">=3.10"
# ///
"""Example demonstrating AIOSQLite driver usage with query mixins.

This example shows how to use the AIOSQLite driver directly with its built-in query
mixin functionality for common database operations.
"""

import asyncio

from rich import print

from sqlspec import SQLSpec, sql
from sqlspec.adapters.aiosqlite import AiosqliteConfig

__all__ = ("aiosqlite_example", "main")


async def aiosqlite_example() -> None:
    """Demonstrate asynchronous database driver usage with query mixins."""
    # Create SQLSpec instance with AIOSQLite
    spec = SQLSpec()
    db = spec.add_config(AiosqliteConfig(pool_config={"database": ":memory:"}))

    # Get a driver directly (drivers now have built-in query methods)
    async with spec.provide_session(db) as driver:
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
        print(f"[cyan]All products:[/cyan] {products}")

        # Select one product using query mixin
        laptop = await driver.select_one("SELECT * FROM products WHERE name = ?", "Laptop")
        print(f"[cyan]Laptop:[/cyan] {laptop}")

        # Select scalar value using query mixin
        avg_price = await driver.select_value("SELECT AVG(price) FROM products")
        print(f"[cyan]Average price:[/cyan] ${avg_price:.2f}")

        # Update
        result = await driver.execute("UPDATE products SET price = price * 0.9 WHERE price > ?", 100.0)
        print(f"[yellow]Applied 10% discount to {result.rows_affected} expensive products[/yellow]")

        # Use query builder with async driver
        query = sql.select("name", "price").from_("products").where("price < ?").order_by("price")
        cheap_products = await driver.select(query, 100.0)
        print(f"[cyan]Cheap products:[/cyan] {cheap_products}")

        # Demonstrate pagination
        page_products = await driver.select("SELECT * FROM products ORDER BY price LIMIT ? OFFSET ?", 2, 1)
        total_count = await driver.select_value("SELECT COUNT(*) FROM products")
        print(f"[cyan]Products page 2:[/cyan] {len(page_products)} items[cyan], Total:[/cyan] {total_count}")


async def main_async() -> None:
    """Run AIOSQLite example with proper cleanup."""
    print("[bold blue]=== AIOSQLite Driver Example ===[/bold blue]")
    await aiosqlite_example()
    print("[green]✅ AIOSQLite example completed successfully![/green]")


def main() -> None:
    """Run AIOSQLite example."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
