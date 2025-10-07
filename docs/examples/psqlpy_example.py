# /// script
# dependencies = [
#   "sqlspec[psqlpy]",
#   "rich",
# ]
# requires-python = ">=3.10"
# ///
"""Example demonstrating PSQLPy driver usage with query mixins.

This example shows how to use the psqlpy (Rust-based) async PostgreSQL driver
with the development PostgreSQL container started by `make infra-up`.
"""

import asyncio

from rich import print

from sqlspec import SQLSpec, sql
from sqlspec.adapters.psqlpy import PsqlpyConfig

__all__ = ("main", "psqlpy_example")


async def psqlpy_example() -> None:
    """Demonstrate PSQLPy database driver usage with query mixins."""
    # Create SQLSpec instance with PSQLPy (connects to dev container)
    spec = SQLSpec()
    db = spec.add_config(
        PsqlpyConfig(
            pool_config={
                "host": "localhost",
                "port": 5433,
                "username": "postgres",
                "password": "postgres",
                "db_name": "postgres",
            }
        )
    )

    # Get a driver directly (drivers now have built-in query methods)
    async with spec.provide_session(db) as driver:
        # Create a table
        await driver.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                customer_name TEXT NOT NULL,
                order_total DECIMAL(10,2) NOT NULL,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'pending'
            )
        """)

        # Clean up any existing data
        await driver.execute("TRUNCATE TABLE orders RESTART IDENTITY")

        # Insert data
        await driver.execute(
            "INSERT INTO orders (customer_name, order_total, status) VALUES ($1, $2, $3)",
            "John Doe",
            150.75,
            "completed",
        )

        # Insert multiple rows
        await driver.execute_many(
            "INSERT INTO orders (customer_name, order_total, status) VALUES ($1, $2, $3)",
            [
                ("Jane Smith", 89.50, "pending"),
                ("Bob Johnson", 234.00, "completed"),
                ("Alice Brown", 45.25, "cancelled"),
                ("Charlie Wilson", 312.80, "pending"),
            ],
        )

        # Select all orders using query mixin
        orders = await driver.select("SELECT * FROM orders ORDER BY order_total")
        print(f"[cyan]All orders:[/cyan] {orders}")

        # Select one order using query mixin
        john_order = await driver.select_one("SELECT * FROM orders WHERE customer_name = $1", "John Doe")
        print(f"[cyan]John's order:[/cyan] {john_order}")

        # Select one or none (no match) using query mixin
        nobody = await driver.select_one_or_none("SELECT * FROM orders WHERE customer_name = $1", "Nobody")
        print(f"[cyan]Nobody:[/cyan] {nobody}")

        # Select scalar value using query mixin
        total_revenue = await driver.select_value("SELECT SUM(order_total) FROM orders WHERE status = $1", "completed")
        print(f"[cyan]Total completed revenue:[/cyan] ${total_revenue:.2f}")

        # Update
        result = await driver.execute("UPDATE orders SET status = $1 WHERE order_total < $2", "processed", 100.0)
        print(f"[yellow]Processed {result.rows_affected} small orders[/yellow]")

        # Delete
        result = await driver.execute("DELETE FROM orders WHERE status = $1", "cancelled")
        print(f"[yellow]Removed {result.rows_affected} cancelled orders[/yellow]")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = sql.select("*").from_("orders").where("status = $1")
        pending_orders = await driver.select(query, "pending")
        print(f"[cyan]Pending orders:[/cyan] {pending_orders}")

        # Query builder with comparison
        query = (
            sql.select("customer_name", "order_total")
            .from_("orders")
            .where("order_total > $1")
            .order_by("order_total DESC")
        )
        large_orders = await driver.select(query, 200.0)
        print(f"[cyan]Large orders:[/cyan] {large_orders}")

        # Demonstrate pagination
        page_orders = await driver.select("SELECT * FROM orders ORDER BY customer_name LIMIT $1 OFFSET $2", 2, 0)
        total_count = await driver.select_value("SELECT COUNT(*) FROM orders")
        print(f"[cyan]Page 1:[/cyan] {page_orders}[cyan], Total:[/cyan] {total_count}")


def main() -> None:
    """Run PSQLPy example."""
    print("[bold blue]=== PSQLPy (Rust PostgreSQL) Driver Example ===[/bold blue]")
    try:
        asyncio.run(psqlpy_example())
        print("[green]✅ PSQLPy example completed successfully![/green]")
    except Exception as e:
        print(f"[red]❌ PSQLPy example failed: {e}[/red]")
        print("[yellow]Make sure PostgreSQL is running with: make infra-up[/yellow]")


if __name__ == "__main__":
    main()
