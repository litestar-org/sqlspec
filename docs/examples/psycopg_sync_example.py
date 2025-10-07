# ruff: noqa: FBT003
# /// script
# dependencies = [
#   "sqlspec[psycopg]",
#   "rich",
# ]
# requires-python = ">=3.10"
# ///
"""Example demonstrating psycopg sync driver usage with query mixins.

This example shows how to use the psycopg synchronous driver with the development
PostgreSQL container started by `make infra-up`.
"""

from rich import print

from sqlspec import SQLSpec, sql
from sqlspec.adapters.psycopg import PsycopgSyncConfig

__all__ = ("main", "psycopg_sync_example")


def psycopg_sync_example() -> None:
    """Demonstrate psycopg sync database driver usage with query mixins."""
    # Create SQLSpec instance with psycopg sync (connects to dev container)
    spec = SQLSpec()
    db = spec.add_config(
        PsycopgSyncConfig(
            pool_config={
                "host": "localhost",
                "port": 5433,
                "user": "postgres",
                "password": "postgres",
                "dbname": "postgres",
            }
        )
    )

    # Get a driver directly (drivers now have built-in query methods)
    with spec.provide_session(db) as driver:
        # Create a table
        driver.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                registration_date DATE DEFAULT CURRENT_DATE,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)

        # Clean up any existing data
        driver.execute("TRUNCATE TABLE customers RESTART IDENTITY")

        # Insert data
        driver.execute(
            "INSERT INTO customers (name, email, is_active) VALUES (%s, %s, %s)",
            "Alice Cooper",
            "alice@example.com",
            True,
        )

        # Insert multiple rows
        driver.execute_many(
            "INSERT INTO customers (name, email, is_active) VALUES (%s, %s, %s)",
            [
                ("Bob Miller", "bob@example.com", True),
                ("Carol Davis", "carol@company.org", True),
                ("David Lee", "david@example.com", False),
                ("Emma White", "emma@startup.io", True),
            ],
        )

        # Select all customers using query mixin
        customers = driver.select("SELECT * FROM customers ORDER BY name")
        print(f"[cyan]All customers:[/cyan] {customers}")

        # Select one customer using query mixin
        alice = driver.select_one("SELECT * FROM customers WHERE name = %s", "Alice Cooper")
        print(f"[cyan]Alice:[/cyan] {alice}")

        # Select one or none (no match) using query mixin
        nobody = driver.select_one_or_none("SELECT * FROM customers WHERE name = %s", "Nobody")
        print(f"[cyan]Nobody:[/cyan] {nobody}")

        # Select scalar value using query mixin
        active_count = driver.select_value("SELECT COUNT(*) FROM customers WHERE is_active = %s", True)
        print(f"[cyan]Active customers:[/cyan] {active_count}")

        # Update
        result = driver.execute("UPDATE customers SET is_active = %s WHERE email LIKE %s", False, "%@startup.io")
        print(f"[yellow]Deactivated {result.rows_affected} startup customers[/yellow]")

        # Delete
        result = driver.execute("DELETE FROM customers WHERE is_active = %s", False)
        print(f"[yellow]Removed {result.rows_affected} inactive customers[/yellow]")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = sql.select("*").from_("customers").where("is_active = %s")
        active_customers = driver.select(query, True)
        print(f"[cyan]Active customers:[/cyan] {active_customers}")

        # Query builder with LIKE
        query = sql.select("name", "email").from_("customers").where("email LIKE %s").order_by("name")
        example_customers = driver.select(query, "%@example.com")
        print(f"[cyan]Example.com customers:[/cyan] {example_customers}")

        # Demonstrate pagination
        page_customers = driver.select("SELECT * FROM customers ORDER BY name LIMIT %s OFFSET %s", 2, 0)
        total_count = driver.select_value("SELECT COUNT(*) FROM customers")
        print(f"[cyan]Page 1:[/cyan] {page_customers}[cyan], Total:[/cyan] {total_count}")


def main() -> None:
    """Run psycopg sync example."""
    print("[bold blue]=== psycopg (sync) Driver Example ===[/bold blue]")
    try:
        psycopg_sync_example()
        print("[green]✅ psycopg sync example completed successfully![/green]")
    except Exception as e:
        print(f"[red]❌ psycopg sync example failed: {e}[/red]")
        print("[yellow]Make sure PostgreSQL is running with: make infra-up[/yellow]")


if __name__ == "__main__":
    main()
