# /// script
# dependencies = [
#   "sqlspec[psycopg]",
#   "rich",
# ]
# requires-python = ">=3.10"
# ///
"""Example demonstrating psycopg async driver usage with query mixins.

This example shows how to use the psycopg asynchronous driver with the development
PostgreSQL container started by `make infra-up`.
"""

import asyncio

from rich import print

from sqlspec import SQLSpec, sql
from sqlspec.adapters.psycopg import PsycopgAsyncConfig

__all__ = ("main", "psycopg_async_example")


async def psycopg_async_example() -> None:
    """Demonstrate psycopg async database driver usage with query mixins."""
    # Create SQLSpec instance with psycopg async (connects to dev container)
    spec = SQLSpec()
    db = spec.add_config(
        PsycopgAsyncConfig(
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
    async with spec.provide_session(db) as driver:
        # Create a table
        await driver.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                account_id INTEGER NOT NULL,
                amount DECIMAL(15,2) NOT NULL,
                transaction_type TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Clean up any existing data
        await driver.execute("TRUNCATE TABLE transactions RESTART IDENTITY")

        # Insert data
        await driver.execute(
            "INSERT INTO transactions (account_id, amount, transaction_type, description) VALUES (%s, %s, %s, %s)",
            1001,
            250.00,
            "deposit",
            "Initial deposit",
        )

        # Insert multiple rows
        await driver.execute_many(
            "INSERT INTO transactions (account_id, amount, transaction_type, description) VALUES (%s, %s, %s, %s)",
            [
                (1001, -50.00, "withdrawal", "ATM withdrawal"),
                (1002, 1000.00, "deposit", "Salary deposit"),
                (1001, -25.99, "purchase", "Online purchase"),
                (1002, -150.00, "transfer", "Transfer to savings"),
            ],
        )

        # Select all transactions using query mixin
        transactions = await driver.select("SELECT * FROM transactions ORDER BY created_at")
        print(f"[cyan]All transactions:[/cyan] {transactions}")

        # Select one transaction using query mixin
        deposit = await driver.select_one("SELECT * FROM transactions WHERE transaction_type = %s", "deposit")
        print(f"[cyan]First deposit:[/cyan] {deposit}")

        # Select one or none (no match) using query mixin
        nothing = await driver.select_one_or_none("SELECT * FROM transactions WHERE transaction_type = %s", "nothing")
        print(f"[cyan]Nothing:[/cyan] {nothing}")

        # Select scalar value using query mixin
        account_balance = await driver.select_value("SELECT SUM(amount) FROM transactions WHERE account_id = %s", 1001)
        print(f"[cyan]Account 1001 balance:[/cyan] ${account_balance:.2f}")

        # Update
        result = await driver.execute(
            "UPDATE transactions SET description = %s WHERE amount < %s", "Small transaction", 0
        )
        print(f"[yellow]Updated {result.rows_affected} negative transactions[/yellow]")

        # Delete
        result = await driver.execute("DELETE FROM transactions WHERE ABS(amount) < %s", 30.0)
        print(f"[yellow]Removed {result.rows_affected} small transactions[/yellow]")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = sql.select("*").from_("transactions").where("account_id = %s")
        account_transactions = await driver.select(query, 1002)
        print(f"[cyan]Account 1002 transactions:[/cyan] {account_transactions}")

        # Query builder with comparison
        query = sql.select("description", "amount").from_("transactions").where("amount > %s").order_by("amount DESC")
        large_transactions = await driver.select(query, 100.0)
        print(f"[cyan]Large transactions:[/cyan] {large_transactions}")

        # Demonstrate pagination
        page_transactions = await driver.select(
            "SELECT * FROM transactions ORDER BY created_at LIMIT %s OFFSET %s", 2, 0
        )
        total_count = await driver.select_value("SELECT COUNT(*) FROM transactions")
        print(f"[cyan]Page 1:[/cyan] {page_transactions}[cyan], Total:[/cyan] {total_count}")


def main() -> None:
    """Run psycopg async example."""
    print("[bold blue]=== psycopg (async) Driver Example ===[/bold blue]")
    try:
        asyncio.run(psycopg_async_example())
        print("[green]✅ psycopg async example completed successfully![/green]")
    except Exception as e:
        print(f"[red]❌ psycopg async example failed: {e}[/red]")
        print("[yellow]Make sure PostgreSQL is running with: make infra-up[/yellow]")


if __name__ == "__main__":
    main()
