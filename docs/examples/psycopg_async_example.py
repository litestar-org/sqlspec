"""Example demonstrating psycopg async driver usage with query mixins.

This example shows how to use the psycopg asynchronous driver with the development
PostgreSQL container started by `make infra-up`.
"""

import asyncio

from sqlspec import SQLSpec
from sqlspec.adapters.psycopg import PsycopgAsyncConfig
from sqlspec.builder import Select

__all__ = ("main", "psycopg_async_example")


async def psycopg_async_example() -> None:
    """Demonstrate psycopg async database driver usage with query mixins."""
    # Create SQLSpec instance with psycopg async (connects to dev container)
    spec = SQLSpec()
    config = PsycopgAsyncConfig(
        pool_config={
            "host": "localhost",
            "port": 5433,
            "user": "postgres",
            "password": "postgres",
            "dbname": "postgres",
        }
    )
    spec.add_config(config)

    # Get a driver directly (drivers now have built-in query methods)
    async with spec.provide_session(config) as driver:
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
        print(f"All transactions: {transactions}")

        # Select one transaction using query mixin
        deposit = await driver.select_one("SELECT * FROM transactions WHERE transaction_type = %s", "deposit")
        print(f"First deposit: {deposit}")

        # Select one or none (no match) using query mixin
        nothing = await driver.select_one_or_none("SELECT * FROM transactions WHERE transaction_type = %s", "nothing")
        print(f"Nothing: {nothing}")

        # Select scalar value using query mixin
        account_balance = await driver.select_value("SELECT SUM(amount) FROM transactions WHERE account_id = %s", 1001)
        print(f"Account 1001 balance: ${account_balance:.2f}")

        # Update
        result = await driver.execute(
            "UPDATE transactions SET description = %s WHERE amount < %s", "Small transaction", 0
        )
        print(f"Updated {result.rows_affected} negative transactions")

        # Delete
        result = await driver.execute("DELETE FROM transactions WHERE ABS(amount) < %s", 30.0)
        print(f"Removed {result.rows_affected} small transactions")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = Select("*").from_("transactions").where("account_id = %s")
        account_transactions = await driver.select(query, 1002)
        print(f"Account 1002 transactions: {account_transactions}")

        # Query builder with comparison
        query = Select("description", "amount").from_("transactions").where("amount > %s").order_by("amount DESC")
        large_transactions = await driver.select(query, 100.0)
        print(f"Large transactions: {large_transactions}")

        # Demonstrate pagination
        page_transactions = await driver.select(
            "SELECT * FROM transactions ORDER BY created_at LIMIT %s OFFSET %s", 2, 0
        )
        total_count = await driver.select_value("SELECT COUNT(*) FROM transactions")
        print(f"Page 1: {page_transactions}, Total: {total_count}")


def main() -> None:
    """Run psycopg async example."""
    print("=== psycopg (async) Driver Example ===")
    try:
        asyncio.run(psycopg_async_example())
        print("✅ psycopg async example completed successfully!")
    except Exception as e:
        print(f"❌ psycopg async example failed: {e}")
        print("Make sure PostgreSQL is running with: make infra-up")


if __name__ == "__main__":
    main()
