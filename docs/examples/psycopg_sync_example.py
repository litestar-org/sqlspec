# ruff: noqa: FBT003
"""Example demonstrating psycopg sync driver usage with query mixins.

This example shows how to use the psycopg synchronous driver with the development
PostgreSQL container started by `make infra-up`.
"""

from sqlspec import SQLSpec
from sqlspec.adapters.psycopg import PsycopgSyncConfig
from sqlspec.builder import Select

__all__ = ("main", "psycopg_sync_example")


def psycopg_sync_example() -> None:
    """Demonstrate psycopg sync database driver usage with query mixins."""
    # Create SQLSpec instance with psycopg sync (connects to dev container)
    spec = SQLSpec()
    config = PsycopgSyncConfig(
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
    with spec.provide_session(config) as driver:
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
        print(f"All customers: {customers}")

        # Select one customer using query mixin
        alice = driver.select_one("SELECT * FROM customers WHERE name = %s", "Alice Cooper")
        print(f"Alice: {alice}")

        # Select one or none (no match) using query mixin
        nobody = driver.select_one_or_none("SELECT * FROM customers WHERE name = %s", "Nobody")
        print(f"Nobody: {nobody}")

        # Select scalar value using query mixin
        active_count = driver.select_value("SELECT COUNT(*) FROM customers WHERE is_active = %s", True)
        print(f"Active customers: {active_count}")

        # Update
        result = driver.execute("UPDATE customers SET is_active = %s WHERE email LIKE %s", False, "%@startup.io")
        print(f"Deactivated {result.rows_affected} startup customers")

        # Delete
        result = driver.execute("DELETE FROM customers WHERE is_active = %s", False)
        print(f"Removed {result.rows_affected} inactive customers")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = Select("*").from_("customers").where("is_active = %s")
        active_customers = driver.select(query, True)
        print(f"Active customers: {active_customers}")

        # Query builder with LIKE
        query = Select("name", "email").from_("customers").where("email LIKE %s").order_by("name")
        example_customers = driver.select(query, "%@example.com")
        print(f"Example.com customers: {example_customers}")

        # Demonstrate pagination
        page_customers = driver.select("SELECT * FROM customers ORDER BY name LIMIT %s OFFSET %s", 2, 0)
        total_count = driver.select_value("SELECT COUNT(*) FROM customers")
        print(f"Page 1: {page_customers}, Total: {total_count}")


def main() -> None:
    """Run psycopg sync example."""
    print("=== psycopg (sync) Driver Example ===")
    try:
        psycopg_sync_example()
        print("✅ psycopg sync example completed successfully!")
    except Exception as e:
        print(f"❌ psycopg sync example failed: {e}")
        print("Make sure PostgreSQL is running with: make infra-up")


if __name__ == "__main__":
    main()
