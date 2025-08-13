# type: ignore
"""Example demonstrating asyncmy driver usage with query mixins.

This example shows how to use the asyncmy driver with the development MySQL
container started by `make infra-up`.
"""

import asyncio

from sqlspec import SQLSpec
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.builder import Select

__all__ = ("asyncmy_example", "main")


async def asyncmy_example() -> None:
    """Demonstrate asyncmy database driver usage with query mixins."""
    # Create SQLSpec instance with MySQL (connects to dev container)
    spec = SQLSpec()
    config = AsyncmyConfig(
        pool_config={
            "host": "localhost",
            "port": 3307,
            "user": "root",
            "password": "mysql",
            "database": "test"
        }
    )
    spec.add_config(config)

    # Get a driver directly (drivers now have built-in query methods)
    async with spec.provide_session(config) as driver:
        # Create a table
        await driver.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                id INT AUTO_INCREMENT PRIMARY KEY,
                item_name VARCHAR(255) NOT NULL,
                quantity INT NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                supplier VARCHAR(255)
            )
        """)

        # Clean up any existing data
        await driver.execute("TRUNCATE TABLE inventory")

        # Insert data
        await driver.execute("INSERT INTO inventory (item_name, quantity, price, supplier) VALUES (%s, %s, %s, %s)", 
                           "Laptop", 50, 1299.99, "TechCorp")

        # Insert multiple rows
        await driver.execute_many(
            "INSERT INTO inventory (item_name, quantity, price, supplier) VALUES (%s, %s, %s, %s)",
            [
                ("Mouse", 200, 25.99, "TechCorp"),
                ("Keyboard", 150, 89.99, "TechCorp"), 
                ("Monitor", 75, 399.99, "DisplayCo"),
                ("Headphones", 100, 159.99, "AudioPlus")
            ],
        )

        # Select all items using query mixin
        items = await driver.select("SELECT * FROM inventory ORDER BY price")
        print(f"All inventory: {items}")

        # Select one item using query mixin
        laptop = await driver.select_one("SELECT * FROM inventory WHERE item_name = %s", "Laptop")
        print(f"Laptop: {laptop}")

        # Select one or none (no match) using query mixin
        nothing = await driver.select_one_or_none("SELECT * FROM inventory WHERE item_name = %s", "Nothing")
        print(f"Nothing: {nothing}")

        # Select scalar value using query mixin
        total_value = await driver.select_value("SELECT SUM(quantity * price) FROM inventory")
        print(f"Total inventory value: ${total_value:.2f}")

        # Update
        result = await driver.execute("UPDATE inventory SET quantity = quantity + %s WHERE supplier = %s", 10, "TechCorp")
        print(f"Added stock for {result.rows_affected} TechCorp items")

        # Delete
        result = await driver.execute("DELETE FROM inventory WHERE quantity < %s", 80)
        print(f"Removed {result.rows_affected} low-stock items")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = Select("*").from_("inventory").where("supplier = %s")
        techcorp_items = await driver.select(query, "TechCorp")
        print(f"TechCorp items: {techcorp_items}")

        # Query builder with comparison
        query = Select("item_name", "price").from_("inventory").where("price > %s").order_by("price")
        expensive_items = await driver.select(query, 200.0)
        print(f"Expensive items: {expensive_items}")

        # Demonstrate pagination
        page_items = await driver.select("SELECT * FROM inventory ORDER BY item_name LIMIT %s OFFSET %s", 2, 0)
        total_count = await driver.select_value("SELECT COUNT(*) FROM inventory")
        print(f"Page 1: {page_items}, Total: {total_count}")


def main() -> None:
    """Run asyncmy example."""
    print("=== asyncmy Driver Example ===")
    try:
        asyncio.run(asyncmy_example())
        print("✅ asyncmy example completed successfully!")
    except Exception as e:
        print(f"❌ asyncmy example failed: {e}")
        print("Make sure MySQL is running with: make infra-up")


if __name__ == "__main__":
    main()