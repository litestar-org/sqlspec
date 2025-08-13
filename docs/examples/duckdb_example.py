# type: ignore
"""Example demonstrating DuckDB driver usage with query mixins.

This example shows how to use the DuckDB driver (no container needed).
"""

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckdbConfig
from sqlspec.builder import Select

__all__ = ("duckdb_example", "main")


def duckdb_example() -> None:
    """Demonstrate DuckDB database driver usage with query mixins."""
    # Create SQLSpec instance with DuckDB (in-memory)
    spec = SQLSpec()
    config = DuckdbConfig(pool_config={"database": ":memory:"})
    spec.add_config(config)

    # Get a driver directly (drivers now have built-in query methods)
    with spec.provide_session(config) as driver:
        # Create a table
        driver.execute("""
            CREATE TABLE analytics (
                id INTEGER PRIMARY KEY,
                event_name VARCHAR NOT NULL,
                user_id INTEGER NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                properties JSON
            )
        """)

        # Insert data
        driver.execute("INSERT INTO analytics (event_name, user_id, properties) VALUES (?, ?, ?)", 
                      "page_view", 1001, '{"page": "/home", "source": "organic"}')

        # Insert multiple rows
        driver.execute_many(
            "INSERT INTO analytics (event_name, user_id, properties) VALUES (?, ?, ?)",
            [
                ("click", 1001, '{"element": "button", "text": "Sign Up"}'),
                ("page_view", 1002, '{"page": "/pricing", "source": "google"}'), 
                ("purchase", 1001, '{"amount": 99.99, "product": "Pro Plan"}'),
                ("click", 1002, '{"element": "link", "text": "Learn More"}')
            ],
        )

        # Select all events using query mixin
        events = await driver.select("SELECT * FROM analytics ORDER BY timestamp")
        print(f"All events: {events}")

        # Select one event using query mixin
        purchase = await driver.select_one("SELECT * FROM analytics WHERE event_name = ?", "purchase")
        print(f"Purchase event: {purchase}")

        # Select one or none (no match) using query mixin
        nothing = await driver.select_one_or_none("SELECT * FROM analytics WHERE event_name = ?", "nothing")
        print(f"Nothing: {nothing}")

        # Select scalar value using query mixin - DuckDB-specific analytics
        unique_users = await driver.select_value("SELECT COUNT(DISTINCT user_id) FROM analytics")
        print(f"Unique users: {unique_users}")

        # Update
        result = await driver.execute("UPDATE analytics SET properties = ? WHERE event_name = ?", 
                                    '{"updated": true}', "click")
        print(f"Updated {result.rows_affected} click events")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = Select("*").from_("analytics").where("user_id = ?")
        user_events = await driver.select(query, 1001)
        print(f"User 1001 events: {user_events}")

        # Query builder with JSON extraction (DuckDB-specific)
        query = Select("event_name", "json_extract_string(properties, '$.page') as page").from_("analytics").where("event_name = ?")
        page_views = await driver.select(query, "page_view")
        print(f"Page views: {page_views}")

        # Demonstrate pagination
        page_events = await driver.select("SELECT * FROM analytics ORDER BY timestamp LIMIT ? OFFSET ?", 2, 1)
        total_count = await driver.select_value("SELECT COUNT(*) FROM analytics")
        print(f"Page 2: {page_events}, Total: {total_count}")


def main() -> None:
    """Run DuckDB example."""
    print("=== DuckDB Driver Example ===")
    try:
        duckdb_example()
        print("✅ DuckDB example completed successfully!")
    except Exception as e:
        print(f"❌ DuckDB example failed: {e}")


if __name__ == "__main__":
    main()