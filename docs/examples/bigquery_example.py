# type: ignore
"""Example demonstrating BigQuery driver usage with query mixins.

This example shows how to use the BigQuery adapter with the development BigQuery
emulator started by `make infra-up`.
"""

from sqlspec import SQLSpec
from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.builder import Select

__all__ = ("bigquery_example", "main")


def bigquery_example() -> None:
    """Demonstrate BigQuery database driver usage with query mixins."""
    # Create SQLSpec instance with BigQuery (connects to dev emulator)
    spec = SQLSpec()
    config = BigQueryConfig(
        pool_config={
            "project": "test-project",
            "api_endpoint": "http://localhost:9050"
        }
    )
    spec.add_config(config)

    # Get a driver directly (drivers now have built-in query methods)
    with spec.provide_session(config) as driver:
        # Create a dataset
        driver.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        
        # Create a table
        driver.execute("""
            CREATE TABLE IF NOT EXISTS analytics.web_events (
                event_id STRING NOT NULL,
                user_id STRING NOT NULL,
                event_type STRING NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                page_url STRING,
                user_agent STRING,
                session_id STRING
            )
        """)

        # Clean up existing data
        driver.execute("DELETE FROM analytics.web_events WHERE TRUE")

        # Insert data
        driver.execute("""
            INSERT INTO analytics.web_events (event_id, user_id, event_type, timestamp, page_url, session_id)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP(), ?, ?)
        """, "evt_001", "user_123", "page_view", "/home", "sess_abc")

        # Insert multiple rows
        driver.execute_many(
            """
            INSERT INTO analytics.web_events (event_id, user_id, event_type, timestamp, page_url, session_id)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP(), ?, ?)
            """,
            [
                ("evt_002", "user_123", "click", "/home", "sess_abc"),
                ("evt_003", "user_456", "page_view", "/pricing", "sess_def"),
                ("evt_004", "user_456", "purchase", "/checkout", "sess_def"),
                ("evt_005", "user_789", "page_view", "/about", "sess_ghi")
            ],
        )

        # Select all events using query mixin
        events = driver.select("SELECT * FROM analytics.web_events ORDER BY timestamp")
        print(f"All events: {events}")

        # Select one event using query mixin
        purchase = driver.select_one("SELECT * FROM analytics.web_events WHERE event_type = ?", "purchase")
        print(f"Purchase event: {purchase}")

        # Select one or none (no match) using query mixin
        nothing = driver.select_one_or_none("SELECT * FROM analytics.web_events WHERE event_type = ?", "nothing")
        print(f"Nothing: {nothing}")

        # Select scalar value using query mixin
        total_events = driver.select_value("SELECT COUNT(*) FROM analytics.web_events")
        print(f"Total events: {total_events}")

        # Update
        result = driver.execute("UPDATE analytics.web_events SET user_agent = ? WHERE user_id = ?", 
                               "Updated Browser", "user_123")
        print(f"Updated {result.rows_affected} events for user_123")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = Select("*").from_("analytics.web_events").where("user_id = ?")
        user_events = driver.select(query, "user_456")
        print(f"User 456 events: {user_events}")

        # Query builder with aggregation
        query = Select("user_id", "COUNT(*) as event_count").from_("analytics.web_events").where("event_type = ?").group_by("user_id")
        page_views = driver.select(query, "page_view")
        print(f"Page view counts: {page_views}")

        # Demonstrate pagination
        page_events = driver.select("SELECT * FROM analytics.web_events ORDER BY timestamp LIMIT ? OFFSET ?", 2, 1)
        total_count = driver.select_value("SELECT COUNT(*) FROM analytics.web_events")
        print(f"Page 2: {page_events}, Total: {total_count}")


def main() -> None:
    """Run BigQuery example."""
    print("=== BigQuery Driver Example ===")
    try:
        bigquery_example()
        print("✅ BigQuery example completed successfully!")
    except Exception as e:
        print(f"❌ BigQuery example failed: {e}")
        print("Make sure BigQuery emulator is running with: make infra-up")


if __name__ == "__main__":
    main()