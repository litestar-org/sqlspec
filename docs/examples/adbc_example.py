# type: ignore
"""Example demonstrating ADBC driver usage with query mixins.

This example shows how to use the ADBC (Arrow Database Connectivity) driver
with the development PostgreSQL container started by `make infra-up`.
"""

from sqlspec import SQLSpec
from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.builder import Select

__all__ = ("adbc_example", "main")


def adbc_example() -> None:
    """Demonstrate ADBC database driver usage with query mixins."""
    # Create SQLSpec instance with ADBC (connects to dev PostgreSQL container)
    spec = SQLSpec()
    config = AdbcConfig(
        pool_config={
            "driver": "adbc_driver_postgresql",
            "uri": "postgresql://postgres:postgres@localhost:5433/postgres"
        }
    )
    spec.add_config(config)

    # Get a driver directly (drivers now have built-in query methods)
    with spec.provide_session(config) as driver:
        # Create a table
        driver.execute("""
            CREATE TABLE IF NOT EXISTS analytics_data (
                id SERIAL PRIMARY KEY,
                metric_name TEXT NOT NULL,
                metric_value DOUBLE PRECISION NOT NULL,
                dimensions JSONB,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Clean up any existing data
        driver.execute("TRUNCATE TABLE analytics_data RESTART IDENTITY")

        # Insert data
        driver.execute("INSERT INTO analytics_data (metric_name, metric_value, dimensions) VALUES ($1, $2, $3)", 
                      "page_views", 1250.0, '{"source": "organic", "device": "desktop"}')

        # Insert multiple rows
        driver.execute_many(
            "INSERT INTO analytics_data (metric_name, metric_value, dimensions) VALUES ($1, $2, $3)",
            [
                ("conversion_rate", 0.045, '{"funnel": "signup", "campaign": "summer"}'),
                ("revenue", 15420.50, '{"product": "pro", "region": "us"}'), 
                ("bounce_rate", 0.32, '{"page": "landing", "source": "paid"}'),
                ("session_duration", 180.5, '{"device": "mobile", "browser": "chrome"}')
            ],
        )

        # Select all metrics using query mixin
        metrics = driver.select("SELECT * FROM analytics_data ORDER BY recorded_at")
        print(f"All metrics: {metrics}")

        # Select one metric using query mixin
        revenue = driver.select_one("SELECT * FROM analytics_data WHERE metric_name = $1", "revenue")
        print(f"Revenue metric: {revenue}")

        # Select one or none (no match) using query mixin
        nothing = driver.select_one_or_none("SELECT * FROM analytics_data WHERE metric_name = $1", "nothing")
        print(f"Nothing: {nothing}")

        # Select scalar value using query mixin
        avg_value = driver.select_value("SELECT AVG(metric_value) FROM analytics_data WHERE metric_value > $1", 1.0)
        print(f"Average metric value: {avg_value:.2f}")

        # Update
        result = driver.execute("UPDATE analytics_data SET dimensions = $1 WHERE metric_name = $2", 
                               '{"updated": true}', "bounce_rate")
        print(f"Updated {result.rows_affected} bounce rate records")

        # Delete
        result = driver.execute("DELETE FROM analytics_data WHERE metric_value < $1", 1.0)
        print(f"Removed {result.rows_affected} low-value metrics")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = Select("*").from_("analytics_data").where("metric_name = $1")
        page_view_metrics = driver.select(query, "page_views")
        print(f"Page view metrics: {page_view_metrics}")

        # Query builder with JSON operations (PostgreSQL-specific)
        query = Select("metric_name", "metric_value", "dimensions->>'device' as device").from_("analytics_data").where("dimensions->>'device' = $1")
        mobile_metrics = driver.select(query, "mobile")
        print(f"Mobile metrics: {mobile_metrics}")

        # Demonstrate pagination
        page_metrics = driver.select("SELECT * FROM analytics_data ORDER BY metric_value DESC LIMIT $1 OFFSET $2", 2, 0)
        total_count = driver.select_value("SELECT COUNT(*) FROM analytics_data")
        print(f"Page 1: {page_metrics}, Total: {total_count}")


def main() -> None:
    """Run ADBC example."""
    print("=== ADBC (Arrow Database Connectivity) Driver Example ===")
    try:
        adbc_example()
        print("✅ ADBC example completed successfully!")
    except Exception as e:
        print(f"❌ ADBC example failed: {e}")
        print("Make sure PostgreSQL is running with: make infra-up")


if __name__ == "__main__":
    main()