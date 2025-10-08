# /// script
# dependencies = [
#   "sqlspec[adbc]",
#   "rich",
# ]
# requires-python = ">=3.10"
# ///
"""Example demonstrating ADBC driver usage with query mixins.

This example shows how to use the ADBC (Arrow Database Connectivity) driver
with the development PostgreSQL container started by `make infra-up`.
"""

from rich import print

from sqlspec import SQLSpec, sql
from sqlspec.adapters.adbc import AdbcConfig

__all__ = ("adbc_example", "main")


def adbc_example() -> None:
    """Demonstrate ADBC database driver usage with query mixins."""
    # Create SQLSpec instance with ADBC (connects to dev PostgreSQL container)
    spec = SQLSpec()
    db = spec.add_config(
        AdbcConfig(connection_config={"uri": "postgresql://postgres:postgres@localhost:5433/postgres"})
    )

    # Get a driver directly (drivers now have built-in query methods)
    with spec.provide_session(db) as driver:
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
        driver.execute(
            "INSERT INTO analytics_data (metric_name, metric_value, dimensions) VALUES ($1, $2, $3::jsonb)",
            "page_views",
            1250.0,
            '{"source": "organic", "device": "desktop"}',
        )

        # Insert multiple rows
        driver.execute_many(
            "INSERT INTO analytics_data (metric_name, metric_value, dimensions) VALUES ($1, $2, $3::jsonb)",
            [
                ("conversion_rate", 0.045, '{"funnel": "signup", "campaign": "summer"}'),
                ("revenue", 15420.50, '{"product": "pro", "region": "us"}'),
                ("bounce_rate", 0.32, '{"page": "landing", "source": "paid"}'),
                ("session_duration", 180.5, '{"device": "mobile", "browser": "chrome"}'),
            ],
        )

        # Select all metrics using query mixin
        metrics = driver.select("SELECT * FROM analytics_data ORDER BY recorded_at")
        print(f"[cyan]All metrics:[/cyan] {metrics}")

        # Select one metric using query mixin
        revenue = driver.select_one("SELECT * FROM analytics_data WHERE metric_name = $1", "revenue")
        print(f"[cyan]Revenue metric:[/cyan] {revenue}")

        # Select one or none (no match) using query mixin
        nothing = driver.select_one_or_none("SELECT * FROM analytics_data WHERE metric_name = $1", "nothing")
        print(f"[cyan]Nothing:[/cyan] {nothing}")

        # Select scalar value using query mixin
        avg_value = driver.select_value("SELECT AVG(metric_value) FROM analytics_data WHERE metric_value > $1", 1.0)
        print(f"[cyan]Average metric value:[/cyan] {avg_value:.2f}")

        # Update
        result = driver.execute(
            "UPDATE analytics_data SET dimensions = $1::jsonb WHERE metric_name = $2",
            '{"updated": true}',
            "bounce_rate",
        )
        print(f"[yellow]Updated {result.rows_affected} bounce rate records[/yellow]")

        # Delete
        result = driver.execute("DELETE FROM analytics_data WHERE metric_value < $1", 1.0)
        print(f"[yellow]Removed {result.rows_affected} low-value metrics[/yellow]")

        # Use query builder with driver - this demonstrates the QueryBuilder parameter fix
        query = sql.select("*").from_("analytics_data").where("metric_name = $1")
        page_view_metrics = driver.select(query, "page_views")
        print(f"[cyan]Page view metrics:[/cyan] {page_view_metrics}")

        # JSON operations (PostgreSQL-specific) - using raw SQL due to SQLGlot JSON operator conversion
        mobile_metrics = driver.select(
            "SELECT metric_name, metric_value, dimensions->>'device' as device FROM analytics_data WHERE dimensions->>'device' = $1",
            "mobile",
        )
        print(f"[cyan]Mobile metrics:[/cyan] {mobile_metrics}")

        # Demonstrate pagination
        page_metrics = driver.select("SELECT * FROM analytics_data ORDER BY metric_value DESC LIMIT $1 OFFSET $2", 2, 0)
        total_count = driver.select_value("SELECT COUNT(*) FROM analytics_data")
        print(f"[cyan]Page 1:[/cyan] {page_metrics}, [cyan]Total:[/cyan] {total_count}")


def main() -> None:
    """Run ADBC example."""
    print("[bold cyan]=== ADBC (Arrow Database Connectivity) Driver Example ===[/bold cyan]")
    try:
        adbc_example()
        print("[green]✅ ADBC example completed successfully![/green]")
    except Exception as e:
        print(f"[red]❌ ADBC example failed: {e}[/red]")
        print("[yellow]Make sure PostgreSQL is running with: make infra-up[/yellow]")


if __name__ == "__main__":
    main()
