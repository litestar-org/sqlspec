"""Integration tests for end-to-end storage workflows and real-world scenarios."""

import json
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def temp_directory() -> Generator[Path, None, None]:
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def analytics_database() -> Generator[SqliteDriver, None, None]:
    """Create a SQLite database with analytics-style data."""
    config = SqliteConfig(database=":memory:", statement_config=SQLConfig(strict_mode=False))

    with config.provide_session() as driver:
        # Create realistic analytics schema
        driver.execute_script("""
            CREATE TABLE users (
                user_id INTEGER PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                email TEXT,
                country TEXT,
                signup_date DATE,
                subscription_type TEXT DEFAULT 'free',
                last_active_date DATE
            );

            CREATE TABLE events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                event_type TEXT NOT NULL,
                event_data TEXT,  -- JSON string
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                session_id TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            CREATE TABLE revenue (
                transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount REAL NOT NULL,
                currency TEXT DEFAULT 'USD',
                transaction_date DATE,
                product_type TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
        """)

        # Insert sample analytics data
        users_data = [
            (1001, "alice_smith", "alice@example.com", "USA", "2024-01-15", "premium", "2024-01-20"),
            (1002, "bob_jones", "bob@test.org", "Canada", "2024-01-18", "free", "2024-01-19"),
            (1003, "charlie_brown", "charlie@demo.net", "UK", "2024-01-20", "premium", "2024-01-21"),
            (1004, "diana_prince", "diana@sample.io", "Australia", "2024-01-22", "business", "2024-01-22"),
            (1005, "eve_adams", "eve@mock.com", "Germany", "2024-01-25", "free", "2024-01-25"),
        ]

        events_data = [
            (1001, "login", '{"source": "web", "device": "desktop"}', "2024-01-20 09:00:00", "sess_001"),
            (1001, "page_view", '{"page": "/dashboard", "duration": 120}', "2024-01-20 09:05:00", "sess_001"),
            (1001, "feature_used", '{"feature": "export", "format": "csv"}', "2024-01-20 09:10:00", "sess_001"),
            (1002, "login", '{"source": "mobile", "device": "android"}', "2024-01-19 14:30:00", "sess_002"),
            (1002, "page_view", '{"page": "/reports", "duration": 45}', "2024-01-19 14:35:00", "sess_002"),
            (1003, "login", '{"source": "web", "device": "tablet"}', "2024-01-21 11:15:00", "sess_003"),
            (
                1003,
                "feature_used",
                '{"feature": "analytics", "filters": ["country", "date"]}',
                "2024-01-21 11:20:00",
                "sess_003",
            ),
            (1004, "api_call", '{"endpoint": "/api/v1/data", "method": "GET"}', "2024-01-22 16:45:00", "sess_004"),
        ]

        revenue_data = [
            (1001, 29.99, "USD", "2024-01-16", "premium_subscription"),
            (1003, 29.99, "USD", "2024-01-21", "premium_subscription"),
            (1004, 99.99, "USD", "2024-01-23", "business_subscription"),
            (1001, 9.99, "USD", "2024-01-20", "feature_addon"),
            (1003, 4.99, "USD", "2024-01-21", "data_export"),
        ]

        driver.execute_many("INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?)", users_data)
        driver.execute_many(
            "INSERT INTO events (user_id, event_type, event_data, timestamp, session_id) VALUES (?, ?, ?, ?, ?)",
            events_data,
        )
        driver.execute_many(
            "INSERT INTO revenue (user_id, amount, currency, transaction_date, product_type) VALUES (?, ?, ?, ?, ?)",
            revenue_data,
        )

        yield driver


def test_daily_analytics_export_workflow(analytics_database: SqliteDriver, temp_directory: Path) -> None:
    """Test a complete daily analytics export workflow."""
    export_date = "2024-01-20"

    # Step 1: Export user activity for the day
    user_activity_file = temp_directory / f"user_activity_{export_date.replace('-', '')}.parquet"

    analytics_database.export_to_storage(
        """
        SELECT
            u.user_id,
            u.username,
            u.country,
            u.subscription_type,
            COUNT(e.event_id) as event_count,
            COUNT(DISTINCT e.session_id) as session_count,
            MIN(e.timestamp) as first_activity,
            MAX(e.timestamp) as last_activity
        FROM users u
        LEFT JOIN events e ON u.user_id = e.user_id
            AND DATE(e.timestamp) = ?
        GROUP BY u.user_id, u.username, u.country, u.subscription_type
        ORDER BY event_count DESC
        """,
        [export_date],  # Pass parameters as positional arg
        destination_uri=str(user_activity_file),
    )

    # Step 2: Export revenue data for the day
    revenue_file = temp_directory / f"revenue_{export_date.replace('-', '')}.parquet"

    analytics_database.export_to_storage(
        """
        SELECT
            r.*,
            u.username,
            u.country,
            u.subscription_type
        FROM revenue r
        JOIN users u ON r.user_id = u.user_id
        WHERE r.transaction_date = ?
        ORDER BY r.amount DESC
        """,
        [export_date],  # Pass parameters as positional arg
        destination_uri=str(revenue_file),
    )

    # Step 3: Create summary report
    summary_data = {}

    # Read activity data
    if user_activity_file.exists():
        activity_table = pq.read_table(user_activity_file)
        summary_data["total_active_users"] = activity_table.num_rows
        if activity_table.num_rows > 0:
            total_events = sum(activity_table["event_count"].to_pylist())  # type: ignore[arg-type]
            total_sessions = sum(activity_table["session_count"].to_pylist())  # type: ignore[arg-type]
            summary_data["total_events"] = total_events
            summary_data["total_sessions"] = total_sessions

    # Read revenue data
    if revenue_file.exists():
        revenue_table = pq.read_table(revenue_file)
        summary_data["total_transactions"] = revenue_table.num_rows
        if revenue_table.num_rows > 0:
            daily_revenue = sum(revenue_table["amount"].to_pylist())  # type: ignore[arg-type]
            summary_data["daily_revenue"] = daily_revenue

    # Save summary
    summary_file = temp_directory / f"daily_summary_{export_date.replace('-', '')}.json"
    summary_data["export_date"] = export_date
    summary_data["files_generated"] = [str(user_activity_file.name), str(revenue_file.name)]

    with open(summary_file, "w") as f:
        json.dump(summary_data, f, indent=2)

    # Verify workflow completed successfully
    assert user_activity_file.exists()
    assert revenue_file.exists()
    assert summary_file.exists()

    # Verify data integrity
    assert summary_data["total_active_users"] >= 0
    assert summary_data["total_events"] >= 0
    if "daily_revenue" in summary_data:
        assert summary_data["daily_revenue"] > 0


def test_user_segmentation_export(analytics_database: SqliteDriver, temp_directory: Path) -> None:
    """Test user segmentation analysis and export."""
    # Create different user segments
    segments = {
        "premium_users": "SELECT * FROM users WHERE subscription_type = 'premium'",
        "business_users": "SELECT * FROM users WHERE subscription_type = 'business'",
        "free_users": "SELECT * FROM users WHERE subscription_type = 'free'",
        "international_users": "SELECT * FROM users WHERE country != 'USA'",
        "recent_signups": "SELECT * FROM users WHERE signup_date >= '2024-01-20'",
    }

    segment_stats = {}

    for segment_name, query in segments.items():
        # Export segment data
        segment_file = temp_directory / f"segment_{segment_name}.parquet"

        analytics_database.export_to_storage(query, destination_uri=str(segment_file))

        # Collect segment statistics
        if segment_file.exists():
            table = pq.read_table(segment_file)
            segment_stats[segment_name] = {
                "user_count": table.num_rows,
                "countries": list(set(table["country"].to_pylist())) if table.num_rows > 0 else [],
                "file_path": str(segment_file),
            }

    # Create comprehensive segment report
    segment_report = {
        "analysis_date": "2024-01-25",
        "total_segments": len(segments),
        "segment_details": segment_stats,
        "insights": [],
    }

    # Add insights based on data
    premium_count = segment_stats.get("premium_users", {}).get("user_count", 0)
    if isinstance(premium_count, int) and premium_count > 0:
        segment_report["insights"].append("Premium users present in dataset")

    intl_count = segment_stats.get("international_users", {}).get("user_count", 0)
    if isinstance(intl_count, int) and intl_count > 0:
        segment_report["insights"].append("International user base detected")

    # Save segment analysis
    report_file = temp_directory / "user_segmentation_report.json"
    with open(report_file, "w") as f:
        json.dump(segment_report, f, indent=2)

    # Verify segmentation worked
    assert report_file.exists()
    assert segment_report["total_segments"] == len(segments)

    # Verify at least some segments have users
    total_users_in_segments = 0
    for stats in segment_stats.values():
        if isinstance(stats, dict):
            user_count = stats.get("user_count", 0)
            if isinstance(user_count, (int, float)):
                total_users_in_segments += user_count
    assert total_users_in_segments > 0


def test_event_analytics_pipeline(analytics_database: SqliteDriver, temp_directory: Path) -> None:
    """Test event analytics data pipeline."""
    # Step 1: Export raw event data
    raw_events_file = temp_directory / "raw_events.parquet"

    analytics_database.export_to_storage(
        """
        SELECT
            e.*,
            u.username,
            u.country,
            u.subscription_type
        FROM events e
        JOIN users u ON e.user_id = u.user_id
        ORDER BY e.timestamp
        """,
        destination_uri=str(raw_events_file),
    )

    # Step 2: Create event type summary
    event_summary_file = temp_directory / "event_type_summary.csv"

    analytics_database.export_to_storage(
        """
        SELECT
            event_type,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(DISTINCT session_id) as unique_sessions,
            DATE(MIN(timestamp)) as first_seen,
            DATE(MAX(timestamp)) as last_seen
        FROM events
        GROUP BY event_type
        ORDER BY event_count DESC
        """,
        destination_uri=str(event_summary_file),
        format="csv",
    )

    # Step 3: User journey analysis
    user_journey_file = temp_directory / "user_journeys.json"

    analytics_database.export_to_storage(
        """
        SELECT
            user_id,
            GROUP_CONCAT(event_type, ' -> ') as event_sequence,
            COUNT(*) as total_events,
            COUNT(DISTINCT DATE(timestamp)) as active_days
        FROM events
        GROUP BY user_id
        ORDER BY total_events DESC
        """,
        destination_uri=str(user_journey_file),
        format="json",
    )

    # Verify pipeline outputs
    assert raw_events_file.exists()
    assert event_summary_file.exists()
    assert user_journey_file.exists()

    # Verify raw events data
    raw_table = pq.read_table(raw_events_file)
    assert raw_table.num_rows > 0
    assert "event_type" in raw_table.column_names
    assert "username" in raw_table.column_names

    # Verify CSV summary
    csv_content = event_summary_file.read_text()
    assert "event_type" in csv_content
    assert "event_count" in csv_content

    # Verify JSON journeys
    with open(user_journey_file) as f:
        journey_data = json.load(f)

    assert isinstance(journey_data, list)
    assert len(journey_data) > 0
    assert "user_id" in journey_data[0]


def test_revenue_analytics_workflow(analytics_database: SqliteDriver, temp_directory: Path) -> None:
    """Test comprehensive revenue analytics workflow."""
    # Monthly revenue summary
    monthly_revenue_file = temp_directory / "monthly_revenue_summary.parquet"

    analytics_database.export_to_storage(
        """
        SELECT
            SUBSTR(transaction_date, 1, 7) as month,
            product_type,
            COUNT(*) as transaction_count,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_transaction_value,
            MIN(amount) as min_transaction,
            MAX(amount) as max_transaction
        FROM revenue
        GROUP BY month, product_type
        ORDER BY month, total_revenue DESC
        """,
        destination_uri=str(monthly_revenue_file),
    )

    # User revenue breakdown
    user_revenue_file = temp_directory / "user_revenue_breakdown.parquet"

    analytics_database.export_to_storage(
        """
        SELECT
            u.user_id,
            u.username,
            u.country,
            u.subscription_type,
            COUNT(r.transaction_id) as total_transactions,
            COALESCE(SUM(r.amount), 0) as total_spent,
            COALESCE(AVG(r.amount), 0) as avg_transaction_value,
            MIN(r.transaction_date) as first_purchase,
            MAX(r.transaction_date) as last_purchase
        FROM users u
        LEFT JOIN revenue r ON u.user_id = r.user_id
        GROUP BY u.user_id, u.username, u.country, u.subscription_type
        ORDER BY total_spent DESC
        """,
        destination_uri=str(user_revenue_file),
    )

    # Revenue insights export
    insights_file = temp_directory / "revenue_insights.json"

    # Generate insights from the data
    total_revenue_result = analytics_database.execute("SELECT SUM(amount) as total FROM revenue")
    avg_transaction_result = analytics_database.execute("SELECT AVG(amount) as avg FROM revenue")
    top_product_result = analytics_database.execute(
        "SELECT product_type, SUM(amount) as revenue FROM revenue GROUP BY product_type ORDER BY revenue DESC LIMIT 1"
    )

    insights = {
        "analysis_period": "2024-01",
        "total_revenue": total_revenue_result.data[0]["total"] if total_revenue_result.data else 0,
        "average_transaction": avg_transaction_result.data[0]["avg"] if avg_transaction_result.data else 0,
        "top_product": top_product_result.data[0] if top_product_result.data else None,
        "files_generated": [str(monthly_revenue_file.name), str(user_revenue_file.name)],
    }

    with open(insights_file, "w") as f:
        json.dump(insights, f, indent=2)

    # Verify workflow
    assert monthly_revenue_file.exists()
    assert user_revenue_file.exists()
    assert insights_file.exists()

    # Verify data quality
    monthly_table = pq.read_table(monthly_revenue_file)
    user_table = pq.read_table(user_revenue_file)

    assert monthly_table.num_rows > 0
    assert user_table.num_rows > 0

    # Verify insights make sense
    total_rev = insights["total_revenue"]
    assert isinstance(total_rev, (int, float)) and total_rev > 0
    avg_trans = insights["average_transaction"]
    assert isinstance(avg_trans, (int, float)) and avg_trans > 0
    assert insights["top_product"] is not None


def test_data_backup_and_archival_workflow(analytics_database: SqliteDriver, temp_directory: Path) -> None:
    """Test data backup and archival workflow."""
    backup_dir = temp_directory / "backups"
    backup_dir.mkdir()

    # Create timestamped backups
    timestamp = "20240125_120000"

    # Full data backup
    tables_to_backup = ["users", "events", "revenue"]
    backup_manifest: dict[str, Any] = {"backup_timestamp": timestamp, "tables": [], "total_records": 0}

    for table_name in tables_to_backup:
        backup_file = backup_dir / f"{table_name}_{timestamp}.parquet"

        # Export table with metadata
        analytics_database.export_to_storage(
            f"SELECT * FROM {table_name}",
            destination_uri=str(backup_file),
            compression="gzip",  # Use compression for archival
        )

        if backup_file.exists():
            table = pq.read_table(backup_file)
            table_info = {
                "table_name": table_name,
                "record_count": table.num_rows,
                "file_path": str(backup_file.name),
                "file_size_bytes": backup_file.stat().st_size,
                "columns": table.column_names,
            }
            backup_manifest["tables"].append(table_info)
            backup_manifest["total_records"] += table.num_rows

    # Save backup manifest
    manifest_file = backup_dir / f"backup_manifest_{timestamp}.json"
    with open(manifest_file, "w") as f:
        json.dump(backup_manifest, f, indent=2)

    # Verify backup completed
    assert manifest_file.exists()
    tables_list = backup_manifest["tables"]
    assert isinstance(tables_list, list)
    assert len(tables_list) == len(tables_to_backup)
    total_records = backup_manifest["total_records"]
    assert isinstance(total_records, int) and total_records > 0

    # Verify all backup files exist
    for table_info in backup_manifest["tables"]:
        backup_path = backup_dir / table_info["file_path"]
        assert backup_path.exists()
        assert backup_path.stat().st_size > 0


def test_multi_format_export_workflow(analytics_database: SqliteDriver, temp_directory: Path) -> None:
    """Test exporting the same data to multiple formats for different use cases."""
    base_query = """
        SELECT
            u.username,
            u.country,
            u.subscription_type,
            COUNT(e.event_id) as total_events,
            COUNT(r.transaction_id) as total_purchases,
            COALESCE(SUM(r.amount), 0) as total_spent
        FROM users u
        LEFT JOIN events e ON u.user_id = e.user_id
        LEFT JOIN revenue r ON u.user_id = r.user_id
        GROUP BY u.username, u.country, u.subscription_type
        ORDER BY total_spent DESC
    """

    # Export to different formats for different use cases
    formats = {
        "parquet": {"use_case": "data_analysis", "compression": "snappy"},
        "csv": {"use_case": "spreadsheet_import", "compression": None},
        "json": {"use_case": "api_consumption", "compression": None},
    }

    export_results = {}

    for format_name, config in formats.items():
        output_file = temp_directory / f"user_summary.{format_name}"

        export_kwargs: dict[str, Any] = {"format": format_name}
        compression = config.get("compression") if isinstance(config, dict) else None
        if compression:
            export_kwargs["compression"] = compression

        analytics_database.export_to_storage(
            base_query, destination_uri=str(output_file), _config=None, **export_kwargs
        )

        if output_file.exists():
            file_size = output_file.stat().st_size
            use_case = config.get("use_case", "unknown") if isinstance(config, dict) else "unknown"
            export_results[format_name] = {
                "file_path": str(output_file),
                "file_size": file_size,
                "use_case": use_case,
                "success": True,
            }
        else:
            export_results[format_name] = {"success": False}

    # Create export summary
    summary_file = temp_directory / "multi_format_export_summary.json"
    summary = {
        "export_date": "2024-01-25",
        "base_query": base_query.strip(),
        "formats_exported": export_results,
        "total_formats": len([r for r in export_results.values() if r.get("success")]),
    }

    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)

    # Verify multi-format export
    assert summary_file.exists()
    total_formats = summary["total_formats"]
    assert isinstance(total_formats, int) and total_formats >= 2  # At least 2 formats should succeed

    # Verify data consistency across formats
    for format_name, result in export_results.items():
        if result.get("success"):
            file_path_str = result.get("file_path")
            if isinstance(file_path_str, str):
                file_path = Path(file_path_str)
                assert file_path.exists()
                assert file_path.stat().st_size > 0
