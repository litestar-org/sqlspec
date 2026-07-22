"""ADBC Arrow residuals for backend-specific analytical SQL.

The shared Arrow and parameter-codec contracts own generic Arrow table output,
NULL preservation, large result handling, PostgreSQL arrays, SQLite binary data,
and DuckDB nested type materialization. This module keeps DuckDB analytical SQL
coverage that is specific to that backend.
"""

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.adbc import AdbcConfig
from tests.integration.fixtures.adbc import xfail_if_driver_missing


@pytest.mark.xdist_group("duckdb")
@pytest.mark.adbc
@xfail_if_driver_missing
def test_arrow_duckdb_advanced_analytics() -> None:
    """DuckDB-backed ADBC handles analytical aggregations and window functions."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE analytics_test_adbc (
                id INTEGER,
                category TEXT,
                value DOUBLE,
                timestamp TIMESTAMP,
                tags TEXT[]
            )
        """)

        analytical_data = [
            (1, "A", 100.5, "2024-01-01 10:00:00", ["tag1", "tag2"]),
            (2, "B", 200.3, "2024-01-01 11:00:00", ["tag2", "tag3"]),
            (3, "A", 150.7, "2024-01-01 12:00:00", ["tag1", "tag3"]),
            (4, "C", 300.2, "2024-01-01 13:00:00", ["tag1"]),
            (5, "B", 250.8, "2024-01-01 14:00:00", ["tag2"]),
        ]
        for row in analytical_data:
            session.execute("INSERT INTO analytics_test_adbc VALUES (?, ?, ?, ?, ?)", row)

        analytical_query = session.execute("""
            SELECT
                category,
                COUNT(*) as record_count,
                AVG(value) as avg_value,
                STDDEV(value) as stddev_value,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median_value,
                list_distinct(flatten(ARRAY_AGG(tags))) as all_tags,
                MIN(timestamp) as first_timestamp,
                MAX(timestamp) as last_timestamp
            FROM analytics_test_adbc
            GROUP BY category
            ORDER BY category
        """)

        assert isinstance(analytical_query, SQLResult)
        data = analytical_query.get_data()
        assert len(data) == 3
        category_a = next(row for row in data if row["category"] == "A")
        assert category_a["record_count"] == 2
        assert abs(category_a["avg_value"] - 125.6) < 0.1

        window_query = session.execute("""
            SELECT
                id,
                category,
                value,
                LAG(value) OVER (PARTITION BY category ORDER BY timestamp) as prev_value,
                LEAD(value) OVER (PARTITION BY category ORDER BY timestamp) as next_value,
                ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as value_rank
            FROM analytics_test_adbc
            ORDER BY category, timestamp
        """)

        assert len(window_query.get_data()) == 5
