# Test module converted from docs example - code-block 10
"""Minimal smoke test for drivers_and_querying example 10."""

from sqlspec.adapters.duckdb import DuckDBConfig


def test_example_10_duckdb_config() -> None:
    from sqlspec import SQLSpec

    spec = SQLSpec()
    # In-memory
    config = DuckDBConfig()

    # Persistent
    config = DuckDBConfig(pool_config={"database": "analytics.duckdb"})

    with spec.provide_session(config) as session:
        # Create table from Parquet
        session.execute("""
           CREATE TABLE users AS
           SELECT * FROM read_parquet('users.parquet')
       """)

        # Analytical query
        session.execute("""
           SELECT date_trunc('day', created_at) as day,
                  count(*) as user_count
           FROM users
           GROUP BY day
           ORDER BY day
       """)
