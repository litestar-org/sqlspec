# Test module converted from docs example - code-block 10
"""Minimal smoke test for drivers_and_querying example 10."""

from pathlib import Path

__all__ = ("test_example_10_duckdb_config",)


def test_example_10_duckdb_config() -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.duckdb import DuckDBConfig

    spec = SQLSpec()
    # In-memory
    config = DuckDBConfig()

    # Persistent
    config = DuckDBConfig(pool_config={"database": "analytics.duckdb"})

    with spec.provide_session(config) as session:
        # Create table from Parquet
        session.execute(f"""
           CREATE TABLE if not exists users AS
           SELECT * FROM read_parquet('{Path(__file__).parent.parent / "queries/users.parquet"}')
       """)

        # Analytical query
        session.execute("""
           SELECT date_trunc('day', created_at) as day,
                  count(*) as user_count
           FROM users
           GROUP BY day
           ORDER BY day
       """)
    # end-example
