# Test module converted from docs example - code-block 10
"""Minimal smoke test for drivers_and_querying example 10."""

from pathlib import Path

import pytest

__all__ = ("test_example_10_duckdb_config",)

pytestmark = pytest.mark.xdist_group("duckdb")


def test_example_10_duckdb_config(tmp_path: Path) -> None:
    # start-example
    import tempfile

    from sqlspec import SQLSpec
    from sqlspec.adapters.duckdb import DuckDBConfig

    # Use a temporary directory for the DuckDB database for test isolation
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "analytics.duckdb"

        spec = SQLSpec()
        # In-memory
        in_memory_db = spec.add_config(DuckDBConfig())
        persistent_db = spec.add_config(DuckDBConfig(connection_config={"database": str(db_path)}))

        try:
            # Test with in-memory config
            with spec.provide_session(in_memory_db) as session:
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

            # Test with persistent config
            with spec.provide_session(persistent_db) as session:
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
        finally:
            # Close the pool for the persistent config
            in_memory_db.close_pool()
            persistent_db.close_pool()
            # The TemporaryDirectory context manager handles directory cleanup automatically
    # end-example
