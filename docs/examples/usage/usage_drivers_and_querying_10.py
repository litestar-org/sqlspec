# Test module converted from docs example - code-block 10
"""Minimal smoke test for drivers_and_querying example 10."""

import tempfile
from pathlib import Path

import pytest

__all__ = ("test_example_10_duckdb_config",)

pytestmark = pytest.mark.xdist_group("duckdb")


def test_example_10_duckdb_config(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.duckdb import DuckDBConfig

    # Use a temporary directory for the DuckDB database for test isolation
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "analytics.duckdb"

        spec = SQLSpec()
        # In-memory
        in_memory_config = DuckDBConfig()

        # Persistent (using the temporary file in the temporary directory)
        persistent_config = DuckDBConfig(pool_config={"database": str(db_path)})

        try:
            # Test with in-memory config
            with spec.provide_session(in_memory_config) as session:
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
            with spec.provide_session(persistent_config) as session:
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
            persistent_config.close_pool()
            # The TemporaryDirectory context manager handles directory cleanup automatically
    # end-example
