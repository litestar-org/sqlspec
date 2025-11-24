# Test module converted from docs example - code-block 3
"""Minimal smoke test for drivers_and_querying example 3."""

import os

import pytest
from pytest_databases.docker.postgres import PostgresService

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_example_3_sync",)


def test_example_3_sync(postgres_service: PostgresService) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.psycopg import PsycopgSyncConfig

    spec = SQLSpec()
    dsn = os.environ.get("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/test")

    # Sync version
    config = PsycopgSyncConfig(pool_config={"conninfo": dsn, "min_size": 5, "max_size": 10})
    db = spec.add_config(config)

    with spec.provide_session(db) as session:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS usage3_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE
        );
        """
        session.execute(create_table_query)
        # Insert with RETURNING
        session.execute(
            "INSERT INTO usage3_users (name, email) VALUES (%s, %s) RETURNING id", "Jane", "jane@example.com"
        )
        session.execute("SELECT * FROM usage3_users")
    # end-example

    spec.close_pool(db)
