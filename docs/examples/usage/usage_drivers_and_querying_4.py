# Test module converted from docs example - code-block 4
"""Minimal smoke test for drivers_and_querying example 4."""

import os

from pytest_databases.docker.postgres import PostgresService

__all__ = ("test_example_4_async",)


async def test_example_4_async(postgres_service: PostgresService) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    spec = SQLSpec()
    dsn = os.environ.get("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/test")

    # Async version
    config = PsycopgAsyncConfig(pool_config={"conninfo": dsn, "min_size": 5, "max_size": 10})
    db = spec.add_config(config)

    async with spec.provide_session(db) as session:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS usage4_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE
        );
        """
        await session.execute(create_table_query)
        # Insert with RETURNING
        await session.execute(
            "INSERT INTO usage4_users (name, email) VALUES (%s, %s) RETURNING id", "Bill", "bill@example.com"
        )
        await session.execute("SELECT * FROM usage4_users")
    # end-example

    await spec.close_pool(db)
