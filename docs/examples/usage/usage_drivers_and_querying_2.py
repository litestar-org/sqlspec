# Test module converted from docs example - code-block 2
"""Minimal smoke test for drivers_and_querying example 2."""

import os

from pytest_databases.docker.postgres import PostgresService

__all__ = ("test_example_2_importable",)


async def test_example_2_importable(postgres_service: PostgresService) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    spec = SQLSpec()
    dsn = os.environ.get("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/test")
    db = spec.add_config(AsyncpgConfig(pool_config={"dsn": dsn, "min_size": 10, "max_size": 20}))
    async with spec.provide_session(db) as session:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE
        );
        """
        await session.execute(create_table_query)
        # Insert with RETURNING
        result = await session.execute(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id", "Gretta", "gretta@example.com"
        )
        new_id = result.scalar()
        print(f"Inserted user with ID: {new_id}")
        # Basic query
        result = await session.execute("SELECT * FROM users WHERE id = $1", 1)
        user = result.one()
        print(f"User: {user}")
    # end-example
