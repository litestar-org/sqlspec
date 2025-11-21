# Test module converted from docs example - code-block 5
"""Minimal smoke test for drivers_and_querying example 5."""

import os

from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLSpec
from sqlspec.adapters.psqlpy import PsqlpyConfig

__all__ = ("test_example_5_construct_config",)


async def test_example_5_construct_config(postgres_service: PostgresService) -> None:
    # start-example
    spec = SQLSpec()
    dsn = os.environ.get("SQLSPEC_USAGE_PG_DSN", "postgresql://localhost/test")
    config = PsqlpyConfig(pool_config={"dsn": dsn})
    assert config is not None
    async with spec.provide_session(config) as session:
        create_table_query = """CREATE TABLE IF NOT EXISTS usage5_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE
        );        """
        await session.execute(create_table_query)
        # Insert with RETURNING
        await session.execute(
            "INSERT INTO usage5_users (name, email) VALUES ($1, $2) RETURNING id", "Bob", "bob@example.com"
        )
        await session.execute("SELECT * FROM usage5_users WHERE id = $1", 1)
    # end-example
