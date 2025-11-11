# Test module converted from docs example - code-block 5
"""Minimal smoke test for drivers_and_querying example 5."""

from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLSpec
from sqlspec.adapters.psqlpy import PsqlpyConfig


async def test_example_5_construct_config(postgres_service: PostgresService) -> None:
    spec = SQLSpec()
    config = PsqlpyConfig(
        pool_config={
            "dsn": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    assert config is not None
    async with spec.provide_session(config) as session:
        create_table_query = """CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE
        );        """
        await session.execute(create_table_query)
        # Insert with RETURNING
        await session.execute(
            "INSERT INTO users (name, email) VALUES ($1, $2) ETURNING id", "Alice", "alice@example.com"
        )
        await session.execute("SELECT * FROM users WHERE id = $1", 1)
