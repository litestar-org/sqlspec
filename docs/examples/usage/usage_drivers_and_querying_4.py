# Test module converted from docs example - code-block 4
"""Minimal smoke test for drivers_and_querying example 4."""

from pytest_databases.docker.postgres import PostgresService


async def test_example_4_async(postgres_service: PostgresService) -> None:
    from sqlspec import SQLSpec
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    spec = SQLSpec()
    # Async version
    config = PsycopgAsyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "min_size": 5,
            "max_size": 10,
        }
    )

    async with spec.provide_session(config) as session:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE
        );
        """
        await session.execute(create_table_query)
        # Insert with RETURNING
        await session.execute(
            "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id", "Alice", "alice@example.com"
        )
        await session.execute("SELECT * FROM users")
