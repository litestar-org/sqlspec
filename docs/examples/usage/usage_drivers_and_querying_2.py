# Test module converted from docs example - code-block 2
"""Minimal smoke test for drivers_and_querying example 2."""

from pytest_databases.docker.postgres import PostgresService


async def test_example_2_importable(postgres_service: PostgresService) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    spec = SQLSpec()
    db = spec.add_config(
        AsyncpgConfig(
            pool_config={
                "dsn": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
                "min_size": 10,
                "max_size": 20,
            }
        )
    )
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
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id", "Alice", "alice@example.com"
        )
        new_id = result.scalar()
        print(f"Inserted user with ID: {new_id}")
        # Basic query
        result = await session.execute("SELECT * FROM users WHERE id = $1", 1)
        user = result.one()
        print(f"User: {user}")
    # end-example
