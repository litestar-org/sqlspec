# Example from docs/usage/drivers_and_querying.rst - code-block 2
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

async def example_asyncpg():
    spec = SQLSpec()
    db = spec.add_config(
        AsyncpgConfig(
            pool_config={
                "dsn": "postgresql://user:pass@localhost:5432/mydb",
                "min_size": 10,
                "max_size": 20,
            }
        )
    )

    async with spec.provide_session(db) as session:
        # Basic query
        result = await session.execute("SELECT * FROM users WHERE id = $1", 1)
        user = result.one()

        # Insert with RETURNING
        result = await session.execute(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
            "Alice",
            "alice@example.com"
        )
        new_id = result.scalar()
        print(user, new_id)

if __name__ == "__main__":
    import asyncio
    asyncio.run(example_asyncpg())

