# Example from docs/usage/drivers_and_querying.rst - code-block 1
# Typical driver usage
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

async def example():
    spec = SQLSpec()
    db = spec.add_config(AsyncpgConfig(pool_config={}))

    async with spec.provide_session(db) as session:
        result = await session.execute("SELECT 1")
        print(result)

if __name__ == "__main__":
    import asyncio
    asyncio.run(example())

