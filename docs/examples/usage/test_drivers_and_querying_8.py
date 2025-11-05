# Example from docs/usage/drivers_and_querying.rst - code-block 8
from sqlspec.adapters.aiosqlite import AiosqliteConfig

async def example_aiosqlite():
    config = AiosqliteConfig(
        pool_config={"database": "myapp.db"}
    )

    # async with spec.provide_session(config) as session:
    #     await session.execute(...)

if __name__ == "__main__":
    import asyncio
    asyncio.run(example_aiosqlite())

