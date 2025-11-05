# Example from docs/usage/drivers_and_querying.rst - code-block 9
from sqlspec.adapters.asyncmy import AsyncmyConfig

async def example_asyncmy():
    config = AsyncmyConfig(
        pool_config={
            "host": "localhost",
            "port": 3306,
            "user": "myuser",
            "password": "mypassword",
            "database": "mydb",
            "minsize": 1,
            "maxsize": 10,
        }
    )

    # async with spec.provide_session(config) as session:
    #     result = await session.execute("SELECT * FROM users WHERE id = %s", 1)

if __name__ == "__main__":
    pass

