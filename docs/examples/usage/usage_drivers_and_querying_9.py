# Test module converted from docs example - code-block 9
"""Minimal smoke test for drivers_and_querying example 9."""

from pytest_databases.docker.mysql import MySQLService

__all__ = ("test_example_9_asyncmy_config",)


async def test_example_9_asyncmy_config(mysql_service: MySQLService) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncmy import AsyncmyConfig

    spec = SQLSpec()

    config = AsyncmyConfig(
        pool_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "minsize": 1,
            "maxsize": 10,
        }
    )

    async with spec.provide_session(config) as session:
        create_table_query = """CREATE TABLE IF NOT EXISTS usage9_users (
           id INT PRIMARY KEY,
           name VARCHAR(100),
           email VARCHAR(100)
       );"""
        await session.execute(create_table_query)
        # insert a user
        await session.execute(
            "INSERT INTO usage9_users (id, name, email) VALUES (%s, %s, %s)", (1, "John Doe", "john.doe@example.com")
        )
        # query the user
        await session.execute("SELECT * FROM usage9_users WHERE id = %s", 1)
    # end-example
