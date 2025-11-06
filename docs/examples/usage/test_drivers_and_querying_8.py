# Test module converted from docs example - code-block 8
"""Minimal smoke test for drivers_and_querying example 8."""


async def test_example_8_aiosqlite_config() -> None:
    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    config = AiosqliteConfig(pool_config={"database": "myapp.db"})
    spec = SQLSpec()

    async with spec.provide_session(config) as session:
        create_table_query = """CREATE TABLE IF NOT EXISTS users (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           name TEXT NOT NULL
       );"""
        await session.execute(create_table_query)
        await session.execute("INSERT INTO users (name) VALUES (?)", "Bob")
        await session.execute("SELECT * FROM users")
