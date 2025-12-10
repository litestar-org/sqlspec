# Test module converted from docs example - code-block 8
"""Minimal smoke test for drivers_and_querying example 8."""

__all__ = ("test_example_8_aiosqlite_config",)


from pathlib import Path


async def test_example_8_aiosqlite_config(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    database_file = tmp_path / "myapp.db"
    config = AiosqliteConfig(connection_config={"database": database_file})
    spec = SQLSpec()

    async with spec.provide_session(config) as session:
        create_table_query = """CREATE TABLE IF NOT EXISTS usage8_users (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           name TEXT NOT NULL
       );"""
        await session.execute(create_table_query)
        await session.execute("INSERT INTO usage8_users (name) VALUES (?)", "Bob")
        await session.execute("SELECT * FROM usage8_users")
    # end-example
