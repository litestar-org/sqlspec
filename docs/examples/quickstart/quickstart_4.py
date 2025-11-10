"""Async quickstart example."""

import pytest

__all__ = ("test_quickstart_4",)


@pytest.mark.asyncio
async def test_quickstart_4() -> None:
    """Demonstrate async SQLSpec usage."""
    # start-example
    from pydantic import BaseModel

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    class User(BaseModel):
        id: int
        name: str
        email: str

    db_manager = SQLSpec()
    db = db_manager.add_config(AiosqliteConfig(pool_config={"database": ":memory:"}))

    async with db_manager.provide_session(db) as session:
        await session.execute(
            """
            CREATE TABLE users (id INTEGER, name TEXT, email TEXT)
            """
        )
        await session.execute("INSERT INTO users VALUES (?, ?, ?)", 1, "Alice", "alice@example.com")
        user = await session.select_one("SELECT * FROM users WHERE id = ?", 1, schema_type=User)
        print(f"User: {user.name}")
    # end-example

    assert user == User(id=1, name="Alice", email="alice@example.com")
