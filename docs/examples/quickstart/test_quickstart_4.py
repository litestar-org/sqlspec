from pydantic import BaseModel

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig


class User(BaseModel):
    id: int
    name: str
    email: str


async def test_quickstart_4() -> None:
    db_manager = SQLSpec()
    db = db_manager.add_config(AiosqliteConfig(pool_config={"database": ":memory:"}))

    async with db_manager.provide_session(db) as session:
        # Create table
        _ = await session.execute("""
            CREATE TABLE users (id INTEGER, name TEXT, email TEXT)
        """)

        # Insert data
        _ = await session.execute("INSERT INTO users VALUES (?, ?, ?)", 1, "Alice", "alice@example.com")

        # Type-safe async query
        user = await session.select_one("SELECT * FROM users WHERE id = ?", 1, schema_type=User)

        print(f"User: {user.name}")

    assert user == User(id=1, name="Alice", email="alice@example.com")
    assert isinstance(user, User)
    assert user.name == "Alice"
    assert user.email == "alice@example.com"
