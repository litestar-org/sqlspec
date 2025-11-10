import os
from typing import Any

from pydantic import BaseModel

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

__all__ = ("User", "test_quickstart_5")


class User(BaseModel):
    id: int
    name: str
    email: str


def _pool_config() -> "dict[str, Any]":
    return {
        "host": os.getenv("SQLSPEC_QUICKSTART_PG_HOST", "localhost"),
        "port": int(os.getenv("SQLSPEC_QUICKSTART_PG_PORT", "5432")),
        "user": os.getenv("SQLSPEC_QUICKSTART_PG_USER", "postgres"),
        "password": os.getenv("SQLSPEC_QUICKSTART_PG_PASSWORD", "postgres"),
        "database": os.getenv("SQLSPEC_QUICKSTART_PG_DATABASE", "mydb"),
    }


async def _seed_users(session: Any) -> None:
    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        )
        """
    )
    await session.execute("TRUNCATE TABLE users")
    await session.execute("INSERT INTO users (id, name, email) VALUES ($1, $2, $3)", 1, "Alice", "alice@example.com")


async def test_quickstart_5() -> None:
    db_manager = SQLSpec()
    db = db_manager.add_config(AsyncpgConfig(pool_config=_pool_config()))

    async with db_manager.provide_session(db) as session:
        await _seed_users(session)

        # PostgreSQL uses $1, $2 for parameters instead of ?
        user = await session.select_one("SELECT * FROM users WHERE id = $1", 1, schema_type=User)
        print(f"User: {user.name}")

    assert user == User(id=1, name="Alice", email="alice@example.com")
