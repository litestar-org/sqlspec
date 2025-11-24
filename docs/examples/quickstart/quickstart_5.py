"""Async PostgreSQL quickstart example."""

import pytest

pytestmark = pytest.mark.xdist_group("postgres")

__all__ = ("test_quickstart_5",)


@pytest.mark.asyncio
async def test_quickstart_5() -> None:
    """Demonstrate async PostgreSQL usage with SQLSpec."""
    # start-example
    import os
    from typing import Any

    from pydantic import BaseModel

    from sqlspec import SQLSpec
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    class User(BaseModel):
        id: int
        name: str
        email: str

    def pool_config() -> "dict[str, Any]":
        return {
            "host": os.getenv("SQLSPEC_QUICKSTART_PG_HOST", "localhost"),
            "port": int(os.getenv("SQLSPEC_QUICKSTART_PG_PORT", "5432")),
            "user": os.getenv("SQLSPEC_QUICKSTART_PG_USER", "postgres"),
            "password": os.getenv("SQLSPEC_QUICKSTART_PG_PASSWORD", "postgres"),
            "database": os.getenv("SQLSPEC_QUICKSTART_PG_DATABASE", "mydb"),
        }

    async def seed_users(session: Any) -> None:
        await session.execute(
            """
            CREATE TABLE IF NOT EXISTS qs5_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
            """
        )
        await session.execute("TRUNCATE TABLE qs5_users")
        await session.execute(
            "INSERT INTO qs5_users (id, name, email) VALUES ($1, $2, $3)", 1, "Alice", "alice@example.com"
        )

    db_manager = SQLSpec()
    db = db_manager.add_config(AsyncpgConfig(pool_config=pool_config()))

    async with db_manager.provide_session(db) as session:
        await seed_users(session)
        user = await session.select_one("SELECT * FROM qs5_users WHERE id = $1", 1, schema_type=User)
        print(f"User: {user.name}")
    # end-example

    assert user == User(id=1, name="Alice", email="alice@example.com")
