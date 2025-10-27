"""FastAPI application backed by SQLSpec and AioSQLite."""

import asyncio
from typing import Annotated, Any

from fastapi import Depends, FastAPI

from docs.examples.shared.configs import aiosqlite_registry
from docs.examples.shared.data import ARTICLES, CREATE_ARTICLES
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.core.statement import SQL

__all__ = ("get_session", "list_articles", "main", "on_startup", "seed_database")


registry = aiosqlite_registry()
config = registry.get_config(AiosqliteConfig)
app = FastAPI()


async def seed_database() -> None:
    """Create the demo schema and seed rows."""
    async with config.provide_session() as session:
        await session.execute(CREATE_ARTICLES)
        for row in ARTICLES:
            await session.execute(
                SQL(
                    """
                    INSERT OR REPLACE INTO articles (id, title, body)
                    VALUES (:id, :title, :body)
                    """
                ),
                row,
            )


@app.on_event("startup")
async def on_startup() -> None:
    """Seed data when the FastAPI app starts."""
    await seed_database()


async def get_session() -> "AiosqliteDriver":
    """Yield an AioSQLite session for request handlers."""
    async with config.provide_session() as session:
        yield session


@app.get("/articles")
async def list_articles(db_session: Annotated["AiosqliteDriver", Depends(get_session)]) -> "list[dict[str, Any]]":
    """Return all demo articles sorted by ID."""
    result = await db_session.execute(SQL("SELECT id, title, body FROM articles ORDER BY id"))
    return result.all()


def main() -> None:
    """Seed the database without running the ASGI server."""
    asyncio.run(seed_database())


if __name__ == "__main__":
    main()
