"""FastAPI application backed by SQLSpec and SQLite."""

import asyncio
from collections.abc import Iterator
from typing import Annotated, Any

from fastapi import Depends, FastAPI

from docs.examples.shared.configs import sqlite_registry
from docs.examples.shared.data import ARTICLES, CREATE_ARTICLES
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.core import SQL

__all__ = ("get_session", "list_articles", "main", "on_startup", "seed_database")


registry = sqlite_registry()
config = registry.get_config(SqliteConfig)
app = FastAPI()


def seed_database() -> None:
    """Create the demo schema and seed rows."""
    with config.provide_session() as session:
        session.execute(CREATE_ARTICLES)
        for row in ARTICLES:
            session.execute(
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
    """Seed data when the ASGI app boots."""
    await asyncio.to_thread(seed_database)


def get_session() -> "Iterator[SqliteDriver]":
    """Yield a synchronous SQLite driver for request handlers."""
    with config.provide_session() as session:
        yield session


@app.get("/articles")
def list_articles(db_session: Annotated["SqliteDriver", Depends(get_session)]) -> "list[dict[str, Any]]":
    """Return all demo articles sorted by ID."""
    result = db_session.execute("SELECT id, title, body FROM articles ORDER BY id")
    return result.all()


def main() -> None:
    """Seed the database without running the ASGI server."""
    seed_database()


if __name__ == "__main__":
    main()
