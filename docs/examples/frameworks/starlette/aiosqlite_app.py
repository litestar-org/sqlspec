"""Starlette routes serving SQLSpec-backed AioSQLite data."""

import asyncio

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from docs.examples.shared.configs import aiosqlite_registry
from docs.examples.shared.data import ARTICLES, CREATE_ARTICLES
from sqlspec.core import SQL

__all__ = ("list_articles", "main", "seed_database")


registry, config = aiosqlite_registry()


async def seed_database() -> None:
    """Create tables and seed rows for the Starlette example."""
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


async def list_articles(_request: "Request") -> "JSONResponse":
    """Return JSON payload describing the article dataset."""
    async with config.provide_session() as session:
        result = await session.execute(SQL("SELECT id, title, body FROM articles ORDER BY id"))
        return JSONResponse(result.all())


routes = [Route("/articles", list_articles)]
app = Starlette(debug=True, routes=routes, on_startup=[seed_database])


def main() -> None:
    """Seed the database outside of ASGI startup."""
    asyncio.run(seed_database())


if __name__ == "__main__":
    main()
