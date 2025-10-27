"""Litestar application backed by SQLSpec and AioSQLite."""

import asyncio
from typing import Any

from litestar import Litestar, get

from docs.examples.shared.configs import aiosqlite_registry
from docs.examples.shared.data import ARTICLES, CREATE_ARTICLES
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.core.statement import SQL
from sqlspec.extensions.litestar import SQLSpecPlugin

registry = aiosqlite_registry()
config = registry.get_config(AiosqliteConfig)
plugin = SQLSpecPlugin(sqlspec=registry)


async def seed_database() -> None:
    """Ensure the demo schema exists and seed a few rows."""
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


@get("/articles")
async def list_articles(db_session: "AiosqliteDriver") -> "list[dict[str, Any]]":
    """Return all demo articles."""
    result = await db_session.execute(SQL("SELECT id, title, body FROM articles ORDER BY id"))
    return result.all()


app = Litestar(route_handlers=[list_articles], on_startup=[seed_database], plugins=[plugin], debug=True)


def main() -> None:
    """Seed the database once when run as a script."""
    asyncio.run(seed_database())


if __name__ == "__main__":
    main()
