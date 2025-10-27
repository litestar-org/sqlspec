"""Litestar example backed by SQLSpec and SQLite."""

from typing import Any

from litestar import Litestar, get

from docs.examples.shared.configs import sqlite_registry
from docs.examples.shared.data import ARTICLES, CREATE_ARTICLES
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.core.statement import SQL
from sqlspec.extensions.litestar import SQLSpecPlugin

registry = sqlite_registry()
config = registry.get_config(SqliteConfig)
plugin = SQLSpecPlugin(sqlspec=registry)


def seed_database() -> None:
    """Create the articles table and seed demo rows."""
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


@get("/articles", sync_to_thread=False)
def list_articles(db_session: "SqliteDriver") -> "list[dict[str, Any]]":
    """Return all articles from SQLite."""
    result = db_session.execute(SQL("SELECT id, title, body FROM articles ORDER BY id"))
    return result.all()


app = Litestar(route_handlers=[list_articles], on_startup=[seed_database], plugins=[plugin], debug=True)


def main() -> None:
    """Seed SQLite once when invoked as a script."""
    seed_database()


if __name__ == "__main__":
    main()
