"""Shared schema and sample rows for documentation examples."""

from sqlspec.core.statement import SQL

__all__ = ("ARTICLES", "CREATE_ARTICLES")

CREATE_ARTICLES = SQL(
    """
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        body TEXT NOT NULL
    )
    """
)

ARTICLES: "tuple[dict[str, object], ...]" = (
    {"id": 1, "title": "Getting started", "body": "SQLSpec stays close to SQL."},
    {"id": 2, "title": "Adapters", "body": "Pick the driver that fits your stack."},
)
