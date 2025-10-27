"""Flask routes using SQLSpec's synchronous SQLite adapter."""

from flask import Flask, Response, jsonify

from docs.examples.shared.configs import sqlite_registry
from docs.examples.shared.data import ARTICLES, CREATE_ARTICLES
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core.statement import SQL

__all__ = ("list_articles", "main", "seed_database")


registry = sqlite_registry()
config = registry.get_config(SqliteConfig)
app = Flask(__name__)


def seed_database() -> None:
    """Create the articles table and seed rows."""
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


@app.get("/articles")
def list_articles() -> "Response":
    """Return the article dataset as JSON."""
    with config.provide_session() as session:
        result = session.execute("SELECT id, title, body FROM articles ORDER BY id")
        return jsonify(result.all())


def main() -> None:
    """Seed the database without starting Flask."""
    seed_database()


if __name__ == "__main__":
    main()


# Seed eagerly so importing the module for smoke tests prepares the dataset.
seed_database()
