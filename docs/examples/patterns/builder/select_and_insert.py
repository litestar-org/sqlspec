"""Showcase SQL builder fluency with a compact SQLite dataset."""

from docs.examples.shared.data import ARTICLES
from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> None:
    """Create a table, insert demo rows, and fetch results with the builder API."""
    registry = SQLSpec()
    config = registry.add_config(SqliteConfig(pool_config={"database": ":memory:"}))
    with registry.provide_session(config) as session:
        session.execute(
            """
            CREATE TABLE articles (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                body TEXT NOT NULL
            )
            """
        )
        session.execute_many(
            """
            INSERT INTO articles (id, title, body)
            VALUES (:id, :title, :body)
            """,
            ARTICLES,
        )
        query = sql.select("id", "title").from_("articles").where("title LIKE ?")
        rows = session.select(query, "%SQL%")
        print({"rows": rows})


if __name__ == "__main__":
    main()
