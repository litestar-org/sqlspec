"""Litestar example that serves DuckDB data through SQLSpec."""

from typing import Any

from litestar import Litestar, get

from docs.examples.shared.configs import duckdb_registry
from docs.examples.shared.data import ARTICLES, CREATE_ARTICLES
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.core.statement import SQL
from sqlspec.extensions.litestar import SQLSpecPlugin

registry = duckdb_registry()
config = registry.get_config(DuckDBConfig)
plugin = SQLSpecPlugin(sqlspec=registry)


def seed_database() -> None:
    """Create the articles table and insert demo rows."""
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
def list_articles(db_session: "DuckDBDriver") -> "list[dict[str, Any]]":
    """Return the DuckDB article dataset."""
    result = db_session.execute(SQL("SELECT id, title, body FROM articles ORDER BY id"))
    return result.all()


app = Litestar(route_handlers=[list_articles], on_startup=[seed_database], plugins=[plugin], debug=True)


def main() -> None:
    """Seed DuckDB once when invoked as a script."""
    seed_database()


if __name__ == "__main__":
    main()
