"""Dispatch requests to dedicated SQLite configs per tenant."""

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import SQL

__all__ = ("TenantRouter", "main")


TENANTS: "tuple[str, ...]" = ("acme", "orbital")


class TenantRouter:
    """Maintain isolated SqliteConfig instances for each tenant slug."""

    def __init__(self, tenants: "tuple[str, ...]") -> None:
        self._configs = {slug: SqliteConfig(pool_config={"database": ":memory:"}, bind_key=slug) for slug in tenants}

    def insert_article(self, slug: str, title: str) -> None:
        config = self._configs[slug]
        with config.provide_session() as session:
            session.execute(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL
                )
                """
            )
            session.execute("INSERT INTO articles (title) VALUES (:title)", {"title": title})

    def list_titles(self, slug: str) -> "list[str]":
        config = self._configs[slug]
        with config.provide_session() as session:
            result = session.execute(SQL("SELECT title FROM articles ORDER BY id"))
            return [row["title"] for row in result.all()]


def main() -> None:
    """Insert one record per tenant and print the isolated results."""
    router = TenantRouter(TENANTS)
    router.insert_article("acme", "Acme onboarding")
    router.insert_article("orbital", "Orbital checklist")
    payload: dict[str, list[str]] = {slug: router.list_titles(slug) for slug in TENANTS}
    print(payload)


if __name__ == "__main__":
    main()
