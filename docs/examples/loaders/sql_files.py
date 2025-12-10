"""Load named SQL statements from disk and execute them with SQLite."""

from pathlib import Path

from sqlspec import SQLFileLoader, SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


QUERIES = Path(__file__).resolve().parents[1] / "queries" / "users.sql"


def main() -> None:
    """Create the demo schema, run a named insert, and list rows."""
    loader = SQLFileLoader()
    loader.load_sql(QUERIES)
    registry = SQLSpec()
    config = registry.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    with registry.provide_session(config) as session:
        session.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                email TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login_at TIMESTAMP
            )
            """
        )
        session.execute(
            loader.get_sql("create_user"),
            {"username": "quinn", "email": "quinn@example.com", "password_hash": "demo", "is_active": True},
        )
        rows = session.select(loader.get_sql("list_active_users"), {"limit": 5, "offset": 0})
        totals = session.select(loader.get_sql("count_users_by_status"))
        print({"active": rows, "totals": totals})


if __name__ == "__main__":
    main()
