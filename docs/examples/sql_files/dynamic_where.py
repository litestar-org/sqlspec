"""Append dynamic WHERE predicates onto a named base SQL statement."""

from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Chain where, where_eq, and where_like filters onto a named SQL query."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    spec.add_named_sql("list_users", "SELECT id, name, status FROM users")

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, status TEXT)")
        session.execute(
            "INSERT INTO users (name, status) VALUES ('Alice', 'active'), ('Bob', 'inactive'), ('Charlie', 'active')"
        )

        base_query = spec.get_sql("list_users")
        active_users = session.select(base_query.where("status = 'active'"))
        bob = session.select(base_query.where_eq("name", "Bob"))
        alice_only = session.select(base_query.where_eq("status", "active").where_like("name", "A%"))

    assert len(active_users) == 2
    assert len(bob) == 1
    assert alice_only == [{"id": 1, "name": "Alice", "status": "active"}]
    return alice_only


if __name__ == "__main__":
    print(main())
