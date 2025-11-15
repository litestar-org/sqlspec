"""Positional parameter styles across drivers."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_example_17_positional_parameters",)


def test_example_17_positional_parameters() -> None:
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with spec.provide_session(db) as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, status TEXT, name TEXT)")

        # start-example
        session.execute("INSERT INTO users (id, status, name) VALUES (?, ?, ?)", 1, "active", "Alice")
        user = session.select_one("SELECT name, status FROM users WHERE id = ?", 1)
        # end-example

        assert user["name"] == "Alice"
        assert user["status"] == "active"
