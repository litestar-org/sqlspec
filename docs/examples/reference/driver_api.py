"""Fetch single optional rows directly with driver convenience methods."""

from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> dict[str, Any] | None:
    """Query a single optional user row via session.select_one_or_none."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("INSERT INTO users (name) VALUES ('Ada')")
        row = session.select_one_or_none("SELECT name FROM users WHERE id = ?", 1)
        assert row == {"name": "Ada"}
        return row


if __name__ == "__main__":
    print(main())
