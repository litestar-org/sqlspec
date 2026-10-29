"""Execute a parameterized SQLite query and fetch a typed dictionary row."""

from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> dict[str, Any]:
    """Run the first-query quickstart example."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, points INTEGER)")
        session.execute("INSERT INTO users (name, points) VALUES (:name, :points)", name="Ada", points=42)
        count = session.select_value("SELECT COUNT(*) FROM users", value_type=int)
        row = session.select_one("SELECT id, name, points FROM users WHERE name = :name", name="Ada")
        assert count == 1
        assert row == {"id": 1, "name": "Ada", "points": 42}
        return row


if __name__ == "__main__":
    print(main())
