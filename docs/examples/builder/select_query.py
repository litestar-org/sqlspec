"""Build and execute a parameterized SELECT query with the fluent SQL builder."""

from typing import Any

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> dict[str, Any]:
    """Query a single team record from an in-memory SQLite database."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("INSERT INTO teams (name) VALUES ('SQLSpec')")

        query = sql.select("id", "name").from_("teams").where("name = ?")
        row = session.execute(query, "SQLSpec").one()
        assert row == {"id": 1, "name": "SQLSpec"}
        return row


if __name__ == "__main__":
    print(main())
