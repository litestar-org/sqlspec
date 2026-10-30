"""Open a synchronous SQLite session and execute a parameterized query."""

from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Insert and fetch a note in an in-memory SQLite database."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute(
            """
            CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                body TEXT
            )
            """
        )
        # --8<-- [start:example]
        session.execute(
            """
            INSERT INTO notes (body)
            VALUES (?)
            """,
            "Hello, SQLSpec",
        )
        rows = session.select(
            """
            SELECT id, body
            FROM notes
            """
        )
        # --8<-- [end:example]
        assert rows == [{"id": 1, "body": "Hello, SQLSpec"}]
        return rows


if __name__ == "__main__":
    print(main())
