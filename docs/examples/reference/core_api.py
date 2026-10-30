"""Transform projections on a core SQL statement with select_only."""

from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import SQL

__all__ = ("main",)


def main() -> dict[str, Any] | None:
    """Rewrite a SELECT projection with SQL.select_only and execute it."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
        session.execute("INSERT INTO notes (body) VALUES ('Note')")
        # --8<-- [start:example]
        query = SQL(
            """
            SELECT id, body
            FROM notes
            WHERE id = :note_id OR id = :note_id
            """
        ).select_only("body")
        row = session.execute(query, note_id=1).one_or_none()
        # --8<-- [end:example]
        assert row == {"body": "Note"}
        return row


if __name__ == "__main__":
    print(main())
