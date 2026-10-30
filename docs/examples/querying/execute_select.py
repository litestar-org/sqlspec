"""Execute a SELECT statement and materialize all returned rows as dictionaries."""

from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Seed two team rows and return all records ordered by ID."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute(
            """
            CREATE TABLE teams (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
            """
        )
        session.execute(
            """
            INSERT INTO teams (name)
            VALUES ('Litestar'), ('SQLSpec')
            """
        )
        # --8<-- [start:example]
        rows = session.execute(
            """
            SELECT id, name
            FROM teams
            ORDER BY id
            """
        ).all()
        # --8<-- [end:example]
        assert rows == [{"id": 1, "name": "Litestar"}, {"id": 2, "name": "SQLSpec"}]
        return rows


if __name__ == "__main__":
    print(main())
