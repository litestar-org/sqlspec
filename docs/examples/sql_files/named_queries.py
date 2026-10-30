"""Register inline named SQL statements and execute them with bound keyword parameters."""

from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> dict[str, Any]:
    """Register find_team on SQLSpec and execute it by name."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    # --8<-- [start:example]
    spec.add_named_sql(
        "find_team",
        """
        SELECT id, name
        FROM teams
        WHERE name = :name
        """,
    )

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("INSERT INTO teams (name) VALUES ('Litestar'), ('SQLSpec')")
        row = session.execute(spec.get_sql("find_team"), name="SQLSpec").one()
        # --8<-- [end:example]
        assert row == {"id": 2, "name": "SQLSpec"}
        return row


if __name__ == "__main__":
    print(main())
