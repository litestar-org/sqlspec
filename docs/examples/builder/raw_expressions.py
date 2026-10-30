"""Combine raw SQL expressions and RETURNING clauses inside fluent builder queries."""

from typing import Any

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> dict[str, Any]:
    """Aggregate event counts with sql.raw() and insert a row using RETURNING."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, created_at TEXT DEFAULT (datetime('now')))"
        )
        session.execute("INSERT INTO events (name) VALUES ('signup'), ('login')")

        raw_count = sql.raw("COUNT(*) AS total")
        grouped = session.execute(sql.select("name", raw_count).from_("events").group_by("name")).all()
        assert len(grouped) == 2

        insert_returning = sql.insert("events").columns("name").values("logout").returning("id", "name")
        inserted = session.execute(insert_returning).one()
        assert inserted["name"] == "logout"
        return inserted


if __name__ == "__main__":
    print(main())
