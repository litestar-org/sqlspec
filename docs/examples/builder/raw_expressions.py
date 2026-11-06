"""Combine raw SQL expressions and RETURNING clauses inside fluent builder queries."""

from typing import Any

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> dict[str, Any]:
    """Aggregate event counts with sql.raw() and insert a row using RETURNING."""
    # --8<-- [start:example]
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("INSERT INTO events (name) VALUES ('signup'), ('login'), (NULL)")

        label = sql.raw("COALESCE(name, :fallback) AS label", fallback="unknown")
        query = sql.select(label, sql.count().as_("total")).from_("events").group_by("name")
        grouped = session.execute(query).all()
        assert len(grouped) == 3

        insert_returning = sql.insert("events").columns("name").values("logout").returning("id", "name")
        inserted = session.execute(insert_returning).one()
        assert inserted["name"] == "logout"
    # --8<-- [end:example]
    return inserted


if __name__ == "__main__":
    print(main())
