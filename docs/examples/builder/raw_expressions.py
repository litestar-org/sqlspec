from __future__ import annotations

from pathlib import Path

__all__ = ("test_raw_expressions",)


def test_raw_expressions(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "raw.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute(
            "create table events (  id integer primary key,  name text,  created_at text default (datetime('now')))"
        )
        session.execute("insert into events (name) values ('signup'), ('login')")

        # sql.raw() creates a raw SQL expression for use inside builders
        raw_count = sql.raw("COUNT(*)")
        query = sql.select("name", raw_count).from_("events").group_by("name")
        result = session.execute(query)
        print(result.all())

        # Use RETURNING clause with INSERT
        insert_returning = sql.insert("events").columns("name").values("logout").returning("id", "name")
        new_row = session.execute(insert_returning)
        print(new_row.one())  # {"id": 3, "name": "logout"}
    # end-example

    assert new_row.one()["name"] == "logout"
