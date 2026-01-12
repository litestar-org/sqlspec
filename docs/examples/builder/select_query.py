from __future__ import annotations

from pathlib import Path

__all__ = ("test_builder_select",)


def test_builder_select(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "builder.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists teams (id integer primary key, name text)")
        session.execute("insert into teams (name) values ('SQLSpec')")

        query = sql.select("id", "name").from_("teams").where("name = ?")
        result = session.execute(query, "SQLSpec")
        print(result.one())
    # end-example

    assert result.one() == {"id": 1, "name": "SQLSpec"}
