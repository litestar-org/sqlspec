from __future__ import annotations

from pathlib import Path

__all__ = ("test_load_sql_files",)


def test_load_sql_files(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    sql_file = tmp_path / "queries.sql"
    sql_file.write_text("-- name: list_teams\nselect id, name from teams order by id;\n")

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    spec.load_sql_files(sql_file)

    with spec.provide_session(config) as session:
        session.execute("create table teams (id integer primary key, name text)")
        session.execute("insert into teams (name) values ('Litestar'), ('SQLSpec')")
        result = session.execute(spec.get_sql("list_teams"))
        rows = result.all()
    # end-example

    assert rows == [{"id": 1, "name": "Litestar"}, {"id": 2, "name": "SQLSpec"}]
