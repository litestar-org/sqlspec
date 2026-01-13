from __future__ import annotations

from pathlib import Path

__all__ = ("test_execute_select",)


def test_execute_select(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "selects.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists teams (id integer primary key, name text)")
        session.execute("insert into teams (name) values ('Litestar'), ('SQLSpec')")
        result = session.execute("select id, name from teams order by id")
        print(result.all())
    # end-example

    assert result.all() == [{"id": 1, "name": "Litestar"}, {"id": 2, "name": "SQLSpec"}]
