from __future__ import annotations

from pathlib import Path

__all__ = ("test_first_query",)


def test_first_query(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "queries.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists users (id integer primary key, name text)")
        session.execute("insert into users (name) values (?)", ("Ada",))
        result = session.execute("select id, name from users where name = ?", ("Ada",))
        print(result.one())
    # end-example

    assert result.one() == {"id": 1, "name": "Ada"}
