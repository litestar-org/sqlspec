from __future__ import annotations

from pathlib import Path

__all__ = ("test_core_api",)


def test_core_api(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.core import SQL

    db_path = tmp_path / "core_api.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists notes (id integer primary key, body text)")
        session.execute("insert into notes (body) values ('Note')")
        query = SQL("select id, body from notes where id = :note_id or id = :note_id").select_only("body")
        result = session.execute(query, {"note_id": 1})
        print(result.one_or_none())
    # end-example

    assert result.one_or_none() == {"body": "Note"}
