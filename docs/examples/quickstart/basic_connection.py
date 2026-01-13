from __future__ import annotations

from pathlib import Path

__all__ = ("test_basic_connection",)


def test_basic_connection(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "sqlspec.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists notes (id integer primary key, body text)")
        session.execute("insert into notes (body) values (?)", ("Hello, SQLSpec!",))
        result = session.execute("select id, body from notes")
        print(result.all())
    # end-example

    assert result.all() == [{"id": 1, "body": "Hello, SQLSpec!"}]
