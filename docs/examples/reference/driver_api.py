from __future__ import annotations

from pathlib import Path

__all__ = ("test_driver_api",)


def test_driver_api(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "driver_api.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists users (id integer primary key, name text)")
        session.execute("insert into users (name) values ('Ada')")
        row = session.select_one_or_none("select name from users where id = ?", (1,))
        print(row)
    # end-example

    assert row == {"name": "Ada"}
