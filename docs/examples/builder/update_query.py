from __future__ import annotations

from pathlib import Path

__all__ = ("test_builder_update",)


def test_builder_update(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "builder_update.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists users (id integer primary key, name text)")
        session.execute("insert into users (name) values ('Old')")
        query = sql.update("users").set("name", "New").where("id = 1")
        result = session.execute(query)
        print(result.rows_affected)
    # end-example

    assert result.rows_affected == 1
