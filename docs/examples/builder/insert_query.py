from __future__ import annotations

from pathlib import Path

__all__ = ("test_builder_insert",)


def test_builder_insert(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "builder_insert.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists users (id integer primary key, name text)")
        query = sql.insert("users").columns("name").values("Ada")
        result = session.execute(query)
        print(result.rows_affected)
    # end-example

    assert result.rows_affected == 1
