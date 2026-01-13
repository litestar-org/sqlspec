from __future__ import annotations

from pathlib import Path

__all__ = ("test_query_modifiers",)


def test_query_modifiers(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.core import SQL

    db_path = tmp_path / "modifiers.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists users (id integer primary key, name text, status text)")
        session.execute("insert into users (name, status) values ('Ada', 'active'), ('Bob', 'inactive')")

        query = (
            SQL("select id, name, status from users")
            .where_eq("status", "active")
            .select_only("id", "name")
            .paginate(page=1, page_size=10)
        )
        result = session.execute(query)
        print(result.all())
    # end-example

    assert result.all() == [{"id": 1, "name": "Ada"}]
