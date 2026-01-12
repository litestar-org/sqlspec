from __future__ import annotations

from pathlib import Path

__all__ = ("test_execute_insert",)


def test_execute_insert(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "inserts.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists todos (id integer primary key, task text)")
        result = session.execute("insert into todos (task) values (?)", ("Ship docs",))
        print({"rows": result.rows_affected, "last_id": result.last_inserted_id})
    # end-example

    assert result.rows_affected == 1
