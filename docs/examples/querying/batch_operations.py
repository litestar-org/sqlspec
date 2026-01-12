from __future__ import annotations

from pathlib import Path

__all__ = ("test_batch_operations",)


def test_batch_operations(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "batch.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists projects (id integer primary key, name text)")
        result = session.execute_many(
            "insert into projects (name) values (?)", [("Docs",), ("Tooling",), ("Examples",)]
        )
        print(result.rows_affected)
    # end-example

    assert result.rows_affected == 3
