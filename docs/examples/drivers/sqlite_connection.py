from __future__ import annotations

from pathlib import Path

__all__ = ("test_sqlite_connection",)


def test_sqlite_connection(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "driver.sqlite"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists health (id integer primary key)")
        result = session.execute("select count(*) as total from health")
        print(result.one())
    # end-example

    assert result.one() == {"total": 0}
