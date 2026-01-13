from __future__ import annotations

from pathlib import Path

__all__ = ("test_configuration",)


def test_configuration(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.core import StatementConfig

    db_path = tmp_path / "app.db"
    statement_config = StatementConfig(enable_validation=False)

    spec = SQLSpec()
    primary = spec.add_config(
        SqliteConfig(connection_config={"database": str(db_path)}, statement_config=statement_config)
    )

    with spec.provide_session(primary) as session:
        session.execute("create table if not exists health (id integer primary key, ok bool)")
        result = session.execute("select * from health")
        print(result.all())
    # end-example

    assert result.all() == []
