from __future__ import annotations

from pathlib import Path

__all__ = ("test_transaction_handling",)


def test_transaction_handling(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_path = tmp_path / "transactions.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists ledger (id integer primary key, note text)")
        session.begin()
        session.execute("insert into ledger (note) values ('committed')")
        session.commit()

        session.begin()
        session.execute("insert into ledger (note) values ('rolled back')")
        session.rollback()

        result = session.execute("select note from ledger order by id")
        print(result.all())
    # end-example

    assert result.all() == [{"note": "committed"}]
