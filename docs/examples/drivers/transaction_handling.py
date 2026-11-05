"""Manage transaction context managers and explicit begin, commit, and rollback boundaries."""

from typing import Any

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

with spec.provide_session(config) as session:
    session.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, note TEXT)")
    with session.transaction():
        session.execute("INSERT INTO ledger (note) VALUES ('context-committed')")

    session.begin()
    session.execute("INSERT INTO ledger (note) VALUES ('committed')")
    session.commit()

    session.begin()
    session.execute("INSERT INTO ledger (note) VALUES ('rolled back')")
    session.rollback()

    rows = session.select("SELECT note FROM ledger ORDER BY id")
# --8<-- [end:example]

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Commit two ledger entries, roll back a third entry, and verify persisted rows."""
    assert rows == [{"note": "context-committed"}, {"note": "committed"}]
    return rows


if __name__ == "__main__":
    print(main())
