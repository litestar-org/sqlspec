"""Manage explicit begin, commit, and rollback transaction boundaries on a session."""

from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Commit one ledger entry, roll back a second entry, and verify persisted rows."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, note TEXT)")
        session.begin()
        session.execute("INSERT INTO ledger (note) VALUES ('committed')")
        session.commit()

        session.begin()
        session.execute("INSERT INTO ledger (note) VALUES ('rolled back')")
        session.rollback()

        rows = session.select("SELECT note FROM ledger ORDER BY id")
        assert rows == [{"note": "committed"}]
        return rows


if __name__ == "__main__":
    print(main())
