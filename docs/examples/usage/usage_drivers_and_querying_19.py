"""Type coercion helper example."""

import datetime as dt

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_example_19_type_coercion",)


def test_example_19_type_coercion() -> None:
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(db) as session:
        session.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, is_active INTEGER, ts TEXT)")

        # start-example
        session.execute(
            "INSERT INTO events (id, is_active, ts) VALUES (:id, :is_active, :ts)",
            {"id": 1, "is_active": True, "ts": dt.datetime(2025, 1, 1, 12, 0, 0)},
        )
        row = session.select_one("SELECT is_active, ts FROM events WHERE id = :id", id=1)
        # end-example

        assert row["is_active"] == 1
        assert row["ts"].startswith("2025-01-01")
