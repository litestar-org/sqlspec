"""Named parameter styles example."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_example_18_named_parameters",)


def test_example_18_named_parameters() -> None:
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with spec.provide_session(db) as session:
        session.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, action TEXT, created_at TEXT)")

        # start-example
        session.execute(
            "INSERT INTO logs (id, action, created_at) VALUES (:id, :action, :created)",
            id=1,
            action="created",
            created="2025-01-01T00:00:00",
        )
        row = session.select_one("SELECT action FROM logs WHERE id = :id", id=1)
        # end-example

        assert row["action"] == "created"
