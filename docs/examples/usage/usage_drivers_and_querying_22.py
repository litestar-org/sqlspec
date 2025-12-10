"""Batch insert performance example."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_example_22_batch_operations",)


def test_example_22_batch_operations() -> None:
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(db) as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

        # start-example
        names = ["alice", "bob", "carla"]
        session.execute_many("INSERT INTO users (name) VALUES (?)", [(name,) for name in names])
        # end-example

        count = session.select_value("SELECT COUNT(*) FROM users")
        assert count == len(names)
