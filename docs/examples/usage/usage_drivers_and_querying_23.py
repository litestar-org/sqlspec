"""Scalar helper example."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_example_23_select_value",)


def test_example_23_select_value() -> None:
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(db) as session:
        session.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, count INTEGER)")
        session.execute("INSERT INTO metrics (id, count) VALUES (?, ?)", 1, 42)

        # start-example
        result = session.select_value("SELECT count FROM metrics WHERE id = ?", 1)
        # end-example
        assert result == 42
