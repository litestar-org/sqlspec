"""select_one_or_none helper example."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_example_24_select_one_or_none",)


def test_example_24_select_one_or_none() -> None:
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(db) as session:
        session.execute("CREATE TABLE emails (id INTEGER PRIMARY KEY, address TEXT)")
        session.execute("INSERT INTO emails (id, address) VALUES (?, ?)", 1, "user@example.com")

        # start-example
        missing = session.select_one_or_none("SELECT * FROM emails WHERE id = ?", 99)
        assert missing is None

        record = session.select_one_or_none("SELECT * FROM emails WHERE id = ?", 1)
        assert record is not None
        assert record["address"] == "user@example.com"
        # end-example
