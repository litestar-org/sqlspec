"""SQLResult helper example."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_example_25_sql_result_helpers",)


def test_example_25_sql_result_helpers() -> None:
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with spec.provide_session(db) as session:
        session.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY, status TEXT)")
        session.execute_many("INSERT INTO tasks (id, status) VALUES (?, ?)", [(1, "todo"), (2, "done"), (3, "todo")])

        # start-example
        result = session.execute("SELECT status FROM tasks ORDER BY id")
        assert result.get_count() == 3
        assert not result.is_empty()
        first = result.get_first()
        assert first is not None
        assert first["status"] == "todo"
        # end-example
