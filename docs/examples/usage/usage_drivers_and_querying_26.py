"""StatementStack example."""

from sqlspec import SQLSpec, StatementStack
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_example_26_statement_stack",)


def test_example_26_statement_stack() -> None:
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    stack = StatementStack()
    stack = stack.push_execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    stack = stack.push_execute("INSERT INTO users (name) VALUES (?)", "stack-user")

    with spec.provide_session(db) as session:
        # start-example
        results = session.execute_stack(stack)
        assert len(results) == 2
        inserted = session.select_one("SELECT name FROM users WHERE id = 1")
        assert inserted["name"] == "stack-user"
        # end-example
