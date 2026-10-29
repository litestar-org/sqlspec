"""Batch ordered DDL, DML, and SELECT operations into a single StatementStack execution."""

from typing import Any

from sqlspec import SQLResult, SQLSpec, StatementStack
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> list[dict[str, Any]]:
    """Execute a multi-step StatementStack and return the final SELECT rows."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    stack = (
        StatementStack()
        .push_execute("CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT)")
        .push_execute_many("INSERT INTO teams (name) VALUES (:name)", [{"name": "Litestar"}, {"name": "SQLSpec"}])
        .push_execute("SELECT id, name FROM teams ORDER BY id")
    )

    with spec.provide_session(config) as session:
        results = session.execute_stack(stack)
        final_result = results[-1].result
        assert isinstance(final_result, SQLResult)
        rows = final_result.all()
        assert rows == [{"id": 1, "name": "Litestar"}, {"id": 2, "name": "SQLSpec"}]
        return rows


if __name__ == "__main__":
    print(main())
