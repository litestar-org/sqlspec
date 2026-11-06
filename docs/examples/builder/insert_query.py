"""Build and execute a parameterized INSERT statement with the fluent SQL builder."""

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> int:
    """Insert a row into an in-memory SQLite table and return rows_affected."""
    # --8<-- [start:example]
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

        query = sql.insert("users").columns("name").values("Ada").returning("id", "name")
        result = session.execute(query)
        assert result.one() == {"id": 1, "name": "Ada"}
    # --8<-- [end:example]
    return result.rows_affected


if __name__ == "__main__":
    print(main())
