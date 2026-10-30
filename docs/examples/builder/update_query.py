"""Build and execute an UPDATE statement with the fluent SQL builder."""

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> int:
    """Update a row in an in-memory SQLite table and return rows_affected."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("INSERT INTO users (name) VALUES ('Old')")
        query = sql.update("users").set("name", "New").where("id = 1")
        result = session.execute(query)
        assert result.rows_affected == 1
        return result.rows_affected


if __name__ == "__main__":
    print(main())
