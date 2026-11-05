"""Build and execute a parameterized DELETE statement with the fluent SQL builder."""

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> int:
    """Delete expired rows from an in-memory SQLite table."""
    # --8<-- [start:example]
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE sessions (id INTEGER PRIMARY KEY, expired INTEGER)")
        session.execute_many("INSERT INTO sessions (expired) VALUES (?)", [(0,), (1,), (1,)])

        delete_expired = sql.delete("sessions").where_eq("expired", 1)
        result = session.execute(delete_expired)
        assert result.rows_affected == 2
    # --8<-- [end:example]
    return result.rows_affected


if __name__ == "__main__":
    print(main())
