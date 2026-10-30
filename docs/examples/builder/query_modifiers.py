"""Apply ORDER BY, LIMIT, OFFSET, and dialect-aware FOR UPDATE row locks in the SQL builder."""

from typing import Any

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> tuple[list[dict[str, Any]], str]:
    """Execute a filtered query and render a PostgreSQL FOR UPDATE SKIP LOCKED query."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                status TEXT
            )
            """
        )
        session.execute(
            """
            INSERT INTO users (name, status) VALUES
                ('Ada', 'active'),
                ('Bob', 'inactive'),
                ('Charlie', 'active')
            """
        )

        # --8<-- [start:example]
        query = (
            sql.select("id", "name").from_("users").where_eq("status", "active").order_by("name").limit(10).offset(0)
        )
        rows = session.execute(query).all()
        # --8<-- [end:example]

    pg_sql = (
        sql
        .select("id", "name", dialect="postgres")
        .from_("users")
        .where_eq("status", "active")
        .for_update(skip_locked=True)
        .to_sql()
    )
    assert rows == [{"id": 1, "name": "Ada"}, {"id": 3, "name": "Charlie"}]
    assert "FOR UPDATE SKIP LOCKED" in pg_sql
    return rows, pg_sql


if __name__ == "__main__":
    print(main())
