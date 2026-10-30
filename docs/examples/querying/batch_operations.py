"""Execute batched inserts, scalar projections, and paginated queries in SQLSpec."""

from typing import Any

from sqlspec import SQL, SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main",)


def main() -> tuple[list[dict[str, Any]], int]:
    """Insert multiple users with execute_many and read scalar and paginated results."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT
            )
            """
        )
        session.execute_many(
            """
            INSERT INTO users (name, email)
            VALUES (?, ?)
            """,
            [("Alice", "alice@example.com"), ("Bob", "bob@example.com"), ("Charlie", "charlie@example.com")],
        )

        # --8<-- [start:example]
        count = session.select_value("SELECT COUNT(*) FROM users", value_type=int)
        email = session.select_value_or_none("SELECT email FROM users WHERE name = ?", "Nobody")
        query = SQL("SELECT id, name FROM users ORDER BY id").paginate(page=1, page_size=2)
        data, total = session.select_with_total(query)
        # --8<-- [end:example]

    assert count == 3
    assert email is None
    assert total == 3
    assert len(data) == 2
    return data, total


if __name__ == "__main__":
    page_rows, total_count = main()
    print(f"Page has {len(page_rows)} rows, total matching: {total_count}")
