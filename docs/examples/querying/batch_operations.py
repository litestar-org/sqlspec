"""Execute batched inserts, scalar projections, and paginated queries in SQLSpec."""

from typing import Any

# --8<-- [start:example]
from sqlspec import SQL, SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

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
    rows = [(u, f"{u.lower()}@example.com") for u in ("Alice", "Bob", "Charlie")]
    session.execute_many(
        """
        INSERT INTO users (name, email)
        VALUES (?, ?)
        """,
        rows,
    )

    count = session.select_value("SELECT COUNT(*) FROM users", value_type=int)
    email = session.select_value_or_none(
        """
        SELECT email
        FROM users
        WHERE name = ?
        """,
        "Nobody",
    )
    base_sql = SQL("SELECT id, name FROM users ORDER BY id")
    query = base_sql.paginate(page=1, page_size=2)
    data, total = session.select_with_total(query, count_with_window=True)
# --8<-- [end:example]

__all__ = ("main",)


def main() -> tuple[list[dict[str, Any]], int]:
    """Insert multiple users with execute_many and read scalar and paginated results."""
    assert count == 3
    assert email is None
    assert total == 3
    assert len(data) == 2
    return data, total


if __name__ == "__main__":
    page_rows, total_count = main()
    print(f"Page has {len(page_rows)} rows, total matching: {total_count}")
