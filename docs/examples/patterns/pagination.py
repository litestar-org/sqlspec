"""Paginate queries with SQL.paginate(), select_with_total(), and fluent builder modifiers."""

from typing import Any

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import SQL

__all__ = ("main",)


def main() -> tuple[list[dict[str, Any]], int]:
    """Run offset pagination via SQL.paginate, select_with_total, and the fluent builder."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute(
            """
            CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)
            """
        )
        session.execute_many("INSERT INTO items (name) VALUES (?)", [(f"Item {i}",) for i in range(1, 51)])

        # --8<-- [start:example]
        page_two = session.select(SQL("SELECT id, name FROM items ORDER BY id").paginate(page=2, page_size=10))
        page_one, total = session.select_with_total(
            SQL("SELECT id, name FROM items ORDER BY id").paginate(page=1, page_size=10)
        )
        builder_rows = session.execute(
            sql.select("id", "name").from_("items").where_like("name", "Item%").order_by("id").limit(10).offset(0)
        ).all()
        # --8<-- [end:example]

    assert len(page_two) == 10
    assert page_two[0]["name"] == "Item 11"
    assert len(page_one) == 10
    assert len(builder_rows) == 10
    assert total == 50
    return page_two, total


if __name__ == "__main__":
    rows, total_rows = main()
    print(f"Page 2 first={rows[0]['name']}, total={total_rows}")
