"""Paginate ordered query results forward and backward using keyset cursors."""

from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import CursorFilter, CursorKeys, LimitOffsetFilter
from sqlspec.service import SQLSpecSyncService

__all__ = ("main",)


def main() -> tuple[int, list[dict[str, Any]]]:
    """Compare limit-offset pagination with bidirectional keyset cursor pagination."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    keys: CursorKeys = [("created_at", "desc"), ("id", "desc")]
    query = "SELECT id, name, created_at FROM items"

    try:
        with spec.provide_session(config) as session:
            session.execute(
                """
                CREATE TABLE items (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    created_at TEXT
                )
                """
            )
            session.execute_many(
                """
                INSERT INTO items (id, name, created_at)
                VALUES (?, ?, ?)
                """,
                [(i, f"Item {i}", f"2026-01-{1 + i // 5:02d}") for i in range(1, 26)],
            )
            # --8<-- [start:example]
            keys: CursorKeys = [("created_at", "desc"), ("id", "desc")]
            query = """
                SELECT id, name, created_at
                FROM items
            """

            service = SQLSpecSyncService(session)
            offset_page = service.paginate_limit_offset(query, LimitOffsetFilter(limit=10, offset=0))
            first_page = service.paginate_cursor(query, CursorFilter(keys, limit=10))
            second_page = service.paginate_cursor(query, CursorFilter(keys, limit=10, cursor=first_page.next_cursor))
            previous_page = service.paginate(query, CursorFilter(keys, limit=10, cursor=second_page.previous_cursor))
            # --8<-- [end:example]

        assert offset_page.total == 25
        assert len(offset_page.items) == 10
        assert len(first_page.items) == 10
        assert len(second_page.items) == 10
        assert {row["id"] for row in first_page.items}.isdisjoint(row["id"] for row in second_page.items)
        assert previous_page.items == first_page.items
        return offset_page.total, first_page.items
    finally:
        config.close_pool()


if __name__ == "__main__":
    total_count, first_items = main()
    print(f"Total: {total_count}, first page count: {len(first_items)}")
