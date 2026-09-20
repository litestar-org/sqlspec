from pathlib import Path

__all__ = ("test_cursor_pagination",)


def test_cursor_pagination(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.core import CursorFilter, CursorKeys, LimitOffsetFilter
    from sqlspec.service import SQLSpecSyncService

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(tmp_path / "cursor.db")}))
    keys: CursorKeys = [("created_at", "desc"), ("id", "desc")]
    query = "select id, name, created_at from items"

    try:
        with spec.provide_session(config) as session:
            session.execute("create table items (id integer primary key, name text, created_at text)")
            session.execute_many(
                "insert into items (id, name, created_at) values (?, ?, ?)",
                [(i, f"Item {i}", f"2026-01-{1 + i // 5:02d}") for i in range(1, 26)],
            )
            service = SQLSpecSyncService(session)
            offset_page = service.paginate_limit_offset(query, LimitOffsetFilter(limit=10, offset=0))
            page = service.paginate_cursor(query, CursorFilter(keys, limit=10))
            second = service.paginate_cursor(query, CursorFilter(keys, limit=10, cursor=page.next_cursor))
            previous = service.paginate(query, CursorFilter(keys, limit=10, cursor=second.previous_cursor))
    finally:
        config.close_pool()
    # end-example

    assert offset_page.total == 25
    assert len(offset_page.items) == 10
    assert len(page.items) == 10
    assert len(second.items) == 10
    assert {row["id"] for row in page.items}.isdisjoint(row["id"] for row in second.items)
    assert previous.items == page.items
