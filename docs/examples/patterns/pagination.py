from __future__ import annotations

from pathlib import Path

__all__ = ("test_pagination",)


def test_pagination(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.core import SQL

    db_path = tmp_path / "pagination.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table items (id integer primary key, name text)")
        session.execute_many(
            "insert into items (name) values (?)",
            [(f"Item {i}",) for i in range(1, 51)],
        )

        # Approach 1: Use SQL.paginate() for simple offset pagination
        query = SQL("select id, name from items order by id").paginate(page=2, page_size=10)
        page_data = session.select(query)
        print(f"Page 2: {len(page_data)} items, first={page_data[0]['name']}")

        # Approach 2: Use select_with_total for pagination with total count
        paged_query = SQL("select id, name from items order by id").paginate(page=1, page_size=10)
        data, total = session.select_with_total(paged_query)
        print(f"Page 1: {len(data)} items, total: {total}")

        # Approach 3: Use the builder for dynamic pagination
        builder_query = (
            sql.select("id", "name")
            .from_("items")
            .where_like("name", "Item%")
            .order_by("id")
            .limit(10)
            .offset(0)
        )
        result = session.execute(builder_query)
        print(f"Builder: {len(result.all())} items")
    # end-example

    assert len(page_data) == 10
    assert page_data[0]["name"] == "Item 11"
    assert total == 50
