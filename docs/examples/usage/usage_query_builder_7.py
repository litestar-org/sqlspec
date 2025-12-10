from pathlib import Path

__all__ = ("test_example_7",)


def test_example_7(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example7.db"
    config = SqliteConfig(
        connection_config={
            "database": database.name,
            "timeout": 5.0,
            "check_same_thread": False,
            "cached_statements": 100,
            "uri": False,
        }
    )
    with db.provide_session(config) as session:
        session.execute("""CREATE TABLE if not exists users(id int primary key, name text, status text)""")
        session.execute(
            """CREATE TABLE if not exists orders(id int primary key, user_id int, total real, created_at timestamp)"""
        )
        # start-example
        # COUNT
        # Using raw SQL aggregation
        raw_count = sql.select("COUNT(*) as total").from_("users")
        row_count = session.select_value(raw_count)
        print(row_count)

        # Using the builder's count helper
        counted = sql.select(sql.count().as_("total")).from_("users")
        counted_value = session.select_value(counted)
        print(counted_value)

        # GROUP BY
        query = sql.select("status", "COUNT(*) as count").from_("users").group_by("status")
        session.execute(query)

        # HAVING
        query = (
            sql.select("user_id", "COUNT(*) as order_count").from_("orders").group_by("user_id").having("COUNT(*) > ?")
        )
        session.execute(query, 1)

        # Multiple aggregations
        query = (
            sql.select(sql.column("created_at").as_("date"), "COUNT(*) as orders", "SUM(total) as revenue")
            .from_("orders")
            .group_by("date")
        )
        session.execute(query)
        # end-example
