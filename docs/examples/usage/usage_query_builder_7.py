from pathlib import Path

def test_example_7(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example7.db"
    config = SqliteConfig(
        pool_config={
            "database": database.name,
            "timeout": 5.0,
            "check_same_thread": False,
            "cached_statements": 100,
            "uri": False,
        }
    )
    with db.provide_session(config) as session:
        session.execute("""CREATE TABLE if not exists users(id int primary key, name text, status text)""")
        session.execute("""CREATE TABLE if not exists orders(id int primary key, user_id int, total real, created_at timestamp)""")
        # start-example
        # COUNT
        query = sql.select("COUNT(*) as total").from_("users")
        result1 = session.execute(query)

        # GROUP BY
        query = (
            sql.select("status", "COUNT(*) as count")
            .from_("users")
            .group_by("status")
        )
        result2 = session.execute(query)

        # HAVING
        query = (
            sql.select("user_id", "COUNT(*) as order_count")
            .from_("orders")
            .group_by("user_id")
            .having("COUNT(*) > ?")
        )
        result3 = session.execute(query, 1)

        # Multiple aggregations
        query = (
            sql.select(
                "DATE(created_at) as date",
                "COUNT(*) as orders",
                "SUM(total) as revenue"
            )
            .from_("orders")
            .group_by("DATE(created_at)")
        )
        result4 = session.execute(query)
        # end-example

