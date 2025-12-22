from pathlib import Path

__all__ = ("test_example_5",)


def test_example_5(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example5.db"
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
        # Create minimal tables for join queries
        session.execute("""CREATE TABLE if not exists users(id int primary key, name text)""")
        session.execute("""CREATE TABLE if not exists orders(id int primary key, user_id int, total real)""")
        session.execute("""CREATE TABLE if not exists order_items(id int primary key, order_id int, product_id int)""")
        session.execute("""CREATE TABLE if not exists products(id int primary key, name text)""")
        # start-example
        # INNER JOIN
        query = sql.select("u.id", "u.name", "o.total").from_("users u").join("orders o", "u.id = o.user_id")
        session.execute(query)
        # SQL: SELECT u.id, u.name, o.total FROM users u
        #      INNER JOIN orders o ON u.id = o.user_id

        # LEFT JOIN
        query = (
            sql
            .select("u.id", "u.name", "COUNT(o.id) as order_count")
            .from_("users u")
            .left_join("orders o", "u.id = o.user_id")
            .group_by("u.id", "u.name")
        )
        session.execute(query)

        # Multiple JOINs
        query = (
            sql
            .select("u.name", "o.id", "p.name as product")
            .from_("users u")
            .join("orders o", "u.id = o.user_id")
            .join("order_items oi", "o.id = oi.order_id")
            .join("products p", "oi.product_id = p.id")
        )
        session.execute(query)
        # end-example
