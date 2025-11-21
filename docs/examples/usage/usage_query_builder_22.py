from pathlib import Path

__all__ = ("test_example_22",)


def test_example_22(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example22.db"
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
        session.execute("""CREATE TABLE if not exists users(id integer primary key autoincrement, name text)""")
        session.execute("""CREATE TABLE if not exists orders(id integer primary key autoincrement, user_id int)""")
        # start-example
        # WITH clause
        cte = sql.select("user_id", "COUNT(*) as order_count").from_("orders").group_by("user_id")

        query = (
            sql.select("u.name", "c.order_count")
            .with_("user_orders", cte)
            .from_("users u")
            .join("user_orders c", "u.id = c.user_id")
        )
        session.execute(query)
        # end-example
