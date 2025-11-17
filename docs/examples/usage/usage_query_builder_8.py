from pathlib import Path

def test_example_8(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example8.db"
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
        session.execute("""CREATE TABLE if not exists users(id int primary key, name text)""")
        session.execute("""CREATE TABLE if not exists orders(id int primary key, user_id int, total real)""")
        # start-example
        # Subquery in WHERE
        subquery = sql.select("id").from_("orders").where("total > ?")
        query = (
            sql.select("*")
            .from_("users")
            .where(f"id IN ({subquery})")
        )
        result1 = session.execute(query, 100)

        # Subquery in FROM
        subquery = (
            sql.select("user_id", "COUNT(*) as order_count")
            .from_("orders")
            .group_by("user_id")
        )
        query = (
            sql.select("u.name", "o.order_count")
            .from_("users u")
            .join(f"({subquery}) o", "u.id = o.user_id")
        )
        result2 = session.execute(query)
        # end-example

