from pathlib import Path

__all__ = ("test_example_32",)


def test_example_32(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example32.db"
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
        session.execute(
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, status text)"""
        )
        session.execute("""CREATE TABLE if not exists orders(id integer primary key autoincrement, user_id int)""")

        # Insert test data
        session.execute("INSERT INTO users (name, status) VALUES ('Alice', 'active'), ('Bob', 'inactive')")
        session.execute("INSERT INTO orders (user_id) VALUES (1), (1), (1), (1), (1), (1)")

        # start-example
        # Before: Raw SQL
        session.execute(
            """
            SELECT u.id, u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.status = ?
            GROUP BY u.id, u.name
            HAVING COUNT(o.id) > ?
            ORDER BY order_count DESC
            LIMIT ?
        """,
            "active",
            5,
            10,
        )

        # After: Query Builder
        query = (
            sql.select("users.id", "users.name", "COUNT(orders.id) as order_count")
            .from_("users")
            .left_join("orders", "users.id = orders.user_id")
            .where("users.status = 'active'")
            .group_by("users.id", "users.name")
            .having("COUNT(orders.id) > 5")
            .order_by("order_count DESC")
            .limit(10)
        )
        session.execute(query)
        # end-example
