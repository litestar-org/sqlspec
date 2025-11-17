from pathlib import Path

__all__ = ("test_example_3", )


def test_example_3(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example3.db"
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
        create_table_query = """CREATE TABLE if not exists users(id int primary key,name text,email text, status text, created_at timestamp, role text)"""
        _ = session.execute(create_table_query)
        # start-example
        # Simple WHERE
        query = sql.select("*").from_("users").where("status = ?")
        session.execute(query, "active")

        # Multiple conditions (AND)
        query = sql.select("*").from_("users").where("status = ?").where("created_at > ?")
        # SQL: SELECT * FROM users WHERE status = ? AND created_at > ?
        session.execute(query, "active", "2024-01-01")

        # OR conditions
        query = sql.select("*").from_("users").where("status = ? OR role = ?")
        session.execute(query, "active", "admin")

        # IN clause
        query = sql.select("*").from_("users").where("id IN (?, ?, ?)")
        session.execute(query, 1, 2, 3)
        # end-example
