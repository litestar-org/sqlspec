from pathlib import Path

__all__ = ("test_example_2",)


def test_example_2(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example2.db"
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
        create_table_query = """CREATE TABLE if not exists users(id int primary key,name text,email text, status text, created_at timestamp)"""
        _ = session.execute(create_table_query)
        # start-example
        # Simple select
        query = sql.select("*").from_("users")
        # SQL: SELECT * FROM users
        session.execute(query)

        # Specific columns
        query = sql.select("id", "name", "email").from_("users")
        # SQL: SELECT id, name, email FROM users
        session.execute(query)

        # With table alias
        query = sql.select("u.id", "u.name").from_("users u")
        # SQL: SELECT u.id, u.name FROM users u
        session.execute(query)
        # end-example
