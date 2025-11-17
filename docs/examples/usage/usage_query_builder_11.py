from pathlib import Path

__all__ = ("test_example_11", )


def test_example_11(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example11.db"
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
        session.execute(
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, email text, created_at timestamp)"""
        )
        # start-example
        # PostgreSQL RETURNING clause
        (sql.insert("users").columns("name", "email").values("?", "?").returning("id", "created_at"))

        # SQLite does not support RETURNING, so we skip execution for this example.
        # end-example
