from pathlib import Path

__all__ = ("test_example_9",)


def test_example_9(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example9.db"
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
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, email text)"""
        )
        # start-example
        # Single row insert
        query = sql.insert("users").columns("name", "email").values("Alice", "alice@example.com")
        # SQL: INSERT INTO users (name, email) VALUES (:name, :email)

        session.execute(query)
        # end-example
