from pathlib import Path
from typing import Any

__all__ = ("test_example_14",)


def test_example_14(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example14.db"
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
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, email text, status text)"""
        )
        # Insert test data
        session.execute("INSERT INTO users (name, email, status) VALUES ('Alice', 'alice@example.com', 'inactive')")

        # start-example
        # Dynamic update builder
        def update_user(user_id: Any, **fields: Any) -> Any:
            query = sql.update("users")

            for field, value in fields.items():
                query = query.set(field, value)

            query = query.where(f"id = {user_id}")

            return session.execute(query)

        # Usage
        update_user(1, name="Alice Updated", email="alice.new@example.com", status="active")
        # end-example
