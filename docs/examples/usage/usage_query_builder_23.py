import datetime
from pathlib import Path
from typing import Any

__all__ = ("test_example_23",)


def test_example_23(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example23.db"
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
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, email text, status text, created_at date)"""
        )
        # start-example
        # Base query
        base_query = sql.select("id", "name", "email", "status").from_("users")

        # Add filters based on context
        def active_users() -> Any:
            return base_query.where("status = 'active'")

        def recent_users(days: int = 7) -> Any:
            return base_query.where("created_at >= ?")

        # Use in different contexts
        session.execute(active_users())
        session.execute(recent_users(), datetime.date.today() - datetime.timedelta(days=7))
        # end-example
