import datetime
from pathlib import Path

__all__ = ("test_example_16",)


def test_example_16(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example16.db"
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
            """CREATE TABLE if not exists users(id integer primary key autoincrement, status text, last_login date)"""
        )
        # start-example
        # Delete with multiple conditions
        query = sql.delete().from_("users").where("status = ?").where("last_login < ?")

        session.execute(query, "inactive", datetime.date(2024, 1, 1))
        # end-example
