from pathlib import Path

__all__ = ("test_example_13", )


def test_example_13(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example13.db"
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
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, email text, updated_at timestamp)"""
        )
        # start-example
        # Update multiple columns
        query = (
            sql.update("users")
            .set("name", "?")
            .set("email", "?")
            .set("updated_at", "CURRENT_TIMESTAMP")
            .where("id = ?")
        )

        session.execute(query, 1, "New Name", "newemail@example.com")
        # end-example
