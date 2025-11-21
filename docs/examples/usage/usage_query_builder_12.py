from pathlib import Path

__all__ = ("test_example_12",)


def test_example_12(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example12.db"
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
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, email text)"""
        )
        # start-example
        # Update with WHERE
        query = sql.update("users").set("email", "?").where("id = ?")
        # SQL: UPDATE users SET email = ? WHERE id = ?

        session.execute(query, 1, "newemail@example.com")
        # print(f"Updated {result.rows_affected} rows")
        # end-example
