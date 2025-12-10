from pathlib import Path

__all__ = ("test_example_12",)


def test_example_12(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example12.db"
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
        # Insert test data
        session.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")

        # start-example
        # Update with WHERE
        query = sql.update("users").set("email", "newemail@example.com").where("id = 1")
        # SQL: UPDATE users SET email = :email WHERE id = 1

        session.execute(query)
        # print(f"Updated {result.rows_affected} rows")
        # end-example
