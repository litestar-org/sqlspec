from pathlib import Path

__all__ = ("test_example_13",)


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
        # Insert test data
        session.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")

        # start-example
        # Update multiple columns
        from sqlglot import exp

        query = (
            sql.update("users")
            .set("name", "New Name")
            .set("email", "newemail@example.com")
            .set("updated_at", exp.CurrentTimestamp())
            .where("id = 1")
        )

        session.execute(query)
        # end-example
