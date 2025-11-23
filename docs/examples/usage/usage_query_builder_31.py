from pathlib import Path

__all__ = ("test_example_31",)


def test_example_31(tmp_path: Path) -> None:
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example31.db"
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
        session.execute("""CREATE TABLE if not exists events(id integer primary key autoincrement, data text)""")

        # Insert test data with JSON
        import json

        session.execute("INSERT INTO events (data) VALUES (?)", json.dumps({"name": "Alice", "age": 30}))
        session.execute("INSERT INTO events (data) VALUES (?)", json.dumps({"name": "Bob", "age": 25}))

        # start-example
        # SQLite JSON functions (use raw SQL)
        # Note: SQLite uses json_extract() instead of PostgreSQL's ->> operator
        session.execute("SELECT json_extract(data, '$.name') FROM events WHERE json_extract(data, '$.name') = ?", "Alice")
        # end-example
