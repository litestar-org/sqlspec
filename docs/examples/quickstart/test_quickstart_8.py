from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig


def test_quickstart_8() -> None:
    # Build a query programmatically
    query = sql.select("id", "name", "email").from_("users").where("age > ?").order_by("name")

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with db_manager.provide_session(db) as session:
        # Setup
        _ = session.execute("""
            CREATE TABLE users (id INTEGER, name TEXT, email TEXT, age INTEGER)
        """)
        _ = session.execute("INSERT INTO users VALUES (?, ?, ?, ?)", 1, "Alice", "alice@example.com", 30)

        # Execute built query
        results = session.select(query, 25)
        print(results)

    assert len(results) == 1
    assert results[0] == {"id": 1, "name": "Alice", "email": "alice@example.com"}
