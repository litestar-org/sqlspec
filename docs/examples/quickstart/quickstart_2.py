from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

db_manager = SQLSpec()
db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

with db_manager.provide_session(db) as session:
    # Create a table
    _ = session.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        )
    """)

    # Insert data
    _ = session.execute(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        "Alice", "alice@example.com"
    )

    # Insert multiple rows
    _ = session.execute_many(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        [
            ("Bob", "bob@example.com"),
            ("Charlie", "charlie@example.com"),
        ]
    )

    # Query all users
    users = session.select("SELECT * FROM users")
    print(f"All users: {users}")

    # Query single user
    alice = session.select_one("SELECT * FROM users WHERE name = ?", "Alice")
    print(f"Alice: {alice}")

    # Query scalar value
    count = session.select_value("SELECT COUNT(*) FROM users")
    print(f"Total users: {count}")


