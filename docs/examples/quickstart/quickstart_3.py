from pydantic import BaseModel

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig


class User(BaseModel):
    id: int
    name: str
    email: str

db_manager = SQLSpec()
db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

with db_manager.provide_session(db) as session:
    # Setup
    _ = session.execute("""
        CREATE TABLE users (id INTEGER, name TEXT, email TEXT)
    """)
    _ = session.execute(
        "INSERT INTO users VALUES (?, ?, ?)",
        1, "Alice", "alice@example.com"
    )

    # Type-safe query - returns User instance
    user = session.select_one(
        "SELECT * FROM users WHERE id = ?",
        1,
        schema_type=User
    )

    # Now you have type hints and autocomplete!
    print(f"User: {user.name} ({user.email})")  # IDE knows these fields exist

    # Multiple results
    all_users = session.select(
        "SELECT * FROM users",
        schema_type=User
    )
    for u in all_users:
        print(f"User: {u.name}")  # Each item is a typed User

