# Test module converted from docs example - code-block 6
"""Minimal smoke test for drivers_and_querying example 6."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig


def test_example_6_sqlite_config() -> None:
    spec = SQLSpec()

    config = SqliteConfig(pool_config={"database": "myapp.db", "timeout": 5.0, "check_same_thread": False})

    with spec.provide_session(config) as session:
        # Create table
        session.execute("""
           CREATE TABLE IF NOT EXISTS users (
               id INTEGER PRIMARY KEY,
               name TEXT NOT NULL
           )
       """)

        # Insert with parameters
        session.execute("INSERT INTO users (name) VALUES (?)", "Alice")

        # Query
        result = session.execute("SELECT * FROM users")
        result.all()
