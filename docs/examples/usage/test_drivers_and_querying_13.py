# Test module converted from docs example - code-block 13
"""Minimal smoke test for drivers_and_querying example 13."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig


def test_example_13_placeholder() -> None:
    spec = SQLSpec()
    config = SqliteConfig(pool_config={"database": ":memory:", "timeout": 5.0, "check_same_thread": False})
    with spec.provide_session(config) as session:
        create_table_query = (
            """create table if not exists users (id default int primary key, name varchar(128), email text)"""
        )

        session.execute(create_table_query)
        # Examples are documentation snippets; ensure module importable
        result = session.execute("SELECT * FROM users WHERE id = ?", 1)

        # INSERT query
        result = session.execute("INSERT INTO users (name, email) VALUES (?, ?)", "Alice", "alice@example.com")

        # UPDATE query
        result = session.execute("UPDATE users SET email = ? WHERE id = ?", "newemail@example.com", 1)
        print(f"Updated {result.rows_affected} rows")

        # DELETE query
        result = session.execute("DELETE FROM users WHERE id = ?", 1)
        config.close_pool()
