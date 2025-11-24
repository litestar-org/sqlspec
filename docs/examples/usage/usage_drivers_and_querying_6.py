# Test module converted from docs example - code-block 6
"""Minimal smoke test for drivers_and_querying example 6."""

from pathlib import Path

from sqlspec import SQLSpec

__all__ = ("test_example_6_sqlite_config",)


def test_example_6_sqlite_config(tmp_path: Path) -> None:
    # start-example
    from sqlspec.adapters.sqlite import SqliteConfig

    # Use a temporary file for the SQLite database for test isolation
    db_path = tmp_path / "test_usage6.db"

    spec = SQLSpec()

    db = spec.add_config(
        SqliteConfig(pool_config={"database": db_path.name, "timeout": 5.0, "check_same_thread": False})
    )

    with spec.provide_session(db) as session:
        # Create table
        session.execute("""
        CREATE TABLE IF NOT EXISTS usage6_users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)

        # Insert with parameters
        session.execute("INSERT INTO usage6_users (name) VALUES (?)", "Alice")

        # Query
        result = session.execute("SELECT * FROM usage6_users")
        result.all()
    # end-example
