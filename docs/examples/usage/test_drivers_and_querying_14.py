"for drivers_and_querying example 14."""

import pytest


def test_example_14_placeholder() -> None:
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()
    config = SqliteConfig(pool_config={"database": ":memory:", "timeout": 5.0, "check_same_thread": False})
    with spec.provide_session(config) as session:
        create_table_query = """create table if not exists users (id default int primary key, name varchar(128), email text, status varchar(32))"""

        _ = session.execute(create_table_query)
        # Batch examples are documentation-only

        # Batch insert
        session.execute_many(
            "INSERT INTO users (id, name, email, status) VALUES (?, ?, ?, ?)",
            [
                (1, "Alice", "alice@example.com", "active"),
                (2, "Bob", "bob@example.com", "inactive"),
                (3, "Charlie", "charlie@example.com", "active"),
            ],
        )
        # Batch update
        session.execute_many("UPDATE users SET status = ? WHERE id = ?", [("inactive", 1), ("active", 2)])
        results = session.select("SELECT * FROM users")
        print(results)
        # Returns list of dictionaries: [{"id": 1, "name": "Alice", ...}, ...]
        user = session.select_one("SELECT * FROM users WHERE id = ?", 1)
        print(user)
        # Returns single dictionary: {"id": 1, "name": "Alice", ...}
        # Raises NotFoundError if no results
        # Raises MultipleResultsFoundError if multiple results
        user = session.select_one_or_none("SELECT * FROM users WHERE email = ?", "nobody@example.com")
        # Returns dictionary or None
        # Raises MultipleResultsFoundError if multiple results
        count = session.select_value("SELECT COUNT(*) FROM users")
        # Returns: 3
        latest_id = session.select_value("SELECT MAX(id) FROM users")
        # Returns: 3
        result = session.execute("SELECT id, name, email FROM users")
        # Access raw data
        result.data              # List of dictionaries
        result.column_names      # ["id", "name", "email"]
        result.rows_affected     # For INSERT/UPDATE/DELETE
        result.operation_type    # "SELECT", "INSERT", etc.
        # Convenience methods
        with pytest.raises(ValueError):
            user = result.one()              # Single row (raises if not exactly 1)
        with pytest.raises(ValueError):
            user = result.one_or_none()      # Single row or None
        with pytest.raises(ValueError):
            value = result.scalar()          # First column of first row

