# type: ignore
"""Example demonstrating SQLite driver usage with query mixins.

This example shows how to use the SQLite driver directly with its built-in query
mixin functionality for common database operations.
"""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.builder import Select

__all__ = ("main", "sqlite_example")


def sqlite_example() -> None:
    """Demonstrate synchronous SQLite database driver usage with query mixins."""
    # Create SQLSpec instance with SQLite
    spec = SQLSpec()
    config = SqliteConfig(pool_config={"database": ":memory:"})
    spec.add_config(config)

    # Get a driver directly (drivers now have built-in query methods)
    with spec.provide_session(config) as driver:
        # Create a table
        driver.execute("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL
                )
            """)

        # Insert data
        driver.execute("INSERT INTO users (name, email) VALUES (?, ?)", "Alice", "alice@example.com")

        # Insert multiple rows
        driver.execute_many(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            [("Bob", "bob@example.com"), ("Charlie", "charlie@example.com")],
        )

        # Select all users using query mixin
        users = driver.select("SELECT * FROM users")
        print(f"All users: {users}")

        # Select one user using query mixin
        alice = driver.select_one("SELECT * FROM users WHERE name = ?", "Alice")
        print(f"Alice: {alice}")

        # Select one or none (no match) using query mixin
        nobody = driver.select_one_or_none("SELECT * FROM users WHERE name = ?", "Nobody")
        print(f"Nobody: {nobody}")

        # Select scalar value using query mixin
        user_count = driver.select_value("SELECT COUNT(*) FROM users")
        print(f"User count: {user_count}")

        # Update
        result = driver.execute("UPDATE users SET email = ? WHERE name = ?", "alice.doe@example.com", "Alice")
        print(f"Updated {result.rows_affected} rows")

        # Delete
        result = driver.execute("DELETE FROM users WHERE name = ?", "Charlie")
        print(f"Deleted {result.rows_affected} rows")

        # Use query builder with driver - this demonstrates the fix
        query = Select("*").from_("users").where("email LIKE ?")
        matching_users = driver.select(query, "%@example.com%")
        print(f"Matching users: {matching_users}")

        # Demonstrate pagination
        page_users = driver.select("SELECT * FROM users ORDER BY id LIMIT ? OFFSET ?", 1, 0)
        total_count = driver.select_value("SELECT COUNT(*) FROM users")
        print(f"Page 1: {page_users}, Total: {total_count}")


def main() -> None:
    """Run SQLite example."""
    print("=== SQLite Driver Example ===")
    sqlite_example()
    print("✅ SQLite example completed successfully!")


if __name__ == "__main__":
    main()
