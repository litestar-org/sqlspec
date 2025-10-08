# type: ignore
# /// script
# dependencies = [
#   "sqlspec[sqlite]",
#   "rich",
# ]
# requires-python = ">=3.10"
# ///
"""Example demonstrating SQLite driver usage with query mixins.

This example shows how to use the SQLite driver directly with its built-in query
mixin functionality for common database operations.
"""

from rich import print

from sqlspec import SQLSpec, sql
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("main", "sqlite_example")


def sqlite_example() -> None:
    """Demonstrate synchronous SQLite database driver usage with query mixins."""
    # Create SQLSpec instance with SQLite
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    # Get a driver directly (drivers now have built-in query methods)
    with spec.provide_session(db) as driver:
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
        print(f"[cyan]All users:[/cyan] {users}")

        # Select one user using query mixin
        alice = driver.select_one("SELECT * FROM users WHERE name = ?", "Alice")
        print(f"[cyan]Alice:[/cyan] {alice}")

        # Select one or none (no match) using query mixin
        nobody = driver.select_one_or_none("SELECT * FROM users WHERE name = ?", "Nobody")
        print(f"[cyan]Nobody:[/cyan] {nobody}")

        # Select scalar value using query mixin
        user_count = driver.select_value("SELECT COUNT(*) FROM users")
        print(f"[cyan]User count:[/cyan] {user_count}")

        # Update
        result = driver.execute("UPDATE users SET email = ? WHERE name = ?", "alice.doe@example.com", "Alice")
        print(f"[yellow]Updated {result.rows_affected} rows[/yellow]")

        # Delete
        result = driver.execute("DELETE FROM users WHERE name = ?", "Charlie")
        print(f"[yellow]Deleted {result.rows_affected} rows[/yellow]")

        # Use query builder with driver - this demonstrates the fix
        query = sql.select("*").from_("users").where("email LIKE ?")
        matching_users = driver.select(query, "%@example.com%")
        print(f"[cyan]Matching users:[/cyan] {matching_users}")

        # Demonstrate pagination
        page_users = driver.select("SELECT * FROM users ORDER BY id LIMIT ? OFFSET ?", 1, 0)
        total_count = driver.select_value("SELECT COUNT(*) FROM users")
        print(f"[cyan]Page 1:[/cyan] {page_users}[cyan], Total:[/cyan] {total_count}")


def main() -> None:
    """Run SQLite example."""
    print("[bold blue]=== SQLite Driver Example ===[/bold blue]")
    sqlite_example()
    print("[green]✅ SQLite example completed successfully![/green]")


if __name__ == "__main__":
    main()
