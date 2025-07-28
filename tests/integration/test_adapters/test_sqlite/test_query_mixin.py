"""Integration tests for SQLite driver with query mixin functionality."""

from collections.abc import Generator
from typing import Any

import pytest

from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.exceptions import NotFoundError
from sqlspec.statement.sql import SQL


@pytest.fixture
def sqlite_driver() -> Generator[SqliteDriver, None, None]:
    """Create a SQLite driver with a test table."""
    import sqlite3

    # Create in-memory database
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    driver = SqliteDriver(conn)

    # Create test table
    driver.execute_script("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER
        );

        INSERT INTO users (name, email, age) VALUES
            ('John Doe', 'john@example.com', 30),
            ('Jane Smith', 'jane@example.com', 25),
            ('Bob Johnson', 'bob@example.com', 35),
            ('Alice Brown', 'alice@example.com', 28),
            ('Charlie Davis', 'charlie@example.com', 32);
    """)

    yield driver

    conn.close()


class TestSqliteQueryMixin:
    """Test query mixin methods with SQLite driver."""

    def test_select_one_success(self, sqlite_driver: SqliteDriver) -> None:
        """Test select_one returns exactly one row."""
        result: dict[str, Any] = sqlite_driver.select_one("SELECT * FROM users WHERE id = 1")
        assert result["id"] == 1
        assert result["name"] == "John Doe"
        assert result["email"] == "john@example.com"

    def test_select_one_no_rows(self, sqlite_driver: SqliteDriver) -> None:
        """Test select_one raises when no rows found."""
        with pytest.raises(NotFoundError):
            sqlite_driver.select_one("SELECT * FROM users WHERE id = 999")

    def test_select_one_multiple_rows(self, sqlite_driver: SqliteDriver) -> None:
        """Test select_one raises when multiple rows found."""
        with pytest.raises(ValueError, match="Expected exactly one row"):
            sqlite_driver.select_one("SELECT * FROM users WHERE age > 25")

    def test_select_one_or_none_success(self, sqlite_driver: SqliteDriver) -> None:
        """Test select_one_or_none returns one row when found."""
        result = sqlite_driver.select_one_or_none("SELECT * FROM users WHERE email = 'jane@example.com'")
        assert result is not None
        assert result["id"] == 2
        assert result["name"] == "Jane Smith"

    def test_select_one_or_none_no_rows(self, sqlite_driver: SqliteDriver) -> None:
        """Test select_one_or_none returns None when no rows found."""
        result = sqlite_driver.select_one_or_none("SELECT * FROM users WHERE email = 'notfound@example.com'")
        assert result is None

    def test_select_one_or_none_multiple_rows(self, sqlite_driver: SqliteDriver) -> None:
        """Test select_one_or_none raises when multiple rows found."""
        with pytest.raises(ValueError, match="Expected at most one row"):
            sqlite_driver.select_one_or_none("SELECT * FROM users WHERE age < 35")

    def test_select_value_success(self, sqlite_driver: SqliteDriver) -> None:
        """Test select_value returns single scalar value."""
        result = sqlite_driver.select_value("SELECT COUNT(*) FROM users")
        assert result == 5

    def test_select_value_specific_column(self, sqlite_driver: SqliteDriver) -> None:
        """Test select_value returns specific column value."""
        result = sqlite_driver.select_value("SELECT name FROM users WHERE id = 3")
        assert result == "Bob Johnson"

    def test_select_value_no_rows(self, sqlite_driver: SqliteDriver) -> None:
        """Test select_value raises when no rows found."""
        with pytest.raises(NotFoundError):
            sqlite_driver.select_value("SELECT name FROM users WHERE id = 999")

    def test_select_value_or_none_success(self, sqlite_driver: SqliteDriver) -> None:
        """Test select_value_or_none returns value when found."""
        result = sqlite_driver.select_value_or_none("SELECT age FROM users WHERE name = 'Alice Brown'")
        assert result == 28

    def test_select_value_or_none_no_rows(self, sqlite_driver: SqliteDriver) -> None:
        """Test select_value_or_none returns None when no rows."""
        result = sqlite_driver.select_value_or_none("SELECT age FROM users WHERE name = 'Unknown'")
        assert result is None

    def test_select_returns_all_rows(self, sqlite_driver: SqliteDriver) -> None:
        """Test select returns all matching rows."""
        results: list[dict[str, Any]] = sqlite_driver.select("SELECT * FROM users ORDER BY id")
        assert len(results) == 5
        assert results[0]["name"] == "John Doe"
        assert results[4]["name"] == "Charlie Davis"

    def test_select_with_filter(self, sqlite_driver: SqliteDriver) -> None:
        """Test select with WHERE clause."""
        results: list[dict[str, Any]] = sqlite_driver.select("SELECT * FROM users WHERE age >= 30 ORDER BY age")
        assert len(results) == 3
        assert results[0]["name"] == "John Doe"
        assert results[1]["name"] == "Charlie Davis"
        assert results[2]["name"] == "Bob Johnson"

    def test_select_with_parameters(self, sqlite_driver: SqliteDriver) -> None:
        """Test select methods with parameterized queries."""
        # Test with named parameters
        result: dict[str, Any] = sqlite_driver.select_one(
            SQL("SELECT * FROM users WHERE email = :email", email="bob@example.com")
        )
        assert result["name"] == "Bob Johnson"

        # Test with positional parameters
        results: list[dict[str, Any]] = sqlite_driver.select(SQL("SELECT * FROM users WHERE age > ? ORDER BY age", 30))
        assert len(results) == 2
        assert results[0]["age"] == 32
        assert results[1]["age"] == 35

    def test_complex_query_with_joins(self, sqlite_driver: SqliteDriver) -> None:
        """Test query methods with more complex SQL."""
        # Create a related table
        sqlite_driver.execute_script("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                total DECIMAL(10,2),
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            INSERT INTO orders (user_id, total) VALUES
                (1, 100.50),
                (1, 250.00),
                (2, 75.25),
                (3, 500.00);
        """)

        # Test complex query with aggregation
        result = sqlite_driver.select_value("""
            SELECT COUNT(DISTINCT u.id)
            FROM users u
            INNER JOIN orders o ON u.id = o.user_id
            WHERE o.total > 100
        """)
        assert result == 2  # Users 1 and 3 have orders > 100
